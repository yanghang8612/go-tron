#!/usr/bin/env python3
"""Offline, resumable mainnet history-staging migration orchestrator.

This program never parses Pebble rows. A capable gtron performs all inventory,
copy, receipt, route and complete-verification work. It is deliberately strict:
any unknown CLI result leaves the node stopped under its persistent latch.
"""

import argparse
from contextlib import ExitStack
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import stat
import subprocess
import sys
import tempfile
import time
import uuid

import mainnet_release as release


APP = Path('/data/gtron')
MAIN = APP / 'main'
CLI_LOG_DIR = APP / 'history-staging-ops'
REPO = APP / 'go-tron'
DATADIR = MAIN / 'datadir'
SOURCE = DATADIR / 'gtron' / 'chaindata'
TARGET = DATADIR / 'gtron' / 'history-staging'
COLD = DATADIR / 'gtron' / 'state-snapshots'
LOCK = APP / 'start.lock'
TIMER = 'gtron-deploy.timer'
DEPLOY_SERVICE = 'gtron-deploy.service'
SERVICE = 'gtron.service'
GUARD_INSTALL = Path('/usr/local/libexec/gtron-history-staging-guard.py')
RELEASE_INSTALL = Path('/usr/local/libexec/gtron-mainnet-release.py')
SPACE_INSTALL = Path('/usr/local/libexec/gtron-mainnet-space-guard.py')
DROPIN = Path('/etc/systemd/system/gtron.service.d/zz-history-staging-guard.conf')
DEPLOY_TIMEOUT_DROPIN = Path('/etc/systemd/system/gtron-deploy.service.d/zz-history-staging-deploy-timeout.conf')
VERSION = 1
HEX40 = re.compile(r'[0-9a-f]{40}\Z')
HEX64 = re.compile(r'[0-9a-f]{64}\Z')
CLI_STDERR_LIMIT = 16 << 20
CLI_RUN_TIMEOUT = 24 * 60 * 60
REPIN_INTENT = MAIN / 'HISTORY_STAGING_REPIN_INTENT.json'
UPGRADE_BINDING = Path('/var/lib/gtron-history-staging/apply-upgrade.json')
PLAN_DIR = DATADIR / 'gtron' / 'history-staging-plans'


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def command(argv, timeout=120, stream_stderr=False):
    result = subprocess.run(argv, stdin=subprocess.DEVNULL,
                            stdout=subprocess.PIPE,
                            stderr=None if stream_stderr else subprocess.PIPE,
                            timeout=timeout, check=False)
    require(result.returncode == 0,
            '%s failed (%d): %s' % (' '.join(map(str, argv)), result.returncode,
                                    result.stderr.decode('utf-8', 'replace')[-2000:]
                                    if result.stderr is not None else 'see streamed stderr'))
    return result.stdout.decode('utf-8', 'replace')


def systemctl(*args, timeout=120):
    return command(['/bin/systemctl', *args], timeout)


def unit_state(unit):
    active = subprocess.run(['/bin/systemctl', 'is-active', '--quiet', unit],
                            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL, timeout=30, check=False).returncode == 0
    enabled = subprocess.run(['/bin/systemctl', 'is-enabled', '--quiet', unit],
                             stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL, timeout=30, check=False).returncode == 0
    return {'active': active, 'enabled': enabled}


def verify_stopped(unit):
    fields = ['--property=ActiveState']
    if not unit.endswith('.timer'):
        fields.append('--property=MainPID')
    props = command(['/bin/systemctl', 'show', unit, *fields, '--no-pager'], 30)
    parsed = dict(line.split('=', 1) for line in props.splitlines() if '=' in line)
    require(parsed.get('ActiveState') in ('inactive', 'failed') and
            (unit.endswith('.timer') or parsed.get('MainPID') == '0'),
            unit + ' still has a running process or timer')


def sha_file(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as stream:
        while True:
            chunk = stream.read(1 << 20)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(path, value):
    release.atomic_root(path, release.json_bytes(value), 0o644)


def safe_identity(data):
    require(isinstance(data, dict) and data.get('version') == VERSION and
            data.get('state') in ('MIGRATION_IN_PROGRESS', 'VERIFIED_PENDING_ACTIVATION') and
            isinstance(data.get('source_commit'), str) and
            HEX40.fullmatch(data['source_commit']) and
            isinstance(data.get('candidate_sha256'), str) and
            HEX64.fullmatch(data['candidate_sha256']) and
            isinstance(data.get('job_id'), str) and
            re.fullmatch(r'[0-9a-f]{32}', data['job_id']) and
            type(data.get('service_was_active')) is bool and
            type(data.get('timer_was_active')) is bool and
            type(data.get('timer_was_enabled')) is bool and
            ('staging_health_timeout_sec' not in data or
             type(data.get('staging_health_timeout_sec')) is int and
             release.STAGING_HEALTH_MIN <= data['staging_health_timeout_sec'] <= release.STAGING_HEALTH_MAX) and
            isinstance(data.get('source_config_args'), list) and
            all(isinstance(arg, str) for arg in data['source_config_args']) and
            isinstance(data.get('source_config_sha256'), str),
            'invalid durable migration latch')
    if 'legacy_manifest_sha256' in data:
        require(isinstance(data['legacy_manifest_sha256'], str) and
                HEX64.fullmatch(data['legacy_manifest_sha256']),
                'invalid pinned legacy manifest SHA')
    for key in ('source', 'target', 'cold', 'candidate'):
        require(isinstance(data.get(key), str) and os.path.isabs(data[key]),
                'migration latch lacks absolute ' + key)
    if data['state'] == 'VERIFIED_PENDING_ACTIVATION':
        require(data.get('prune_mode') in ('snap', 'archive') and
                type(data.get('history_window')) is int and
                0 < data['history_window'] < (1 << 64),
                'verified migration latch lacks frozen history mode/window')
    return data


def source_config_from_argv(argv):
    """Freeze the exact service retention/chain flags used by the new CLI."""
    value_flags = {'--config', '--prune.mode', '--snapshot.fork-config-hash'}
    boolean_flags = {'--history.enabled'}
    result = []
    config_path = None
    index = 1
    while index < len(argv):
        arg = argv[index]
        name, separator, inline = arg.partition('=')
        if name in value_flags:
            if not separator:
                require(index + 1 < len(argv), 'service flag lacks value: ' + name)
                index += 1
                inline = argv[index]
            require(inline != '', 'service flag has empty value: ' + name)
            result.extend([name, inline])
            if name == '--config':
                config_path = inline
        elif name in boolean_flags:
            result.append(arg)
        elif name in ('--testnet', '--dev', '--genesis'):
            raise RuntimeError('mainnet history-staging migration refuses non-mainnet service flags')
        index += 1
    config_sha = sha_file(config_path) if config_path else ''
    return result, config_sha


def verify_source_config(latch):
    args = latch['source_config_args']
    paths = [args[i + 1] for i in range(len(args) - 1) if args[i] == '--config']
    require(len(paths) <= 1, 'duplicate source config paths')
    digest = sha_file(paths[0]) if paths else ''
    require(digest == latch['source_config_sha256'],
            'source config bytes changed since durable migration latch')


def load_latch():
    raw, _ = release.root_bytes(release.STAGING_LATCH, 16384)
    return safe_identity(json.loads(raw))


def write_latch(data):
    atomic_json(release.STAGING_LATCH, safe_identity(data))


def remove_latch():
    os.unlink(release.STAGING_LATCH)
    fd = os.open(str(release.STAGING_LATCH.parent), os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def remove_durable_file(path):
    os.unlink(path)
    fd = os.open(str(path.parent), os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def stable_manifest_sha(path):
    """Hash the stopped manifest without accepting a symlink or concurrent rewrite."""
    fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW)
    try:
        before = os.fstat(fd)
        require(stat.S_ISREG(before.st_mode), 'legacy cold manifest is not regular')
        digest = hashlib.sha256()
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            while True:
                chunk = stream.read(1 << 20)
                if not chunk:
                    break
                digest.update(chunk)
        after = os.fstat(fd)
        named = os.stat(str(path), follow_symlinks=False)
        require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) ==
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) ==
                (named.st_dev, named.st_ino, named.st_size, named.st_mtime_ns),
                'legacy cold manifest changed while hashing')
        return digest.hexdigest()
    finally:
        os.close(fd)


def read_prepared():
    raw, _ = release.root_bytes(release.STAGING_PREPARED, 16384)
    prepared = json.loads(raw)
    require(isinstance(prepared, dict) and prepared.get('version') == VERSION and
            isinstance(prepared.get('legacy_binary_sha256'), str) and
            HEX64.fullmatch(prepared['legacy_binary_sha256']),
            'invalid existing prepared reader fence')
    return prepared


def repin_identity(latch):
    return {key: latch[key] for key in ('job_id', 'source_commit', 'candidate_sha256',
                                        'candidate', 'source', 'target', 'cold')}


def read_repin_intent():
    if not os.path.lexists(REPIN_INTENT):
        return None
    raw, _ = release.root_bytes(REPIN_INTENT, 16384)
    intent = json.loads(raw)
    require(isinstance(intent, dict) and intent.get('version') == VERSION and
            intent.get('state') == 'REPIN_PREPLAN' and
            isinstance(intent.get('old'), dict) and
            isinstance(intent.get('new'), dict) and
            isinstance(intent.get('legacy_manifest_sha256'), str) and
            HEX64.fullmatch(intent['legacy_manifest_sha256']),
            'invalid durable pre-plan repin intent')
    return intent


def install_checked(source, destination, mode):
    raw = Path(source).read_bytes()
    require(raw, 'empty reviewed asset: ' + str(source))
    release.atomic_root(destination, raw, mode)
    require(Path(destination).read_bytes() == raw,
            'installed reviewed asset differs: ' + str(destination))


def pinned_candidate(candidate, source_commit, expected_sha):
    require(candidate.is_absolute() and candidate.is_file() and
            HEX40.fullmatch(source_commit) and HEX64.fullmatch(expected_sha),
            'candidate path/source/SHA invalid')
    require(sha_file(candidate) == expected_sha, 'candidate SHA differs')
    buildinfo = command(['/data/go/bin/go', 'version', '-m', str(candidate)], 30)
    for token in ('\tvcs.revision=' + source_commit, '\t-tags=sapling',
                  '\tCGO_ENABLED=1', '\tGOOS=linux', '\tGOARCH=amd64'):
        require(token in buildinfo, 'candidate build info differs: ' + token.strip())
    target_dir = release.RELEASES / ('staging-' + source_commit[:12] + '-' + expected_sha[:16])
    target_dir.mkdir(mode=0o755, parents=False, exist_ok=True)
    os.chown(target_dir, 0, 0)
    os.chmod(target_dir, 0o755)
    pinned = target_dir / 'gtron'
    if pinned.exists():
        require(release.root_sha(pinned) == expected_sha, 'existing pinned release differs')
    else:
        install_checked(candidate, pinned, 0o755)
    require(release.root_sha(pinned) == expected_sha, 'pinned candidate SHA differs')
    capability = json.loads(command([str(pinned), 'db', 'history-staging', 'capability'], 30))
    require(capability == {'format_version': 1, 'history_staging_reader': True,
                           'protocol_version': 1},
            'candidate does not expose the complete history-staging capability')
    return pinned


def canonical_paths():
    paths = {'source': str(SOURCE.resolve(strict=True)),
             'target': str(TARGET.resolve(strict=False)),
             'cold': str(COLD.resolve(strict=True))}
    for a, b in (('source', 'target'), ('source', 'cold'), ('target', 'cold')):
        left, right = Path(paths[a]), Path(paths[b])
        require(left != right and left not in right.parents and right not in left.parents,
                'source, target and cold paths overlap')
    return paths


def install_startup_fences(old_sha, candidate_sha, source_commit, paths, service_state,
                           timer_state, health_timeout=None):
    prepared = {'version': VERSION, 'legacy_binary_sha256': old_sha,
                'candidate_sha256': candidate_sha, 'source_commit': source_commit,
                'source': paths['source'], 'target': paths['target'], 'cold': paths['cold'],
                'service_was_active': service_state['active'],
                'timer_was_active': timer_state['active'],
                'timer_was_enabled': timer_state['enabled']}
    atomic_json(release.STAGING_PREPARED, prepared)
    install_checked(REPO / 'scripts' / 'history_staging_guard.py', GUARD_INSTALL, 0o755)
    install_checked(REPO / 'scripts' / 'mainnet_release.py', RELEASE_INSTALL, 0o755)
    install_checked(REPO / 'scripts' / 'dev' / 'mainnet_space_guard.py', SPACE_INSTALL, 0o755)
    install_checked(REPO / 'scripts' / 'start.sh', APP / 'start.sh', 0o755)
    install_checked(REPO / 'deploy' / 'systemd' / 'zz-history-staging-guard.conf', DROPIN, 0o644)
    DEPLOY_TIMEOUT_DROPIN.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
    install_checked(REPO / 'deploy' / 'systemd' / 'zz-history-staging-deploy-timeout.conf',
                    DEPLOY_TIMEOUT_DROPIN, 0o644)
    systemctl('daemon-reload', timeout=30)
    release.require_staging_deploy_timeout(health_timeout or release.staging_health_timeout())
    pre = release.show('ExecStartPre').get('ExecStartPre', '')
    require(pre.count(str(GUARD_INSTALL)) == 1 and
            pre.count(str(SPACE_INSTALL)) == 1,
            'effective startup guard set is missing or duplicated')
    command(['/usr/bin/python3', str(GUARD_INSTALL), 'check-service'], 30)


def flock_file(stack, path):
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    stack.callback(os.close, fd)
    fcntl.flock(fd, fcntl.LOCK_EX)
    return fd


def storage_locks(stack, paths):
    Path(paths['target']).mkdir(mode=0o700, exist_ok=True)
    for name in ('source', 'target', 'cold'):
        path = Path(paths[name])
        require(path.is_dir(), 'missing storage directory: ' + str(path))
        flock_file(stack, path / '.history-staging-migration.lock')


def cli_result(latch, action, *options):
    verify_source_config(latch)
    commandline = [latch['candidate'], 'db', 'history-staging', action,
                   '--datadir', str(DATADIR), '--staging-dir', latch['target'],
                   '--snapshot.dir', latch['cold'], '--job-id', latch['job_id'],
                   '--candidate-sha256', latch['candidate_sha256']]
    if latch.get('upgrade_binding'):
        commandline.extend(['--upgrade-binding', latch['upgrade_binding']])
        if action == 'inspect' and '--plan-id' not in options:
            commandline.extend(['--plan-id', latch['plan_id']])
    if latch.get('legacy_manifest_sha256'):
        commandline.extend(['--legacy-manifest-sha256', latch['legacy_manifest_sha256']])
    commandline.extend([*latch['source_config_args'], *options])
    last = None
    with tempfile.TemporaryFile() as output:
        returncode, error_tail = stream_cli_stderr(commandline, output, latch, action)
        require(returncode == 0,
                'gtron %s failed (%d): %s' %
                (action, returncode, error_tail.decode('utf-8', 'replace')))
        require(output.tell() <= 256 << 20, 'gtron progress output exceeded 256 MiB')
        output.seek(0)
        for line in output:
            require(len(line) <= 64 << 10 and line.endswith(b'\n'),
                    'gtron emitted an oversized or incomplete JSONL record')
            if not line.strip():
                continue
            record = json.loads(line)
            last = validate_cli_record(latch, record)
    verify_source_config(latch)
    require(last is not None and last.get('phase') == action,
            'gtron did not return a final durable ' + action + ' result')
    return last


def stream_cli_stderr(commandline, output, latch, action):
    """Bound, persist and forward heartbeat stderr without changing JSONL stdout."""
    require(action in ('inspect', 'migrate', 'apply', 'resume', 'upgrade-check') and
            re.fullmatch(r'[0-9a-f]{32}', latch['job_id']) is not None,
            'invalid migration log identity')
    CLI_LOG_DIR.mkdir(mode=0o700, exist_ok=True)
    directory = CLI_LOG_DIR.lstat()
    require(stat.S_ISDIR(directory.st_mode) and directory.st_uid == os.geteuid() and
            stat.S_IMODE(directory.st_mode) == 0o700,
            'unsafe root-owned history-staging operation log directory')
    path = CLI_LOG_DIR / ('%s_%s.stderr.log' % (latch['job_id'], action))
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    process = None
    tail = b''
    try:
        with os.fdopen(fd, 'wb') as log:
            process = subprocess.Popen(commandline, stdin=subprocess.DEVNULL,
                                       stdout=output, stderr=subprocess.PIPE)
            deadline = time.monotonic() + CLI_RUN_TIMEOUT
            total = 0
            with selectors.DefaultSelector() as selector:
                selector.register(process.stderr, selectors.EVENT_READ)
                while True:
                    remaining = deadline - time.monotonic()
                    require(remaining > 0, 'gtron %s exceeded bounded 24 h run' % action)
                    if not selector.select(timeout=min(30, remaining)):
                        continue
                    chunk = os.read(process.stderr.fileno(), 8192)
                    if not chunk:
                        break
                    total += len(chunk)
                    require(total <= CLI_STDERR_LIMIT,
                            'gtron %s stderr exceeded bounded 16 MiB log' % action)
                    log.write(chunk)
                    log.flush()
                    sys.stderr.write(chunk.decode('utf-8', 'replace'))
                    sys.stderr.flush()
                    tail = (tail + chunk)[-2000:]
            return process.wait(timeout=max(1, deadline - time.monotonic())), tail
    except Exception:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=30)
        raise
    finally:
        if process is not None and process.stderr is not None:
            process.stderr.close()


def validate_cli_record(latch, record):
    """Validate one real gtron JSONL record against the durable job identity."""
    require(isinstance(record, dict) and record.get('version') == VERSION and
            record.get('job_id') == latch['job_id'] and
            record.get('candidate_sha256') == latch['candidate_sha256'] and
            record.get('source') == latch['source'] and
            record.get('target') == latch['target'],
            'gtron returned invalid migration identity/progress')
    return record


def restore_timer(latch):
    if latch['timer_was_enabled']:
        systemctl('enable', TIMER, timeout=30)
    else:
        systemctl('disable', TIMER, timeout=30)
    if latch['timer_was_active']:
        systemctl('start', TIMER, timeout=30)
    else:
        systemctl('stop', TIMER, timeout=30)


def finalize_done(job_id):
    """Resume the timer-intent tail after the durable latch was removed."""
    require(not os.path.lexists(REPIN_INTENT),
            'pending pre-plan repin intent blocks completion and timer restoration')
    raw, _ = release.root_bytes(MAIN / 'HISTORY_STAGING_MIGRATION_DONE.json', 16384)
    done = json.loads(raw)
    require(isinstance(done, dict) and done.get('version') == VERSION and
            done.get('state') == 'DONE' and done.get('job_id') == job_id and
            type(done.get('service_was_active')) is bool and
            type(done.get('timer_was_active')) is bool and
            type(done.get('timer_was_enabled')) is bool and
            isinstance(done.get('candidate_sha256'), str) and
            HEX64.fullmatch(done['candidate_sha256']),
            'completion journal identity differs')
    require(not os.path.lexists(release.STAGING_LATCH),
            'durable migration latch still requires resume')
    current = release.inspect()
    require(current['sha'] == done['candidate_sha256'] and
            current['source'] == done['source_commit'] and
            current['active'] == done['service_was_active'] and
            current.get('staging') is not None,
            'completed service identity or original run intent differs')
    if current['active']:
        release.wallet_head()
    else:
        verify_stopped(SERVICE)
    restore_timer(done)
    return done


def migrate_under_latch(latch):
    require(not os.path.lexists(REPIN_INTENT),
            'pending pre-plan repin intent requires an exact repin-preplan retry')
    if latch.get('state') == 'MIGRATION_IN_PROGRESS':
        # The fixed legacy-manifest SHA belongs to offline planning and copy.
        # A reader may already have published a newer manifest when pending
        # activation is retried. activate-staging checks its durable intent
        # and fixed candidate; the new reader verifies the persisted route
        # barrier during startup before it can pass the health check.
        cli_result(latch, 'inspect')
        if latch.get('plan_id'):
            progress = cli_result(latch, 'resume', '--plan-id', latch['plan_id'])
        else:
            plan = cli_result(latch, 'migrate', '--max-buckets', '0')
            plan_id = plan.get('plan_id')
            require(isinstance(plan_id, str) and HEX64.fullmatch(plan_id),
                    'gtron migration plan lacks a durable plan ID')
            latch['plan_id'] = plan_id
            write_latch(latch)
            progress = cli_result(latch, 'apply', '--plan-id', plan_id)
        require(progress.get('durable') is True,
                'gtron did not confirm durable migration phase')
        verified = cli_result(latch, 'inspect', '--verify-complete',
                              '--plan-id', latch['plan_id'])
        require(verified.get('verified_complete') is True,
                'gtron did not verify complete source/route coverage')
        require(verified.get('prune_mode') in ('snap', 'archive') and
                type(verified.get('history_window')) is int and
                0 < verified['history_window'] < (1 << 64),
                'gtron did not return frozen verified history mode/window')
        latch['prune_mode'] = verified['prune_mode']
        latch['history_window'] = verified['history_window']
        latch['state'] = 'VERIFIED_PENDING_ACTIVATION'
        write_latch(latch)
    health_timeout = latch.get('staging_health_timeout_sec', release.staging_health_timeout())
    result = json.loads(command(['/usr/bin/python3', str(RELEASE_INSTALL),
                                 'activate-staging', '--source', latch['source_commit'],
                                 '--candidate', latch['candidate'],
                                 '--sha256', latch['candidate_sha256'],
                                 '--staging-health-timeout-sec', str(health_timeout)],
                                health_timeout + 15 * 60, stream_stderr=True))
    require(result.get('activated') is True and
            result.get('binary_sha256') == latch['candidate_sha256'] and
            result.get('service_was_active') == latch['service_was_active'],
            'activation helper did not attest fixed reader identity')
    if not latch['service_was_active']:
        verified = cli_result(latch, 'inspect', '--verify-complete',
                              '--plan-id', latch['plan_id'])
        require(verified.get('verified_complete') is True,
                'inactive service failed final offline verification')
        verify_stopped(SERVICE)
    done = dict(latch, state='DONE', completed_unix=int(time.time()))
    atomic_json(MAIN / 'HISTORY_STAGING_MIGRATION_DONE.json', done)
    remove_latch()
    return finalize_done(latch['job_id'])


def repin_preplan(job_id, candidate, source_commit, expected_sha, legacy_manifest_sha):
    """Replace only a failed, provably pristine pre-plan candidate under the latch."""
    require(os.geteuid() == 0, 'root required')
    require(re.fullmatch(r'[0-9a-f]{32}', job_id or '') is not None and
            isinstance(source_commit, str) and HEX40.fullmatch(source_commit) and
            isinstance(expected_sha, str) and HEX64.fullmatch(expected_sha) and
            isinstance(legacy_manifest_sha, str) and HEX64.fullmatch(legacy_manifest_sha),
            'invalid pre-plan repin identity')
    require(os.path.lexists(release.STAGING_LATCH) and
            not os.path.lexists(release.STAGING_REQUIRED),
            'repin requires an unfinished migration latch without an active reader')
    # This command never stops units: a running writer or deploy is a hard error.
    for unit in (SERVICE, TIMER, DEPLOY_SERVICE):
        verify_stopped(unit)
    with ExitStack() as stack:
        flock_file(stack, LOCK)
        for unit in (SERVICE, TIMER, DEPLOY_SERVICE):
            verify_stopped(unit)
        latch = load_latch()
        require(latch['job_id'] == job_id and latch['state'] == 'MIGRATION_IN_PROGRESS' and
                not latch.get('plan_id'),
                'repin requires the same unfinished pre-plan migration job')
        require(latch.get('legacy_manifest_sha256', legacy_manifest_sha) == legacy_manifest_sha,
                'repin legacy manifest pin differs from durable latch')
        require(canonical_paths() == {key: latch[key] for key in ('source', 'target', 'cold')},
                'repin storage paths differ from durable latch')
        storage_locks(stack, latch)
        prepared = read_prepared()
        require(all(prepared.get(key) == latch[key] for key in
                    ('source', 'target', 'cold', 'service_was_active',
                     'timer_was_active', 'timer_was_enabled')),
                'prepared reader fence differs from original migration intent')
        require(GUARD_INSTALL.is_file() and DROPIN.is_file() and
                (APP / 'start.sh').is_file(),
                'persistent startup/deployment guards are missing')
        manifest = Path(latch['cold']) / 'manifest.json'
        require(stable_manifest_sha(manifest) == legacy_manifest_sha,
                'legacy cold manifest SHA differs from stopped bytes')

        intent = read_repin_intent()
        old_identity = repin_identity(latch) if intent is None else intent['old']
        new_identity = dict(old_identity, source_commit=source_commit,
                            candidate_sha256=expected_sha)
        if intent is not None:
            require(intent['job_id'] == job_id and intent['old'] == old_identity and
                    intent['new'].get('source_commit') == source_commit and
                    intent['new'].get('candidate_sha256') == expected_sha and
                    intent['legacy_manifest_sha256'] == legacy_manifest_sha,
                    'a different durable repin intent is pending')
            new_identity = intent['new']
        require(repin_identity(latch) in (old_identity, new_identity),
                'latch is neither the old nor the pending new candidate')
        require((prepared.get('source_commit'), prepared.get('candidate_sha256')) in
                ((old_identity['source_commit'], old_identity['candidate_sha256']),
                 (new_identity['source_commit'], new_identity['candidate_sha256'])),
                'prepared fence is neither the old nor the pending new candidate')
        pinned = pinned_candidate(Path(candidate), source_commit, expected_sha)
        require(intent is None or str(pinned) == new_identity['candidate'],
                'repin retry candidate path differs from durable intent')
        new_identity['candidate'] = str(pinned)
        probe = dict(latch, source_commit=source_commit, candidate_sha256=expected_sha,
                     candidate=str(pinned), legacy_manifest_sha256=legacy_manifest_sha)
        result = cli_result(probe, 'inspect', '--verify-pristine')
        require(result.get('pristine') is True,
                'new candidate did not prove the old job is pristine')
        require(stable_manifest_sha(manifest) == legacy_manifest_sha,
                'legacy cold manifest changed during pristine inspection')
        for unit in (SERVICE, TIMER, DEPLOY_SERVICE):
            verify_stopped(unit)

        if intent is None:
            intent = {'version': VERSION, 'state': 'REPIN_PREPLAN', 'job_id': job_id,
                      'old': old_identity, 'new': new_identity,
                      'legacy_manifest_sha256': legacy_manifest_sha}
            atomic_json(REPIN_INTENT, intent)
        revised_prepared = dict(prepared, source_commit=source_commit,
                                candidate_sha256=expected_sha)
        atomic_json(release.STAGING_PREPARED, revised_prepared)
        revised_latch = dict(latch, source_commit=source_commit,
                             candidate_sha256=expected_sha, candidate=str(pinned),
                             legacy_manifest_sha256=legacy_manifest_sha)
        write_latch(revised_latch)
        require(repin_identity(load_latch()) == new_identity and
                read_prepared() == revised_prepared,
                'durable pre-plan repin did not converge')
        remove_durable_file(REPIN_INTENT)
        return {'version': VERSION, 'phase': 'repin-preplan', 'job_id': job_id,
                'source_commit': source_commit, 'candidate_sha256': expected_sha,
                'candidate': str(pinned), 'legacy_manifest_sha256': legacy_manifest_sha,
                'pristine': True}


def read_apply_upgrade():
    raw, _ = release.root_bytes(UPGRADE_BINDING, 65536)
    value = json.loads(raw)
    require(isinstance(value, dict) and value.get('version') == VERSION and
            value.get('state') in ('PRECHECK', 'AUTHORIZED', 'DONE') and
            all(isinstance(value.get(key), dict) for key in
                ('old', 'new', 'old_latch', 'new_latch', 'old_prepared', 'new_prepared')),
            'invalid apply upgrade journal')
    return value


def write_apply_upgrade(value):
    directory = UPGRADE_BINDING.parent
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    for parent in (directory, *directory.parents):
        info = os.lstat(str(parent))
        require(stat.S_ISDIR(info.st_mode) and info.st_uid == 0 and
                not info.st_mode & 0o022 and
                (parent != directory or stat.S_IMODE(info.st_mode) == 0o700),
                'unsafe apply upgrade journal parent')
    release.atomic_root(UPGRADE_BINDING, release.json_bytes(value), 0o600)


def verify_apply_upgrade_guards():
    for installed, source in ((GUARD_INSTALL, REPO / 'scripts' / 'history_staging_guard.py'),
                              (RELEASE_INSTALL, REPO / 'scripts' / 'mainnet_release.py')):
        raw, _ = release.root_bytes(installed, 1 << 20)
        require(raw == source.read_bytes(), 'installed pending upgrade fence differs from reviewed helper')


def apply_repin(job_id, candidate, source_commit, expected_sha, legacy_manifest_sha):
    """Replace an interrupted executor while retaining its immutable sealed plan.

    Every journal switch is retryable with precisely the same request. The
    pending marker stays present until both reader fences match the DONE journal.
    This operation does not resume DB writes; the separate resume action does.
    """
    require(os.geteuid() == 0, 'root required')
    require(re.fullmatch(r'[0-9a-f]{32}', job_id or '') is not None and
            HEX40.fullmatch(source_commit or '') and HEX64.fullmatch(expected_sha or '') and
            HEX64.fullmatch(legacy_manifest_sha or ''), 'invalid apply upgrade identity')
    require(os.path.lexists(release.STAGING_LATCH) and
            not os.path.lexists(release.STAGING_REQUIRED),
            'apply upgrade requires unfinished migration without an active reader')
    for unit in (SERVICE, TIMER, DEPLOY_SERVICE):
        verify_stopped(unit)
    with ExitStack() as stack:
        flock_file(stack, LOCK)
        for unit in (SERVICE, TIMER, DEPLOY_SERVICE):
            verify_stopped(unit)
        latch = load_latch()
        require(latch['job_id'] == job_id and latch['state'] == 'MIGRATION_IN_PROGRESS' and
                HEX64.fullmatch(latch.get('plan_id', '')),
                'apply upgrade requires the same published unfinished plan')
        require(canonical_paths() == {key: latch[key] for key in ('source', 'target', 'cold')},
                'apply upgrade storage paths differ')
        storage_locks(stack, latch)
        prepared = read_prepared()
        verify_source_config(latch)
        require(latch.get('legacy_manifest_sha256') == legacy_manifest_sha and
                stable_manifest_sha(Path(latch['cold']) / 'manifest.json') == legacy_manifest_sha,
                'apply upgrade frozen manifest differs')
        require(GUARD_INSTALL.is_file() and DROPIN.is_file() and (APP / 'start.sh').is_file(),
                'persistent startup/deployment guards are missing')
        verify_apply_upgrade_guards()
        pinned = pinned_candidate(Path(candidate), source_commit, expected_sha)
        journal = read_apply_upgrade() if os.path.lexists(UPGRADE_BINDING) else None
        if journal is None:
            require(not latch.get('upgrade_binding') and
                    all(prepared.get(key) == latch[key] for key in
                        ('source', 'target', 'cold', 'service_was_active',
                         'timer_was_active', 'timer_was_enabled', 'source_commit', 'candidate_sha256')),
                    'apply upgrade original prepared/latch identity differs or another repin is pending')
            require(latch['candidate_sha256'] != expected_sha,
                    'apply upgrade requires a different executor')
            require(release.root_sha(latch['candidate']) == latch['candidate_sha256'],
                    'original executor SHA differs from durable latch')
            old = repin_identity(latch)
            new = dict(old, candidate=str(pinned), source_commit=source_commit,
                       candidate_sha256=expected_sha)
            revised_latch = dict(latch, source_commit=source_commit, candidate=str(pinned),
                                 candidate_sha256=expected_sha,
                                 plan_producer_sha256=latch['candidate_sha256'],
                                 upgrade_binding=str(UPGRADE_BINDING))
            revised_prepared = dict(prepared, source_commit=source_commit,
                                    candidate_sha256=expected_sha)
            journal = {'version': VERSION, 'state': 'PRECHECK', 'job_id': job_id,
                       'plan_id': latch['plan_id'], 'plan_producer_sha256': latch['candidate_sha256'],
                       'old': old, 'new': new, 'old_latch': latch, 'new_latch': revised_latch,
                       'old_prepared': prepared, 'new_prepared': revised_prepared}
            # A durable pending marker precedes every journal or identity switch.
            marker = {'version': VERSION, 'state': 'REPIN_APPLY',
                      'job_id': job_id, 'plan_id': latch['plan_id'],
                      'new': new, 'binding': str(UPGRADE_BINDING)}
            if os.path.lexists(REPIN_INTENT):
                raw, _ = release.root_bytes(REPIN_INTENT, 16384)
                require(json.loads(raw) == marker, 'different durable apply upgrade request is pending')
            else:
                atomic_json(REPIN_INTENT, marker)
            write_apply_upgrade(journal)
        else:
            require(journal['job_id'] == job_id and journal['plan_id'] == latch['plan_id'] and
                    journal['new']['source_commit'] == source_commit and
                    journal['new']['candidate_sha256'] == expected_sha and
                    journal['new']['candidate'] == str(pinned),
                    'different durable apply upgrade request is pending')
            require((latch == journal['old_latch'] and prepared == journal['old_prepared']) or
                    (journal['state'] != 'PRECHECK' and latch == journal['old_latch'] and
                     prepared == journal['new_prepared']) or
                    (journal['state'] != 'PRECHECK' and latch == journal['new_latch'] and
                     prepared == journal['new_prepared']),
                    'apply upgrade prepared/latch switch is inconsistent')
            if journal['state'] == 'DONE' and not os.path.lexists(REPIN_INTENT):
                return dict(version=VERSION, phase='apply-repin', job_id=job_id,
                            plan_id=journal['plan_id'], candidate_sha256=expected_sha,
                            plan_producer_sha256=journal['plan_producer_sha256'])
        raw, _ = release.root_bytes(REPIN_INTENT, 16384)
        intent = json.loads(raw)
        require(intent == {'version': VERSION, 'state': 'REPIN_APPLY', 'job_id': job_id,
                           'plan_id': journal['plan_id'], 'new': journal['new'],
                           'binding': str(UPGRADE_BINDING)}, 'pending apply upgrade marker differs')
        probe = journal['new_latch']
        result = cli_result(probe, 'upgrade-check', '--plan-id', journal['plan_id'])
        require(result.get('phase') == 'upgrade-check', 'new executor did not validate sealed plan')
        require(stable_manifest_sha(Path(latch['cold']) / 'manifest.json') == legacy_manifest_sha,
                'frozen manifest changed during upgrade validation')
        for unit in (SERVICE, TIMER, DEPLOY_SERVICE):
            verify_stopped(unit)
        require(load_latch() == latch and read_prepared() == prepared,
                'reader fences changed during upgrade validation')
        if journal['state'] == 'PRECHECK':
            journal = dict(journal, state='AUTHORIZED')
            write_apply_upgrade(journal)
        atomic_json(release.STAGING_PREPARED, journal['new_prepared'])
        write_latch(journal['new_latch'])
        require(load_latch() == journal['new_latch'] and read_prepared() == journal['new_prepared'],
                'apply upgrade durable fences did not converge')
        write_apply_upgrade(dict(journal, state='DONE'))
        remove_durable_file(REPIN_INTENT)
        return dict(version=VERSION, phase='apply-repin', job_id=job_id,
                    plan_id=journal['plan_id'], candidate_sha256=expected_sha,
                    plan_producer_sha256=journal['plan_producer_sha256'])


def checked_unpublished_job_temps(plan_dir, latch, manifest_sha, owner=0):
    """Identify only exact, unpublished temporary plans for this frozen job."""
    if not os.path.lexists(plan_dir):
        return []
    directory = os.lstat(plan_dir)
    require(stat.S_ISDIR(directory.st_mode) and directory.st_uid == owner and
            stat.S_IMODE(directory.st_mode) == 0o700,
            'unsafe history-staging plan directory')
    pattern = re.compile(r'\.' + re.escape(latch['job_id']) + r'\.plan\.[A-Za-z0-9]+\.tmp\Z')
    result = []
    with os.scandir(plan_dir) as entries:
        for entry in entries:
            require(len(result) < 128, 'too many unpublished plan temporaries')
            require(pattern.fullmatch(entry.name) is not None,
                    'published, foreign, or unexpected history-staging plan file exists')
            path = Path(plan_dir) / entry.name
            info = entry.stat(follow_symlinks=False)
            require(stat.S_ISREG(info.st_mode) and info.st_uid == owner and
                    info.st_nlink == 1 and stat.S_IMODE(info.st_mode) == 0o600 and
                    0 < info.st_size <= 16 << 30,
                    'unsafe or unbounded unpublished plan temporary')
            fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW)
            try:
                opened = os.fstat(fd)
                require((opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns) ==
                        (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns),
                        'unpublished plan temporary changed before inspection')
                with os.fdopen(fd, 'rb', closefd=False) as stream:
                    first = stream.readline((1 << 20) + 1)
                require(first.endswith(b'\n') and len(first) <= 1 << 20,
                        'unpublished plan has no bounded durable header')
                header = json.loads(first)
                require(isinstance(header, dict) and header.get('version') == VERSION and
                        header.get('job_id') == latch['job_id'] and
                        header.get('candidate_sha256') == latch['candidate_sha256'] and
                        header.get('manifest_sha256') == manifest_sha and
                        header.get('paths') == {key: latch[key] for key in
                                                ('source', 'target', 'cold')},
                        'unpublished plan header differs from frozen old job')
            finally:
                os.close(fd)
            result.append((path, info))
    return sorted(result, key=lambda item: str(item[0]))


def discard_preplan_tmp(job_id, candidate, source_commit, expected_sha, legacy_manifest_sha):
    """Remove only old-job plan temp files after an independent storage proof."""
    require(os.geteuid() == 0 and re.fullmatch(r'[0-9a-f]{32}', job_id or '') and
            isinstance(source_commit, str) and HEX40.fullmatch(source_commit) and
            isinstance(expected_sha, str) and HEX64.fullmatch(expected_sha) and
            isinstance(legacy_manifest_sha, str) and HEX64.fullmatch(legacy_manifest_sha),
            'invalid root pre-plan temporary cleanup identity')
    require(os.path.lexists(release.STAGING_LATCH) and
            not os.path.lexists(release.STAGING_REQUIRED) and
            not os.path.lexists(REPIN_INTENT),
            'temporary cleanup requires a migration latch without active reader or repin')
    for unit in (SERVICE, TIMER, DEPLOY_SERVICE):
        verify_stopped(unit)
    with ExitStack() as stack:
        flock_file(stack, LOCK)
        for unit in (SERVICE, TIMER, DEPLOY_SERVICE):
            verify_stopped(unit)
        latch = load_latch()
        require(latch['job_id'] == job_id and latch['state'] == 'MIGRATION_IN_PROGRESS' and
                not latch.get('plan_id'),
                'temporary cleanup requires the same unfinished pre-plan job')
        require(latch.get('legacy_manifest_sha256', legacy_manifest_sha) == legacy_manifest_sha,
                'temporary cleanup legacy manifest pin differs from durable latch')
        require(canonical_paths() == {key: latch[key] for key in ('source', 'target', 'cold')},
                'temporary cleanup storage paths differ from durable latch')
        storage_locks(stack, latch)
        prepared = read_prepared()
        require(all(prepared.get(key) == latch[key] for key in
                    ('source', 'target', 'cold', 'service_was_active',
                     'timer_was_active', 'timer_was_enabled',
                     'source_commit', 'candidate_sha256')),
                'temporary cleanup prepared fence differs from old migration identity')
        require(GUARD_INSTALL.is_file() and DROPIN.is_file() and
                (APP / 'start.sh').is_file(),
                'persistent startup/deployment guards are missing')
        manifest = Path(latch['cold']) / 'manifest.json'
        require(stable_manifest_sha(manifest) == legacy_manifest_sha,
                'legacy cold manifest SHA differs from stopped bytes')
        temps = checked_unpublished_job_temps(PLAN_DIR, latch, legacy_manifest_sha)
        pinned = pinned_candidate(Path(candidate), source_commit, expected_sha)
        probe = dict(latch, source_commit=source_commit, candidate_sha256=expected_sha,
                     candidate=str(pinned), legacy_manifest_sha256=legacy_manifest_sha)
        storage = cli_result(probe, 'inspect', '--verify-pristine-storage')
        require(storage.get('storage_pristine') is True,
                'new candidate did not prove source and staging stores pristine')
        require(stable_manifest_sha(manifest) == legacy_manifest_sha,
                'legacy cold manifest changed during storage inspection')
        for unit in (SERVICE, TIMER, DEPLOY_SERVICE):
            verify_stopped(unit)
        require(load_latch() == latch and read_prepared() == prepared and
                not os.path.lexists(release.STAGING_REQUIRED) and
                not os.path.lexists(REPIN_INTENT),
                'migration identity changed during temporary cleanup proof')
        after_inventory = checked_unpublished_job_temps(PLAN_DIR, latch, legacy_manifest_sha)
        require(len(after_inventory) == len(temps) and
                all(path == later_path and
                    (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) ==
                    (later.st_dev, later.st_ino, later.st_size, later.st_mtime_ns)
                    for (path, before), (later_path, later) in zip(temps, after_inventory)),
                'unpublished plan inventory changed during storage proof')
        for path, before in temps:
            current = os.lstat(path)
            require((current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns) ==
                    (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) and
                    stat.S_ISREG(current.st_mode) and current.st_uid == before.st_uid and
                    stat.S_IMODE(current.st_mode) == 0o600 and current.st_nlink == 1,
                    'unpublished plan temporary changed before removal')
            os.unlink(path)
        if temps:
            fd = os.open(str(PLAN_DIR), os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        strict = cli_result(probe, 'inspect', '--verify-pristine')
        require(strict.get('pristine') is True,
                'old job is not strictly pristine after temporary cleanup')
        return {'version': VERSION, 'phase': 'discard-preplan-tmp',
                'job_id': job_id, 'removed': len(temps), 'pristine': True}


def run_new(candidate, source_commit, expected_sha, health_timeout=None,
            legacy_manifest_sha=None):
    require(os.geteuid() == 0, 'root required')
    require(legacy_manifest_sha is None or
            isinstance(legacy_manifest_sha, str) and HEX64.fullmatch(legacy_manifest_sha),
            'invalid explicit legacy manifest SHA')
    require(not os.path.lexists(release.STAGING_LATCH) and
            not os.path.lexists(release.STAGING_REQUIRED) and
            not os.path.lexists(REPIN_INTENT),
            'migration already entered or reader already active; use resume')
    paths = canonical_paths()
    pinned = pinned_candidate(Path(candidate), source_commit, expected_sha)
    health_timeout = health_timeout or release.staging_health_timeout()
    old = release.inspect()
    config_args, config_sha = source_config_from_argv(old['argv'])
    service_state, timer_state = unit_state(SERVICE), unit_state(TIMER)
    require(old['sha'] == release.root_sha(old['binary']) and
            old['active'] == service_state['active'],
            'current mainnet reader identity/state changed')
    entered = False
    fences_verified = False
    try:
        systemctl('stop', TIMER)
        systemctl('stop', DEPLOY_SERVICE, timeout=600)
        verify_stopped(DEPLOY_SERVICE)
        install_startup_fences(old['sha'], expected_sha, source_commit,
                               paths, service_state, timer_state, health_timeout)
        fences_verified = True
        with ExitStack() as stack:
            flock_file(stack, LOCK)
            verify_stopped(DEPLOY_SERVICE)
            fresh = release.inspect()
            require(fresh['binary'] == old['binary'] and fresh['sha'] == old['sha'] and
                    fresh['source'] == old['source'] and
                    fresh['active'] == old['active'],
                    'mainnet reader changed before durable migration latch')
            latch = dict(version=VERSION, state='MIGRATION_IN_PROGRESS',
                         source_commit=source_commit, candidate_sha256=expected_sha,
                         candidate=str(pinned), job_id=uuid.uuid4().hex,
                         source=paths['source'], target=paths['target'], cold=paths['cold'],
                         service_was_active=service_state['active'],
                         timer_was_active=timer_state['active'],
                         timer_was_enabled=timer_state['enabled'],
                         staging_health_timeout_sec=health_timeout,
                         source_config_args=config_args,
                         source_config_sha256=config_sha)
            if legacy_manifest_sha is not None:
                latch['legacy_manifest_sha256'] = legacy_manifest_sha
            write_latch(latch)
            entered = True
            systemctl('stop', SERVICE, timeout=650)
            verify_stopped(SERVICE)
            storage_locks(stack, paths)
            return migrate_under_latch(latch)
    except Exception:
        if entered and os.path.lexists(release.STAGING_LATCH):
            try:
                systemctl('stop', SERVICE, timeout=650)
            except Exception:
                pass
        else:
            # Failure before durable latch is not a migration. Restore the
            # timer's previous intent; do not claim the service was stopped.
            if fences_verified and timer_state['active']:
                try:
                    systemctl('start', TIMER, timeout=30)
                except Exception:
                    pass
        raise


def resume(job_id):
    require(os.geteuid() == 0, 'root required')
    require(re.fullmatch(r'[0-9a-f]{32}', job_id or '') is not None,
            'invalid resume job ID')
    if not os.path.lexists(release.STAGING_LATCH):
        return finalize_done(job_id)
    require(not os.path.lexists(REPIN_INTENT),
            'durable repin is pending; retry repin-preplan with the same job/candidate/source/SHA')
    latch = load_latch()
    require(latch['job_id'] == job_id and
            release.root_sha(latch['candidate']) == latch['candidate_sha256'] and
            canonical_paths() == {key: latch[key] for key in ('source', 'target', 'cold')},
            'resume identity/path differs from durable latch')
    systemctl('stop', TIMER)
    systemctl('stop', DEPLOY_SERVICE, timeout=600)
    verify_stopped(DEPLOY_SERVICE)
    with ExitStack() as stack:
        flock_file(stack, LOCK)
        verify_stopped(DEPLOY_SERVICE)
        systemctl('stop', SERVICE, timeout=650)
        verify_stopped(SERVICE)
        storage_locks(stack, latch)
        try:
            return migrate_under_latch(latch)
        except Exception:
            if os.path.lexists(release.STAGING_LATCH):
                try:
                    systemctl('stop', SERVICE, timeout=650)
                except Exception:
                    pass
            raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest='mode')
    start = modes.add_parser('run')
    start.add_argument('--candidate', type=Path, required=True)
    start.add_argument('--source', required=True)
    start.add_argument('--sha256', required=True)
    start.add_argument('--staging-health-timeout-sec',
                       help='bounded capable-reader health wait (600..86400 seconds; default 43200)')
    start.add_argument('--legacy-manifest-sha256',
                       help='exact stopped legacy cold manifest SHA; required when Chain is absent')
    again = modes.add_parser('resume')
    again.add_argument('--job-id', required=True)
    upgrade = modes.add_parser('apply-repin')
    upgrade.add_argument('--job-id', required=True)
    upgrade.add_argument('--candidate', type=Path, required=True)
    upgrade.add_argument('--source', required=True)
    upgrade.add_argument('--sha256', required=True)
    upgrade.add_argument('--legacy-manifest-sha256', required=True)
    repin = modes.add_parser('repin-preplan')
    repin.add_argument('--job-id', required=True)
    repin.add_argument('--candidate', type=Path, required=True)
    repin.add_argument('--source', required=True)
    repin.add_argument('--sha256', required=True)
    repin.add_argument('--legacy-manifest-sha256', required=True)
    discard = modes.add_parser('discard-preplan-tmp')
    discard.add_argument('--job-id', required=True)
    discard.add_argument('--candidate', type=Path, required=True)
    discard.add_argument('--source', required=True)
    discard.add_argument('--sha256', required=True)
    discard.add_argument('--legacy-manifest-sha256', required=True)
    args = parser.parse_args()
    require(args.mode in ('run', 'resume', 'repin-preplan', 'apply-repin', 'discard-preplan-tmp'),
            'run, resume, repin-preplan or discard-preplan-tmp action required')
    if args.mode == 'run':
        health_timeout = (release.staging_health_timeout(args.staging_health_timeout_sec)
                          if args.staging_health_timeout_sec is not None else None)
        result = run_new(args.candidate, args.source, args.sha256, health_timeout,
                         args.legacy_manifest_sha256)
    elif args.mode == 'resume':
        result = resume(args.job_id)
    elif args.mode == 'apply-repin':
        result = apply_repin(args.job_id, args.candidate, args.source,
                             args.sha256, args.legacy_manifest_sha256)
    elif args.mode == 'repin-preplan':
        result = repin_preplan(args.job_id, args.candidate, args.source,
                               args.sha256, args.legacy_manifest_sha256)
    else:
        result = discard_preplan_tmp(args.job_id, args.candidate, args.source,
                                     args.sha256, args.legacy_manifest_sha256)
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print('history-staging migration: ' + str(error), file=sys.stderr)
        if os.path.lexists(release.STAGING_LATCH):
            try:
                latch = load_latch()
                if os.path.lexists(REPIN_INTENT):
                    print('durable repin is pending; retry repin-preplan with its exact arguments',
                          file=sys.stderr)
                else:
                    print('resume: /usr/bin/python3 ' + sys.argv[0] + ' resume --job-id ' +
                          latch['job_id'], file=sys.stderr)
            except Exception:
                print('migration latch remains; inspect it before any restart',
                      file=sys.stderr)
        sys.exit(1)
