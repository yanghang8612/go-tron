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
import subprocess
import sys
import tempfile
import time
import uuid

import mainnet_release as release


APP = Path('/data/gtron')
MAIN = APP / 'main'
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
VERSION = 1
HEX40 = re.compile(r'[0-9a-f]{40}\Z')
HEX64 = re.compile(r'[0-9a-f]{64}\Z')


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def command(argv, timeout=120):
    result = subprocess.run(argv, stdin=subprocess.DEVNULL,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            timeout=timeout, check=False)
    require(result.returncode == 0,
            '%s failed (%d): %s' % (' '.join(map(str, argv)), result.returncode,
                                    result.stderr.decode('utf-8', 'replace')[-2000:]))
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
    props = command(['/bin/systemctl', 'show', unit,
                     '--property=ActiveState', '--property=MainPID', '--no-pager'], 30)
    parsed = dict(line.split('=', 1) for line in props.splitlines() if '=' in line)
    require(parsed.get('ActiveState') not in ('active', 'activating') and
            parsed.get('MainPID') in ('0', ''), unit + ' still has a running process')


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
            isinstance(data.get('source_config_args'), list) and
            all(isinstance(arg, str) for arg in data['source_config_args']) and
            isinstance(data.get('source_config_sha256'), str),
            'invalid durable migration latch')
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


def install_startup_fences(old_sha, candidate_sha, source_commit, paths, service_state, timer_state):
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
    systemctl('daemon-reload', timeout=30)
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
                   '--candidate-sha256', latch['candidate_sha256'],
                   *latch['source_config_args'], *options]
    last = None
    with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
        result = subprocess.run(commandline, stdin=subprocess.DEVNULL,
                                stdout=output, stderr=errors,
                                timeout=24 * 60 * 60, check=False)
        errors.seek(0)
        require(result.returncode == 0,
                'gtron %s failed (%d): %s' %
                (action, result.returncode, errors.read(2000).decode('utf-8', 'replace')))
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
    cli_result(latch, 'inspect')
    if latch.get('state') == 'MIGRATION_IN_PROGRESS':
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
    result = json.loads(command(['/usr/bin/python3', str(RELEASE_INSTALL),
                                 'activate-staging', '--source', latch['source_commit'],
                                 '--candidate', latch['candidate'],
                                 '--sha256', latch['candidate_sha256']], 300))
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


def run_new(candidate, source_commit, expected_sha):
    require(os.geteuid() == 0, 'root required')
    require(not os.path.lexists(release.STAGING_LATCH) and
            not os.path.lexists(release.STAGING_REQUIRED),
            'migration already entered or reader already active; use resume')
    paths = canonical_paths()
    pinned = pinned_candidate(Path(candidate), source_commit, expected_sha)
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
                               paths, service_state, timer_state)
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
                         source_config_args=config_args,
                         source_config_sha256=config_sha)
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
    again = modes.add_parser('resume')
    again.add_argument('--job-id', required=True)
    args = parser.parse_args()
    require(args.mode in ('run', 'resume'), 'run or resume action required')
    if args.mode == 'run':
        result = run_new(args.candidate, args.source, args.sha256)
    else:
        result = resume(args.job_id)
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print('history-staging migration: ' + str(error), file=sys.stderr)
        if os.path.lexists(release.STAGING_LATCH):
            try:
                latch = load_latch()
                print('resume: /usr/bin/python3 ' + sys.argv[0] + ' resume --job-id ' +
                      latch['job_id'], file=sys.stderr)
            except Exception:
                print('migration latch remains; inspect it before any restart',
                      file=sys.stderr)
        sys.exit(1)
