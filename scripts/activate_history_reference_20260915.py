#!/usr/bin/env python3
"""Plan (default), then execute one pinned in-place R1 reader transition.

Only small private configuration/process evidence is copied. Execute never
restores an old reader: after any stopped-phase failure it retains all files,
stops the service, and records the error. No service/data mutation in plan.
"""
import argparse
import base64
import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import pwd
import re
import shlex
import signal
import stat
import subprocess
import sys
import tempfile
import time
import types

REPO = Path('/data/gtron/go-tron')
RELEASE = Path('/data/gtron/releases/20260915-history-reference-v2')
BINARY = RELEASE / 'gtron-inspect'
SCRIPT = 'scripts/activate_history_reference_20260915.py'
PARENT = '4689873c8e1b0cf123fba499ea80cd262c3329fc'
PARENT_PATH = 'scripts/activate_history_shared_read_20260915.py'
PARENT_SHA = 'a032a79b96a4fbc5c0c27f87d4b68d8281e3bebb84ba297761931df0f5319137'
OLD_BINARY = '/data/gtron/releases/20260915-history-gc-accounting/gtron-inspect'
OLD_SHA = 'b2ddf7fe0a299de7aed1f07bd86b3bd840c03b336ece05de23f4ea8ad5d873a1'
OLD_SOURCE = '197b73a920a826f21b460b3a85cc0e7ba1727d99'
DATADIR = '/data/gtron/main/datadir'
SNAPSHOTS = DATADIR + '/gtron/state-snapshots'
R1_GUARD = '/usr/local/libexec/gtron-history-reference-reader-guard.py'
R1_MARKER = str(RELEASE / 'reference-reader-required.json')
FLAG = '--history.reference-container=true'
SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
CRITICAL = ('TestHistoryReferenceRunnerPublishesOncePrunesAndReopens',
            'TestHistoryReferenceRunnerCancellationAndClosePreventPublication',
            'TestHistoryReferenceMigrationCrashResume',
            'TestHistoryReferenceMigrationNewCorruptionNeverDeletesSource',
            'TestHistoryReferenceMergeFullOldTrioOracle')

# A standalone boot guard; only its explicitly pinned, already installed parser
# is reused. Neither Git nor this deployment transaction is needed at boot.
R1_GUARD_SOURCE = '''#!/usr/bin/env python3
import argparse, hashlib, json, os, shlex, subprocess, types
PATH = '/usr/local/libexec/gtron-history-shared-reader-guard.py'
SHA = '036eb75572646f81ffdb2da1d0f270b4f3ff74ea1b573bdcff7e1d6114c52103'
def check(g, args, value, marker):
    expected = dict(version=1, container_format='GTHREF01', binary=args.binary,
                    binary_sha256=args.sha256, source_commit=args.source)
    g.require(marker == expected, 'R1 permanent reader marker missing or changed')
    g.check_command(value, args.binary, args.sha256, args.source,
                    g.reader_marker(args.binary, args.sha256, args.source))
    argv = shlex.split(g.parse_commands(value)[0]['argv[]'])
    g.require([a for a in argv if a.split('=')[0] == '--history.reference-container'] ==
              ['--history.reference-container=true'], 'R1 writer flag changed')
    g.require(g.hash_root_file(args.binary, 512 << 20) == args.sha256, 'R1 binary changed')
def main():
    import stat
    fd = os.open(PATH, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as f:
        st = os.fstat(f.fileno())
        if not stat.S_ISREG(st.st_mode) or st.st_uid or st.st_mode & 0o022 or st.st_size > 65536:
            raise RuntimeError('unsafe parser dependency')
        data = f.read(65537)
    if hashlib.sha256(data).hexdigest() != SHA:
        raise RuntimeError('pinned parser bytes changed')
    g = types.ModuleType('pinned_shared_guard')
    exec(compile(data, PATH, 'exec'), g.__dict__)
    p = argparse.ArgumentParser()
    for key in ('binary', 'sha256', 'source', 'marker'): p.add_argument('--' + key, required=True)
    a = p.parse_args()
    marker = json.loads(g.read_root_file(a.marker, 4096))
    out = subprocess.check_output(['/bin/systemctl', 'show', 'gtron.service',
          '--property=ExecStart', '--no-pager'], stdin=subprocess.DEVNULL, timeout=15).decode().strip()
    g.require(out.startswith('ExecStart='), 'missing effective ExecStart')
    check(g, a, out[len('ExecStart='):], marker)
    print(json.dumps({'reference_reader_guard': True, 'source_commit': a.source}))
if __name__ == '__main__': main()
'''


class Interrupted(BaseException):
    pass


def child_run(argv, timeout=60, cwd=None, env=None, demote=None, stdout=None, stderr=None):
    """Own one child process group, including timeout/signal paths and reap."""
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, SIGNALS)
    child = None
    def prepare_child():
        # Do not leave the executed child with the parent's spawn mask. Any
        # demotion here uses pre-resolved IDs/groups, without name-service I/O.
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)
        if demote is not None: demote()
    try:
        child = subprocess.Popen(argv, cwd=str(cwd or REPO), env=env, preexec_fn=prepare_child,
                                 stdin=subprocess.DEVNULL, stdout=stdout or subprocess.PIPE,
                                 stderr=stderr or subprocess.PIPE, start_new_session=True)
        # A pending signal may raise here, after the handle is available and
        # inside the cleanup region (including fork-to-exec transitions).
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)
        out, err = child.communicate(timeout=timeout)
        if child.returncode:
            raise subprocess.CalledProcessError(child.returncode, argv, output=out, stderr=err)
        return out
    except BaseException:
        signal.pthread_sigmask(signal.SIG_BLOCK, SIGNALS)
        try:
            if child is not None:
                try: os.killpg(child.pid, signal.SIGTERM)
                except ProcessLookupError: pass
                try: child.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    try: os.killpg(child.pid, signal.SIGKILL)
                    except ProcessLookupError: pass
                    child.communicate()
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)
        raise
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)


def load_engine():
    data = child_run(['git', 'show', PARENT + ':' + PARENT_PATH])
    if hashlib.sha256(data).hexdigest() != PARENT_SHA:
        raise RuntimeError('pinned filesystem/health helper differs')
    e = types.ModuleType('reference_activation_primitives'); e.__file__ = str(Path(__file__).resolve())
    exec(compile(data, PARENT_PATH, 'exec'), e.__dict__)
    e.REPO, e.RELEASE, e.BINARY, e.TRANSACTION = REPO, RELEASE, BINARY, RELEASE / 'reference-activation'
    e.OLD_BINARY, e.OLD_SHA, e.OLD_SOURCE, e.FLAGS = OLD_BINARY, OLD_SHA, OLD_SOURCE, [FLAG]
    e.run = child_run
    e.expected_config = lambda record, new: expected_config(e, record, new)
    e.check_files = lambda record, target=None: check_files(e, record, target)
    # None of the inherited mutation/recovery workflows is part of R1 activation.
    for name in ('restore', 'transact', 'admit', 'validate_candidate', 'main'):
        delattr(e, name)
    return e


def item(e, path, data, mode=0o644):
    return dict(path=str(path), uid=0, gid=0, mode=mode,
                data_b64=base64.b64encode(data).decode(), sha256=e.sha(data))


def marker(args):
    return dict(version=1, container_format='GTHREF01', binary=str(BINARY),
                binary_sha256=args.binary_sha, source_commit=args.source_revision)


def guard_command(args):
    return ['/usr/bin/python3', R1_GUARD, '--binary', str(BINARY), '--sha256', args.binary_sha,
            '--source', args.source_revision, '--marker', R1_MARKER]


def make_plan(e, files, args):
    bypath = {f['path']: f for f in files}; g = e.guard()
    e.require(json.loads(e.content(bypath[e.MARKER])) == g.reader_marker(OLD_BINARY, OLD_SHA, OLD_SOURCE),
              'current shared reader marker differs')
    old = e.content(bypath[e.EXEC])
    e.require(old.count(OLD_BINARY.encode()) == 1 and b'--history.backlog' not in old and
              b'--history.reference-container' not in old, 'ambiguous executable/reference/admission options')
    for flag in ('--history.shared-read-workers=4', '--history.shared-chunk-cache=true'):
        e.require(old.count(flag.encode()) == 1 and old.count(flag.split('=')[0].encode()) == 1,
                  'current R4/cache topology differs')
    new = old.replace(OLD_BINARY.encode(), (str(BINARY) + ' ' + FLAG).encode(), 1)
    drop = e.content(bypath[e.DROPIN])
    e.require(R1_GUARD.encode() not in drop and drop.endswith(b'\n'), 'unexpected reference guard/drop-in ending')
    sections = re.findall(br'^\s*\[([^]\r\n]+)\]\s*$', drop, re.M)
    e.require(sections and sections[-1] == b'Service', 'shared drop-in final section is not Service')
    for a, b in ((OLD_BINARY, str(BINARY)), (OLD_SHA, args.binary_sha), (OLD_SOURCE, args.source_revision)):
        e.require(drop.count(a.encode()) == 1, 'ambiguous shared guard identity')
        drop = drop.replace(a.encode(), b.encode(), 1)
    drop += ('ExecStartPre=' + ' '.join(guard_command(args)) + '\n').encode()
    shared = (json.dumps(g.reader_marker(BINARY, args.binary_sha, args.source_revision), sort_keys=True) + '\n').encode()
    # All of these are durable before migration or any new node start.
    return [item(e, R1_GUARD, R1_GUARD_SOURCE.encode(), 0o755),
            item(e, R1_MARKER, (json.dumps(marker(args), sort_keys=True) + '\n').encode()),
            item(e, RELEASE / 'reader-required.json', shared),
            e.changed(bypath[e.MARKER], shared), e.changed(bypath[e.EXEC], new), e.changed(bypath[e.DROPIN], drop)]


def expected_config(e, record, new):
    result = copy.deepcopy(record['configuration'])
    if new:
        args = argparse.Namespace(**record['args'])
        result['ExecStart'][0]['path'] = str(BINARY)
        result['ExecStart'][0]['argv[]'] = result['ExecStart'][0]['argv[]'].replace(OLD_BINARY, str(BINARY) + ' ' + FLAG, 1)
        for cmd in result['ExecStartPre']:
            if e.GUARD in shlex.split(cmd['argv[]']):
                cmd['argv[]'] = cmd['argv[]'].replace(OLD_BINARY, str(BINARY)).replace(OLD_SHA, args.binary_sha).replace(OLD_SOURCE, args.source_revision)
        result['ExecStartPre'].append({'path': '/usr/bin/python3', 'argv[]': ' '.join(guard_command(args)), 'ignore_errors': 'no'})
    return result


def test_proof(e, prepared):
    common = ['/data/go/bin/go', 'test', '-json', '-p', '2', '-tags', 'sapling']
    for name, packages, focused in (('native-focused-tests.jsonl', ('cmd/gtron', 'core/rawdb', 'core/rawdb/pebbledb', 'core/state/snapshots'), True),
                                    ('native-full-tests.jsonl', ('core/state/snapshots', 'cmd/gtron'), False)):
        rows = [r for r in prepared['native_commands'] if r['log'] == name]
        e.require(len(rows) == 1, 'native test phase must be bound exactly once')
        argv, prefix = rows[0]['argv'], common + ['./' + p for p in packages]
        e.require(argv[:len(prefix)] == prefix and argv[-2:] == ['-count=1', '-timeout=300s'] and
                  ((focused and len(argv) == len(prefix) + 4 and argv[len(prefix)] == '-run') or
                   (not focused and len(argv) == len(prefix) + 2)), 'native test package/scope differs')
        passed, pkgs = set(), set()
        for line in e.regular(RELEASE / name, 64 << 20).splitlines():
            try: row = json.loads(line)
            except ValueError:
                e.require(not line.lstrip().startswith(b'{'), 'malformed native test JSON'); continue
            e.require(isinstance(row, dict) and row.get('Action') != 'fail', 'native test failure')
            if row.get('Action') == 'pass':
                if row.get('Test'): passed.add((row['Package'], row['Test']))
                else: pkgs.add(row['Package'])
        prefix = 'github.com/tronprotocol/go-tron/'
        e.require({prefix + p for p in packages} <= pkgs, 'native package PASS missing')
        e.require({(prefix + 'core/state/snapshots', t) for t in CRITICAL} <= passed, 'critical R1 PASS missing')


def validate_candidate(e, args):
    for name, length in (('source_revision', 40), ('script_revision', 40), ('binary_sha', 64), ('prepared_sha', 64)):
        e.require(re.fullmatch('[0-9a-f]{%d}' % length, getattr(args, name, '') or ''), 'exact candidate identity required')
    e.require(e.run(['git', 'rev-parse', args.source_revision + '^{commit}']).decode().strip() == args.source_revision and
              e.run(['git', 'show', args.script_revision + ':' + SCRIPT]) == e.regular(Path(__file__).resolve()), 'source/activation Git identity differs')
    for path in (RELEASE, BINARY):
        st = path.lstat()
        e.require(st.st_uid == 0 and stat.S_IMODE(st.st_mode) == 0o755 and not path.is_symlink(), 'release/binary not service traversable')
    raw = e.regular(RELEASE / 'prepared.json'); prepared = json.loads(raw)
    e.require(e.sha(raw) == args.prepared_sha and prepared.get('prepared') is True and
              prepared['source_commit'] == args.source_revision and prepared['binary'] == str(BINARY) and
              prepared['binary_sha256'] == args.binary_sha and e.sha(e.regular(BINARY, 512 << 20)) == args.binary_sha,
              'native prepared identity differs')
    e.require(prepared['go_version'] == 'go version go1.25.5 linux/amd64', 'native Go version differs')
    e.require(re.fullmatch('[0-9a-f]{40}', prepared.get('script_commit', '')) and
              e.sha(e.run(['git', 'show', prepared['script_commit'] + ':scripts/prepare_history_reference_20260915.py'])) == prepared['script_sha256'],
              'native prepared builder Git bytes differ')
    env = prepared['build_environment']
    e.require(all(env.get(k) == v for k, v in {'CGO_ENABLED': '1', 'GOMAXPROCS': '2', 'GOTOOLCHAIN': 'local',
              'GOFLAGS': '-mod=readonly', 'GOENV': 'off', 'GOWORK': 'off'}.items()) and 'GOROOT' not in env, 'native build environment differs')
    e.require(len(prepared['native_commands']) >= 8 and len({r['log'] for r in prepared['native_commands']}) == len(prepared['native_commands']),
              'native phases incomplete or duplicate log binding')
    for row in prepared['native_commands']:
        e.require(Path(row['log']).name == row['log'] and row['cwd'] == str(RELEASE / 'source') and row['returncode'] == 0 and
                  e.sha(e.regular(RELEASE / row['log'])) == row['log_sha256'], 'native command/log evidence differs')
    test_proof(e, prepared)
    build = [r for r in prepared['native_commands'] if r['log'] == 'build.log']
    e.require(len(build) == 1 and build[0]['argv'] == ['/data/go/bin/go', 'build', '-p', '2', '-tags', 'sapling',
              '-buildvcs=false', '-o', str(BINARY), './cmd/gtron'], 'native binary build command differs')
    e.require(re.fullmatch(br'nativeSapling=true uncommitted=[0-9a-f]{64}',
              e.regular(RELEASE / 'native-sapling-probe.log').strip()), 'native Sapling evidence missing')
    info = e.regular(RELEASE / 'build-info.txt').decode()
    e.require(all(v in info for v in ('go1.25.5', 'CGO_ENABLED=1', '-tags=sapling', 'GOARCH=amd64', 'GOOS=linux')),
              'native binary build settings differ')
    e.require({'build.log', 'build-info.txt', 'native-sapling-probe.log'} <= {r['log'] for r in prepared['native_commands']},
              'unbound native binary/Sapling logs')
    expected = {}
    for row in e.run(['git', 'ls-tree', '-rz', args.source_revision]).split(b'\0'):
        if row:
            meta, name = row.split(b'\t'); mode, kind, oid = meta.decode().split()
            if kind == 'blob': expected[name.decode()] = (oid, int(mode, 8) & 0o777)
    e.require(set(expected) == set(prepared['manifest']), 'full Git source manifest differs')
    for name, entry in prepared['manifest'].items():
        path = RELEASE / 'source' / name; data = e.regular(path, 64 << 20)
        e.require((entry['git_blob'], entry['mode']) == expected[name] and e.sha(data) == entry['sha256'] and
                  hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest() == entry['git_blob'] and
                  stat.S_IMODE(path.stat().st_mode) == entry['mode'], 'prepared source bytes differ')
    return prepared


def check_files(e, record, target=None):
    changes = {f['path']: f for f in record['plan']}
    old = {f['path']: f for f in record['files']}
    for path, pin in record['parents'].items():
        fd, unused = e.open_parent(path, pin); os.close(fd)
    for path in set(old) | set(changes):
        actual = e.saved(path) if os.path.lexists(path) else None
        expected = changes.get(path, old.get(path)) if target == 'new' else old.get(path)
        e.require(actual == expected if target else actual in (old.get(path), changes.get(path, old.get(path))), 'unapproved file drift')


def plan(e, args):
    validate_candidate(e, args)
    e.require(type(args.migration_trios) is int and 0 <= args.migration_trios <= 16, 'bounded migration count required; zero skips')
    e.require(not os.path.lexists(e.SPACE_HOLD) and not e.TRANSACTION.exists(), 'space hold/previous transaction exists')
    g, props = e.guard(), e.show(); proc = e.process(int(props.get('MainPID', 0)))
    e.require((proc['pid'], proc['ticks'], proc['exe']) == (args.old_pid, args.old_ticks, OLD_BINARY) and
              e.sha(e.regular(OLD_BINARY, 512 << 20)) == OLD_SHA, 'exact current process differs')
    config = e.configuration(g, props)
    e.require(not props.get('EnvironmentFiles') and config['User'] == 'java-tron' and
              config['WorkingDirectory'] == '/data/gtron/main', 'unreviewed service user/path/environment files')
    account = pwd.getpwnam(config['User'])
    e.require(account.pw_uid != 0, 'migration must run as service account')
    e.require(len(config['ExecStart']) == 1 and shlex.split(config['ExecStart'][0]['argv[]']) == proc['argv'], 'effective process argv differs')
    pre = config['ExecStartPre']
    e.require(len(pre) == 2 and all(c['ignore_errors'] == 'no' for c in pre) and
              shlex.split(pre[0]['argv[]']) == ['/usr/bin/python3', e.SPACE_GUARD, 'check'] and
              shlex.split(pre[1]['argv[]']) == ['/usr/bin/python3', e.GUARD, '--binary', OLD_BINARY,
              '--sha256', OLD_SHA, '--source', OLD_SOURCE, '--marker', e.MARKER], 'existing two mandatory guards differ')
    paths = [props['FragmentPath']] + shlex.split(props['DropInPaths'])
    e.require(e.EXEC in paths and e.DROPIN in paths, 'effective startup paths differ')
    # Snapshot only tiny immutable reader identities, without traversing data.
    ancestors = sorted(RELEASE.parent.glob('*/reader-required.json'))
    e.require(len(ancestors) <= 64, 'too many permanent reader identities')
    paths += [e.GUARD, e.MARKER, e.SPACE_GUARD, e.SPACE_CONFIG] + [str(p) for p in ancestors]
    files = [e.saved(p) for p in dict.fromkeys(paths)]
    e.require(sum(len(e.content(f)) for f in files) <= 2 << 20, 'configuration backup exceeds small evidence limit')
    changes = make_plan(e, files, args); parents = {}
    for f in changes:
        if f['path'] not in paths: e.require(not os.path.lexists(f['path']), 'new permanent target already exists')
        fd, parents[f['path']] = e.open_parent(f['path']); os.close(fd)
    head, metrics = e.observe()
    e.require(props['ActiveState'] == 'active' and head > 0, 'old service not healthy')
    g.check_command(props['ExecStart'], OLD_BINARY, OLD_SHA, OLD_SOURCE, json.loads(e.regular(e.MARKER)))
    record = dict(version=1, args=vars(args), files=files, plan=changes, parents=parents,
                  process=proc, configuration=config, control_group=props['ControlGroup'],
                  process_start=metrics['process/start/unix_nano']['value'], head=head,
                  service_uid=account.pw_uid, service_gid=account.pw_gid, created_at=time.time(),
                  systemctl_cat_b64=base64.b64encode(e.run(['/bin/systemctl', 'cat', 'gtron.service'])).decode())
    check_files(e, record, 'old'); e.healthy(record, False)
    e.require(e.process(args.old_pid) == proc, 'old process changed during plan')
    e.create_transaction(); e.save('plan.json', record)
    return {'planned': True, 'pid': proc['pid'], 'plan_sha256': e.sha(e.regular(e.TRANSACTION / 'plan.json')),
            'migration_trios': args.migration_trios, 'service_modified': False, 'database_copied': False}


def migrate(e, record):
    count = record['args']['migration_trios']
    if not count: return {'skipped': True}
    e.require(int(e.show().get('MainPID', 0)) == 0, 'service restarted before migration')
    account = pwd.getpwnam(record['configuration']['User'])
    e.require((account.pw_uid, account.pw_gid) == (record['service_uid'], record['service_gid']), 'service account changed')
    groups = os.getgrouplist(account.pw_name, account.pw_gid)
    def demote():
        os.setgroups(groups); os.setgid(account.pw_gid); os.setuid(account.pw_uid)
    env = dict(row.decode().split('=', 1) for row in e.environment(record['process']['environ']))
    argv = [str(BINARY), 'db', 'migrate-history-reference', '--datadir', DATADIR, '--max-trios', str(count), '--yes']
    start = time.time(); code = None
    with open(str(e.TRANSACTION / 'migration.stdout'), 'xb') as out, open(str(e.TRANSACTION / 'migration.stderr'), 'xb') as err:
        os.fchmod(out.fileno(), 0o600); os.fchmod(err.fileno(), 0o600)
        try:
            child_run(argv, timeout=1800, cwd=record['configuration']['WorkingDirectory'], env=env, demote=demote, stdout=out, stderr=err)
            code = 0
        except subprocess.CalledProcessError as failure:
            code = failure.returncode; raise
        finally:
            for stream in (out, err): stream.flush(); os.fsync(stream.fileno())
            e.save('migration-command.json', dict(argv=argv, started_unix=start, finished_unix=time.time(),
                   wall_seconds=time.time() - start, returncode=code, service_uid=account.pw_uid,
                   stdout_sha256=e.sha(e.regular(e.TRANSACTION / 'migration.stdout', 32 << 20)),
                   stderr_sha256=e.sha(e.regular(e.TRANSACTION / 'migration.stderr', 32 << 20))))
    result = json.loads(e.regular(e.TRANSACTION / 'migration.stdout', 32 << 20))
    completed = result.get('result', {}).get('completedTrios')
    e.require(result.get('dryRun') is False and not result.get('error') and result.get('databasePath') == DATADIR + '/gtron/chaindata' and
              result.get('snapshotDir') == SNAPSHOTS and result.get('result', {}).get('journalPending') is False and
              result['result'].get('snapshotDir') == SNAPSHOTS and result['result'].get('dryRun') is False and
              type(completed) is int and 0 <= completed <= count and
              all(type(result.get(k)) is int and result[k] >= 0 for k in
                  ('freeBytesBefore', 'freeBytesAfter', 'largestAdmittedWorkBytes')),
              'migration returned incomplete/mismatched report; leave stopped')
    e.save('migration.json', result); return result


def execute(e, record):
    validate_candidate(e, argparse.Namespace(**record['args']))
    e.require(not os.path.lexists(e.SPACE_HOLD) and not (e.TRANSACTION / 'journal.json').exists(), 'hold or already attempted transaction')
    check_files(e, record, 'old')
    e.require(e.process(record['process']['pid']) == record['process'] and e.configuration(e.guard(), e.show()) == record['configuration'],
              'planned live identity changed')
    phase = 'stopping'
    try:
        e.save('journal.json', {'phase': phase, 'at': time.time(), 'never_restore_old_reader': True})
        e.stop(record); check_files(e, record, 'old')
        phase = 'installing-reader-guards'
        for f in record['plan']: e.write_atomic(f, parent_pin=record['parents'][f['path']])
        e.run(['/bin/systemctl', 'daemon-reload'])
        e.require(e.configuration(e.guard(), e.show()) == expected_config(e, record, True), 'new effective unit differs')
        check_files(e, record, 'new')
        # Execute all three real guards while still stopped. No data writer runs
        # until durable markers/config and their actual boot checks have passed.
        for cmd in expected_config(e, record, True)['ExecStartPre']: e.run(shlex.split(cmd['argv[]']))
        phase = 'migration'; e.save('journal.json', {'phase': phase, 'at': time.time(), 'never_restore_old_reader': True})
        migrate(e, record)
        check_files(e, record, 'new'); phase = 'starting-new-reader'
        e.require(not os.path.lexists(e.SPACE_HOLD), 'space hold armed during migration')
        e.run(['/bin/systemctl', 'start', 'gtron.service'], timeout=180)
        result = e.healthy(record, True); e.save('activated.json', result)
        return dict(result, activated=True, reference_container=True)
    except BaseException as error:
        previous = signal.pthread_sigmask(signal.SIG_BLOCK, SIGNALS)
        try:
            try: e.save('failure.json', dict(e.failure_details(error), phase=phase, no_old_reader_rollback=True))
            except BaseException: pass
            try:
                e.stop(record); e.save('stopped-after-failure.json', {'stopped': True, 'at': time.time(), 'phase': phase})
            except BaseException as stop_error:
                try: e.save('stop-failed.json', e.failure_details(stop_error))
                except BaseException: pass
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)
        raise


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('mode', nargs='?', choices=('plan', 'execute'), default='plan')
    for name in ('source-revision', 'script-revision', 'binary-sha', 'prepared-sha', 'plan-sha'): p.add_argument('--' + name)
    p.add_argument('--old-pid', type=int, default=4919); p.add_argument('--old-ticks', type=int)
    p.add_argument('--migration-trios', type=int, default=1)
    args = p.parse_args()
    if os.geteuid() != 0 or sys.platform != 'linux': raise RuntimeError('native root Linux only')
    os.umask(0o077)
    for sig in SIGNALS: signal.signal(sig, lambda signum, frame: (_ for _ in ()).throw(Interrupted('operator signal')))
    e = load_engine()
    lock = os.open('/run/lock/gtron-history-reference-activation.lock', os.O_CREAT | os.O_NOFOLLOW | os.O_RDWR, 0o600)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.mode == 'plan': result = plan(e, args)
        else:
            raw = e.regular(e.TRANSACTION / 'plan.json')
            e.require(e.sha(raw) == args.plan_sha, 'exact private plan checksum required')
            result = execute(e, json.loads(raw))
        print(json.dumps(result, sort_keys=True), flush=True)
    except BaseException as error:
        # Also retain plan/preflight failures before the transaction exists.
        # The private detail may contain process configuration: never print it.
        try:
            fd, name = tempfile.mkstemp(prefix='reference-activation-failure-', suffix='.json', dir=str(RELEASE))
            with os.fdopen(fd, 'wb') as out:
                os.fchmod(out.fileno(), 0o600)
                out.write(json.dumps(e.failure_details(error), sort_keys=True).encode()); out.flush(); os.fsync(out.fileno())
            parent = os.open(str(RELEASE), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try: os.fsync(parent)
            finally: os.close(parent)
        except BaseException: pass
        raise
    finally: os.close(lock)


if __name__ == '__main__':
    try: main()
    except BaseException as error:
        if isinstance(error, SystemExit): raise
        print(json.dumps({'complete': False, 'error_type': type(error).__name__}), file=sys.stderr)
        sys.exit(1)
