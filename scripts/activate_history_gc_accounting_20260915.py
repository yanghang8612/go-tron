#!/usr/bin/env python3
"""One native GC-accounting upgrade, preserving the existing R4/cache topology.

Reuse the pinned transaction/dirfd/recovery implementation. Explicit overrides
change only current identity, ten ancestor proofs, native admission and the
executable substitution. Old-binary ABBA results do not attest this candidate.
"""
import argparse
import base64
import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import signal
import stat
import subprocess
import sys
import types

REPO = Path('/data/gtron/go-tron')
PARENT_REVISION = '4689873c8e1b0cf123fba499ea80cd262c3329fc'
PARENT_PATH = 'scripts/activate_history_shared_read_20260915.py'
PARENT_SHA = 'a032a79b96a4fbc5c0c27f87d4b68d8281e3bebb84ba297761931df0f5319137'
SOURCE_REVISION = '197b73a920a826f21b460b3a85cc0e7ba1727d99'
SCRIPT_PATH = 'scripts/activate_history_gc_accounting_20260915.py'
RELEASE = Path('/data/gtron/releases/20260915-history-gc-accounting')
EVIDENCE = Path('/data/gtron/releases/20260915-gc-accounting-activation-evidence')
CURRENT_RELEASE = Path('/data/gtron/releases/20260915-history-shared-read')
CURRENT_SOURCE = '9d4262fcfa78bf7de8a03391e18eb0896293652e'
CURRENT_SHA = '6e45c9aacb230990c77ff6f2e1ada748f9da7aca658412596db6c5f09d4e60fd'
CURRENT_PREPARED_SHA = 'ce5a58cf3f05d1b2d7f575360fc449a54fab26db555fa089bfb1953e686aa856'
CURRENT_ADMISSION_SHA = '34a5a139fbce53d21a82966b4759456d78156aa98b41a83eb619c009bc5e35a2'
CURRENT_ACTIVATED_SHA = '1ea6b5b5ff8bc0602300195ee0c7258d174961757626b5a7afdc33cc203484be'
CURRENT_PID, CURRENT_TICKS, CURRENT_START = 28539, 4518622532, 1789451731808697642
LIVE_SHA = '5eaba5f98ed79ad1b83fe6fd9f0432e34a8643dc5c31859741f68503eb57ba0f'
SPACE_SHA = 'a38cf02487a084183115e73f46170a637aa325d7515011ea0f1862423878152b'
PRESERVED_FLAGS = ('--history.shared-read-workers=4', '--history.shared-chunk-cache=true')
SNAPSHOT_TESTS = tuple(('core/state/snapshots', name) for name in (
    'TestHistoryIndependentGCDoesNotShrinkRowDensity',
    'TestHistoryIndependentGCTimingBoundsAndLegacy',
    'TestHistoryIndependentGCCompleteMaintenanceStillCharged'))
PRUNING_TESTS = tuple(('core/state/pruning', name) for name in (
    'TestWorkerIndependentGCTimerIncludesScanProofAndGuard',
    'TestLifecycleIndependentGCTimingReachesDensity'))
GC_METRIC = 'state/snapshot/cold/history/budget/density_gc_work'


def test_log(engine, name, packages, required=()):
    passed, package_passes = set(), set()
    for row in engine.regular(RELEASE / name, 64 << 20).splitlines():
        try:
            event = json.loads(row)
        except ValueError:
            engine.require(not row.lstrip().startswith(b'{'), 'malformed native test event'); continue
        engine.require(isinstance(event, dict) and event.get('Action') != 'fail', 'native test failed or invalid event')
        if event.get('Action') == 'pass':
            if event.get('Test'): passed.add((event['Package'], event['Test']))
            else: package_passes.add(event['Package'])
    prefix = 'github.com/tronprotocol/go-tron/'
    engine.require({(prefix + package, name) for package, name in required} <= passed,
                   'critical native PASS event missing')
    engine.require({prefix + name for name in packages} <= package_passes, 'native package PASS missing')


def native_proof(engine, args, prepared):
    data = engine.regular(RELEASE / 'gc-accounting-native.json'); proof = json.loads(data)
    engine.require(engine.sha(data) == args.native_sha and proof.get('complete') is True and
                   proof['source_commit'] == SOURCE_REVISION and proof['binary'] == str(engine.BINARY) and
                   proof['binary_sha256'] == args.binary_sha and proof['prepared_sha256'] == args.prepared_sha and
                   proof['go_version'] == prepared['go_version'] and proof['build_environment'] == prepared['build_environment'],
                   'GC pruning native proof differs')
    common = ['/data/go/bin/go', 'test', '-json', '-p', '2', '-tags', 'sapling', './core/state/pruning']
    pattern = '^(' + '|'.join(name for unused, name in PRUNING_TESTS) + ')$'
    expected = [('native-gc-pruning-focused.jsonl', common + ['-run', pattern, '-count=1', '-timeout=300s']),
                ('native-gc-pruning-full.jsonl', common + ['-count=1', '-timeout=300s'])]
    engine.require(len(proof['native_commands']) == len(expected), 'native pruning phases missing')
    for row, (name, argv) in zip(proof['native_commands'], expected):
        engine.require(row['log'] == name and row['argv'] == argv and row['cwd'] == str(RELEASE / 'source') and
                       row['returncode'] == 0 and engine.sha(engine.regular(RELEASE / name)) == row['log_sha256'],
                       'native pruning command evidence differs')
        test_log(engine, name, ('core/state/pruning',), PRUNING_TESTS)
    return proof


def base_test_commands(engine, prepared):
    common = ['/data/go/bin/go', 'test', '-json', '-p', '2', '-tags', 'sapling']
    for name, packages, focused in (
            ('native-focused-tests.jsonl', ('cmd/gtron', 'core/rawdb', 'core/rawdb/pebbledb', 'core/state/snapshots'), True),
            ('native-full-tests.jsonl', ('core/state/snapshots', 'cmd/gtron'), False)):
        rows = [row for row in prepared['native_commands'] if row['log'] == name]
        engine.require(len(rows) == 1, 'native test log must be bound exactly once')
        row, prefix = rows[0], common + ['./' + p for p in packages]
        argv = row['argv']
        engine.require(row['cwd'] == str(RELEASE / 'source') and argv[:len(prefix)] == prefix and
                       argv[-2:] == ['-count=1', '-timeout=300s'], 'native test command differs')
        if focused:
            engine.require(len(argv) == len(prefix) + 4 and argv[len(prefix)] == '-run' and
                           all(re.search(argv[len(prefix) + 1], name) for unused, name in SNAPSHOT_TESTS),
                           'native focused test selection differs')
        else:
            engine.require(len(argv) == len(prefix) + 2, 'native full package was filtered')


def predecessor(engine):
    records = {}
    for name, checksum in (('prepared.json', CURRENT_PREPARED_SHA), ('activation/admission.json', CURRENT_ADMISSION_SHA),
                           ('activation/activated.json', CURRENT_ACTIVATED_SHA)):
        data = engine.regular(CURRENT_RELEASE / name)
        engine.require(engine.sha(data) == checksum, 'previous native/activation proof differs')
        records[name] = json.loads(data)
    prepared, admission, active = (records[name] for name in ('prepared.json', 'activation/admission.json', 'activation/activated.json'))
    engine.require(prepared.get('prepared') is True and prepared['source_commit'] == CURRENT_SOURCE and
                   prepared['binary_sha256'] == CURRENT_SHA and prepared['binary'] == engine.OLD_BINARY and
                   admission['args']['source_revision'] == CURRENT_SOURCE and admission['args']['binary_sha'] == CURRENT_SHA and
                   (active['pid'], active['ticks'], active['process_start']) == (CURRENT_PID, CURRENT_TICKS, CURRENT_START),
                   'previous prepared/active identity differs')


def validate_candidate(engine, args):
    engine.require(SOURCE_REVISION, 'GC accounting source identity is not frozen')
    for value, length in ((args.source_revision, 40), (args.script_revision, 40), (args.binary_sha, 64), (args.prepared_sha, 64), (args.native_sha, 64)):
        engine.require(re.fullmatch('[0-9a-f]{%d}' % length, value or ''), 'exact candidate identity required')
    engine.require(args.source_revision == SOURCE_REVISION, 'reviewed source differs')
    engine.require(engine.run(['git', 'rev-parse', args.source_revision + '^{commit}']).decode().strip() == SOURCE_REVISION,
                   'source Git identity differs')
    engine.require(engine.run(['git', 'show', args.script_revision + ':' + SCRIPT_PATH]) == engine.regular(Path(__file__).resolve()),
                   'activation script Git bytes differ')
    predecessor(engine)
    for path in (RELEASE, engine.BINARY):
        info = path.stat()
        engine.require(info.st_uid == 0 and stat.S_IMODE(info.st_mode) == 0o755 and not path.is_symlink(),
                       'candidate release/binary not service traversable')
    data = engine.regular(RELEASE / 'prepared.json'); prepared = json.loads(data)
    engine.require(engine.sha(data) == args.prepared_sha and prepared.get('prepared') is True and
                   prepared['source_commit'] == SOURCE_REVISION and prepared['binary'] == str(engine.BINARY) and
                   prepared['binary_sha256'] == args.binary_sha and engine.sha(engine.regular(engine.BINARY, 512 << 20)) == args.binary_sha,
                   'native candidate attestation differs')
    engine.require(prepared['go_version'] == 'go version go1.25.5 linux/amd64' and len(prepared['native_commands']) >= 8 and
                   all(Path(row['log']).name == row['log'] and row['returncode'] == 0 and
                       engine.sha(engine.regular(RELEASE / row['log'])) == row['log_sha256']
                       for row in prepared['native_commands']), 'native command evidence differs')
    env = prepared['build_environment']
    engine.require(all(env.get(key) == value for key, value in
                   {'CGO_ENABLED': '1', 'GOMAXPROCS': '2', 'GOTOOLCHAIN': 'local', 'GOFLAGS': '-mod=readonly',
                    'GOENV': 'off', 'GOWORK': 'off'}.items()) and 'GOROOT' not in env, 'native build environment differs')
    engine.require({'native-focused-tests.jsonl', 'native-full-tests.jsonl'} <=
                   {row['log'] for row in prepared['native_commands']}, 'native focused/full log hashes missing')
    base_test_commands(engine, prepared)
    required = {tuple(row) for row in prepared['native_focused_tests']['required_tests']}
    engine.require(set(SNAPSHOT_TESTS) <= required, 'required cost/recovery native tests absent')
    test_log(engine, 'native-focused-tests.jsonl', ('cmd/gtron', 'core/rawdb', 'core/rawdb/pebbledb', 'core/state/snapshots'), required)
    test_log(engine, 'native-full-tests.jsonl', ('cmd/gtron', 'core/state/snapshots'), SNAPSHOT_TESTS)
    native_proof(engine, args, prepared)
    expected = {}
    for row in engine.run(['git', 'ls-tree', '-rz', SOURCE_REVISION]).split(b'\0'):
        if row:
            meta, name = row.split(b'\t'); mode, kind, oid = meta.decode().split()
            if kind == 'blob': expected[name.decode()] = (oid, int(mode, 8) & 0o777)
    engine.require(set(expected) == set(prepared['manifest']), 'full source manifest differs')
    for name, entry in prepared['manifest'].items():
        path = RELEASE / 'source' / name; data = engine.regular(path, 64 << 20)
        engine.require((entry['git_blob'], entry['mode']) == expected[name] and engine.sha(data) == entry['sha256'] and
                       hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest() == entry['git_blob'] and
                       stat.S_IMODE(path.stat().st_mode) == entry['mode'], 'prepared source differs from Git')
    return prepared


def make_plan(engine, files, guard, checksum, source):
    items = {row['path']: row for row in files}
    engine.require(json.loads(engine.content(items[engine.MARKER])) == guard.reader_marker(engine.OLD_BINARY, CURRENT_SHA, CURRENT_SOURCE),
                   'old reader marker differs')
    original = engine.content(items[engine.EXEC])
    engine.require(original.count(engine.OLD_BINARY.encode()) == 1 and b'--history.backlog' not in original,
                   'ambiguous original executable/admission configuration')
    for flag in PRESERVED_FLAGS:
        engine.require(original.count(flag.encode()) == 1 and original.count(flag.split('=')[0].encode()) == 1,
                       'current shared-read flags differ')
    executable = original.replace(engine.OLD_BINARY.encode(), str(engine.BINARY).encode(), 1)
    dropin = engine.content(items[engine.DROPIN])
    for old, new in ((engine.OLD_BINARY, str(engine.BINARY)), (CURRENT_SHA, checksum), (CURRENT_SOURCE, source)):
        engine.require(dropin.count(old.encode()) == 1, 'ambiguous guard pin')
        dropin = dropin.replace(old.encode(), new.encode(), 1)
    marker = (json.dumps(guard.reader_marker(engine.BINARY, checksum, source), sort_keys=True) + '\n').encode()
    return [engine.changed(items[engine.EXEC], executable), engine.changed(items[engine.DROPIN], dropin), engine.changed(items[engine.MARKER], marker)]


def expected_config(engine, record, new):
    config = copy.deepcopy(record['configuration'])
    if new:
        command = config['ExecStart'][0]
        command['path'] = str(engine.BINARY)
        command['argv[]'] = command['argv[]'].replace(engine.OLD_BINARY, str(engine.BINARY), 1)
        for command in config['ExecStartPre']:
            if engine.GUARD in command['argv[]']:
                command['argv[]'] = command['argv[]'].replace(engine.OLD_BINARY, str(engine.BINARY)).replace(CURRENT_SHA, record['args']['binary_sha']).replace(CURRENT_SOURCE, record['args']['source_revision'])
    return config


def healthy(engine, original, record, new, timeout=240):
    result = original(record, new, timeout)
    if new:
        before = engine.show(); proc = engine.process(result['pid'])
        engine.require(before.get('ActiveState') == 'active' and int(before.get('MainPID', 0)) == result['pid'] and
                       proc['ticks'] == result['ticks'], 'GC telemetry process changed')
        head, metrics = engine.observe()
        value = metrics.get(GC_METRIC, {}).get('value')
        engine.require(type(value) is int and value >= 0, 'GC density metric contract missing')
        engine.require(metrics.get('process/start/unix_nano', {}).get('value') == result['process_start'] and head >= result['head'],
                       'GC telemetry identity/head differs')
        after = engine.show()
        engine.require(after.get('ActiveState') == 'active' and int(after.get('MainPID', 0)) == result['pid'] and
                       engine.process(result['pid']) == proc and
                       proc['exe'] == str(engine.BINARY) and proc['argv'] == [str(engine.BINARY)] + record['process']['argv'][1:] and
                       engine.environment(proc['environ']) == engine.environment(record['process']['environ']) and
                       engine.configuration(engine.guard(), after) == engine.expected_config(record, True) and
                       after.get('ControlGroup') == record['control_group'], 'GC telemetry final process/config differs')
    return result


def admit(engine, args):
    engine.validate_candidate(args)
    engine.require(args.live_evidence == str(EVIDENCE / 'live-before.json') and args.live_evidence_sha == LIVE_SHA and
                   args.space_evidence_sha == SPACE_SHA, 'reviewed fresh live evidence pin differs')
    engine.require(not os.path.lexists(engine.SPACE_HOLD), 'space protection hold is armed')
    data = engine.regular(args.live_evidence); evidence = json.loads(data)
    engine.require(engine.sha(data) == LIVE_SHA and len(evidence['ancestor_markers']) == 10, 'live evidence or ten ancestors differ')
    space_data = engine.regular(engine.SPACE_EVIDENCE); space = json.loads(space_data)
    engine.require(engine.sha(space_data) == SPACE_SHA and {f['path'] for f in space} == {engine.SPACE_GUARD, engine.SPACE_CONFIG},
                   'space dependency evidence differs')
    for row in evidence['files'] + evidence['ancestor_markers'] + space:
        engine.require(engine.saved(row['path']) == row, 'old pinned file changed')
    guard, props = engine.guard(), engine.show(); proc = engine.process(int(props['MainPID']))
    recorded = {}
    for row in base64.b64decode(evidence['systemctl_show_b64']).decode().splitlines():
        key, sep, value = row.partition('=')
        if sep: recorded[key] = recorded[key] + ' ; ' + value if key in recorded and key.startswith('Exec') else value
    engine.require(engine.configuration(guard, props) == engine.configuration(guard, recorded) and
                   engine.run(['/bin/systemctl', 'cat', 'gtron.service']) == base64.b64decode(evidence['systemctl_cat_b64']),
                   'captured effective service configuration changed')
    engine.require((proc['pid'], proc['ticks'], proc['exe']) == (CURRENT_PID, CURRENT_TICKS, engine.OLD_BINARY) and
                   engine.sha(engine.regular(engine.OLD_BINARY, 512 << 20)) == CURRENT_SHA, 'current process identity differs')
    engine.require(base64.b64decode(evidence['proc']['cmdline']).rstrip(b'\0').decode().split('\0') == proc['argv'] and
                   evidence['proc']['environ'] == proc['environ'], 'current argv/environment differs')
    engine.require(not props.get('EnvironmentFiles'), 'unreviewed environment-file dependency')
    paths = [props['FragmentPath']] + shlex.split(props['DropInPaths'])
    files = [engine.saved(path) for path in dict.fromkeys(paths + [engine.SPACE_GUARD, engine.SPACE_CONFIG] + [row['path'] for row in evidence['files']])]
    engine.require(engine.EXEC in paths and engine.DROPIN in paths, 'effective startup drop-ins missing')
    plan = engine.make_plan(files, guard, args.binary_sha, args.source_revision); parents = {}
    for path in [row['path'] for row in plan] + [RELEASE / 'reader-required.json']:
        fd, parents[str(path)] = engine.open_parent(path); os.close(fd)
    head, metrics = engine.observe()
    engine.require(props['ActiveState'] == 'active' and head > 0 and metrics['process/start/unix_nano']['value'] == CURRENT_START,
                   'current reader/process metrics not healthy')
    guard.check_command(props['ExecStart'], engine.OLD_BINARY, CURRENT_SHA, CURRENT_SOURCE, json.loads(engine.content(next(f for f in files if f['path'] == engine.MARKER))))
    engine.require(engine.process(CURRENT_PID) == proc, 'current process changed while admitting')
    record = dict(version=1, args=vars(args), process=proc, files=files, plan=plan, ancestors=evidence['ancestor_markers'], parents=parents,
                  configuration=engine.configuration(guard, props), control_group=props['ControlGroup'], head=head, process_start=CURRENT_START,
                  systemctl_cat_b64=base64.b64encode(engine.run(['/bin/systemctl', 'cat', 'gtron.service'])).decode(), created_at=engine.time.time())
    engine.healthy(record, False)
    engine.require(engine.process(CURRENT_PID) == proc and not engine.TRANSACTION.exists(), 'process changed or transaction exists')
    engine.create_transaction(); engine.save('admission.json', record)
    return {'admitted': True, 'admission_sha256': engine.sha(engine.regular(engine.TRANSACTION / 'admission.json')), 'pid': proc['pid']}


def load_engine():
    blob = subprocess.check_output(['git', 'show', PARENT_REVISION + ':' + PARENT_PATH], cwd=str(REPO), stdin=subprocess.DEVNULL, timeout=60)
    if hashlib.sha256(blob).hexdigest() != PARENT_SHA: raise RuntimeError('reviewed transaction bytes differ')
    engine = types.ModuleType('gc_accounting_transaction'); engine.__file__ = str(Path(__file__).resolve())
    exec(compile(blob, PARENT_PATH, 'exec'), engine.__dict__)
    engine.REPO, engine.RELEASE, engine.BINARY, engine.TRANSACTION = REPO, RELEASE, RELEASE / 'gtron-inspect', RELEASE / 'activation'
    engine.OLD_BINARY, engine.OLD_SHA, engine.OLD_SOURCE = str(CURRENT_RELEASE / 'gtron-inspect'), CURRENT_SHA, CURRENT_SOURCE
    engine.OLD_PID, engine.OLD_TICKS, engine.SCRIPT, engine.FLAGS = CURRENT_PID, CURRENT_TICKS, SCRIPT_PATH, []
    engine.SPACE_EVIDENCE = str(EVIDENCE / 'space-dependencies.json')
    engine.validate_candidate = lambda args: validate_candidate(engine, args)
    engine.make_plan = lambda files, guard, checksum, source: make_plan(engine, files, guard, checksum, source)
    engine.expected_config = lambda record, new: expected_config(engine, record, new)
    engine.admit = lambda args: admit(engine, args)
    original_healthy = engine.healthy
    engine.healthy = lambda record, new, timeout=240: healthy(engine, original_healthy, record, new, timeout)
    return engine


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('admit', 'activate', 'rollback'))
    for name in ('source-revision', 'script-revision', 'binary-sha', 'prepared-sha', 'native-sha', 'live-evidence', 'live-evidence-sha', 'space-evidence-sha', 'admission-sha'):
        parser.add_argument('--' + name)
    args = parser.parse_args()
    if os.geteuid() != 0 or sys.platform != 'linux': raise RuntimeError('native root Linux execution required')
    os.umask(0o077); engine = load_engine()
    for sig in engine.SIGNALS:
        signal.signal(sig, lambda number, frame: (_ for _ in ()).throw(engine.OperatorInterrupted('operator signal')))
    lock = os.open('/run/lock/gtron-history-shared-read-activation.lock', os.O_CREAT | os.O_NOFOLLOW | os.O_RDWR, 0o600)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.mode == 'admit': result = engine.admit(args)
        else:
            data = engine.regular(engine.TRANSACTION / 'admission.json')
            engine.require(engine.sha(data) == args.admission_sha, 'exact saved admission checksum required')
            result = engine.transact(json.loads(data), args.mode == 'activate')
        print(json.dumps(result, sort_keys=True), flush=True)
    except BaseException as error:
        try: engine.private_failure(error)
        except BaseException: pass
        raise
    finally:
        os.close(lock)


if __name__ == '__main__':
    try: main()
    except BaseException as error:
        if isinstance(error, SystemExit): raise
        print(json.dumps({'complete': False, 'error_type': type(error).__name__}), file=sys.stderr)
        sys.exit(1)
