#!/usr/bin/env python3
"""Plan, execute, or delete one fixed-path fresh-mainnet cutover.

Execute stops gtron normally, atomically quarantines only the derived gtron
subtree on the same filesystem, creates an empty replacement, installs the
pinned candidate and reader guards, and starts fresh. It never copies the old
database and has no old-database rollback mode. Deletion is a separate action.
"""
import argparse
import base64
import copy
import fcntl
import hashlib
import http.client
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
import time
import types
import uuid


REPO = Path('/data/gtron/go-tron')
BASE = 'cdbd49f865cc98b868675d78721a2c06d16c9dec'
HELPER_PATH = 'scripts/activate_history_shared_read_20260915.py'
HELPER_SHA = 'a032a79b96a4fbc5c0c27f87d4b68d8281e3bebb84ba297761931df0f5319137'
SCRIPT_PATH = 'scripts/cutover_fresh_mainnet_20260918.py'
RELEASE = Path('/data/gtron/releases/20260918-fresh-mainnet')
BINARY = RELEASE / 'gtron'
PREPARED = RELEASE / 'prepared.json'
ACCEPTANCE = RELEASE / 'fresh-acceptance/summary.json'
TRANSACTION = RELEASE / 'fresh-cutover'
MAIN = Path('/data/gtron/main')
DATADIR = MAIN / 'datadir'
TREE = DATADIR / 'gtron'
NODEKEY = DATADIR / 'nodekey'
PEERS = DATADIR / 'p2p-peers'
QUARANTINE_PREFIX = '.gtron-quarantine-'
QUARANTINE_CHILD = 'old-gtron'
MEMORY_DROPIN = '/etc/systemd/system/gtron.service.d/zz-memory-budget-20260918.conf'
SHARED_DROPIN = '/etc/systemd/system/gtron.service.d/zz-history-shared-reader.conf'
SHARED_GUARD = '/usr/local/libexec/gtron-history-shared-reader-guard.py'
R1_GUARD = '/usr/local/libexec/gtron-history-reference-reader-guard.py'
SPACE_GUARD = '/usr/local/libexec/gtron-mainnet-space-guard.py'
SPACE_CONFIG = '/etc/gtron/mainnet-space-guard.json'
SPACE_HOLD = '/var/lib/gtron-mainnet-disk-stop.hold'
GLOBAL_MARKER = '/data/gtron/main/HISTORY_SHARED_CHUNKS_REQUIRES_READER_V3.json'
NEW_SHARED_MARKER = str(RELEASE / 'reader-required.json')
NEW_R1_MARKER = str(RELEASE / 'reference-reader-required.json')
GENESIS = '00000000000000001ebf88508a03865c71d452e25f4d51194196a1d22b6653dc'
SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
FORMAT_FLAGS = {'datadir': str(DATADIR), 'prune.mode': 'snap', 'db.cache': '4096',
                'history.shared-read-workers': '4', 'history.shared-chunk-cache': 'true',
                'history.reference-container': 'true'}
MEMORY_ENV = {'GOMEMLIMIT': '8GiB', 'GOGC': '100',
              'GTRON_SNAPSHOT_MANIFEST_CACHE_BUDGET_BYTES': '536870912'}
REQUIRED_TESTS = {
    ('core/state/snapshots', 'TestPrepareRetiredMetadataSweepOnlyForgetsConfirmedMissing'),
    ('core/state/snapshots', 'TestPrepareRetiredMetadataSweepFailsClosed'),
    ('core/state/snapshots', 'TestRetiredMetadataSweepCursorBoundedAndMutationSafe'),
    ('core/state/snapshots', 'TestRunnerRetiredMetadataCursorCommitsOnlyAfterIntegration'),
    ('core/state/snapshots', 'TestRunnerRetiredMetadataSweepCancellationAndGuardRace'),
    ('core/state/snapshots', 'TestManifestCacheAuthenticatesCurrentBytes'),
    ('core/state/snapshots', 'TestManifestPublicationSeedsExactDecodedView'),
    ('core/rawdb', 'TestStateHistorySpanColdOrderOracle'),
}
REWARD_GOLDEN = ('core/reward', 'TestOldRewardSum_JavaCompoundAssignmentGolden')


class Interrupted(BaseException):
    pass


def load_engine():
    data = subprocess.check_output(['git', 'show', BASE + ':' + HELPER_PATH], cwd=str(REPO),
                                   stdin=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=60)
    if hashlib.sha256(data).hexdigest() != HELPER_SHA:
        raise RuntimeError('pinned transaction helper differs')
    engine = types.ModuleType('fresh_cutover_primitives')
    engine.__file__ = str(Path(__file__).resolve())
    exec(compile(data, HELPER_PATH, 'exec'), engine.__dict__)
    engine.REPO, engine.RELEASE, engine.BINARY, engine.TRANSACTION = REPO, RELEASE, BINARY, TRANSACTION
    return engine


def flag_rows(argv):
    rows, index = {}, 1
    while index < len(argv):
        token = argv[index]
        if not token.startswith('--'):
            index += 1; continue
        raw = token[2:]
        if '=' in raw:
            name, value, width = raw.split('=', 1)[0], raw.split('=', 1)[1], 1
        elif index + 1 < len(argv) and not argv[index + 1].startswith('--'):
            name, value, width = raw, argv[index + 1], 2
        else:
            name, value, width = raw, 'true', 1
        rows.setdefault(name, []).append(value); index += width
    return rows


def validate_chain_argv(engine, argv, binary=None):
    engine.require(argv and (binary is None or argv[0] == binary), 'service executable differs')
    rows = flag_rows(argv)
    for name, value in FORMAT_FLAGS.items():
        engine.require(rows.get(name) == [value], 'fresh format flag differs: ' + name)
    engine.require(rows.get('history.cross-block-dedup') in (['true'], ['false']),
                   'one explicit cross-block history flag required')
    for name in ('p2p.port', 'discover.port', 'external.ip', 'http.port', 'jsonrpc.port',
                 'grpc.port', 'pprof.port', 'pprof.addr'):
        engine.require(len(rows.get(name, ())) <= 1, 'duplicate service network flag: ' + name)
    def value(name, default):
        return rows.get(name, [default])[0]
    engine.require(value('p2p.port', '18888') == '18890' and
                   value('discover.port', '0') in ('0', '18890') and
                   value('http.port', '8090') == '8090' and
                   value('jsonrpc.port', '8545') == '8545' and
                   value('grpc.port', '50051') == '50051' and
                   value('pprof.port', '0') == '6062' and
                   value('pprof.addr', '127.0.0.1') == '127.0.0.1',
                   'effective production network layout differs')
    engine.require(not set(rows) & {'config', 'genesis', 'testnet', 'dev', 'witness', 'snapshot.bootstrap',
                                    'snapshot.reset', 'sync.restart-from', 'sync.replay-stored-to',
                                    'sync.stop-at'},
                   'unsafe service mode present')
    for name in ('snapshot.dir', 'snapshot.etl.tempdir', 'sync.etl.tempdir'):
        engine.require(len(rows.get(name, ())) <= 1, 'duplicate storage path flag: ' + name)
    snapshot_dir = rows.get('snapshot.dir', [str(TREE / 'state-snapshots')])[0]
    engine.require(snapshot_dir == str(TREE / 'state-snapshots'),
                   'snapshot.dir escapes the derived tree being quarantined')
    return rows


def root_item(engine, path, data, mode=0o644):
    return {'path': str(path), 'uid': 0, 'gid': 0, 'mode': mode,
            'data_b64': base64.b64encode(data).decode(), 'sha256': engine.sha(data)}


def r1_marker(binary, checksum, source):
    return (json.dumps({'version': 1, 'container_format': 'GTHREF01', 'binary': str(binary),
                        'binary_sha256': checksum, 'source_commit': source}, sort_keys=True) + '\n').encode()


def decoded_environment(engine, encoded):
    result = {}
    for row in engine.environment(encoded):
        key, sep, value = row.decode().partition('=')
        engine.require(sep and key not in result, 'ambiguous service environment')
        result[key] = value
    return result


def validate_candidate(engine, args):
    for name, length in (('source_revision', 40), ('script_revision', 40),
                         ('binary_sha256', 64), ('prepared_sha256', 64),
                         ('acceptance_sha256', 64)):
        value = getattr(args, name, None)
        if name == 'acceptance_sha256' and value is None:
            continue
        engine.require(re.fullmatch('[0-9a-f]{%d}' % length, value or ''),
                       'exact candidate identity required: ' + name)
    engine.require(engine.run(['git', 'rev-parse', args.source_revision + '^{commit}']).decode().strip() ==
                   args.source_revision, 'candidate source Git identity differs')
    engine.require(engine.run(['git', 'rev-parse', args.script_revision + '^{commit}']).decode().strip() ==
                   args.script_revision and
                   engine.run(['git', 'show', args.script_revision + ':' + SCRIPT_PATH]) ==
                   engine.regular(Path(__file__).resolve(), 512 << 10), 'cutover script Git identity differs')
    release = RELEASE.lstat(); binary = BINARY.lstat()
    engine.require(release.st_uid == 0 and stat.S_IMODE(release.st_mode) == 0o755 and
                   binary.st_uid == 0 and stat.S_IMODE(binary.st_mode) == 0o755 and
                   not RELEASE.is_symlink() and not BINARY.is_symlink(), 'unsafe candidate release')
    raw = engine.regular(PREPARED); prepared = json.loads(raw)
    engine.require(engine.sha(raw) == args.prepared_sha256 and prepared.get('prepared') is True and
                   prepared.get('source_commit') == args.source_revision and
                   prepared.get('binary') == str(BINARY) and
                   prepared.get('binary_sha256') == args.binary_sha256 and
                   engine.sha(engine.regular(BINARY, 512 << 20)) == args.binary_sha256,
                   'candidate prepared identity differs')
    engine.require(prepared.get('candidate_scope') == ['fresh-mainnet', 'retired-metadata-gc',
                                                       'java-voter-reward-parity',
                                                       'r1-shared-history-writer'] and
                   prepared.get('go_environment') ==
                   ['linux', 'amd64', 'v1', '1', 'go1.25.5', 'local'],
                   'candidate scope or native target differs')
    build_environment = prepared.get('build_environment', {})
    engine.require(all(build_environment.get(key) == value for key, value in
                       {'CGO_ENABLED': '1', 'GOMAXPROCS': '2', 'GOFLAGS': '-mod=readonly',
                        'GOENV': 'off', 'GOWORK': 'off', 'GOTOOLCHAIN': 'local',
                        'GOAMD64': 'v1'}.items()), 'native build environment differs')
    engine.require({tuple(row) for row in prepared.get('native_focused_tests', {}).get('required_tests', ())} ==
                   REQUIRED_TESTS and
                   {tuple(row) for row in prepared.get('native_reward_tests', {}).get('required_tests', ())} ==
                   {REWARD_GOLDEN}, 'required native test contract differs')
    commands = prepared.get('native_commands', ())
    engine.require(len(commands) >= 8 and all(row.get('returncode') == 0 and
                   isinstance(row.get('log'), str) and Path(row['log']).name == row['log'] and
                   engine.sha(engine.regular(RELEASE / row['log'])) == row.get('log_sha256')
                   for row in commands), 'native command evidence differs')
    probe = engine.regular(RELEASE / 'native-sapling-probe.log', 1 << 20).decode().strip()
    engine.require(re.fullmatch('nativeSapling=true uncommitted=[0-9a-f]{64}', probe) is not None,
                   'native Sapling probe differs')
    return prepared


def command_contract(engine, config, process, old_sha, old_source):
    engine.require(len(config['ExecStart']) == 1 and
                   shlex.split(config['ExecStart'][0]['argv[]']) == process['argv'],
                   'effective ExecStart differs from process')
    rows = validate_chain_argv(engine, process['argv'], process['exe'])
    commands = config['ExecStartPre']
    engine.require(len(commands) == 3 and all(row['ignore_errors'] == 'no' for row in commands),
                   'exact three mandatory guards required')
    parsed = [shlex.split(row['argv[]']) for row in commands]
    engine.require(parsed[0] == ['/usr/bin/python3', SPACE_GUARD, 'check'],
                   'space guard command differs')
    for command, guard, marker in ((parsed[1], SHARED_GUARD, GLOBAL_MARKER),
                                   (parsed[2], R1_GUARD, None)):
        engine.require(len(command) == 10 and command[:2] == ['/usr/bin/python3', guard] and
                       command[2:8] == ['--binary', process['exe'], '--sha256', old_sha,
                                        '--source', old_source] and command[8] == '--marker' and
                       (command[9] == marker if marker else command[9].endswith('/reference-reader-required.json')),
                       'reader guard command differs')
    return parsed, rows


def replace_exact(engine, data, old, new, count, label):
    engine.require(data.count(old.encode()) == count, 'ambiguous ' + label)
    return data.replace(old.encode(), new.encode())


def build_changes(engine, files, process, old_sha, old_source, args, guard_commands):
    bypath = {row['path']: row for row in files}
    memory = engine.content(bypath[MEMORY_DROPIN])
    for token in (b'MemoryLimit=20G', b'GOMEMLIMIT=8GiB', b'GOGC=100',
                  b'GTRON_SNAPSHOT_MANIFEST_CACHE_BUDGET_BYTES=536870912'):
        engine.require(memory.count(token) == 1, 'memory drop-in contract differs')
    memory = replace_exact(engine, memory, process['exe'], str(BINARY), 1, 'final ExecStart binary')
    shared = engine.content(bypath[SHARED_DROPIN])
    shared = replace_exact(engine, shared, process['exe'], str(BINARY), 2, 'guard binary pins')
    shared = replace_exact(engine, shared, old_sha, args.binary_sha256, 2, 'guard checksum pins')
    shared = replace_exact(engine, shared, old_source, args.source_revision, 2, 'guard source pins')
    old_r1_marker = guard_commands[2][-1]
    shared = replace_exact(engine, shared, old_r1_marker, NEW_R1_MARKER, 1, 'R1 marker path')
    guard = engine.guard()
    shared_marker = (json.dumps(guard.reader_marker(BINARY, args.binary_sha256,
                                                     args.source_revision), sort_keys=True) + '\n').encode()
    reference_marker = r1_marker(BINARY, args.binary_sha256, args.source_revision)
    return [engine.changed(bypath[MEMORY_DROPIN], memory),
            engine.changed(bypath[SHARED_DROPIN], shared),
            engine.changed(bypath[GLOBAL_MARKER], shared_marker),
            root_item(engine, NEW_SHARED_MARKER, shared_marker),
            root_item(engine, NEW_R1_MARKER, reference_marker)]


def file_snapshot(engine, path, limit=4 << 20):
    if not os.path.lexists(path):
        return {'path': str(path), 'exists': False}
    data = engine.regular(path, limit, root=False); info = os.lstat(path)
    engine.require(stat.S_ISREG(info.st_mode) and not stat.S_ISLNK(info.st_mode),
                   'retained state is not a regular file: ' + str(path))
    return {'path': str(path), 'exists': True, 'sha256': engine.sha(data), 'size': len(data),
            'uid': info.st_uid, 'gid': info.st_gid, 'mode': stat.S_IMODE(info.st_mode),
            'dev': info.st_dev, 'inode': info.st_ino}


def tree_info(path):
    info = os.lstat(path)
    if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
        raise RuntimeError('unsafe directory: ' + str(path))
    return {'path': str(path), 'uid': info.st_uid, 'gid': info.st_gid,
            'mode': stat.S_IMODE(info.st_mode), 'dev': info.st_dev, 'inode': info.st_ino}


def expected_config(engine, record):
    result = copy.deepcopy(record['configuration'])
    start = result['ExecStart'][0]
    start['path'] = str(BINARY)
    old = record['process']['exe']
    start['argv[]'] = start['argv[]'].replace(old, str(BINARY), 1)
    for command in result['ExecStartPre']:
        if SHARED_GUARD in command['argv[]'] or R1_GUARD in command['argv[]']:
            command['argv[]'] = command['argv[]'].replace(old, str(BINARY)) \
                .replace(record['args']['old_binary_sha256'], record['args']['binary_sha256']) \
                .replace(record['args']['old_source_revision'], record['args']['source_revision'])
        if R1_GUARD in command['argv[]']:
            command['argv[]'] = command['argv[]'].replace(record['old_r1_marker'], NEW_R1_MARKER)
    return result


def unchanged_files(engine, record, target):
    old = {row['path']: row for row in record['files']}
    changes = {row['path']: row for row in record['changes']}
    for path in old:
        expected = changes[path] if target == 'new' and path in changes else old[path]
        engine.require(engine.saved(path) == expected, 'service configuration drift: ' + path)
    for path in (NEW_SHARED_MARKER, NEW_R1_MARKER):
        if target == 'old':
            engine.require(not os.path.lexists(path), 'candidate marker unexpectedly exists')
        else:
            engine.require(engine.saved(path) == changes[path], 'candidate marker differs')


def effective_template(process, environment, user):
    return {'argv': process['argv'],
            'environment': {key: environment[key] for key in sorted(MEMORY_ENV)},
            'user': user}


def write_effective_argv(engine, process, environment, user):
    value = effective_template(process, environment, user)
    engine.save('effective-argv.json', value)
    return engine.sha(engine.regular(TRANSACTION / 'effective-argv.json'))


def plan(engine, args):
    validate_candidate(engine, args)
    engine.require(not TRANSACTION.exists() and not os.path.lexists(SPACE_HOLD),
                   'existing transaction or disk-space hold')
    props = engine.show(); pid = int(props.get('MainPID', 0)); process = engine.process(pid)
    engine.require(props.get('ActiveState') == 'active' and
                   (pid, process['ticks']) == (args.old_pid, args.old_ticks),
                   'planned live process differs')
    engine.require(engine.sha(engine.regular(process['exe'], 512 << 20)) == args.old_binary_sha256,
                   'old binary identity differs')
    guard = engine.guard(); config = engine.configuration(guard, props)
    engine.require(not props.get('EnvironmentFiles') and config['User'] == 'java-tron' and
                   config['WorkingDirectory'] == str(MAIN) and
                   config['MemoryLimit'] == str(20 << 30), 'service identity or memory limit differs')
    environment = decoded_environment(engine, process['environ'])
    engine.require(all(environment.get(key) == value for key, value in MEMORY_ENV.items()),
                   'effective memory/cache environment differs')
    commands, rows = command_contract(engine, config, process, args.old_binary_sha256,
                                      args.old_source_revision)
    paths = [props['FragmentPath']] + shlex.split(props['DropInPaths'])
    engine.require(MEMORY_DROPIN in paths and SHARED_DROPIN in paths,
                   'required final memory/guard drop-ins missing')
    old_r1_marker = commands[2][-1]
    paths += [SPACE_GUARD, SPACE_CONFIG, SHARED_GUARD, R1_GUARD, GLOBAL_MARKER, old_r1_marker]
    files = [engine.saved(path) for path in dict.fromkeys(paths)]
    bypath = {row['path']: row for row in files}
    engine.require(json.loads(engine.content(bypath[GLOBAL_MARKER])) ==
                   guard.reader_marker(process['exe'], args.old_binary_sha256,
                                       args.old_source_revision), 'current shared marker differs')
    engine.require(json.loads(engine.content(bypath[old_r1_marker])) ==
                   json.loads(r1_marker(process['exe'], args.old_binary_sha256,
                                        args.old_source_revision)), 'current R1 marker differs')
    changes = build_changes(engine, files, process, args.old_binary_sha256,
                            args.old_source_revision, args, commands)
    parents = {}
    for row in changes:
        if row['path'] in (NEW_SHARED_MARKER, NEW_R1_MARKER):
            engine.require(not os.path.lexists(row['path']), 'fresh candidate marker already exists')
        fd, parents[row['path']] = engine.open_parent(row['path']); os.close(fd)
    tree = tree_info(TREE); datadir = tree_info(DATADIR); main = tree_info(MAIN)
    engine.require(tree['dev'] == datadir['dev'] == main['dev'], 'quarantine parent is cross-filesystem')
    account = pwd.getpwnam(config['User'])
    nodekey = file_snapshot(engine, NODEKEY, 1024)
    engine.require(nodekey['exists'] and nodekey['size'] == 64, 'stable 64-byte nodekey required')
    peers = file_snapshot(engine, PEERS, 4 << 20)
    quarantine_name = QUARANTINE_PREFIX + uuid.uuid4().hex
    quarantine = MAIN / quarantine_name
    engine.require(not os.path.lexists(quarantine), 'quarantine name collision')
    record = {'version': 1, 'args': vars(args), 'created_at': time.time(), 'process': process,
              'configuration': config, 'control_group': props.get('ControlGroup'),
              'systemctl_cat_b64': base64.b64encode(engine.run(['/bin/systemctl', 'cat',
                                                               'gtron.service'])).decode(),
              'files': files, 'changes': changes, 'parents': parents,
              'old_r1_marker': old_r1_marker, 'tree': tree, 'datadir': datadir, 'main': main,
              'nodekey': nodekey, 'peers_stopped_baseline': peers,
              'scratch_paths': {name: rows.get(name, [None])[0]
                                for name in ('snapshot.etl.tempdir', 'sync.etl.tempdir')},
              'service_uid': account.pw_uid, 'service_gid': account.pw_gid,
              'quarantine_name': quarantine_name, 'quarantine_path': str(quarantine)}
    engine.create_transaction(); engine.save('plan.json', record)
    effective_sha = write_effective_argv(engine, process, environment, config['User'])
    return {'planned': True, 'plan_sha256': engine.sha(engine.regular(TRANSACTION / 'plan.json')),
            'effective_argv_json': str(TRANSACTION / 'effective-argv.json'),
            'effective_argv_sha256': effective_sha, 'service_modified': False,
            'database_modified': False}


def load_record(engine, args):
    raw = engine.regular(TRANSACTION / 'plan.json')
    engine.require(engine.sha(raw) == args.plan_sha256, 'plan identity differs')
    record = json.loads(raw)
    engine.require(record.get('version') == 1, 'plan version differs')
    for key in ('source_revision', 'script_revision', 'binary_sha256', 'prepared_sha256'):
        engine.require(record['args'][key] == getattr(args, key), 'argument differs from plan: ' + key)
    return record


def validate_acceptance(engine, record, args):
    raw = engine.regular(ACCEPTANCE)
    engine.require(engine.sha(raw) == args.acceptance_sha256, 'acceptance summary identity differs')
    value = json.loads(raw)
    engine.require(value.get('accepted') is True and value.get('source_commit') == args.source_revision and
                   value.get('binary_sha256') == args.binary_sha256 and
                   value.get('prepared_sha256') == args.prepared_sha256 and
                   value.get('genesis') == GENESIS and value.get('prune_mode') == 'snap' and
                   value.get('prune_mode_persisted') is True and len(value.get('runs', ())) == 2 and
                   all(row.get('exit_code') == 0 and row.get('head') == 0 and
                       row.get('genesis') == GENESIS and
                       row.get('manifest_cache_budget') == 536870912 for row in value['runs']),
                   'fresh acceptance contract differs')
    environment = decoded_environment(engine, record['process']['environ'])
    engine.require(value.get('effective_template') ==
                   effective_template(record['process'], environment,
                                      record['configuration']['User']),
                   'acceptance used a different service template')


def save_phase(engine, phase):
    engine.save('journal.json', {'version': 1, 'phase': phase, 'at': time.time(),
                                'automatic_rollback': False, 'old_database_rollback': False})


def stop_planned_service(engine, record):
    props = engine.show(); pid = int(props.get('MainPID', 0))
    engine.require(pid == record['process']['pid'] and engine.process(pid) == record['process'] and
                   props.get('ControlGroup') == record['control_group'],
                   'refusing to stop changed process')
    engine.run(['/bin/systemctl', 'stop', 'gtron.service'], timeout=660)
    props = engine.show()
    engine.require(int(props.get('MainPID', 0)) == 0 and
                   props.get('ActiveState') in ('inactive', 'failed'), 'service did not stop normally')
    engine.require(not (Path('/proc') / str(record['process']['pid'])).exists(),
                   'old service PID still exists after stop')
    no_open_fds(TREE)


def create_quarantine_and_move(engine, record):
    quarantine = Path(record['quarantine_path'])
    engine.require(quarantine.parent == MAIN and quarantine.name == record['quarantine_name'] and
                   quarantine.name.startswith(QUARANTINE_PREFIX), 'invalid quarantine binding')
    os.mkdir(str(quarantine), 0o700)
    os.chown(str(quarantine), 0, 0); os.chmod(str(quarantine), 0o700)
    main_fd = os.open(str(MAIN), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    datadir_fd = os.open(str(DATADIR), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    quarantine_fd = os.open(str(quarantine), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        main_info, datadir_info, quarantine_info = os.fstat(main_fd), os.fstat(datadir_fd), os.fstat(quarantine_fd)
        engine.require((main_info.st_dev, main_info.st_ino) ==
                       (record['main']['dev'], record['main']['inode']) and
                       (datadir_info.st_dev, datadir_info.st_ino) ==
                       (record['datadir']['dev'], record['datadir']['inode']) and
                       quarantine_info.st_dev == record['tree']['dev'] and quarantine_info.st_uid == 0 and
                       stat.S_IMODE(quarantine_info.st_mode) == 0o700, 'directory identity changed')
        current = tree_info(TREE)
        engine.require(current == record['tree'] and not os.path.lexists(quarantine / QUARANTINE_CHILD),
                       'old derived tree identity changed or destination exists')
        os.rename('gtron', QUARANTINE_CHILD, src_dir_fd=datadir_fd, dst_dir_fd=quarantine_fd)
        os.fsync(datadir_fd); os.fsync(quarantine_fd); os.fsync(main_fd)
        os.mkdir('gtron', 0o755, dir_fd=datadir_fd)
        os.chown('gtron', record['service_uid'], record['service_gid'], dir_fd=datadir_fd)
        os.chmod('gtron', 0o755, dir_fd=datadir_fd)
        os.fsync(datadir_fd)
        moved = tree_info(quarantine / QUARANTINE_CHILD)
        fresh = tree_info(TREE)
        engine.require((moved['dev'], moved['inode']) == (record['tree']['dev'], record['tree']['inode']) and
                       fresh['dev'] == record['tree']['dev'] and fresh['inode'] != moved['inode'],
                       'atomic quarantine/fresh-root identity differs')
        return {'container': tree_info(quarantine), 'old_tree': moved, 'fresh_tree': fresh}
    finally:
        os.close(quarantine_fd); os.close(datadir_fd); os.close(main_fd)


def run_new_guards(engine, record):
    config = expected_config(engine, record)
    commands = config['ExecStartPre']
    engine.require(len(commands) == 3, 'mandatory guard count changed')
    for command in commands:
        engine.run(shlex.split(command['argv[]']))


def http_json(port, path, body=None):
    connection = http.client.HTTPConnection('127.0.0.1', port, timeout=5)
    try:
        payload = None if body is None else json.dumps(body)
        connection.request('GET' if body is None else 'POST', path, body=payload,
                           headers={'Content-Type': 'application/json'} if body is not None else {})
        response = connection.getresponse(); raw = response.read((4 << 20) + 1)
        if response.status != 200 or len(raw) > 4 << 20:
            raise RuntimeError('HTTP health response differs')
        return json.loads(raw)
    finally:
        connection.close()


def fresh_block_height(block):
    if not isinstance(block, dict):
        raise RuntimeError('fresh block response is not an object')
    block_id = block.get('blockID', '').lower()
    if re.fullmatch('[0-9a-f]{64}', block_id) is None:
        raise RuntimeError('fresh block ID is invalid')
    raw = block.get('block_header', {}).get('raw_data', {})
    if not isinstance(raw, dict):
        raise RuntimeError('fresh block header is invalid')
    if 'number' not in raw:
        if block_id == GENESIS:
            return 0
        raise RuntimeError('non-genesis block omitted its height')
    number = raw['number']
    if type(number) is not int or number < 0:
        raise RuntimeError('fresh block height is invalid')
    if number == 0 and block_id != GENESIS:
        raise RuntimeError('height-zero block is not mainnet genesis')
    return number


def fresh_observe():
    block = http_json(8090, '/wallet/getnowblock')
    payload = http_json(6062, '/debug/metrics')
    metrics = payload.get('metrics') if isinstance(payload, dict) else None
    if not isinstance(metrics, dict):
        raise RuntimeError('fresh metrics response is invalid')
    return fresh_block_height(block), metrics


def started_health(engine, record, timeout=240):
    expected = expected_config(engine, record); deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        props = engine.show(); pid = int(props.get('MainPID', 0))
        if props.get('ActiveState') != 'active' or not pid:
            time.sleep(2); continue
        process = engine.process(pid)
        engine.require(process['exe'] == str(BINARY) and
                       process['argv'] == [str(BINARY)] + record['process']['argv'][1:] and
                       engine.environment(process['environ']) == engine.environment(record['process']['environ']) and
                       engine.configuration(engine.guard(), props) == expected,
                       'fresh candidate process/config differs')
        try:
            block = http_json(8090, '/wallet/getblockbynum', {'num': 0})
            head, metrics = fresh_observe()
        except (OSError, ValueError, RuntimeError, http.client.HTTPException):
            time.sleep(2); continue
        engine.require(block.get('blockID', '').lower() == GENESIS and head >= 0 and
                       metrics.get('state/snapshot/cold/manifest_cache/budget_bytes', {}).get('value') ==
                       536870912, 'fresh genesis/cache health differs')
        return {'pid': pid, 'ticks': process['ticks'], 'head': head,
                'process_start': metrics['process/start/unix_nano']['value']}
    raise RuntimeError('fresh candidate health timed out')


def execute(engine, record, args):
    validate_candidate(engine, args); validate_acceptance(engine, record, args)
    engine.require(not os.path.lexists(SPACE_HOLD) and not os.path.lexists(record['quarantine_path']),
                   'space hold or quarantine collision')
    unchanged_files(engine, record, 'old')
    engine.require(engine.configuration(engine.guard(), engine.show()) == record['configuration'] and
                   engine.process(record['process']['pid']) == record['process'],
                   'service/config drift since plan')
    phase, started = 'stopping', False
    try:
        save_phase(engine, phase); stop_planned_service(engine, record)
        engine.require(file_snapshot(engine, NODEKEY, 1024) == record['nodekey'],
                       'nodekey changed before quarantine')
        stopped_peers = file_snapshot(engine, PEERS, 4 << 20)
        phase = 'quarantining-derived-tree'; save_phase(engine, phase)
        moved = create_quarantine_and_move(engine, record)
        engine.require(file_snapshot(engine, NODEKEY, 1024) == record['nodekey'] and
                       file_snapshot(engine, PEERS, 4 << 20) == stopped_peers,
                       'cutover modified retained node identity or peer cache')
        phase = 'installing-candidate'; save_phase(engine, phase)
        for row in record['changes']:
            engine.write_atomic(row, parent_pin=record['parents'][row['path']])
        engine.run(['/bin/systemctl', 'daemon-reload'])
        engine.require(engine.configuration(engine.guard(), engine.show()) == expected_config(engine, record),
                       'fresh candidate effective unit differs')
        unchanged_files(engine, record, 'new')
        engine.require(file_snapshot(engine, NODEKEY, 1024) == record['nodekey'] and
                       file_snapshot(engine, PEERS, 4 << 20) == stopped_peers,
                       'retained files changed before candidate start')
        phase = 'guard-preflight'; save_phase(engine, phase); run_new_guards(engine, record)
        phase = 'starting-fresh-candidate'; save_phase(engine, phase); started = True
        engine.run(['/bin/systemctl', 'start', 'gtron.service'], timeout=180)
        health = started_health(engine, record)
        engine.require(file_snapshot(engine, NODEKEY, 1024) == record['nodekey'],
                       'nodekey changed after fresh start')
        activated = dict(health, activated=True, fresh=True, genesis=GENESIS,
                         quarantine=moved, nodekey=record['nodekey'],
                         peer_cache_pre_start=stopped_peers, automatic_rollback=False,
                         old_database_rollback=False)
        engine.save('activated.json', activated)
        return activated
    except BaseException as error:
        previous = signal.pthread_sigmask(signal.SIG_BLOCK, SIGNALS)
        try:
            try:
                engine.save('failure.json', dict(engine.failure_details(error), phase=phase,
                                                 candidate_start_attempted=started,
                                                 automatic_rollback=False,
                                                 old_database_rollback=False))
            except BaseException:
                pass
            try:
                engine.run(['/bin/systemctl', 'stop', 'gtron.service'], timeout=660)
                props = engine.show()
                engine.require(int(props.get('MainPID', 0)) == 0 and
                               props.get('ActiveState') in ('inactive', 'failed'),
                               'failure stop did not complete')
                engine.save('stopped-after-failure.json', {'stopped': True, 'phase': phase, 'at': time.time()})
            except BaseException as stop_error:
                try:
                    engine.save('stop-failed.json', engine.failure_details(stop_error))
                except BaseException:
                    pass
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)
        raise


def scan_no_links_or_mounts(root, expected_dev, max_entries=2000000, timeout=900,
                            mountinfo_path=Path('/proc/self/mountinfo')):
    root = Path(root).resolve()
    mountinfo = Path(mountinfo_path).read_text().splitlines()
    for row in mountinfo:
        fields = row.split()
        if len(fields) < 5:
            raise RuntimeError('invalid mountinfo row')
        mountpoint = fields[4]
        for old, new in (('\\040', ' '), ('\\011', '\t'), ('\\012', '\n'), ('\\134', '\\')):
            mountpoint = mountpoint.replace(old, new)
        candidate = Path(mountpoint)
        if candidate == root or root in candidate.parents:
            raise RuntimeError('quarantine contains a mount point')
    deadline, count = time.monotonic() + timeout, 0
    stack = [root]
    while stack:
        if time.monotonic() > deadline:
            raise RuntimeError('quarantine metadata scan timed out')
        directory = stack.pop()
        with os.scandir(str(directory)) as entries:
            for entry in entries:
                count += 1
                if count > max_entries:
                    raise RuntimeError('quarantine metadata entry cap exceeded')
                info = entry.stat(follow_symlinks=False)
                if stat.S_ISLNK(info.st_mode) or info.st_dev != expected_dev:
                    raise RuntimeError('quarantine contains a symlink or child mount')
                if stat.S_ISDIR(info.st_mode):
                    stack.append(Path(entry.path))
    return count


def no_open_fds(root, max_fds=1000000, proc_root=Path('/proc')):
    prefix, checked = str(Path(root)) + '/', 0
    for proc in Path(proc_root).glob('[0-9]*/fd'):
        try:
            names = os.listdir(str(proc))
        except (FileNotFoundError, ProcessLookupError):
            continue
        except PermissionError:
            raise RuntimeError('cannot inspect process file descriptors: ' + str(proc))
        for name in names:
            checked += 1
            if checked > max_fds:
                raise RuntimeError('open-fd scan cap exceeded')
            try:
                target = os.readlink(str(proc / name))
            except (FileNotFoundError, ProcessLookupError):
                continue
            except PermissionError:
                raise RuntimeError('cannot inspect process file descriptor: ' + str(proc / name))
            if target == str(root) or target.startswith(prefix):
                raise RuntimeError('process still holds quarantine path: ' + str(proc.parent))
    return checked


def remove_tree_at(parent_fd, name, expected_dev, expected_inode=None):
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    directory = os.open(name, flags, dir_fd=parent_fd)
    try:
        info = os.fstat(directory)
        if info.st_dev != expected_dev or expected_inode is not None and info.st_ino != expected_inode:
            raise RuntimeError('delete root identity or filesystem differs')
        for child in os.listdir(directory):
            child_info = os.stat(child, dir_fd=directory, follow_symlinks=False)
            if stat.S_ISLNK(child_info.st_mode) or child_info.st_dev != expected_dev:
                raise RuntimeError('delete encountered symlink or child mount')
            if stat.S_ISDIR(child_info.st_mode):
                remove_tree_at(directory, child, expected_dev)
            elif stat.S_ISREG(child_info.st_mode):
                os.unlink(child, dir_fd=directory)
            else:
                raise RuntimeError('delete encountered unsupported file type')
        os.fsync(directory)
    finally:
        os.close(directory)
    os.rmdir(name, dir_fd=parent_fd)
    os.fsync(parent_fd)


def progress_samples(engine, record, activated):
    samples = []
    for index in range(2):
        props = engine.show(); pid = int(props.get('MainPID', 0)); process = engine.process(pid)
        head, metrics = fresh_observe()
        block = http_json(8090, '/wallet/getblockbynum', {'num': 0})
        engine.require(props.get('ActiveState') == 'active' and pid == activated['pid'] and
                       process['ticks'] == activated['ticks'] and process['exe'] == str(BINARY) and
                       engine.configuration(engine.guard(), props) == expected_config(engine, record) and
                       block.get('blockID', '').lower() == GENESIS and
                       metrics.get('state/snapshot/cold/manifest_cache/budget_bytes', {}).get('value') ==
                       536870912, 'fresh process/genesis/memory identity differs before deletion')
        validate_chain_argv(engine, process['argv'], str(BINARY))
        samples.append({'pid': pid, 'ticks': process['ticks'], 'head': head})
        if index == 0:
            time.sleep(5)
    engine.require(samples[1]['head'] >= samples[0]['head'] and samples[1]['head'] > 0,
                   'fresh head has not demonstrably advanced')
    return samples


def delete_quarantine(engine, record, args):
    validate_candidate(engine, args)
    raw = engine.regular(TRANSACTION / 'activated.json')
    engine.require(engine.sha(raw) == args.activated_sha256, 'activated evidence identity differs')
    activated = json.loads(raw)
    engine.require(activated.get('activated') is True and activated.get('fresh') is True,
                   'fresh activation evidence missing')
    quarantine = Path(record['quarantine_path']); child = quarantine / QUARANTINE_CHILD
    container = tree_info(quarantine); old = tree_info(child); fresh = tree_info(TREE)
    expected = activated['quarantine']
    engine.require(container == expected['container'] and old == expected['old_tree'] and
                   fresh == expected['fresh_tree'] and container['uid'] == 0 and
                   container['mode'] == 0o700 and container['dev'] == old['dev'] == fresh['dev'],
                   'quarantine/fresh directory binding differs')
    engine.require(not os.path.lexists(SPACE_HOLD) and
                   file_snapshot(engine, NODEKEY, 1024) == record['nodekey'],
                   'space hold or nodekey drift blocks deletion')
    unchanged_files(engine, record, 'new')
    samples = progress_samples(engine, record, activated)
    entries = scan_no_links_or_mounts(child, old['dev'])
    fds = no_open_fds(child)
    engine.save('delete-intent.json', {'at': time.time(), 'container': container,
                                      'old_tree': old, 'entries': entries,
                                      'open_fds_checked': fds, 'head_samples': samples})
    container_fd = os.open(str(quarantine), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        info = os.fstat(container_fd)
        engine.require((info.st_dev, info.st_ino) == (container['dev'], container['inode']),
                       'quarantine container moved before delete')
        remove_tree_at(container_fd, QUARANTINE_CHILD, old['dev'], old['inode'])
    finally:
        os.close(container_fd)
    main_fd = os.open(str(MAIN), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        current = tree_info(quarantine)
        engine.require(current == container and not os.listdir(str(quarantine)),
                       'quarantine container differs after old-tree removal')
        os.rmdir(record['quarantine_name'], dir_fd=main_fd); os.fsync(main_fd)
    finally:
        os.close(main_fd)
    props = engine.show(); pid = int(props.get('MainPID', 0)); process = engine.process(pid)
    post = {'fresh_tree': tree_info(TREE), 'nodekey': file_snapshot(engine, NODEKEY, 1024),
            'pid': pid, 'ticks': process['ticks']}
    engine.require(post['fresh_tree'] == expected['fresh_tree'] and post['nodekey'] == record['nodekey'] and
                   props.get('ActiveState') == 'active' and pid == activated['pid'] and
                   process['ticks'] == activated['ticks'] and process['exe'] == str(BINARY) and
                   engine.configuration(engine.guard(), props) == expected_config(engine, record),
                   'fresh service identity changed during quarantine deletion')
    result = {'deleted': True, 'at': time.time(), 'entries': entries,
              'head_samples': samples, 'quarantine_path': str(quarantine), 'post_delete': post}
    engine.save('deleted.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('plan', 'execute', 'delete-quarantine'))
    for name in ('source-revision', 'script-revision', 'old-source-revision',
                 'binary-sha256', 'prepared-sha256', 'old-binary-sha256',
                 'acceptance-sha256', 'plan-sha256', 'activated-sha256'):
        parser.add_argument('--' + name)
    parser.add_argument('--old-pid', type=int); parser.add_argument('--old-ticks', type=int)
    args = parser.parse_args()
    if os.geteuid() != 0 or sys.platform != 'linux':
        raise RuntimeError('root Linux execution required')
    os.umask(0o077)
    for sig in SIGNALS:
        signal.signal(sig, lambda number, frame: (_ for _ in ()).throw(Interrupted('operator signal')))
    engine = load_engine()
    lock = os.open('/run/lock/gtron-fresh-mainnet-cutover.lock',
                   os.O_CREAT | os.O_NOFOLLOW | os.O_RDWR, 0o600)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.mode == 'plan':
            for name, length in (('old_source_revision', 40), ('old_binary_sha256', 64)):
                engine.require(re.fullmatch('[0-9a-f]{%d}' % length, getattr(args, name) or ''),
                               'exact old identity required')
            engine.require(args.old_pid and args.old_ticks, 'exact old process identity required')
            result = plan(engine, args)
        else:
            engine.require(re.fullmatch('[0-9a-f]{64}', args.plan_sha256 or ''), 'exact plan SHA required')
            record = load_record(engine, args)
            if args.mode == 'execute':
                engine.require(re.fullmatch('[0-9a-f]{64}', args.acceptance_sha256 or ''),
                               'exact acceptance SHA required')
                result = execute(engine, record, args)
            else:
                engine.require(re.fullmatch('[0-9a-f]{64}', args.activated_sha256 or ''),
                               'exact activated SHA required')
                result = delete_quarantine(engine, record, args)
        print(json.dumps(result, sort_keys=True), flush=True)
    finally:
        os.close(lock)


if __name__ == '__main__':
    try:
        main()
    except BaseException as error:
        if isinstance(error, SystemExit):
            raise
        print(json.dumps({'complete': False, 'error': str(error)[:8192]}), file=sys.stderr, flush=True)
        sys.exit(1)
