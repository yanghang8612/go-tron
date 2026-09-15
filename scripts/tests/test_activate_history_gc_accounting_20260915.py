import argparse
import ast
import base64
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import stat
import subprocess
import types
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / 'scripts/activate_history_gc_accounting_20260915.py'
spec = importlib.util.spec_from_file_location('activation_gc_tested', str(PATH))
s = importlib.util.module_from_spec(spec); spec.loader.exec_module(s)
PARENT = subprocess.check_output(['git', 'show', s.PARENT_REVISION + ':' + s.PARENT_PATH], cwd=str(ROOT))
TEST_PARENT = subprocess.check_output(['git', 'show', s.PARENT_REVISION + ':scripts/tests/test_activate_history_shared_read_20260915.py'], cwd=str(ROOT))


def load():
    with mock.patch.object(s.subprocess, 'check_output', return_value=PARENT) as run:
        engine = s.load_engine()
    run.assert_called_once()
    return engine


def fixture(engine):
    # Reuse the frozen real transaction fault driver, with this generation's
    # unchanged R4/cache argv and ten permanent predecessors.
    legacy = types.ModuleType('old_gc_transaction_tests')
    legacy.__file__ = str(ROOT / 'scripts/tests/test_activate_history_shared_read_20260915.py')
    exec(compile(TEST_PARENT, legacy.__file__, 'exec'), legacy.__dict__)
    old_fixture = legacy.fixture
    legacy.s = engine
    def current_fixture():
        # The old fixture has no R4 flags; temporarily allow its original plan
        # construction, then reconstruct the exact current fixture below.
        with mock.patch.object(engine, 'make_plan', return_value=[]): guard, record = old_fixture()
        argv = record['process']['argv'] + list(s.PRESERVED_FLAGS)
        record['process']['argv'] = argv
        record['files'][0] = legacy.item(engine.EXEC, ('[Service]\nExecStart=\nExecStart=' + ' '.join(argv) + '\nEnvironment=KEEP=1\n').encode())
        record['configuration']['ExecStart'][0]['argv[]'] = ' '.join(argv)
        record['ancestors'].append(legacy.item(str(s.CURRENT_RELEASE / 'reader-required.json'), b'current permanent'))
        record['process_start'] = s.CURRENT_START
        record['plan'] = engine.make_plan(record['files'], guard, 'b'*64, s.SOURCE_REVISION)
        record['args']['source_revision'] = s.SOURCE_REVISION
        return guard, record
    legacy.fixture = current_fixture
    return legacy


def events(packages, tests):
    return b'\n'.join(json.dumps(row).encode() for row in
                     ([dict(Action='pass', Package='github.com/tronprotocol/go-tron/' + p, Test=t) for p, t in tests] +
                      [dict(Action='pass', Package='github.com/tronprotocol/go-tron/' + p) for p in packages]))


class ActivateGCTests(unittest.TestCase):
    def test_pinned_isolated_load_preserves_generic_transaction(self):
        a, b = load(), load()
        self.assertIsNot(a, b); self.assertEqual(a.__file__, str(PATH))
        a.FLAGS.append('sentinel'); self.assertEqual(b.FLAGS, [])
        self.assertEqual(hashlib.sha256(PARENT).hexdigest(), s.PARENT_SHA)
        self.assertEqual(hashlib.sha256(TEST_PARENT).hexdigest(), 'dc0d78abeb530c2984544b01c0cc52a142732d03095fa970f084ec063c947a36')
        self.assertEqual(b.OLD_SOURCE, s.CURRENT_SOURCE)
        self.assertEqual(b.OLD_PID, 28539); self.assertEqual(b.OLD_TICKS, 4518622532)
        original = types.ModuleType('original'); original.__file__ = str(PATH)
        exec(compile(PARENT, s.PARENT_PATH, 'exec'), original.__dict__)
        for name in ('transact', 'restore', 'stop', 'write_atomic', 'open_parent', 'create_transaction', 'check_files'):
            self.assertEqual(getattr(b, name).__code__.co_code, getattr(original, name).__code__.co_code)
        with mock.patch.object(s.subprocess, 'check_output', return_value=b'raise AssertionError()'):
            with self.assertRaisesRegex(RuntimeError, 'bytes differ'): s.load_engine()

    def test_only_executable_changes_and_space_guard_budget_flags_survive(self):
        e = load(); legacy = fixture(e); guard, record = legacy.fixture()
        before, after = e.content(record['files'][0]), e.content(record['plan'][0])
        self.assertEqual(after.replace(str(e.BINARY).encode(), e.OLD_BINARY.encode()), before)
        for flag in s.PRESERVED_FLAGS: self.assertEqual(after.count(flag.encode()), 1)
        self.assertEqual(e.expected_config(record, True)['ExecStartPre'][0], record['configuration']['ExecStartPre'][0])
        self.assertEqual(len(record['ancestors']), 10)
        self.assertEqual(json.loads(e.content(record['plan'][2])), guard.reader_marker(e.BINARY, 'b'*64, s.SOURCE_REVISION))
        for bad in (before + b' --history.shared-read-workers=4', before.replace(b'workers=4', b'workers=2'),
                    before + b' --history.backlog-high=1000000'):
            files = copy.deepcopy(record['files']); files[0] = legacy.item(e.EXEC, bad)
            with self.assertRaises(RuntimeError): e.make_plan(files, guard, 'b'*64, s.SOURCE_REVISION)

    def test_all_partial_failures_restore_current_reader_and_ten_ancestors(self):
        e = load(); legacy = fixture(e); driver = legacy.ActivationTests()
        failures = ['stop'] + ['write:' + path for path in (e.EXEC, e.DROPIN, e.MARKER, str(e.RELEASE / 'reader-required.json'))]
        failures += ['daemon-reload', 'start', 'health:new', 'save:activated.json']
        for failure in failures:
            with self.subTest(failure=failure):
                record, before, after, trace = driver.transaction_case(failure)
                self.assertEqual(len(record['ancestors']), 10)
                for path, data in before.items(): self.assertEqual(after[path], data)
                self.assertIn('health:old', trace); self.assertIn('save:rollback.json', trace)
        record, before, after, trace = driver.transaction_case(manual=True)
        self.assertNotIn('validate', trace)
        for path, data in before.items(): self.assertEqual(after[path], data)
        record, before, after, trace = driver.transaction_case()
        self.assertIn(str(e.RELEASE / 'reader-required.json'), after)
        for row in record['ancestors']: self.assertEqual(after[row['path']], row)

    def test_candidate_health_metric_and_identity_with_live_resource_changes(self):
        e = load(); legacy = fixture(e); guard, record = legacy.fixture()
        identity = dict(pid=777, ticks=888, process_start=s.CURRENT_START + 100, head=101)
        proc = dict(record['process'], pid=777, ticks=888, exe=str(e.BINARY), argv=[str(e.BINARY)] + record['process']['argv'][1:])
        before = dict(ActiveState='active', MainPID='777', ControlGroup=record['control_group'], CPUUsageNSec='10', MemoryCurrent='100')
        after = dict(before, CPUUsageNSec='20', MemoryCurrent='110')
        for value, changed in ((0, False), (13, False), (None, False), (-1, False), (True, False), (0, True)):
            with self.subTest(value=value, changed=changed):
                metrics = {s.GC_METRIC: {'value': value}, 'process/start/unix_nano': {'value': identity['process_start']}}
                with mock.patch.object(e, 'show', side_effect=[before, after]), \
                     mock.patch.object(e, 'process', side_effect=[proc, dict(proc, ticks=999) if changed else proc]), \
                     mock.patch.object(e, 'observe', return_value=(102, metrics)), mock.patch.object(e, 'guard', return_value=guard), \
                     mock.patch.object(e, 'configuration', return_value=e.expected_config(record, True)):
                    original = mock.Mock(return_value=identity)
                    if type(value) is not int or value < 0 or changed:
                        with self.assertRaises(RuntimeError): s.healthy(e, original, record, True)
                    else: self.assertEqual(s.healthy(e, original, record, True), identity)
        with mock.patch.object(e, 'observe') as observe:
            self.assertEqual(s.healthy(e, lambda *a: identity, record, False), identity)
            observe.assert_not_called()

    def candidate(self, mutation=None):
        e = load(); data = {}; common = ['/data/go/bin/go', 'test', '-json', '-p', '2', '-tags', 'sapling']
        packages = ('cmd/gtron', 'core/rawdb', 'core/rawdb/pebbledb', 'core/state/snapshots')
        focused = common + ['./' + p for p in packages] + ['-run', 'TestHistoryIndependentGC', '-count=1', '-timeout=300s']
        full = common + ['./core/state/snapshots', './cmd/gtron', '-count=1', '-timeout=300s']
        env = {'CGO_ENABLED': '1', 'GOMAXPROCS': '2', 'GOTOOLCHAIN': 'local', 'GOFLAGS': '-mod=readonly', 'GOENV': 'off', 'GOWORK': 'off'}
        source = b'package candidate\n'; blob = hashlib.sha1(b'blob ' + str(len(source)).encode() + b'\0' + source).hexdigest()
        prepared = dict(prepared=True, source_commit=s.SOURCE_REVISION, binary=str(e.BINARY), binary_sha256=e.sha(b'binary'),
                        go_version='go version go1.25.5 linux/amd64', build_environment=env,
                        manifest={'source.go': dict(git_blob=blob, mode=0o644, sha256=e.sha(source))}, native_commands=[],
                        native_focused_tests={'required_tests': [list(p) for p in s.SNAPSHOT_TESTS]})
        def command(name, argv, content):
            data[str(s.RELEASE / name)] = content
            return dict(log=name, argv=argv, cwd=str(s.RELEASE / 'source'), returncode=0, log_sha256=e.sha(content))
        prepared['native_commands'] = [command('native-focused-tests.jsonl', focused, events(packages, s.SNAPSHOT_TESTS)),
                                       command('native-full-tests.jsonl', full, events(('cmd/gtron', 'core/state/snapshots'), s.SNAPSHOT_TESTS))]
        prepared['native_commands'] += [command('phase%d.log' % n, ['native', str(n)], b'ok') for n in range(6)]
        pattern = '^(' + '|'.join(n for p, n in s.PRUNING_TESTS) + ')$'
        proof = dict(complete=True, source_commit=s.SOURCE_REVISION, binary=str(e.BINARY), binary_sha256=prepared['binary_sha256'],
                     go_version=prepared['go_version'], build_environment=env.copy(), native_commands=[
                         command('native-gc-pruning-focused.jsonl', common + ['./core/state/pruning', '-run', pattern, '-count=1', '-timeout=300s'], events(('core/state/pruning',), s.PRUNING_TESTS)),
                         command('native-gc-pruning-full.jsonl', common + ['./core/state/pruning', '-count=1', '-timeout=300s'], events(('core/state/pruning',), s.PRUNING_TESTS))])
        if mutation: mutation(prepared, proof, data)
        # Rehash mutated logs to exercise semantic verification beyond SHA pins.
        for row in prepared['native_commands'] + proof['native_commands']:
            row['log_sha256'] = e.sha(data[str(s.RELEASE / row['log'])])
        data[str(s.RELEASE / 'prepared.json')] = json.dumps(prepared).encode()
        prepared_sha = e.sha(data[str(s.RELEASE / 'prepared.json')]); proof['prepared_sha256'] = prepared_sha
        data[str(s.RELEASE / 'gc-accounting-native.json')] = json.dumps(proof).encode()
        data[str(e.BINARY)] = b'binary'; data[str(s.RELEASE / 'source/source.go')] = source
        data[str(PATH)] = PATH.read_bytes()
        args = argparse.Namespace(source_revision=s.SOURCE_REVISION, script_revision='a'*40, binary_sha=prepared['binary_sha256'],
                                  prepared_sha=prepared_sha, native_sha=e.sha(data[str(s.RELEASE / 'gc-accounting-native.json')]))
        def run(argv):
            if argv[1] == 'rev-parse': return (s.SOURCE_REVISION + '\n').encode()
            if argv[1] == 'show': return PATH.read_bytes()
            if argv[1] == 'ls-tree': return ('100644 blob ' + blob + '\tsource.go\0').encode()
            raise AssertionError(argv)
        return e, args, data, run

    def validate(self, mutation=None):
        e, args, data, run = self.candidate(mutation)
        info = lambda path: types.SimpleNamespace(st_uid=0, st_mode=stat.S_IFREG | (0o644 if str(path).endswith('source.go') else 0o755))
        with mock.patch.object(s, 'predecessor'), mock.patch.object(e, 'regular', side_effect=lambda p, *a: data[str(p)]), \
             mock.patch.object(e, 'run', side_effect=run), mock.patch.object(Path, 'is_symlink', return_value=False):
            # autospec retains the Path receiver for accurate source mode checks.
            with mock.patch.object(Path, 'stat', autospec=True, side_effect=lambda p, *a, **k: info(p)):
                return s.validate_candidate(e, args)

    def test_candidate_accepts_exact_native_proofs_without_old_abba(self):
        self.assertTrue(self.validate()['prepared'])

    def test_native_critical_logs_and_full_scope_cannot_be_forged_by_summary(self):
        cases = [
            lambda p, n, d: n.update(complete=False),
            lambda p, n, d: n.update(source_commit='f'*40),
            lambda p, n, d: n['native_commands'].pop(),
            lambda p, n, d: n['native_commands'][1]['argv'].insert(-2, '-run=TestOnly'),
            lambda p, n, d: p['native_commands'][1]['argv'].insert(-2, '-run=TestOnly'),
            lambda p, n, d: p['native_commands'].append(copy.deepcopy(p['native_commands'][0])),
            lambda p, n, d: p['build_environment'].update(GOMAXPROCS='4'),
            lambda p, n, d: d.update({str(s.RELEASE / 'native-gc-pruning-full.jsonl'): events(('core/state/pruning',), s.PRUNING_TESTS[:-1])}),
            lambda p, n, d: d.update({str(s.RELEASE / 'native-focused-tests.jsonl'): events(('cmd/gtron', 'core/rawdb', 'core/rawdb/pebbledb', 'core/state/snapshots'), s.SNAPSHOT_TESTS[:-1])}),
            lambda p, n, d: d.update({str(s.RELEASE / 'native-gc-pruning-focused.jsonl'): b'{"Action":"fail"}'}),
            lambda p, n, d: d.update({str(s.RELEASE / 'native-full-tests.jsonl'): b'{broken'}),
        ]
        for index, mutate in enumerate(cases):
            with self.subTest(case=index):
                with self.assertRaises(RuntimeError): self.validate(mutate)

    def test_source_missing_native_pin_and_predecessor_identity_fail_closed(self):
        e, args, data, run = self.candidate()
        for change in ({'source_revision': 'f'*40}, {'native_sha': None}, {'prepared_sha': 'short'}):
            with mock.patch.object(e, 'run') as command:
                with self.assertRaises(RuntimeError): s.validate_candidate(e, argparse.Namespace(**dict(vars(args), **change)))
                command.assert_not_called()
        with mock.patch.object(e, 'regular', return_value=b'{}'):
            with self.assertRaisesRegex(RuntimeError, 'previous'): s.predecessor(e)

    def test_admission_requires_ten_saved_ancestors_current_process_and_fresh_metric(self):
        for count, start in ((10, s.CURRENT_START), (9, s.CURRENT_START), (10, s.CURRENT_START - 1)):
            with self.subTest(ancestors=count, start=start):
                e = load(); legacy = fixture(e); guard, record = legacy.fixture()
                fragment = '/etc/systemd/system/gtron.service'
                files = {row['path']: row for row in record['files'] + record['ancestors']}
                files[fragment] = legacy.item(fragment, b'[Service]\nUser=java-tron\n')
                current_prepared = str(s.CURRENT_RELEASE / 'prepared.json')
                files[current_prepared] = legacy.item(current_prepared, b'current prepared')
                props = dict(MainPID=str(s.CURRENT_PID), ActiveState='active', ControlGroup=record['control_group'],
                             FragmentPath=fragment, DropInPaths=e.EXEC + ' ' + e.DROPIN, ExecStart='pinned command')
                show = '\n'.join(key + '=' + value for key, value in props.items()).encode()
                live = dict(files=[files[path] for path in (e.EXEC, e.DROPIN, e.GUARD, e.MARKER, current_prepared)],
                            ancestor_markers=record['ancestors'][:count], systemctl_show_b64=base64.b64encode(show).decode(),
                            systemctl_cat_b64=base64.b64encode(b'whole unit').decode(),
                            proc={'cmdline': base64.b64encode(('\0'.join(record['process']['argv']) + '\0').encode()).decode(),
                                  'environ': record['process']['environ']})
                space = [files[e.SPACE_CONFIG], files[e.SPACE_GUARD]]
                data = {str(s.EVIDENCE / 'live-before.json'): json.dumps(live).encode(),
                        e.SPACE_EVIDENCE: json.dumps(space).encode(), e.OLD_BINARY: b'old binary'}
                live_sha, space_sha = e.sha(data[str(s.EVIDENCE / 'live-before.json')]), e.sha(data[e.SPACE_EVIDENCE])
                args = argparse.Namespace(live_evidence=str(s.EVIDENCE / 'live-before.json'), live_evidence_sha=live_sha,
                                          space_evidence_sha=space_sha, binary_sha='b'*64, source_revision=s.SOURCE_REVISION)
                def save(name, value): data[str(e.TRANSACTION / name)] = json.dumps(value).encode()
                real_sha = e.sha
                with mock.patch.object(s, 'LIVE_SHA', live_sha), mock.patch.object(s, 'SPACE_SHA', space_sha), \
                     mock.patch.object(e, 'sha', side_effect=lambda value: s.CURRENT_SHA if value == b'old binary' else real_sha(value)), \
                     mock.patch.object(e, 'validate_candidate'), mock.patch.object(e, 'regular', side_effect=lambda p, *a: data[str(p)]), \
                     mock.patch.object(e, 'saved', side_effect=lambda p: files[str(p)]), mock.patch.object(e, 'guard', return_value=guard), \
                     mock.patch.object(guard, 'check_command'), mock.patch.object(e, 'show', return_value=props), \
                     mock.patch.object(e, 'configuration', return_value=record['configuration']), \
                     mock.patch.object(e, 'process', return_value=record['process']), mock.patch.object(e, 'run', return_value=b'whole unit'), \
                     mock.patch.object(e, 'open_parent', side_effect=lambda p: (os.open(os.devnull, os.O_RDONLY), {})), \
                     mock.patch.object(e, 'observe', return_value=(100, {'process/start/unix_nano': {'value': start}})), \
                     mock.patch.object(e, 'healthy'), mock.patch.object(e, 'create_transaction') as create, \
                     mock.patch.object(e, 'save', side_effect=save), mock.patch.object(e.os.path, 'lexists', return_value=False):
                    if count == 10 and start == s.CURRENT_START:
                        result = s.admit(e, args)
                        self.assertTrue(result['admitted']); create.assert_called_once()
                        saved = json.loads(data[str(e.TRANSACTION / 'admission.json')])
                        self.assertEqual(len(saved['ancestors']), 10)
                        self.assertEqual(saved['process_start'], s.CURRENT_START)
                        self.assertEqual(saved['plan'][0]['data_b64'], record['plan'][0]['data_b64'])
                    else:
                        with self.assertRaises(RuntimeError): s.admit(e, args)
                        create.assert_not_called()

    def test_python36_syntax(self):
        for path in (PATH, Path(__file__)): ast.parse(path.read_text(), feature_version=(3, 6))


if __name__ == '__main__': unittest.main()
