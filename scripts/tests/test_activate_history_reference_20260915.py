import argparse
import ast
import base64
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import types
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / 'scripts/activate_history_reference_20260915.py'
spec = importlib.util.spec_from_file_location('activate_reference_tested', str(PATH))
s = importlib.util.module_from_spec(spec); spec.loader.exec_module(s)
PARENT = subprocess.check_output(['git', 'show', s.PARENT + ':' + s.PARENT_PATH], cwd=str(ROOT))
GUARD = (ROOT / 'scripts/history_shared_reader_guard.py').read_bytes()


def load():
    with mock.patch.object(s, 'child_run', return_value=PARENT): return s.load_engine()


def serialized(commands):
    return ' ; '.join('{ path=%s ; argv[]=%s ; ignore_errors=%s ; start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; code=(null) ; status=0 }' %
                      (c['path'], c['argv[]'], c['ignore_errors']) for c in commands)


def fixture(e):
    g = e.module(GUARD, e.GUARD_SHA, 'fixture_guard')
    args = argparse.Namespace(source_revision='c' * 40, script_revision='d' * 40, binary_sha='b' * 64,
                              prepared_sha='a' * 64, old_pid=4919, old_ticks=1234, migration_trios=1, mode='plan', plan_sha=None)
    argv = [s.OLD_BINARY, '--history.shared-read-workers=4', '--history.shared-chunk-cache=true',
            '--history.cross-block-dedup=true', '--datadir', s.DATADIR, '--db.cache', '8192', '--http.port', '8090']
    pre = ['/usr/bin/python3', e.GUARD, '--binary', s.OLD_BINARY, '--sha256', s.OLD_SHA, '--source', s.OLD_SOURCE, '--marker', e.MARKER]
    files = [s.item(e, e.EXEC, ('[Service]\nExecStart=\nExecStart=' + ' '.join(argv) + '\nEnvironment=SECRET=unchanged\n').encode()),
             s.item(e, e.DROPIN, ('[Service]\nExecStartPre=' + ' '.join(pre) + '\n').encode()),
             s.item(e, e.MARKER, json.dumps(g.reader_marker(s.OLD_BINARY, s.OLD_SHA, s.OLD_SOURCE)).encode()),
             s.item(e, e.GUARD, GUARD, 0o755), s.item(e, e.SPACE_GUARD, b'space guard', 0o755),
             s.item(e, e.SPACE_CONFIG, b'{"start_bytes":123}'),
             s.item(e, '/data/gtron/releases/prior/reader-required.json', b'permanent prior identity')]
    cmd = lambda argv: {'path': argv[0], 'argv[]': ' '.join(argv), 'ignore_errors': 'no'}
    config = {k: '' for k in e.CONFIG_KEYS}
    config.update(ExecStart=[cmd(argv)], ExecStartPre=[cmd(['/usr/bin/python3', e.SPACE_GUARD, 'check']), cmd(pre)], ExecStartPost=[],
                  User='java-tron', WorkingDirectory='/data/gtron/main', MemoryLimit='10737418240', MemorySoftLimit='8589934592')
    with mock.patch.object(e, 'guard', return_value=g): changes = s.make_plan(e, files, args)
    record = dict(version=1, args=vars(args), files=files, plan=changes, parents={f['path']: {} for f in changes}, configuration=config,
                  process=dict(pid=4919, ticks=1234, exe=s.OLD_BINARY, argv=argv,
                               environ=base64.b64encode(b'GOMEMLIMIT=10GiB\0SECRET=private\0').decode()),
                  process_start=1000, head=100, control_group='/system.slice/gtron.service', service_uid=1003, service_gid=1003)
    return g, record


class ReferenceActivationTests(unittest.TestCase):
    def test_pinned_single_helper_isolation_no_old_mutation_workflow(self):
        a, b = load(), load()
        self.assertIsNot(a, b); self.assertEqual(hashlib.sha256(PARENT).hexdigest(), s.PARENT_SHA)
        a.FLAGS.append('x'); self.assertEqual(b.FLAGS, [s.FLAG])
        for name in ('restore', 'transact', 'admit', 'validate_candidate', 'main'): self.assertFalse(hasattr(a, name))
        with mock.patch.object(s, 'child_run', return_value=b'raise AssertionError()'):
            with self.assertRaisesRegex(RuntimeError, 'helper differs'): s.load_engine()
        ast.parse(PATH.read_text(), feature_version=(3, 6))
        ast.parse(s.R1_GUARD_SOURCE, feature_version=(3, 6))

    def test_plan_changes_exact_reader_and_preserves_flags_limits_guards_and_ancestors(self):
        e = load(); g, r = fixture(e)
        by = {f['path']: f for f in r['plan']}; before = {f['path']: f for f in r['files']}
        self.assertEqual(e.content(by[e.EXEC]).replace((str(s.BINARY) + ' ' + s.FLAG).encode(), s.OLD_BINARY.encode()), e.content(before[e.EXEC]))
        new = s.expected_config(e, r, True)
        self.assertEqual(new['ExecStartPre'][0], r['configuration']['ExecStartPre'][0])
        self.assertEqual(len(new['ExecStartPre']), 3)
        self.assertEqual(new['MemoryLimit'], r['configuration']['MemoryLimit'])
        self.assertEqual(new['MemorySoftLimit'], r['configuration']['MemorySoftLimit'])
        self.assertEqual(json.loads(e.content(by[e.MARKER])), g.reader_marker(s.BINARY, 'b'*64, 'c'*40))
        self.assertEqual(json.loads(e.content(by[s.R1_MARKER])), s.marker(argparse.Namespace(**r['args'])))
        self.assertNotIn('/data/gtron/releases/prior/reader-required.json', by)
        with mock.patch.object(e, 'guard', return_value=g):
            for suffix in (b' --history.reference-container=false', b' --history.backlog-high=999', b' --history.shared-read-workers=4'):
                bad = copy.deepcopy(r['files']); bad[0] = e.changed(bad[0], e.content(bad[0]) + suffix)
                with self.assertRaises(RuntimeError): s.make_plan(e, bad, argparse.Namespace(**r['args']))

    def transaction(self, fail=None):
        e = load(); g, r = fixture(e); actual = {f['path']: copy.deepcopy(f) for f in r['files']}
        events, saved = [], {}; failure = [fail]
        def event(name):
            events.append(name)
            if failure and failure[0] == name: failure.pop(); raise s.Interrupted(name)
        def write(f, parent_pin=None): event('write:' + f['path']); actual[f['path']] = copy.deepcopy(f)
        def run(argv, timeout=60): event('run:' + ' '.join(argv)); return b''
        def save(name, value): event('save:' + name); saved[name] = value
        def stop(record): event('stop')
        def migrate(engine, record):
            event('migration')
            # Every permanent guard/marker must already be on disk.
            for f in r['plan']: self.assertEqual(actual[f['path']], f)
            self.assertEqual(sum(x.startswith('run:/usr/bin/python3') for x in events), 3)
            return {'complete': True}
        def config(guard, props):
            return s.expected_config(e, r, actual[e.EXEC] == next(f for f in r['plan'] if f['path'] == e.EXEC))
        with mock.patch.object(s, 'validate_candidate', side_effect=lambda *x: event('validate')), \
             mock.patch.object(e, 'guard', return_value=g), mock.patch.object(e, 'show', return_value={}), \
             mock.patch.object(e, 'process', return_value=r['process']), mock.patch.object(e, 'configuration', side_effect=config), \
             mock.patch.object(e, 'saved', side_effect=lambda p: copy.deepcopy(actual[str(p)])), \
             mock.patch.object(e.os.path, 'lexists', side_effect=lambda p: str(p) in actual), \
             mock.patch.object(e, 'open_parent', side_effect=lambda *x: (os.open(os.devnull, os.O_RDONLY), {})), \
             mock.patch.object(e, 'write_atomic', side_effect=write), mock.patch.object(e, 'stop', side_effect=stop), \
             mock.patch.object(e, 'run', side_effect=run), mock.patch.object(e, 'save', side_effect=save), \
             mock.patch.object(s, 'migrate', side_effect=migrate), \
             mock.patch.object(e, 'healthy', side_effect=lambda *x: event('health') or {'pid': 6000}):
            if fail:
                with self.assertRaises(s.Interrupted): s.execute(e, r)
            else: self.assertTrue(s.execute(e, r)['activated'])
        return r, actual, events, saved

    def test_success_orders_durable_guards_before_migration_and_start(self):
        r, actual, events, evidence = self.transaction()
        self.assertLess(events.index('migration'), events.index('run:/bin/systemctl start gtron.service'))
        self.assertIn('activated.json', evidence)
        for f in r['files']:
            if f['path'] not in {x['path'] for x in r['plan']}: self.assertEqual(actual[f['path']], f)

    def test_every_stopped_phase_failure_stays_stopped_without_old_rollback_or_deletion(self):
        e = load(); unused, r = fixture(e)
        failures = ['stop'] + ['write:' + f['path'] for f in r['plan']]
        failures += ['run:/bin/systemctl daemon-reload', 'migration', 'run:/bin/systemctl start gtron.service', 'health', 'save:activated.json']
        for failure in failures:
            with self.subTest(failure=failure):
                r, actual, events, evidence = self.transaction(failure)
                self.assertEqual(events[-1], 'save:stopped-after-failure.json')
                self.assertIn('failure.json', evidence)
                self.assertTrue(evidence['failure.json']['no_old_reader_rollback'])
                self.assertNotIn('rollback.json', evidence)
                self.assertTrue(all('rm ' not in x and 'delete' not in x for x in events))
                if failure in ('migration', 'run:/bin/systemctl start gtron.service', 'health', 'save:activated.json'):
                    for f in r['plan']: self.assertEqual(actual[f['path']], f)

    def test_preflight_failure_does_not_stop_current_service(self):
        e = load(); unused, r = fixture(e)
        with mock.patch.object(s, 'validate_candidate', side_effect=RuntimeError('bad candidate')), mock.patch.object(e, 'stop') as stop:
            with self.assertRaisesRegex(RuntimeError, 'bad candidate'): s.execute(e, r)
            stop.assert_not_called()

    def test_inherited_stop_accepts_new_pid_with_reference_flag_and_preserved_r4_env(self):
        e = load(); unused, r = fixture(e)
        new = dict(r['process'], pid=6000, ticks=5678, exe=str(s.BINARY),
                   argv=[str(s.BINARY), s.FLAG] + r['process']['argv'][1:])
        for bad in (None, 'argv', 'environment', 'binary', 'cgroup'):
            proc = copy.deepcopy(new); props = {'MainPID': '6000', 'ActiveState': 'active', 'ControlGroup': r['control_group']}
            if bad == 'argv': proc['argv'].remove('--history.shared-chunk-cache=true')
            elif bad == 'environment': proc['environ'] = base64.b64encode(b'GOMEMLIMIT=20GiB\0').decode()
            elif bad == 'cgroup': props['ControlGroup'] = '/different.service'
            with self.subTest(bad=bad), mock.patch.object(e, 'show', side_effect=[props, {'MainPID': '0', 'ActiveState': 'inactive'}]), \
                 mock.patch.object(e, 'process', return_value=proc), mock.patch.object(e, 'regular', return_value=b'newbinary'), \
                 mock.patch.object(e, 'sha', return_value='a'*64 if bad == 'binary' else 'b'*64), mock.patch.object(e, 'run') as run:
                if bad:
                    with self.assertRaises(RuntimeError): e.stop(r)
                    run.assert_not_called()
                else:
                    e.stop(r); run.assert_called_once_with(['/bin/systemctl', 'stop', 'gtron.service'], timeout=660)

    def test_plan_only_small_private_evidence_no_service_or_migration_mutation(self):
        e = load(); g, r = fixture(e); args = argparse.Namespace(**r['args'])
        fragment = '/etc/systemd/system/gtron.service'
        r['files'].append(s.item(e, fragment, b'[Service]\nUser=java-tron\n'))
        actual = {f['path']: f for f in r['files']}
        props = dict(r['configuration'], MainPID='4919', ActiveState='active', ControlGroup=r['control_group'],
                     FragmentPath=fragment, DropInPaths=e.EXEC + ' ' + e.DROPIN)
        for key in ('ExecStart', 'ExecStartPre', 'ExecStartPost'): props[key] = serialized(props[key]) if props[key] else ''
        writes, commands = {}, []
        real_sha = e.sha
        def regular(path, *a):
            if str(path) == s.OLD_BINARY: return b'oldbinary'
            if Path(path).name == 'plan.json': return b'private plan'
            return e.content(actual[str(path)])
        with mock.patch.object(s, 'validate_candidate'), mock.patch.object(e, 'guard', return_value=g), \
             mock.patch.object(e, 'show', return_value=props), mock.patch.object(e, 'process', return_value=r['process']), \
             mock.patch.object(e, 'saved', side_effect=lambda p: actual[str(p)]), mock.patch.object(e, 'regular', side_effect=regular), \
             mock.patch.object(e, 'sha', side_effect=lambda d: s.OLD_SHA if d == b'oldbinary' else real_sha(d)), \
             mock.patch.object(e, 'observe', return_value=(100, {'process/start/unix_nano': {'value': 1000}})), \
             mock.patch.object(e, 'healthy', return_value={'healthy': True}), \
             mock.patch.object(e, 'open_parent', side_effect=lambda *a: (os.open(os.devnull, os.O_RDONLY), {})), \
             mock.patch.object(e.os.path, 'lexists', side_effect=lambda p: str(p) in actual), \
             mock.patch.object(s.Path, 'glob', return_value=[Path('/data/gtron/releases/prior/reader-required.json')]), \
             mock.patch.object(s.pwd, 'getpwnam', return_value=types.SimpleNamespace(pw_uid=1003, pw_gid=1003)), \
             mock.patch.object(e, 'run', side_effect=lambda argv, **kw: commands.append(argv) or b'private unit cat'), \
             mock.patch.object(e, 'create_transaction') as mkdir, mock.patch.object(e, 'save', side_effect=lambda n, v: writes.update({n: v})), \
             mock.patch.object(e, 'stop') as stop, mock.patch.object(e, 'write_atomic') as write, mock.patch.object(s, 'migrate') as migrate:
            summary = s.plan(e, args)
        self.assertFalse(summary['service_modified']); self.assertFalse(summary['database_copied'])
        self.assertNotIn('SECRET', json.dumps(summary)); self.assertNotIn('environ', summary)
        mkdir.assert_called_once(); stop.assert_not_called(); write.assert_not_called(); migrate.assert_not_called()
        self.assertEqual(commands, [['/bin/systemctl', 'cat', 'gtron.service']])
        self.assertIn('plan.json', writes); self.assertEqual(writes['plan.json']['process']['environ'], r['process']['environ'])

    def test_r1_guard_rejects_marker_missing_binary_mismatch_and_wrong_flag(self):
        e = load(); g, r = fixture(e); args = argparse.Namespace(binary=str(s.BINARY), sha256='b'*64, source='c'*40)
        guard = types.ModuleType('guard_under_test'); exec(compile(s.R1_GUARD_SOURCE, 'guard', 'exec'), guard.__dict__)
        value = serialized(s.expected_config(e, r, True)['ExecStart']); good = s.marker(argparse.Namespace(**r['args']))
        with mock.patch.object(g, 'hash_root_file', return_value='b'*64):
            guard.check(g, args, value, good)
            for bad in (None, {}, dict(good, container_format='old'), dict(good, source_commit='a'*40)):
                with self.assertRaises(RuntimeError): guard.check(g, args, value, bad)
            for v in (value.replace(s.FLAG, '--history.reference-container=false'), value.replace(s.FLAG, ''), value.replace(s.FLAG, s.FLAG + ' ' + s.FLAG)):
                with self.assertRaises(RuntimeError): guard.check(g, args, v, good)
        with mock.patch.object(g, 'hash_root_file', return_value='a'*64):
            with self.assertRaises(RuntimeError): guard.check(g, args, value, good)

    def test_native_proof_requires_unfiltered_full_packages_and_critical_passes(self):
        e = load(); common = ['/data/go/bin/go', 'test', '-json', '-p', '2', '-tags', 'sapling']
        focused = ('cmd/gtron', 'core/rawdb', 'core/rawdb/pebbledb', 'core/state/snapshots'); full = ('core/state/snapshots', 'cmd/gtron')
        phases = [('native-focused-tests.jsonl', focused, ['-run', 'TestHistoryReference']), ('native-full-tests.jsonl', full, [])]
        prepared = {'native_commands': []}; logs = {}
        for name, packages, extra in phases:
            prepared['native_commands'].append(dict(log=name, argv=common + ['./'+p for p in packages] + extra + ['-count=1', '-timeout=300s']))
            rows = [dict(Action='pass', Package='github.com/tronprotocol/go-tron/' + p) for p in packages]
            rows += [dict(Action='pass', Package='github.com/tronprotocol/go-tron/core/state/snapshots', Test=t) for t in s.CRITICAL]
            logs[name] = b'\n'.join(json.dumps(r).encode() for r in rows)
        with mock.patch.object(e, 'regular', side_effect=lambda p, *a: logs[Path(p).name]):
            s.test_proof(e, prepared)
            for mutation in ('filtered', 'duplicate', 'missing', 'failed', 'malformed'):
                bad = copy.deepcopy(prepared); original = dict(logs)
                if mutation == 'filtered': bad['native_commands'][1]['argv'][-2:-2] = ['-run', 'TestHistoryReference']
                elif mutation == 'duplicate': bad['native_commands'].append(bad['native_commands'][0])
                elif mutation == 'missing': logs['native-full-tests.jsonl'] = logs['native-full-tests.jsonl'].replace(s.CRITICAL[0].encode(), b'OtherTest')
                elif mutation == 'failed': logs['native-full-tests.jsonl'] += b'\n{"Action":"fail"}'
                else: logs['native-full-tests.jsonl'] += b'\n{truncated'
                with self.subTest(mutation=mutation), self.assertRaises(RuntimeError): s.test_proof(e, bad)
                logs.clear(); logs.update(original)

    def test_complete_candidate_ties_git_source_builder_native_logs_and_binary(self):
        e = load(); unused, r = fixture(e); args = argparse.Namespace(**r['args'])
        binary, source, builder = b'candidate binary', b'package example\n', b'reviewed prepared builder'
        args.binary_sha = e.sha(binary)
        blob = hashlib.sha1(b'blob ' + str(len(source)).encode() + b'\0' + source).hexdigest()
        env = {'CGO_ENABLED': '1', 'GOMAXPROCS': '2', 'GOTOOLCHAIN': 'local', 'GOFLAGS': '-mod=readonly', 'GOENV': 'off', 'GOWORK': 'off'}
        prepared = dict(prepared=True, source_commit=args.source_revision, binary=str(s.BINARY), binary_sha256=args.binary_sha,
                        script_commit='e'*40, script_sha256=e.sha(builder), go_version='go version go1.25.5 linux/amd64',
                        build_environment=env, manifest={'source.go': dict(git_blob=blob, mode=0o644, sha256=e.sha(source))}, native_commands=[])
        data = {str(s.BINARY): binary, str(s.RELEASE / 'source/source.go'): source}
        common = ['/data/go/bin/go', 'test', '-json', '-p', '2', '-tags', 'sapling']
        def bind(name, argv, text):
            data[str(s.RELEASE / name)] = text
            prepared['native_commands'].append(dict(log=name, argv=argv, returncode=0, cwd=str(s.RELEASE/'source'), log_sha256=e.sha(text)))
        for name, packages, extra in [('native-focused-tests.jsonl', ('cmd/gtron', 'core/rawdb', 'core/rawdb/pebbledb', 'core/state/snapshots'), ['-run', 'Test.*HistoryReference']),
                                      ('native-full-tests.jsonl', ('core/state/snapshots', 'cmd/gtron'), [])]:
            rows = [dict(Action='pass', Package='github.com/tronprotocol/go-tron/' + p) for p in packages]
            rows += [dict(Action='pass', Package='github.com/tronprotocol/go-tron/core/state/snapshots', Test=t) for t in s.CRITICAL]
            bind(name, common + ['./'+p for p in packages] + extra + ['-count=1', '-timeout=300s'], b'\n'.join(json.dumps(x).encode() for x in rows))
        bind('build.log', ['/data/go/bin/go', 'build', '-p', '2', '-tags', 'sapling', '-buildvcs=false', '-o', str(s.BINARY), './cmd/gtron'], b'')
        bind('build-info.txt', ['go', 'version', '-m'], b'go1.25.5 CGO_ENABLED=1 -tags=sapling GOARCH=amd64 GOOS=linux')
        bind('native-sapling-probe.log', ['go', 'run'], b'nativeSapling=true uncommitted=' + b'a'*64 + b'\n')
        for n in range(3): bind('other%d.log' % n, ['help'], b'help')
        def run(argv, **kw):
            if argv[1] == 'rev-parse': return args.source_revision.encode()
            if argv[1] == 'ls-tree': return ('100644 blob ' + blob + '\tsource.go\0').encode()
            if argv[-1].endswith(s.SCRIPT): return PATH.read_bytes()
            return builder
        original = copy.deepcopy(prepared)
        with mock.patch.object(e, 'regular', side_effect=lambda p, *a: PATH.read_bytes() if str(p) == str(PATH) else data[str(p)]), \
             mock.patch.object(e, 'run', side_effect=run), mock.patch.object(s.Path, 'lstat', return_value=types.SimpleNamespace(st_uid=0, st_mode=0o100755)), \
             mock.patch.object(s.Path, 'is_symlink', return_value=False), \
             mock.patch.object(s.Path, 'stat', return_value=types.SimpleNamespace(st_mode=0o100644)):
            for mutation in (None, 'binary', 'source', 'sha', 'builder', 'environment', 'loghash', 'buildargv', 'nativeversion', 'filtered'):
                p = copy.deepcopy(original); source_key = str(s.RELEASE/'source/source.go'); data[source_key] = source
                if mutation == 'binary': p['binary_sha256'] = '0'*64
                elif mutation == 'source': data[source_key] = b'changed source'
                elif mutation == 'builder': p['script_sha256'] = '0'*64
                elif mutation == 'environment': p['build_environment']['CGO_ENABLED'] = '0'
                elif mutation == 'loghash': p['native_commands'][0]['log_sha256'] = '0'*64
                elif mutation == 'buildargv': p['native_commands'][2]['argv'].remove('sapling')
                elif mutation == 'nativeversion': p['go_version'] = 'go version go1.25.5 darwin/arm64'
                elif mutation == 'filtered': p['native_commands'][1]['argv'][-2:-2] = ['-run', 'TestOne']
                raw = json.dumps(p).encode(); data[str(s.RELEASE/'prepared.json')] = raw
                args.prepared_sha = '0'*64 if mutation == 'sha' else e.sha(raw)
                with self.subTest(mutation=mutation):
                    if mutation is None: self.assertEqual(s.validate_candidate(e, args), p)
                    else:
                        with self.assertRaises(RuntimeError): s.validate_candidate(e, args)

    def test_migration_runs_service_uid_and_records_complete_report_cost_and_failures(self):
        e = load(); unused, r = fixture(e)
        good = dict(dryRun=False, databasePath=s.DATADIR+'/gtron/chaindata', snapshotDir=s.SNAPSHOTS,
                    freeBytesBefore=100, freeBytesAfter=90, largestAdmittedWorkBytes=12,
                    result=dict(snapshotDir=s.SNAPSHOTS, dryRun=False, journalPending=False, completedTrios=1))
        for failure in (None, 'rc', 'pending', 'wrongdir', 'count', 'cost', 'error'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                e.TRANSACTION = Path(directory); evidence = {}; report = copy.deepcopy(good)
                if failure == 'pending': report['result']['journalPending'] = True
                elif failure == 'wrongdir': report['databasePath'] = '/different/datadir'
                elif failure == 'count': report['result']['completedTrios'] = 2
                elif failure == 'cost': report.pop('largestAdmittedWorkBytes')
                elif failure == 'error': report['error'] = 'verification failed'
                def run(argv, **kw):
                    self.assertEqual(argv, [str(s.BINARY), 'db', 'migrate-history-reference', '--datadir', s.DATADIR, '--max-trios', '1', '--yes'])
                    self.assertEqual(kw['timeout'], 1800); self.assertEqual(kw['env']['GOMEMLIMIT'], '10GiB')
                    self.assertIsNotNone(kw['demote']); self.assertEqual(kw['cwd'], '/data/gtron/main')
                    kw['stdout'].write(json.dumps(report).encode()); kw['stderr'].write(b'private diagnostic')
                    if failure == 'rc': raise subprocess.CalledProcessError(2, argv)
                with mock.patch.object(s, 'child_run', side_effect=run), mock.patch.object(e, 'show', return_value={'MainPID': '0'}), \
                     mock.patch.object(s.pwd, 'getpwnam', return_value=types.SimpleNamespace(pw_name='java-tron', pw_uid=1003, pw_gid=1003)), \
                     mock.patch.object(s.os, 'getgrouplist', return_value=[1003]), \
                     mock.patch.object(e, 'regular', side_effect=lambda p, *a: Path(p).read_bytes()), \
                     mock.patch.object(e, 'save', side_effect=lambda name, value: evidence.update({name: value})):
                    if failure:
                        with self.assertRaises((RuntimeError, subprocess.CalledProcessError)): s.migrate(e, r)
                    else: self.assertEqual(s.migrate(e, r), good)
                command = evidence['migration-command.json']
                self.assertEqual(command['returncode'], 2 if failure == 'rc' else 0)
                self.assertEqual(command['stdout_sha256'], e.sha((e.TRANSACTION/'migration.stdout').read_bytes()))
                self.assertGreaterEqual(command['wall_seconds'], 0)
                self.assertGreaterEqual(command['finished_unix'], command['started_unix'])
                self.assertEqual('migration.json' in evidence, failure is None)

    def test_real_child_timeout_and_signal_reap_owned_group(self):
        with tempfile.TemporaryDirectory() as directory:
            pidfile = Path(directory) / 'pid'
            script = 'import os,time; open(%r,"w").write(str(os.getpid())); time.sleep(60)' % str(pidfile)
            with self.assertRaises(subprocess.TimeoutExpired):
                s.child_run([sys.executable, '-c', script], timeout=.2, cwd=directory)
            pid = int(pidfile.read_text())
            with self.assertRaises(ProcessLookupError): os.kill(pid, 0)
            previous = signal.signal(signal.SIGALRM, lambda *x: (_ for _ in ()).throw(s.Interrupted('fixture alarm')))
            try:
                signal.setitimer(signal.ITIMER_REAL, .2)
                with self.assertRaises(s.Interrupted): s.child_run([sys.executable, '-c', script], timeout=5, cwd=directory)
                pid = int(pidfile.read_text())
                with self.assertRaises(ProcessLookupError): os.kill(pid, 0)
            finally:
                signal.setitimer(signal.ITIMER_REAL, 0); signal.signal(signal.SIGALRM, previous)

    def test_signal_during_spawn_waits_for_handle_then_reaps_and_child_mask_is_restored(self):
        with tempfile.TemporaryDirectory() as directory:
            pidfile = Path(directory) / 'spawned-pid'; parent = os.getpid()
            previous = signal.signal(signal.SIGTERM, lambda *x: (_ for _ in ()).throw(s.Interrupted('spawn signal')))
            def during_spawn():
                with open(str(pidfile), 'w') as output: output.write(str(os.getpid()))
                os.kill(parent, signal.SIGTERM)
                # The parent is in Popen waiting for this preexec/exec boundary.
                import time
                time.sleep(.1)
            try:
                with self.assertRaises(s.Interrupted):
                    s.child_run([sys.executable, '-c', 'import time; time.sleep(60)'], cwd=directory, demote=during_spawn, timeout=5)
                with self.assertRaises(ProcessLookupError): os.kill(int(pidfile.read_text()), 0)
            finally: signal.signal(signal.SIGTERM, previous)
            result = s.child_run([sys.executable, '-c', 'import signal; print(sorted(int(s) for s in signal.pthread_sigmask(signal.SIG_BLOCK, [])))'], cwd=directory)
            inherited = json.loads(result)
            self.assertFalse(set(inherited).intersection(int(sig) for sig in s.SIGNALS))


if __name__ == '__main__': unittest.main()
