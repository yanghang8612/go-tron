"""Local Git reads and temporary/fake builds; never runs Go or contacts a node."""
import argparse
import ast
import copy
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('parallel_prepare_ops', ROOT / 'scripts/prepare_history_parallel_20260915.py')
m = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)


def git_entry(data, mode=0o644):
    return {'git_blob': hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest(), 'mode': mode}


def archive_file(path, entries):
    with tarfile.open(str(path), 'w') as bundle:
        for name, data, mode in entries:
            info = tarfile.TarInfo(name)
            info.size, info.mode = len(data), mode
            bundle.addfile(info, io.BytesIO(data))


class ParallelPrepareTests(unittest.TestCase):
    def helpers(self):
        with patch.object(m, 'REPO', ROOT):
            return m.pure_helpers()

    def test_loads_only_pinned_pure_definitions(self):
        helper = self.helpers()
        for name in m.PURE_FUNCTIONS:
            self.assertTrue(callable(getattr(helper, name)))
        for name in ('prepare', 'inspect', 'inspect_transaction', 'load_modules', 'pinned_module',
                     'snapshot', 'main', 'production_record', 'live_ops', 'CURRENT_PID', 'SERVICE'):
            self.assertFalse(hasattr(helper, name), name)
        with patch.object(m, 'REPO', ROOT), patch.object(m, 'HELPER_SHA', '0' * 64):
            with self.assertRaisesRegex(RuntimeError, 'helper Git bytes'):
                m.pure_helpers()

    def test_native_environment_excludes_external_test_and_build_overrides(self):
        with patch.dict(os.environ, {'GTRON_HISTORY_VERIFY_FIXTURE': '/production/path',
                                     'GTRON_HISTORY_COMPRESSION_FORMAT': '3', 'GOFLAGS': '-overlay=/tmp/bad',
                                     'GOWORK': '/tmp/bad', 'CGO_CFLAGS': 'bad', 'LD_PRELOAD': 'bad'}):
            env = m.build_environment()
        self.assertFalse(any(k.startswith('GTRON_') for k in env))
        self.assertNotIn('LD_PRELOAD', env)
        self.assertNotIn('CGO_CFLAGS', env)
        self.assertEqual(env['GOENV'], 'off')
        self.assertEqual(env['GOWORK'], 'off')
        self.assertEqual(env['GOFLAGS'], '-mod=readonly')
        self.assertEqual(env['GOMAXPROCS'], '2')
        self.assertEqual(env['GOTOOLCHAIN'], 'local')

    def test_full_identity_rejects_shell_names_and_noncanonical_ids(self):
        for value in ('master', 'a' * 39, 'A' * 40, 'a' * 40 + ';id', None, 123):
            with self.assertRaises(RuntimeError):
                m.full_hex(value, 40)
        self.assertEqual(m.full_hex('a' * 40, 40), 'a' * 40)

    def test_checkout_allows_only_rust_dirty_and_is_recorded_exactly(self):
        with patch.object(m, 'resolve', return_value=m.CHECKOUT), patch.object(m, 'git', return_value=b' M third_party/librustzcash\n'):
            self.assertEqual(m.repository_state()['tracked_status'], ' M third_party/librustzcash\n')
        for dirty in (b' M cmd/gtron/main.go\n', b' M third_party/librustzcash-extra\n'):
            with patch.object(m, 'resolve', return_value=m.CHECKOUT), patch.object(m, 'git', return_value=dirty):
                with self.assertRaisesRegex(RuntimeError, 'tracked'):
                    m.repository_state()
        with patch.object(m, 'resolve', return_value='f' * 40):
            with self.assertRaisesRegex(RuntimeError, 'HEAD'):
                m.repository_state()

    def test_admission_binds_script_source_scope_and_dynamic_manifest(self):
        source, ops = 'a' * 40, 'b' * 40
        args = argparse.Namespace(source_revision=source, script_revision=ops, rust_library_sha256='c' * 64)
        script, manifest = b'exact script', {'main.go': git_entry(b'main'), m.SCRIPT_PATH: git_entry(b'exact script')}
        helper = types.SimpleNamespace(read_regular=Mock(return_value=script),
                                       source_tree_manifest=Mock(return_value=(manifest, {'third_party/librustzcash': 'd' * 40})))
        changes = ['M\t' + path for path in sorted(m.REQUIRED_SOURCE_CHANGES)]
        source_script = [script]
        def fake_git(*argv):
            if argv[0] == 'show':
                return source_script[0] if argv[1].startswith(source + ':') else script
            if argv[0] == 'diff':
                return ('\n'.join(changes) + '\n').encode()
            if argv[0] == 'merge-base':
                return (m.BASE + '\n').encode()
            raise AssertionError(argv)
        def resolved(value):
            return ops if value == 'refs/remotes/origin/master' else value
        with patch.object(m, 'resolve', resolved), patch.object(m, 'git', fake_git), \
                patch.object(m, 'repository_state', return_value={'head': m.CHECKOUT, 'tracked_status': ' M third_party/librustzcash\n'}):
            record = m.admission(args, helper)
            self.assertEqual(record['source_file_count'], 2)
            self.assertEqual(record['source_commit'], source)
            source_script[0] = b'old builder'
            with self.assertRaisesRegex(RuntimeError, 'different builder'):
                m.admission(args, helper)
            source_script[0] = script
            changes.append('M\tcore/unreviewed.go')
            with self.assertRaisesRegex(RuntimeError, 'outside reviewed scope'):
                m.admission(args, helper)
            changes[-1] = 'D\tcmd/gtron/main.go'
            with self.assertRaisesRegex(RuntimeError, 'change kind'):
                m.admission(args, helper)

    def test_reused_extraction_normalizes_only_fresh_git_files(self):
        helper = self.helpers()
        with tempfile.TemporaryDirectory() as root:
            helper.SOURCE = Path(root) / 'source'
            archive = Path(root) / 'source.tar'
            contents = [('plain', b'plain', 0o664), ('dir/run', b'executable', 0o775)]
            manifest = {name: git_entry(data, 0o755 if mode == 0o775 else 0o644) for name, data, mode in contents}
            archive_file(archive, contents)
            helper.extract_private_source(archive, manifest)
            helper.verify_source(None, manifest)
            self.assertEqual((helper.SOURCE / 'plain').stat().st_mode & 0o777, 0o644)
            self.assertEqual((helper.SOURCE / 'dir/run').stat().st_mode & 0o777, 0o755)
            with self.assertRaisesRegex(RuntimeError, 'new source directory'):
                helper.extract_private_source(archive, manifest)
            (helper.SOURCE / 'plain').write_bytes(b'tampered')
            with self.assertRaisesRegex(RuntimeError, 'Git blob'):
                helper.verify_source(None, manifest)

    def test_file_hash_refuses_symlinks_and_oversized_files(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'file'
            path.write_bytes(b'fixed')
            self.assertEqual(m.file_sha(path), hashlib.sha256(b'fixed').hexdigest())
            link = Path(root) / 'link'
            link.symlink_to(path)
            with self.assertRaises(OSError):
                m.file_sha(link)
            with self.assertRaises(RuntimeError):
                m.file_sha(path, limit=4)

    def test_native_test_evidence_requires_named_passes_not_empty_success(self):
        with tempfile.TemporaryDirectory() as root, patch.object(m, 'RELEASE', Path(root)):
            helper = self.helpers()
            path = Path(root) / 'tests.jsonl'
            events = [{'Package': m.MODULE + 'cmd/gtron', 'Action': 'pass'}]
            path.write_text('\n'.join(json.dumps(e) for e in events))
            with self.assertRaisesRegex(RuntimeError, 'required native test'):
                m.verify_test_log(helper, path.name, ('cmd/gtron',), (('cmd/gtron', 'TestReal'),))
            events.append({'Package': m.MODULE + 'cmd/gtron', 'Test': 'TestReal', 'Action': 'pass'})
            path.write_text('go: downloading example\n' + '\n'.join(json.dumps(e) for e in events))
            evidence = m.verify_test_log(helper, path.name, ('cmd/gtron',), (('cmd/gtron', 'TestReal'),))
            self.assertEqual(evidence['diagnostic_lines'], 1)
            events.append({'Package': m.MODULE + 'cmd/gtron', 'Action': 'fail'})
            path.write_text('\n'.join(json.dumps(e) for e in events))
            with self.assertRaisesRegex(RuntimeError, 'reported failure'):
                m.verify_test_log(helper, path.name, ('cmd/gtron',))

    def fake_build(self, failure=None):
        helper = self.helpers()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            release, repo = root / 'release', root / 'repo'
            source, binary = release / 'source', release / 'gtron-inspect'
            library = repo / 'third_party/librustzcash/target/release/librustzcash.a'
            library.parent.mkdir(parents=True)
            library.write_bytes(b'native archive')
            go = root / 'go'
            go.write_bytes(b'fake native go')
            args = argparse.Namespace(source_revision='a' * 40, script_revision='b' * 40, rust_library_sha256=m.file_sha(library))
            state = {'head': m.CHECKOUT, 'tracked_status': ' M third_party/librustzcash\n'}
            record = {'source_commit': args.source_revision, 'script_commit': args.script_revision,
                      'script_sha256': m.file_sha(m.__file__), 'manifest': {'main.go': git_entry(b'fixed source')},
                      'gitlinks': {'third_party/librustzcash': 'c' * 40}, 'checkout_before': state,
                      'source_file_count': 1}
            helper.SOURCE, helper.REPO = source, repo
            calls = []
            def fake_git(*argv):
                self.assertEqual(argv[0], 'archive')
                archive_file(Path(argv[2].split('=', 1)[1]), [('main.go', b'fixed source', 0o664)])
                return b''
            def fake_run(argv, name, env, timeout=1800):
                calls.append((argv, name))
                self.assertEqual(env['GOMAXPROCS'], '2')
                self.assertTrue(argv[0] in (str(go), str(binary)))
                if failure == name:
                    raise RuntimeError('injected command failure')
                if name == 'go-version.txt':
                    text = 'go version go1.25.5 linux/amd64\n'
                elif name.endswith('tests.jsonl'):
                    tests = m.REQUIRED_TESTS if 'focused' in name else ()
                    packages = ('cmd/gtron', 'core/rawdb', 'core/rawdb/pebbledb', 'core/state/snapshots')
                    events = [{'Package': m.MODULE + p, 'Test': t, 'Action': 'pass'} for p, t in tests]
                    events += [{'Package': m.MODULE + p, 'Action': 'pass'} for p in packages]
                    text = '\n'.join(json.dumps(e) for e in events)
                elif name == 'native-sapling-probe.log':
                    text = 'nativeSapling=true uncommitted=' + '0' * 64 + '\n'
                elif name == 'build-info.txt':
                    text = 'go1.25.5 CGO_ENABLED=1 -tags=sapling GOARCH=amd64 GOOS=linux'
                elif name.endswith('-help.txt'):
                    self.assertEqual(argv[1], 'db')
                    self.assertEqual(argv[-1], '--help')
                    text = argv[2] + ' ' + ' '.join(m.HELP_CONTRACTS[argv[2]])
                    if failure == 'missing-help':
                        text = argv[2]
                else:
                    self.assertEqual(name, 'build.log')
                    binary.write_bytes(b'native binary')
                    if failure == 'changed-source':
                        (source / 'main.go').write_bytes(b'tamper')
                    if failure == 'changed-rust':
                        library.write_bytes(b'different native archive')
                    text = ''
                (release / name).write_text(text)
                return {'argv': argv, 'log': name, 'returncode': 0, 'log_sha256': m.file_sha(release / name)}
            if failure == 'existing-release':
                release.mkdir()
                (release / 'old-evidence').write_text('do not replace')
            with patch.multiple(m, REPO=repo, RELEASE=release, SOURCE=source, BINARY=binary, GO=str(go)), \
                    patch.object(m, 'pure_helpers', return_value=helper), patch.object(m, 'admission', return_value=copy.deepcopy(record)), \
                    patch.object(m, 'repository_state', return_value=state), patch.object(m, 'git', fake_git), \
                    patch.object(m, 'run_logged', fake_run):
                if failure:
                    with self.assertRaises(RuntimeError):
                        m.prepare(args)
                    self.assertFalse((release / 'prepared.json').exists())
                    if failure == 'existing-release':
                        self.assertEqual((release / 'old-evidence').read_text(), 'do not replace')
                        self.assertEqual(calls, [])
                    else:
                        self.assertFalse(json.loads((release / 'failure.json').read_text())['prepared'])
                else:
                    result = m.prepare(args)
                    self.assertTrue(result['prepared'])
                    prepared = json.loads((release / 'prepared.json').read_text())
                    self.assertEqual(prepared['binary_sha256'], m.file_sha(binary))
                    self.assertEqual(prepared['rust_library_sha256'], args.rust_library_sha256)
                    self.assertEqual(prepared['manifest']['main.go']['sha256'], hashlib.sha256(b'fixed source').hexdigest())
                    self.assertEqual(prepared['source_file_count'], 1)
                    self.assertEqual(prepared['checkout_after'], state)
                    self.assertEqual(len(prepared['native_commands']), 9)
                    self.assertTrue(source.joinpath('third_party/librustzcash').is_symlink())
                    self.assertEqual(library.read_bytes(), b'native archive')
                    self.assertEqual(prepared['go_binary_sha256'], m.file_sha(go))
                    for argv, name in calls:
                        if 'test' in argv:
                            self.assertIn('-count=1', argv)
                        self.assertNotIn('stop', argv)
                        self.assertNotIn('start', argv)
                        self.assertNotIn('install', argv)

    def test_full_fake_build_attests_only_after_all_native_checks(self):
        self.fake_build()

    def test_failed_build_or_changed_input_never_publishes_prepared(self):
        for failure in ('native-focused-tests.jsonl', 'native-full-tests.jsonl', 'native-sapling-probe.log',
                        'build.log', 'missing-help', 'changed-source', 'changed-rust', 'existing-release'):
            with self.subTest(failure=failure):
                self.fake_build(failure)

    def test_timeout_reaps_only_owned_child_group(self):
        with tempfile.TemporaryDirectory() as root, patch.object(m, 'RELEASE', Path(root)), patch.object(m, 'SOURCE', Path(root)):
            child = Mock(pid=123456)
            child.wait.side_effect = [subprocess.TimeoutExpired('fake', 1), 0, 0]
            child.poll.return_value = None
            with patch.object(m.subprocess, 'Popen', return_value=child) as spawn, patch.object(m.os, 'killpg') as kill:
                with self.assertRaises(subprocess.TimeoutExpired):
                    m.run_logged(['fake'], 'timeout.log', {}, timeout=1)
                self.assertTrue(spawn.call_args[1]['start_new_session'])
                self.assertEqual(kill.call_args_list[0][0], (child.pid, m.signal.SIGTERM))
                self.assertEqual(kill.call_args_list[1][0], (child.pid, m.signal.SIGKILL))
                self.assertEqual(child.wait.call_count, 3)

    def test_cleanup_still_signals_group_after_leader_exit(self):
        child = Mock(pid=123456, returncode=0)
        child.poll.return_value = 0
        with patch.object(m.os, 'killpg') as kill:
            m.stop_child(child)
        self.assertEqual(kill.call_args_list[0][0], (child.pid, m.signal.SIGTERM))
        self.assertEqual(kill.call_args_list[1][0], (child.pid, m.signal.SIGKILL))
        self.assertEqual(child.wait.call_count, 2)

    def test_spawn_failure_restores_signal_mask(self):
        with tempfile.TemporaryDirectory() as root, patch.object(m, 'RELEASE', Path(root)), patch.object(m, 'SOURCE', Path(root)):
            before = m.signal.pthread_sigmask(m.signal.SIG_BLOCK, [])
            with patch.object(m.subprocess, 'Popen', side_effect=OSError('spawn failed')):
                with self.assertRaises(OSError):
                    m.run_logged(['fake'], 'spawn-failure.log', {})
            self.assertEqual(m.signal.pthread_sigmask(m.signal.SIG_BLOCK, []), before)

    def test_signal_at_popen_return_preserves_handle_and_reaps_real_child(self):
        # A real Python child substitutes for Go. A queued TERM lands exactly
        # when assignment's signal mask is restored; finally must own its PID.
        spawned, real_popen = [], subprocess.Popen
        previous_handler = m.signal.signal(m.signal.SIGTERM, m.interrupted)
        try:
            def spawn_then_signal(*args, **kwargs):
                child = real_popen(*args, **kwargs)
                spawned.append(child)
                os.kill(os.getpid(), m.signal.SIGTERM)
                return child
            with tempfile.TemporaryDirectory() as root, patch.object(m, 'RELEASE', Path(root)), patch.object(m, 'SOURCE', Path(root)), \
                    patch.object(m.subprocess, 'Popen', side_effect=spawn_then_signal):
                with self.assertRaises(KeyboardInterrupt):
                    m.run_logged([sys.executable, '-c', 'import time; time.sleep(30)'], 'signal.log', m.build_environment())
            self.assertEqual(len(spawned), 1)
            self.assertIsNotNone(spawned[0].poll())
        finally:
            for child in spawned:
                m.stop_child(child)
            m.signal.signal(m.signal.SIGTERM, previous_handler)

    def test_python36_syntax(self):
        # feature_version is a local parser gate, not a runtime requirement.
        if sys_version_supports_feature_version():
            for path in (Path(m.__file__), Path(__file__)):
                ast.parse(path.read_text(), feature_version=(3, 6))


def sys_version_supports_feature_version():
    return sys.version_info >= (3, 8)


if __name__ == '__main__':
    unittest.main()
