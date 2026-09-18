import ast
import importlib.util
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / 'scripts/cutover_fresh_mainnet_20260918.py'
spec = importlib.util.spec_from_file_location('fresh_cutover', str(PATH))
s = importlib.util.module_from_spec(spec); spec.loader.exec_module(s)


class Engine:
    @staticmethod
    def require(ok, message):
        if not ok:
            raise RuntimeError(message)


def argv(binary='/old/gtron'):
    values = dict(s.FORMAT_FLAGS)
    values['history.cross-block-dedup'] = 'true'
    values.update({'p2p.port': '18890', 'http.port': '8090', 'jsonrpc.port': '8545',
                   'grpc.port': '50051', 'pprof.port': '6062'})
    return [binary] + ['--%s=%s' % item for item in values.items()]


class FreshCutoverTests(unittest.TestCase):
    def test_chain_argv_requires_exact_format_and_rejects_reset_modes(self):
        rows = s.validate_chain_argv(Engine(), argv(), '/old/gtron')
        self.assertEqual(rows['datadir'], [str(s.DATADIR)])
        for bad, message in (
            (argv() + ['--snapshot.reset=true'], 'unsafe service mode'),
            (argv() + ['--config=/etc/gtron.toml'], 'unsafe service mode'),
            (argv() + ['--sync.stop-at=0'], 'unsafe service mode'),
            (argv() + ['--db.cache=4096'], 'fresh format flag differs'),
            ([item for item in argv() if not item.startswith('--prune.mode=')], 'fresh format flag differs'),
        ):
            with self.subTest(message=message), self.assertRaisesRegex(RuntimeError, message):
                s.validate_chain_argv(Engine(), bad, '/old/gtron')
        explicit = argv() + ['--snapshot.dir=' + str(s.TREE / 'state-snapshots'),
                             '--snapshot.etl.tempdir=/data/gtron/main/snapshot-scratch',
                             '--sync.etl.tempdir=/data/gtron/main/sync-scratch']
        rows = s.validate_chain_argv(Engine(), explicit, '/old/gtron')
        self.assertEqual(rows['snapshot.etl.tempdir'], ['/data/gtron/main/snapshot-scratch'])
        with self.assertRaisesRegex(RuntimeError, 'snapshot.dir escapes'):
            s.validate_chain_argv(Engine(), argv() + ['--snapshot.dir=/outside'], '/old/gtron')

    def test_expected_config_changes_only_candidate_and_guard_identities(self):
        old_binary, old_sha, old_source = '/old/gtron', 'a' * 64, 'b' * 40
        config = {
            'ExecStart': [{'path': old_binary, 'argv[]': ' '.join(argv(old_binary))}],
            'ExecStartPre': [
                {'argv[]': '/usr/bin/python3 %s check' % s.SPACE_GUARD},
                {'argv[]': '/usr/bin/python3 %s --binary %s --sha256 %s --source %s --marker %s' %
                            (s.SHARED_GUARD, old_binary, old_sha, old_source, s.GLOBAL_MARKER)},
                {'argv[]': '/usr/bin/python3 %s --binary %s --sha256 %s --source %s --marker /old/reference-reader-required.json' %
                            (s.R1_GUARD, old_binary, old_sha, old_source)},
            ],
        }
        record = {'configuration': config, 'process': {'exe': old_binary},
                  'old_r1_marker': '/old/reference-reader-required.json',
                  'args': {'old_binary_sha256': old_sha, 'old_source_revision': old_source,
                           'binary_sha256': 'c' * 64, 'source_revision': 'd' * 40}}
        got = s.expected_config(Engine(), record)
        self.assertEqual(shlex_split(got['ExecStart'][0]['argv[]'])[0], str(s.BINARY))
        self.assertEqual(len(got['ExecStartPre']), 3)
        self.assertIn(s.NEW_R1_MARKER, got['ExecStartPre'][2]['argv[]'])
        self.assertIn('c' * 64, got['ExecStartPre'][1]['argv[]'])
        self.assertEqual(config['ExecStart'][0]['path'], old_binary)

    def test_metadata_scan_rejects_symlinks_without_following_them(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'old-gtron'; root.mkdir()
            (root / 'db').mkdir(); (root / 'db/file').write_bytes(b'x')
            mountinfo = Path(directory) / 'mountinfo'
            mountinfo.write_text('1 0 1:1 / / rw - apfs disk rw\n')
            dev = root.stat().st_dev
            self.assertEqual(s.scan_no_links_or_mounts(root, dev, mountinfo_path=mountinfo), 2)
            (root / 'outside').symlink_to('/tmp')
            with self.assertRaisesRegex(RuntimeError, 'symlink'):
                s.scan_no_links_or_mounts(root, dev, mountinfo_path=mountinfo)

    def test_descriptor_relative_delete_removes_only_bound_regular_tree(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory); child = parent / s.QUARANTINE_CHILD
            (child / 'nested').mkdir(parents=True); (child / 'nested/file').write_bytes(b'x')
            fd = os.open(str(parent), os.O_RDONLY | os.O_DIRECTORY)
            try:
                s.remove_tree_at(fd, s.QUARANTINE_CHILD, parent.stat().st_dev)
            finally:
                os.close(fd)
            self.assertFalse(child.exists())

    def test_descriptor_relative_delete_refuses_symlink_and_preserves_target(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory); child = parent / s.QUARANTINE_CHILD; child.mkdir()
            outside = parent / 'outside'; outside.write_bytes(b'keep')
            (child / 'link').symlink_to(outside)
            fd = os.open(str(parent), os.O_RDONLY | os.O_DIRECTORY)
            try:
                with self.assertRaisesRegex(RuntimeError, 'symlink'):
                    s.remove_tree_at(fd, s.QUARANTINE_CHILD, parent.stat().st_dev)
            finally:
                os.close(fd)
            self.assertEqual(outside.read_bytes(), b'keep')

    def test_descriptor_relative_delete_rejects_replaced_root_inode(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory); child = parent / s.QUARANTINE_CHILD; child.mkdir()
            expected_inode = child.stat().st_ino
            child.rmdir(); child.mkdir()
            fd = os.open(str(parent), os.O_RDONLY | os.O_DIRECTORY)
            try:
                with self.assertRaisesRegex(RuntimeError, 'root identity'):
                    s.remove_tree_at(fd, s.QUARANTINE_CHILD, parent.stat().st_dev, expected_inode)
            finally:
                os.close(fd)
            self.assertTrue(child.is_dir())

    def test_open_fd_scan_fails_closed_on_proc_permission_error(self):
        with tempfile.TemporaryDirectory() as directory:
            proc_root = Path(directory) / 'proc'; fd_dir = proc_root / '123/fd'; fd_dir.mkdir(parents=True)
            real_listdir = os.listdir
            def listdir(path):
                if Path(path) == fd_dir:
                    raise PermissionError('denied')
                return real_listdir(path)
            with mock.patch.object(s.os, 'listdir', side_effect=listdir), \
                 self.assertRaisesRegex(RuntimeError, 'cannot inspect'):
                s.no_open_fds('/old/tree', proc_root=proc_root)

    def test_fresh_height_accepts_omitted_genesis_number_only(self):
        self.assertEqual(s.fresh_block_height({'blockID': s.GENESIS,
                                               'block_header': {'raw_data': {}}}), 0)
        self.assertEqual(s.fresh_block_height({'blockID': 'f' * 64,
                                               'block_header': {'raw_data': {'number': 3}}}), 3)
        for response in ({}, {'blockID': 'f' * 64, 'block_header': {'raw_data': {}}},
                         {'blockID': 'f' * 64, 'block_header': {'raw_data': {'number': 0}}}):
            with self.assertRaises(RuntimeError):
                s.fresh_block_height(response)

    def test_progress_gate_requires_same_process_and_positive_second_head(self):
        class FakeEngine(Engine):
            def __init__(self, heads):
                self.heads = iter(heads)

            @staticmethod
            def show():
                return {'ActiveState': 'active', 'MainPID': '9'}

            @staticmethod
            def process(pid):
                return {'ticks': 10, 'exe': str(s.BINARY), 'argv': argv(str(s.BINARY))}

            @staticmethod
            def configuration(guard, props):
                return 'expected'

            @staticmethod
            def guard():
                return object()

        activated = {'pid': 9, 'ticks': 10}
        with mock.patch.object(s, 'expected_config', return_value='expected'), \
             mock.patch.object(s, 'http_json', return_value={'blockID': s.GENESIS}), \
             mock.patch.object(s.time, 'sleep'):
            metrics = {'state/snapshot/cold/manifest_cache/budget_bytes': {'value': 536870912}}
            with mock.patch.object(s, 'fresh_observe', side_effect=[(0, metrics), (0, metrics)]), \
                 self.assertRaisesRegex(RuntimeError, 'not demonstrably advanced'):
                s.progress_samples(FakeEngine([0, 0]), {}, activated)
            with mock.patch.object(s, 'fresh_observe', side_effect=[(1, metrics), (2, metrics)]):
                self.assertEqual([row['head'] for row in
                                  s.progress_samples(FakeEngine([1, 2]), {}, activated)], [1, 2])

    def test_python36_fixed_paths_and_no_copy_or_rollback_implementation(self):
        source = PATH.read_text()
        ast.parse(source, feature_version=(3, 6))
        self.assertIn("TREE = DATADIR / 'gtron'", source)
        self.assertIn("QUARANTINE_CHILD = 'old-gtron'", source)
        self.assertIn("os.rename('gtron', QUARANTINE_CHILD, src_dir_fd=datadir_fd", source)
        for forbidden in ('shutil.copy', 'shutil.rmtree', 'renameat2', "choices=('plan', 'execute', 'rollback')"):
            self.assertNotIn(forbidden, source)


def shlex_split(value):
    import shlex
    return shlex.split(value)


if __name__ == '__main__':
    unittest.main()
