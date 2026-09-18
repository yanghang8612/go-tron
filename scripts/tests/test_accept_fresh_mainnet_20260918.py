import ast
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / 'scripts/accept_fresh_mainnet_20260918.py'
spec = importlib.util.spec_from_file_location('fresh_accept', str(PATH))
s = importlib.util.module_from_spec(spec); spec.loader.exec_module(s)


def template():
    flags = {
        'datadir': '/data/gtron/main/datadir', 'log.file': '/data/gtron/main/gtron.log',
        'p2p.port': '18890', 'discover.port': '18890', 'external.ip': '3.12.206.71',
        'http.port': '8090', 'jsonrpc.port': '8545', 'grpc.port': '50051',
        'pprof.port': '6062', 'pprof.addr': '127.0.0.1',
        'prune.mode': 'snap', 'db.cache': '4096', 'history.shared-read-workers': '4',
        'history.shared-chunk-cache': 'true', 'history.reference-container': 'true',
        'history.cross-block-dedup': 'true',
    }
    return {'argv': ['/old/gtron'] + ['--%s=%s' % item for item in flags.items()],
            'environment': dict(s.MEMORY_ENV), 'user': 'java-tron'}


class FreshAcceptanceTests(unittest.TestCase):
    def test_template_requires_exact_fresh_format_memory_and_production_datadir(self):
        argv, environment = s.validate_template(template())
        self.assertEqual(argv[0], '/old/gtron')
        self.assertEqual(environment['GOMEMLIMIT'], '8GiB')
        without_defaulted_network = template()
        without_defaulted_network['argv'] = [row for row in without_defaulted_network['argv']
                                               if not row.startswith('--pprof.addr=')]
        s.validate_template(without_defaulted_network)
        for mutate, message in (
            (lambda row: row['argv'].append('--snapshot.reset=true'), 'unsafe mode'),
            (lambda row: row['argv'].append('--config=/etc/gtron.toml'), 'unsafe mode'),
            (lambda row: row['argv'].append('--db.cache=4096'), 'required production flag differs'),
            (lambda row: row['argv'].__setitem__(1, '--datadir=/tmp/wrong'), 'production datadir'),
            (lambda row: row['environment'].__setitem__('GOMEMLIMIT', '4GiB'), 'memory environment'),
        ):
            row = template(); mutate(row)
            with self.subTest(message=message), self.assertRaisesRegex(RuntimeError, message):
                s.validate_template(row)

    def test_command_replaces_all_bound_endpoints_once_and_holds_genesis(self):
        argv, unused = s.validate_template(template())
        ports = {'p2p': 21001, 'http': 21002, 'jsonrpc': 21003,
                 'pprof': 21004, 'metrics': 21005}
        got = s.command(argv, '/tmp/fresh', '/tmp/fresh.log', ports)
        rows = s.flag_rows(got)
        self.assertEqual(rows['datadir'], ['/tmp/fresh'])
        self.assertEqual(rows['p2p.port'], ['21001'])
        self.assertEqual(rows['discover.port'], ['21001'])
        self.assertEqual(rows['grpc.port'], ['0'])
        self.assertEqual(rows['sync.stop-at'], ['0'])
        self.assertEqual(rows['metrics.port'], ['21005'])
        self.assertEqual(rows['snapshot.dir'], ['/tmp/fresh/gtron/state-snapshots'])
        self.assertEqual(rows['snapshot.etl.tempdir'], ['/tmp/fresh/scratch/snapshot-etl'])
        self.assertEqual(rows['sync.etl.tempdir'], ['/tmp/fresh/scratch/sync-etl'])
        self.assertEqual(got[0], str(s.BINARY))

    def test_command_replaces_explicit_production_snapshot_and_scratch_paths(self):
        row = template()
        row['argv'] += ['--snapshot.dir=/data/gtron/main/datadir/gtron/state-snapshots',
                        '--snapshot.etl.tempdir=/data/gtron/main/snapshot-scratch',
                        '--sync.etl.tempdir=/data/gtron/main/sync-scratch']
        template_argv, unused = s.validate_template(row)
        ports = {'p2p': 21001, 'http': 21002, 'jsonrpc': 21003,
                 'pprof': 21004, 'metrics': 21005}
        got = s.flag_rows(s.command(template_argv, '/tmp/isolated', '/tmp/node.log', ports))
        self.assertEqual(got['snapshot.dir'], ['/tmp/isolated/gtron/state-snapshots'])
        self.assertEqual(got['snapshot.etl.tempdir'], ['/tmp/isolated/scratch/snapshot-etl'])
        self.assertEqual(got['sync.etl.tempdir'], ['/tmp/isolated/scratch/sync-etl'])
        self.assertFalse(any('/data/gtron/main' in value for values in got.values() for value in values))

    def test_dynamic_port_reservation_is_unique_and_supports_p2p_tcp_udp_pair(self):
        sockets, ports = s.reserve_ports()
        try:
            self.assertEqual(len(set(ports.values())), 5)
            self.assertTrue(all(value > 0 for value in ports.values()))
        finally:
            for sock in sockets:
                sock.close()

    def test_run_once_requires_genesis_head_zero_cache_budget_and_clean_term(self):
        class Child:
            pid = 1234

            @staticmethod
            def poll():
                return None

            @staticmethod
            def wait(timeout):
                return 0

        ports = {'p2p': 21001, 'http': 21002, 'jsonrpc': 21003,
                 'pprof': 21004, 'metrics': 21005}
        with tempfile.TemporaryDirectory() as directory, \
             mock.patch.object(s, 'OUTPUT', Path(directory)), \
             mock.patch.object(s, 'reserve_ports', return_value=([], ports)), \
             mock.patch.object(s.subprocess, 'Popen', return_value=Child()), \
             mock.patch.object(s.os, 'killpg') as killpg, \
             mock.patch.object(s, 'metric', return_value=536870912), \
             mock.patch.object(s, 'http_json', side_effect=[
                 {'blockID': s.GENESIS},
                 {'blockID': s.GENESIS, 'block_header': {'raw_data': {}}}]):
            result = s.run_once(template()['argv'], template()['environment'], Path(directory) / 'datadir',
                                Path(directory), 1003, 1003, '/home/java-tron', 1)
        self.assertEqual(result['head'], 0)
        self.assertEqual(result['manifest_cache_budget'], 536870912)
        self.assertEqual(result['exit_code'], 0)
        killpg.assert_called_once_with(1234, s.signal.SIGTERM)

    def test_genesis_height_allows_omitted_number_but_rejects_empty_response(self):
        self.assertEqual(s.block_height({'blockID': s.GENESIS,
                                         'block_header': {'raw_data': {}}}), 0)
        self.assertEqual(s.block_height({'blockID': s.GENESIS,
                                         'block_header': {'raw_data': {'number': 0}}}), 0)
        for response in ({}, {'block_header': {'raw_data': {}}},
                         {'blockID': '0' * 64, 'block_header': {'raw_data': {'number': 0}}}):
            with self.assertRaisesRegex(RuntimeError, 'genesis'):
                s.block_height(response)

    def test_stop_child_escalates_only_its_process_group_and_reaps(self):
        class Child:
            pid = 4321
            waits = 0

            @staticmethod
            def poll():
                return None

            def wait(self, timeout):
                self.waits += 1
                if self.waits == 1:
                    raise s.subprocess.TimeoutExpired('candidate', timeout)
                return -s.signal.SIGKILL

        child = Child()
        with mock.patch.object(s.os, 'killpg') as killpg:
            self.assertEqual(s.stop_child(child), -s.signal.SIGKILL)
        self.assertEqual(killpg.call_args_list,
                         [mock.call(4321, s.signal.SIGTERM), mock.call(4321, s.signal.SIGKILL)])

    def test_python36_and_no_production_service_or_chain_mutation(self):
        ast.parse(PATH.read_text(), feature_version=(3, 6))
        source = PATH.read_text()
        for forbidden in ('systemctl', 'shutil.rmtree', 'os.rename(str(datadir'):
            self.assertNotIn(forbidden, source)


if __name__ == '__main__':
    unittest.main()
