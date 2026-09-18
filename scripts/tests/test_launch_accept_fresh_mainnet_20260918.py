import base64
import importlib.util
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / 'scripts/launch_accept_fresh_mainnet_20260918.py'
spec = importlib.util.spec_from_file_location('fresh_accept_launcher', str(PATH))
s = importlib.util.module_from_spec(spec); spec.loader.exec_module(s)


class FreshAcceptLauncherTests(unittest.TestCase):
    def test_template_rebuilt_from_plan_filters_runtime_only_environment(self):
        rows = [b'GOGC=100', b'GOMEMLIMIT=8GiB',
                b'GTRON_SNAPSHOT_MANIFEST_CACHE_BUDGET_BYTES=536870912',
                b'INVOCATION_ID=secret-runtime-value']
        plan = {'process': {'argv': ['/old/gtron', '--datadir=/data/gtron/main/datadir'],
                            'environ': base64.b64encode(b'\0'.join(rows) + b'\0').decode()},
                'configuration': {'User': 'java-tron'}}
        self.assertEqual(s.planned_template(plan), {
            'argv': plan['process']['argv'], 'environment': s.MEMORY_ENV, 'user': 'java-tron'})

    def test_argv_uses_only_pinned_plan_values_and_computed_template_sha(self):
        plan = {'args': {'source_revision': 'a' * 40, 'script_revision': 'b' * 40,
                         'binary_sha256': 'c' * 64, 'prepared_sha256': 'd' * 64}}
        got = s.accept_argv(plan, 'e' * 64)
        self.assertEqual(got[0:2], ['/usr/bin/python3', str(s.ACCEPT)])
        self.assertEqual(got[got.index('--effective-argv-json') + 1], str(s.TEMPLATE))
        self.assertEqual(got[got.index('--effective-argv-sha256') + 1], 'e' * 64)
        self.assertNotIn('environment', repr(got))

    def test_duplicate_planned_environment_is_rejected(self):
        rows = [b'GOGC=100', b'GOGC=100', b'GOMEMLIMIT=8GiB',
                b'GTRON_SNAPSHOT_MANIFEST_CACHE_BUDGET_BYTES=536870912']
        plan = {'process': {'argv': ['/old/gtron'],
                            'environ': base64.b64encode(b'\0'.join(rows) + b'\0').decode()},
                'configuration': {'User': 'java-tron'}}
        with self.assertRaisesRegex(RuntimeError, 'ambiguous planned environment'):
            s.planned_template(plan)

    def test_launcher_has_no_operator_arguments_or_shell_execution(self):
        source = PATH.read_text()
        self.assertNotIn('argparse', source)
        self.assertNotIn('shell=True', source)
        self.assertIn("EXPECTED_PLAN_SHA256 = 'b66f87f8", source)
        self.assertIn("EXPECTED_TEMPLATE_SHA256 = '7ed4d5d9", source)
        self.assertEqual(s.ACCEPT,
                         s.RELEASE / 'ops/accept_fresh_mainnet_20260918.py')


if __name__ == '__main__':
    unittest.main()
