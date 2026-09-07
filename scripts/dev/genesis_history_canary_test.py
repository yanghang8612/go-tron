import copy
import importlib.util
from pathlib import Path
import unittest


SPEC = importlib.util.spec_from_file_location("canary", Path(__file__).with_name("genesis_history_canary.py"))
canary = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(canary)
ADDRESS = "0x" + "ab" * 20


class FakeClient:
    def __init__(self):
        self.pruned = 0
        self.amounts = {9: 100, 10: 80, 19: 80, 20: 50}
        self.hash_suffix = ""

    def frontiers(self):
        return {"head": 200, "solidified": 190, "published": self.pruned, "pruned": self.pruned}

    def block(self, number, full=False):
        return {"number": hex(number), "hash": "0x" + format(number, "064x") + self.hash_suffix,
                "transactions": [{"from": ADDRESS, "to": "0x" + "00" * 20,
                                  "value": "0x14", "input": "0x", "hash": "hint"}]}

    def rpc(self, method, params):
        if method != "eth_getBalance":
            raise AssertionError("unexpected method")
        return hex(self.amounts[int(params[1], 16)] * canary.SUN_SCALE)


class CanaryTests(unittest.TestCase):
    def test_capture_proves_two_changes_and_verify_needs_completed_prune(self):
        client = FakeClient()
        baseline = canary.discover(client, 10, 2, 10)
        self.assertEqual((baseline["b"], baseline["c"]), (10, 20))
        self.assertEqual(baseline["series"]["balances"]["10"]["sun"], 80)
        with self.assertRaisesRegex(ValueError, "not ready"):
            canary.verify(client, baseline)
        client.pruned = 20
        self.assertEqual(canary.verify(client, baseline)["kind"], "post_prune_balance_canary_pass")

    def test_balance_or_hash_mutation_rejected(self):
        client = FakeClient()
        baseline = canary.discover(client, 10, 2, 10)
        client.pruned = 20
        client.amounts[10] = 79
        with self.assertRaisesRegex(ValueError, "mismatch"):
            canary.verify(client, baseline)
        client.amounts[10] = 80
        client.hash_suffix = "changed"
        with self.assertRaisesRegex(ValueError, "mismatch"):
            canary.verify(client, baseline)

    def test_candidate_appearances_without_two_balance_changes_not_accepted(self):
        client = FakeClient()
        client.amounts[20] = client.amounts[19]
        with self.assertRaisesRegex(ValueError, "no proven"):
            canary.discover(client, 10, 2, 10)

    def test_baseline_already_pruned_is_rejected(self):
        client = FakeClient()
        baseline = canary.discover(client, 10, 2, 10)
        bad = copy.deepcopy(baseline)
        bad["before"]["pruned"] = 10
        with self.assertRaisesRegex(ValueError, "not a pre-prune"):
            canary.verify(client, bad)

    def test_rpc_write_method_is_rejected_before_request(self):
        client = canary.Client(8545, 6071)
        with self.assertRaisesRegex(ValueError, "whitelisted"):
            client.rpc("eth_sendRawTransaction", [])
        self.assertEqual(client.calls, 0)


if __name__ == "__main__":
    unittest.main()
