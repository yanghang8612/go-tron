#!/usr/bin/env python3
import contextlib
import importlib.util
import io
import sys
import unittest
from pathlib import Path


sys.dont_write_bytecode = True


def load_audit_module():
    path = Path(__file__).with_name("nile_19716962_audit.py")
    spec = importlib.util.spec_from_file_location("nile_19716962_audit", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def receipt_from_expected(expected):
    receipt = {}
    data = {"blockNumber": expected["blockNumber"], "fee": expected["fee"], "receipt": receipt}
    for field, value in expected.items():
        if field.startswith("receipt."):
            receipt[field.split(".", 1)[1]] = value
    return data


class Nile19716962AuditTest(unittest.TestCase):
    def run_audit(self, post_json):
        audit = load_audit_module()
        old_argv = sys.argv[:]
        sys.argv = [
            "nile_19716962_audit.py",
            "--gtron-url",
            "http://gtron",
            "--nile-url",
            "http://nile",
            "--socks5",
            "",
            "--nile-socks5",
            "",
        ]
        audit.post_json = post_json
        out = io.StringIO()
        try:
            with contextlib.redirect_stdout(out):
                code = audit.main()
        finally:
            sys.argv = old_argv
        return code, out.getvalue()

    def canonical_response(self, audit, method, payload):
        if method == "wallet/getnodeinfo":
            return audit.RpcResult(200, {"currentBlock": audit.FAIL_BLOCK}, "{}")
        if method == "wallet/getblockbynum":
            return audit.RpcResult(200, {"blockID": audit.FAIL_BLOCK_ID}, "{}")
        if method == "wallet/gettransactioninfobyid":
            txid = payload["value"]
            if txid == audit.FAIL_TX:
                return audit.RpcResult(
                    200,
                    {"blockNumber": audit.FAIL_BLOCK, "receipt": {"net_usage": 269}},
                    "{}",
                )
            for _, historical_txid, expected in audit.HISTORICAL_TXS:
                if txid == historical_txid:
                    return audit.RpcResult(200, receipt_from_expected(expected), "{}")
        raise AssertionError(f"unexpected RPC {method} {payload}")

    def test_passes_when_gtron_matches_public_nile(self):
        audit = load_audit_module()

        def post_json(base_url, method, payload, socks5, timeout):
            return self.canonical_response(audit, method, payload)

        code, out = self.run_audit(post_json)

        self.assertEqual(code, 0, out)
        self.assertIn("OK: gtron matches public Nile", out)
        self.assertNotIn("FAIL ", out)

    def test_fails_on_current_dirty_db_shape(self):
        audit = load_audit_module()
        dirty_receipts = {
            audit.HISTORICAL_TXS[0][1]: {
                "blockNumber": 19_555_385,
                "fee": 15_983_340,
                "receipt": {
                    "energy_fee": 14_963_340,
                    "origin_energy_usage": 140_631,
                    "energy_usage_total": 176_258,
                },
            },
            audit.HISTORICAL_TXS[1][1]: {
                "blockNumber": 19_713_603,
                "fee": 73_778_880,
                "receipt": {
                    "energy_fee": 72_854_880,
                    "origin_energy_usage": 13_200,
                    "energy_usage_total": 186_664,
                },
            },
        }

        def post_json(base_url, method, payload, socks5, timeout):
            if base_url.rstrip("/") == "http://nile":
                return self.canonical_response(audit, method, payload)
            if method == "wallet/getnodeinfo":
                return audit.RpcResult(200, {"currentBlock": audit.FAIL_BLOCK - 1}, "{}")
            if method == "wallet/getblockbynum":
                return audit.RpcResult(404, None, "block not found")
            if method == "wallet/gettransactioninfobyid":
                txid = payload["value"]
                if txid == audit.FAIL_TX:
                    return audit.RpcResult(200, {}, "{}")
                if txid in dirty_receipts:
                    return audit.RpcResult(200, dirty_receipts[txid], "{}")
            raise AssertionError(f"unexpected RPC {method} {payload}")

        code, out = self.run_audit(post_json)

        self.assertEqual(code, 1, out)
        self.assertIn("FAILED 10 check(s)", out)
        self.assertIn("gtron currentBlock", out)
        self.assertIn("gtron first owner overcharge fee", out)
        self.assertIn("gtron second owner overcharge receipt.origin_energy_usage", out)


if __name__ == "__main__":
    unittest.main()
