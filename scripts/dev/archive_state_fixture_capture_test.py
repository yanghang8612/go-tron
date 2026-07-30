#!/usr/bin/env python3
import json
import subprocess
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "dev" / "archive_state_fixture_capture.py"


class FixtureCaptureHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        try:
            request = json.loads(self.rfile.read(length).decode("utf-8"))
        except Exception:
            request = {}
        method = request.get("method")
        self.server.calls.append({"method": method, "params": request.get("params")})
        if method == "eth_getBalance":
            result = "not-hex" if getattr(self.server, "invalid_balance", False) else "0x2a"
        elif method == "eth_getCode":
            result = "0x60016000"
        elif method == "eth_getStorageAt":
            result = "0x" + "00" * 31 + "01"
        else:
            result = None
        body = json.dumps({"jsonrpc": "2.0", "id": request.get("id", 1), "result": result}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        return


class ArchiveStateFixtureCaptureTest(unittest.TestCase):
    def run_server(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), FixtureCaptureHandler)
        server.calls = []
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.shutdown)
        self.addCleanup(server.server_close)
        return server, f"http://127.0.0.1:{server.server_address[1]}"

    def test_captures_json_fixtures(self):
        server, endpoint = self.run_server()
        proc = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--jsonrpc",
                endpoint,
                "--block",
                "0x63",
                "--address",
                "0x410000000000000000000000000000000000000000",
                "--storage-slot",
                "0x01",
            ],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
        )

        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        row = json.loads(proc.stdout)
        self.assertEqual(row["archiveApiBlock"], 99)
        self.assertEqual(row["archiveApiBlockTag"], "0x63")
        self.assertEqual(row["archiveApiAddress"], "0x410000000000000000000000000000000000000000")
        self.assertEqual(row["archiveApiStorageSlot"], "0x01")
        self.assertEqual(row["archiveApiExpectedBalance"], "0x2a")
        self.assertEqual(row["archiveApiExpectedCode"], "0x60016000")
        self.assertEqual(row["archiveApiExpectedStorage"], "0x" + "00" * 31 + "01")
        self.assertEqual(
            server.calls,
            [
                {
                    "method": "eth_getBalance",
                    "params": ["0x410000000000000000000000000000000000000000", "0x63"],
                },
                {
                    "method": "eth_getCode",
                    "params": ["0x410000000000000000000000000000000000000000", "0x63"],
                },
                {
                    "method": "eth_getStorageAt",
                    "params": ["0x410000000000000000000000000000000000000000", "0x01", "0x63"],
                },
            ],
        )

    def test_outputs_shell_args(self):
        _, endpoint = self.run_server()
        proc = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--jsonrpc",
                endpoint,
                "--block",
                "99",
                "--address",
                "0x410000000000000000000000000000000000000000",
                "--format",
                "args",
            ],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
        )

        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("--archive-api-block 99", proc.stdout)
        self.assertIn("--archive-api-address 0x410000000000000000000000000000000000000000", proc.stdout)
        self.assertIn("--archive-api-storage-slot 0x0", proc.stdout)
        self.assertIn("--archive-api-expected-balance 0x2a", proc.stdout)
        self.assertIn("--archive-api-expected-code 0x60016000", proc.stdout)
        self.assertIn("--archive-api-expected-storage 0x0000000000000000000000000000000000000000000000000000000000000001", proc.stdout)

    def test_rejects_invalid_hex_fixture(self):
        server, endpoint = self.run_server()
        server.invalid_balance = True
        proc = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--jsonrpc",
                endpoint,
                "--block",
                "99",
                "--address",
                "0x410000000000000000000000000000000000000000",
            ],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
        )

        self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("eth_getBalance result 'not-hex' is not a non-empty 0x hex quantity", proc.stderr)

    def test_rejects_invalid_address(self):
        proc = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--jsonrpc",
                "http://127.0.0.1:1",
                "--block",
                "99",
                "--address",
                "not-hex",
            ],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("--address must be a non-empty 0x hex string", proc.stderr)


if __name__ == "__main__":
    unittest.main()
