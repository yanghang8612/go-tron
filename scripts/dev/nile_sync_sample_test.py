#!/usr/bin/env python3
import json
import subprocess
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "dev" / "nile_sync_sample.sh"


class NileSampleHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        payloads = {
            "/wallet/getnowblock": {
                "blockID": "0000006400000000000000000000000000000000000000000000000000000000",
                "block_header": {"raw_data": {"number": 100}},
            },
            "/wallet/getnodeinfo": {"currentBlock": 105},
            "/wallet/listnodes": {"nodes": [{"address": "a"}, {"address": "b"}]},
        }
        payload = payloads.get(self.path)
        if payload is None:
            self.send_response(404)
            self.end_headers()
            return
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        return


class NileSyncSampleTest(unittest.TestCase):
    def test_sample_includes_sync_health_and_disk_ratios(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            datadir = tmpdir / "datadir"
            (datadir / "gtron" / "chaindata").mkdir(parents=True)
            (datadir / "gtron" / "ancient").mkdir(parents=True)
            (datadir / "gtron" / "state-snapshots").mkdir(parents=True)
            (datadir / "gtron" / "chaindata" / "hot.bin").write_bytes(b"h" * 2048)
            (datadir / "gtron" / "ancient" / "cold.bin").write_bytes(b"c" * 1024)
            (datadir / "gtron" / "state-snapshots" / "snap.bin").write_bytes(b"s" * 1024)

            server = ThreadingHTTPServer(("127.0.0.1", 0), NileSampleHandler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(server.shutdown)
            self.addCleanup(server.server_close)

            output = tmpdir / "samples.jsonl"
            start_unix = str(int(time.time()) - 50)
            proc = subprocess.run(
                [
                    str(SCRIPT),
                    "--datadir",
                    str(datadir),
                    "--http",
                    f"http://127.0.0.1:{server.server_address[1]}",
                    "--mode",
                    "full",
                    "--start-unix",
                    start_unix,
                    "--output",
                    str(output),
                ],
                cwd=REPO_ROOT,
                check=True,
                text=True,
                capture_output=True,
            )

            row = json.loads(proc.stdout.strip().splitlines()[-1])
            self.assertEqual(row["height"], 100)
            self.assertEqual(row["nodeInfoCurrentBlock"], 105)
            self.assertEqual(row["nodeInfoHeightDelta"], 5)
            self.assertEqual(row["sampleStatus"], "height-mismatch")
            self.assertEqual(row["peers"], 2)
            self.assertGreater(row["blocksPerSecond"], 0)
            self.assertGreater(row["blocksPerMinute"], 0)
            self.assertGreater(row["datadirBytes"], 0)
            self.assertGreater(row["chaindataBytes"], 0)
            self.assertGreater(row["coldArchiveBytes"], 0)
            self.assertGreater(row["bytesPerBlock"], 0)
            self.assertGreater(row["coldToHotBytesRatio"], 0)
            self.assertEqual(row["ancientFiles"], 1)
            self.assertEqual(row["snapshotFiles"], 1)
            self.assertEqual(row["coldArchiveFiles"], 2)
            self.assertEqual(output.read_text(encoding="utf-8").strip(), proc.stdout.strip())


if __name__ == "__main__":
    unittest.main()
