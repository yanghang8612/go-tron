from pathlib import Path
import base64
import hashlib
import json
import zlib

p = Path(__file__).resolve().parent
for name, expected in json.loads((p / 'sha256.json').read_text()).items():
    assert hashlib.sha256((p / name).read_bytes()).hexdigest() == expected, name
for name, expected in json.loads((p / 'transport-checks.json').read_text()).items():
    raw = zlib.decompress(base64.b64decode((p / name).read_text().strip()[7:]))
    assert len(raw) == expected['bytes']
    assert hashlib.sha256(raw).hexdigest() == expected['sha256']
    for filename, text in json.loads(raw).items():
        assert (p / filename).read_text() == text, filename
s = json.loads((p / 'status.json').read_text())
assert s['expected_identity'] and s['pid'] == 10692
assert s['old_inventory_unchanged'] and s['old_files'] == 27652
assert not s['sample_errors']
rows = [json.loads(line) for line in (p / 'runtime-samples.jsonl').read_text().splitlines()]
assert len(rows) == 45
for row in rows:
    assert row['identity']['pid'] == 10692
    assert row['identity']['sha256'] == s['binary_sha256']
    assert not row['errors'] and not row.get('new_error_lines')
    if 'disk_live_sample' in row:
        assert row['genesis_balance_correct'] and row['first_transaction_lookup_correct']
        assert all(probe['correct'] for probe in row['transfer_history_probes'])
        assert all(probe['correct'] for probe in row['tvm_counter_history_probes'])
d = json.loads((p / 'latency-check.json').read_text())
assert d['pid_before'] == d['pid_after'] == 'MainPID=10692'
assert len(d['rows']) == 9 and all(row['correct'] for row in d['rows'])
print('PASS: stable identity, old inventory, continuous historical probes and bounded latency recheck')
