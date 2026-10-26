#!/usr/bin/env python3
"""Verify frozen local ordered-page checks without signing native acceptance."""
import hashlib
import io
import json
import pathlib
import zipfile

P = pathlib.Path(__file__).resolve().parent
sha = lambda data: hashlib.sha256(data).hexdigest()
data = (P/'local-checks.zip').read_bytes()
assert len(data) == 253817 and sha(data) == 'f9920a1cdf55b18149444dd997e578c237f28577f00f7a688f0ea7adb2186d3f'
with zipfile.ZipFile(io.BytesIO(data)) as z:
    assert z.testzip() is None
    names = z.namelist()
    assert len(names) == len(set(names)) == 23
    manifest = json.loads(z.read('evidence-manifest.json'))
    assert set(names) == set(manifest) | {'evidence-manifest.json'}
    files = {name: z.read(name) for name in manifest}
    assert all(sha(files[name]) == digest for name, digest in manifest.items())
state = json.loads(files['local-status.json'])
assert state['complete'] and state['phase'] == 'complete' and not state['production_mutations']
assert len(state['checks']) == 6
assert {r['label'] for r in state['checks']} == {'publication-fixture', 'focused', 'packages', 'race', 'vet', 'sapling-build'}
for row in state['checks']:
    assert row['returncode'] == 0 and sha(files[row['label']+'.log']) == row['log_sha256']
current = json.loads(files['source-inventory.json'])
base = json.loads(files['baseline-source-inventory.json'])
assert len(current) == 2149 and len(base) == 2148
assert sha(files['source-inventory.json']) == state['source_inventory_sha256'] == '3281b8fcd833df0066efba23ac68c6ed329f9a1534ea5be5c75002655b6e9150'
changed = {n for n,h in current.items() if base.get(n) != h}
assert len(changed) == 5 and all(sha(files['source/'+n]) == current[n] for n in changed)
assert b'--- PASS: TestOfflineColumnsOrderedLargeFixture' in files['publication-fixture.log']
assert b'--- FAIL: TestRecordColumnParallelCopiesAnnotations' in files['exploratory-new-tests-01.log']
assert b'--- PASS: TestRecordColumnParallelCopiesAnnotations' in files['exploratory-new-tests-02.log']
assert b'private worker scratch 575079 B; four-worker compression buffer bound 5992408 B' in files['focused.log']
assert any(row.split()[:2] == [b'ok', b'github.com/tronprotocol/go-tron/core/state/snapshots'] for row in files['race.log'].splitlines())
print(json.dumps(dict(local_checks=6, source_files=2149, changed_go_files=5,
                      max_compression_buffer_capacity_bytes=5992408,
                      native_acceptance_complete=False, production_mutations=False), indent=2))
