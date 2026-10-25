#!/usr/bin/env python3
"""Verify immutable local checks, separately from pending native acceptance."""
import hashlib
import io
import json
import pathlib
import zipfile

P = pathlib.Path(__file__).resolve().parent
sha = lambda data: hashlib.sha256(data).hexdigest()
data = (P/'local-checks.zip').read_bytes()
assert len(data) == 243538 and sha(data) == '882826c94f3ec567e2a8c45b5ae8be30f35c6a80cd0feec37ea9e97df732cb89'
with zipfile.ZipFile(io.BytesIO(data)) as z:
    assert z.testzip() is None
    names = z.namelist()
    assert len(names) == len(set(names)) == 21
    manifest = json.loads(z.read('evidence-manifest.json'))
    assert set(names) == set(manifest) | {'evidence-manifest.json'}
    files = {name: z.read(name) for name in manifest}
    assert all(sha(files[name]) == digest for name, digest in manifest.items())
state = json.loads(files['local-status.json'])
assert state['complete'] and state['phase'] == 'complete' and not state['production_mutations']
assert len(state['checks']) == 6
assert {row['label'] for row in state['checks']} == {'page-benchmark', 'focused', 'packages', 'race', 'vet', 'sapling-build'}
for row in state['checks']:
    assert row['returncode'] == 0 and sha(files[row['label']+'.log']) == row['log_sha256']
current = json.loads(files['source-inventory.json'])
base = json.loads(files['baseline-source-inventory.json'])
assert len(current) == state['source_files'] == 2148 and len(base) == 2146
assert sha(files['source-inventory.json']) == state['source_inventory_sha256'] == 'dc5ec54833fe50341583394f732b9d587d59c5275c18fc2c11c11e5e9fbd7150'
changed = {name for name, digest in current.items() if base.get(name) != digest}
assert len(changed) == 4
assert all(sha(files['source/'+name]) == current[name] for name in changed)
assert b'allocations/page 138 -> 0; retained scratch 520476 B' in files['focused.log']
assert b'--- PASS: TestRecordColumnPublishFixture' in files['publish-harness-fixture.log']
assert b'--- SKIP: TestOfflineRecordColumnEncoderReusePublishServer' in files['publish-harness-fixture.log']
print(json.dumps(dict(local_checks=6, source_files=len(current), changed_go_files=4,
                      complete=True, native_acceptance_complete=False, production_mutations=False), indent=2))
