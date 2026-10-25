#!/usr/bin/env python3
"""Verify staged source, every upload part and successful native checks."""
import base64
import hashlib
import io
import json
import pathlib
import zipfile

P = pathlib.Path(__file__).resolve().parent
sha = lambda data: hashlib.sha256(data).hexdigest()
data = (P/'native-checks.zip').read_bytes()
assert len(data) == 258813 and sha(data) == '4095c2f85999f5ec912a783a4495219769da5fab7f9a76daf2c3b728a5d78575'


def unpack(data, count):
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        assert z.testzip() is None
        names = z.namelist()
        assert len(names) == len(set(names)) == count
        manifest_name = 'payload-manifest.json' if 'payload-manifest.json' in names else 'evidence-manifest.json'
        manifest = json.loads(z.read(manifest_name))
        assert set(names) == set(manifest) | {manifest_name}
        files = {name: z.read(name) for name in manifest}
        assert all(sha(files[name]) == digest for name, digest in manifest.items())
    return files


files = unpack(data, 49)
payload = files['native-source-payload.zip']
assert len(payload) == 234428 and sha(payload) == '15d95a1b4e0d608029c714ae5108b827e0e3f7e6a7a6a952f2b29d1934e548d3'
source = unpack(payload, 12)
identity = json.loads(source['encoder-identity.json'])
assert identity['source_commit'] == '0071ef14ace30edf6a9420e254fe31916bd136e5' and not identity['source_tree_dirty']
current = json.loads(source['source-inventory.json'])
base = json.loads(source['base-inventory.json'])
assert len(current) == 2148 and len(base) == 2146
assert sha(source['source-inventory.json']) == identity['source_inventory_sha256'] == 'dc5ec54833fe50341583394f732b9d587d59c5275c18fc2c11c11e5e9fbd7150'
assert sha(source['base-inventory.json']) == identity['base_inventory_sha256'] == '2222de83d69b277003135e120b8e5dba2a9e6c06af07ef670ee204dcddf0bf98'
changed = {n for n, h in current.items() if base.get(n) != h}
assert len(changed) == 4 and changed == set(identity['changed_source_files'])
assert all(sha(source['source/'+name]) == current[name] for name in changed)


def receipt(name, remote):
    data = files[name]
    meta = json.loads(files[name+'.transport.json'])
    assert meta['name'] == remote and meta['nonce']
    assert meta['bytes'] == len(data) and meta['sha256'] == sha(data)
    return json.loads(data)


plan = json.loads(files['native-upload-plan.json'])
assert plan['bytes'] == len(payload) and plan['sha256'] == sha(payload) and len(plan['chunks']) == 12
for part in plan['chunks']:
    i = part['index']
    value = payload[i*20000:(i+1)*20000]
    row = receipt('server-upload-part-%03d.json' % i, 'upload-part-%03d.json' % i)
    assert row['index'] == i and row['bytes'] == part['bytes'] == len(value)
    assert row['sha256'] == part['sha256'] == sha(value)
start = receipt('server-native-start-receipt.json', 'native-start-receipt.json')
assert start['dispatched_now'] and start['proc_exists'] and start['pid'] == 11984
assert start['payload_sha256'] == sha(payload) and start['source_commit'] == identity['source_commit']
assert start['systemctl']['CPUQuotaPerSecUSec'] == '2s' and int(start['systemctl']['MemoryLimit']) == 4 << 30
assert start['systemctl']['Nice'] == '15' and not start['production_mutations']
assert start['production_identity_before'] == start['production_identity_after']
proof = receipt('server-native-source-verification.json', 'source-verification.json')
assert proof == dict(files=2148, tests=1061, inherited_extra_go_files=24, mismatches=[], private_source_inodes=True)
native = receipt('server-native-summary-04.json', 'native-summary.json')
assert native['complete'] and native['phase'] == 'complete' and native['native_checks_only']
assert native['source_commit'] == identity['source_commit'] and native['source_inventory_sha256'] == sha(source['source-inventory.json'])
assert not native['full_fixed_copy_acceptance_complete'] and not native['production_mutations']
labels = {'native-page-benchmark', 'native-focused', 'native-packages', 'native-race', 'native-vet',
          'native-sapling-build', 'native-snapshots-build', 'native-http-build',
          'native-old-publish-build', 'native-new-publish-build'}
assert len(native['checks']) == 10 and {r['label'] for r in native['checks']} == labels
assert all(r['returncode'] == 0 for r in native['checks'])
benchmark = receipt('server-native-page-benchmark-observation.json', 'native-page-benchmark-observation.json')
log = base64.b64decode(benchmark['base64'])
assert log == files['native-page-benchmark.log'] and len(log) == benchmark['bytes']
assert sha(log) == benchmark['sha256'] == next(r['log_sha256'] for r in native['checks'] if r['label'] == 'native-page-benchmark')
publish = receipt('server-publish-status-01.json', 'publish-status.json')
assert publish['samples'] == 8 and not publish['complete'] and not publish['production_mutations']
assert publish['old_binary_sha256'] == native['binaries']['old-publish.test']
assert publish['new_binary_sha256'] == native['binaries']['new-publish.test']
print(json.dumps(dict(native_checks=10, upload_parts=12, source_files=2148,
                      checks_complete=True, publication_complete_at_observation=False,
                      full_fixed_copy_acceptance_complete=False, production_mutations=False), indent=2))
