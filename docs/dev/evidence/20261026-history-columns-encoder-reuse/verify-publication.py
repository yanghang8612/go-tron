#!/usr/bin/env python3
"""Recompute all 45 publication samples and bind their exact output bytes."""
import base64
import collections
import hashlib
import io
import json
import pathlib
import statistics
import zipfile

P = pathlib.Path(__file__).resolve().parent
sha = lambda data: hashlib.sha256(data).hexdigest()


def unpack(data, count):
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        assert z.testzip() is None
        names = z.namelist()
        assert len(names) == len(set(names)) == count
        manifest = json.loads(z.read('evidence-manifest.json'))
        assert set(names) == set(manifest) | {'evidence-manifest.json'}
        files = {name: z.read(name) for name in manifest}
        assert all(sha(files[name]) == digest for name, digest in manifest.items())
    return files


data = (P/'publication-complete.zip').read_bytes()
assert len(data) == 100613 and sha(data) == '221ec43bd415177631a1649f5b3cedb138cdb80166245b5508fbb4d1c6ae002b'
files = unpack(data, 32)


def receipt(name, remote):
    data = files[name]
    meta = json.loads(files[name+'.transport.json'])
    assert meta['name'] == remote and meta['nonce']
    assert meta['nonce'] not in nonces
    nonces.add(meta['nonce'])
    assert meta['bytes'] == len(data) and meta['sha256'] == sha(data)
    return json.loads(data)


pieces, identity, nonces = [], None, set()
for i in range(10):
    row = receipt('server-publish-zip-part-%03d.json' % i, 'publish-zip-part-%03d.json' % i)
    value = base64.b64decode(row['base64'])
    assert row['name'] == 'gtron-columns-encoder-reuse-publish-20261026-01.zip'
    assert row['index'] == i and row['offset'] == sum(map(len, pieces))
    assert row['file_bytes'] == 39422 and row['bytes'] == len(value) and row['sha256'] == sha(value)
    assert row['file_sha256'] == '1abce32c17cb3c988f95b93ff4c6a0e1bf965845557bd34d8dda37c831d64b4a'
    stat = tuple(row[k] for k in ('device', 'inode', 'mtime_ns'))
    assert all(isinstance(v, str) and v.isdigit() for v in stat)
    identity = identity or stat
    assert identity == stat
    pieces.append(value)
inner = b''.join(pieces)
assert inner == files['server-publication-final.zip'] and len(inner) == 39422
assert sha(inner) == '1abce32c17cb3c988f95b93ff4c6a0e1bf965845557bd34d8dda37c831d64b4a'
raw = unpack(inner, 16)
status = json.loads(raw['publish-status.json'])
assert status == receipt('server-publish-status-04.json', 'publish-status.json')
assert status['complete'] and status['samples'] == status['total_samples'] == 45 and status['timed_samples'] == 36
assert status['all_physical_outputs_equal_frozen_reference'] and not status['production_mutations']
assert status['before_identity'] == status['after_identity']
assert sha(raw['publish-samples.jsonl']) == status['samples_sha256'] == '152667ae8e5c60910b2fdcadb18d85cadde2b14d920a3051ffba12f689711f66'
assert sha(raw['publish-summary.json']) == status['summary_sha256'] == 'eaa7409269b6b255d056f0f7810ff8fafc1bec373f83978c358e7289b3423ea0'
assert sha(raw['native-status.json']) == status['native_status_sha256'] == 'f75d69005165e972772231a2b85b837a56b896c0b1ad57a0f695f0681457a87e'
native = json.loads(raw['native-status.json'])
assert native['complete'] and all(r['returncode'] == 0 for r in native['checks']) and len(native['checks']) == 10
assert native['binaries']['old-publish.test'] == status['old_binary_sha256']
assert native['binaries']['new-publish.test'] == status['new_binary_sha256']
proof = json.loads(raw['publish-final-source-proof.json'])
for arm, count, checked in [('old', 2145, 2169), ('policy', 2146, 2170), ('new', 2148, 2172)]:
    source = proof['sources'][arm]
    assert source['product_source_files'] == count and source['checked_files'] == checked
    assert source['mismatches'] == source['unaccounted_go_files'] == []
    assert source['sapling_library_sha256'] == 'b6c99f8fabac36d5a836c644bb79059449c89740bf33670d66088d094447e7ea'
assert proof['binaries'] == native['binaries']
assert proof['private_unit']['ExecMainStatus'] == '0' and proof['private_unit']['MainPID'] == '0'
assert proof['production_identity']['MainPID'] == '0' and not proof['production_mutations']

prior_raw = files['prior-frozen-publish-samples.jsonl']
assert sha(prior_raw) == status['old_samples_sha256'] == '3b0f46db9ff11a369427cedfbe8650fb580763b251866f7fbbe5657d09c85ebf'
prior = {(r['Pair']['Index'], r['Format']): r for r in map(json.loads, prior_raw.splitlines())}
samples = list(map(json.loads, raw['publish-samples.jsonl'].splitlines()))
assert len(samples) == 45 and sum(r['Timed'] for r in samples) == 36
groups = collections.defaultdict(list)
for row in samples:
    baseline = prior[(row['Pair']['Index'], row['Format'])]
    assert row['Pair'] == baseline['Pair'] and row['OutputBytes'] == baseline['OutputBytes']
    assert row['CanonicalAndCompanionSHA'] == baseline['CanonicalAndCompanionSHA']
    assert row['ColumnKeyModes'] == baseline['ColumnKeyModes'] and row['PlainPages'] == baseline['PlainPages']
    output = lambda r: {v['kind']: v['checksum'] for v in r['OutputRefs']}
    assert output(row) == output(baseline)
    assert (row['Implementation'], row['Format']) in {('old', '2'), ('old', '5'), ('new', '5')}
    if row['Timed']:
        groups[(row['Pair']['Index'], row['Implementation'], row['Format'])].append(row)
summary = json.loads(raw['publish-summary.json'])
assert len(summary) == len(groups) == 9
for row in summary:
    values = groups[(row['pair_index'], row['implementation'], row['format'])]
    assert len(values) == row['n'] == 4
    for metric, source in [('median_seconds', 'Seconds'), ('median_cpu_seconds', 'CPUSeconds'),
                           ('median_alloc_bytes', 'AllocBytes'), ('median_gc', 'GCs')]:
        assert row[metric] == statistics.median(v[source] for v in values)
    assert row['min_seconds'] == min(v['Seconds'] for v in values)
    assert row['max_seconds'] == max(v['Seconds'] for v in values)
    assert row['canonical_and_companion_sha'] == values[0]['CanonicalAndCompanionSHA']
    assert row['physical_output_sha'] == output(values[0])
ratios = []
for pair in (458, 2309, 4147):
    new = next(r for r in summary if r['pair_index'] == pair and r['implementation'] == 'new')
    old = next(r for r in summary if r['pair_index'] == pair and r['implementation'] == 'old' and r['format'] == '5')
    plain = next(r for r in summary if r['pair_index'] == pair and r['format'] == '2')
    assert plain['max_seconds'] < new['min_seconds'] < new['max_seconds'] < old['min_seconds']
    ratios.append(dict(pair=pair, new_vs_old_percent=100*(new['median_seconds']/old['median_seconds']-1),
                       new_vs_v2_percent=100*(new['median_seconds']/plain['median_seconds']-1),
                       allocation_saved_bytes=old['median_alloc_bytes']-new['median_alloc_bytes']))
print(json.dumps(dict(samples=45, timed=36, physical_outputs_equal=True, exact_summary_groups=9,
                      ratios=ratios, new_full_muhash_http_restore_complete=False,
                      production_mutations=False), indent=2))
