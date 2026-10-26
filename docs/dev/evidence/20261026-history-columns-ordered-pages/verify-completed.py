"""Independently recompute four-arm publication from raw rows and physical closure."""
import collections
import hashlib
import io
import json
import pathlib
import statistics
import zipfile

P = pathlib.Path(__file__).resolve().parent
sha = lambda b: hashlib.sha256(b).hexdigest()


def archive(path):
    blob = path.read_bytes()
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        names = z.namelist()
        assert z.testzip() is None and len(names) == len(set(names))
        manifest = json.loads(z.read('evidence-manifest.json'))
        assert set(names) == set(manifest) | {'evidence-manifest.json'}
        files = {n:z.read(n) for n in manifest}
        assert all(sha(files[n]) == h for n,h in manifest.items())
    return files


n = archive(P/'native-complete.zip')
assert sha((P/'publication-core.zip').read_bytes()) == 'dae16510d2c8e77c3483cd41970b2446183912f32ccf78bf68218c9249b64b91'
f = archive(P/'publication-core.zip')
native = json.loads(f['native-status.json'])
assert native['complete'] and not native.get('error') and not native['production_mutations']
assert len(native['checks']) == 10 and all(r['returncode'] == 0 for r in native['checks'])
assert {r['label'] for r in native['checks']} == {'native-focused','native-packages','native-race','native-vet','native-sapling-build','native-snapshots-build','native-http-build','native-old-publish-build','native-new-publish-build','native-large-publication-fixture'}
if n is not None:
    assert sha((P/'native-complete.zip').read_bytes()) == '6a0cd0ad3137575e9264d12a1ca31c76ccd082f1fe39e2af35dbdfe0865f2853'
    assert all(sha(n[r['label']+'.log']) == r['log_sha256'] for r in native['checks'])
assert not native['full_fixed_copy_acceptance_complete']
proof = json.loads(f['post-native-source-and-binaries.json'])
if n is not None:
    assert f['native-status.json'] == n['native-status.json']
    assert n['post-native-source-and-binaries.json'] == f['post-native-source-and-binaries.json']
for mode,count,digest in [('new',2149,'3281b8fcd833df0066efba23ac68c6ed329f9a1534ea5be5c75002655b6e9150'),('serial',2148,'dc5ec54833fe50341583394f732b9d587d59c5275c18fc2c11c11e5e9fbd7150')]:
    assert proof[mode]['product_files'] == count and proof[mode]['inherited_go_files'] == 24
    assert proof[mode]['inventory_sha256'] == digest and not proof[mode]['mismatches'] and not proof[mode]['unaccounted_go_files']
    assert proof[mode]['sapling_library_sha256'] == 'b6c99f8fabac36d5a836c644bb79059449c89740bf33670d66088d094447e7ea'
assert proof['binaries'] == native['binaries']
assert proof['native_unit']['MainPID'] == '0' and proof['native_unit']['ExecMainStatus'] == '0'
assert not proof['full_fixed_copy_acceptance_complete']
state = json.loads(f['publish-status.json'])
assert state['complete'] and not state.get('error') and not state['production_mutations']
assert state['samples'] == state['total_samples'] == 60 and state['timed_samples'] == 48
assert state['before_identity'] == state['after_identity']
assert state['before_identity']['MainPID'] == proof['production']['MainPID']
assert state['before_identity']['ActiveState'] == proof['production']['ActiveState']
assert sha(f['publish-samples.jsonl']) == state['samples_sha256']
assert sha(f['publish-summary.json']) == state['summary_sha256']
assert sha(f['native-status.json']) == state['native_status_sha256']
provenance = json.loads(f['unmeasured-plan-provenance.json'])
assert provenance['full_original_plan_sha256'] == state['old_plan_sha256'] == '177f2407b5af8584cf5f982466df60206d3ee8be3dd176cf2aebcb96f86bf78c'
assert provenance['full_original_plan_bytes'] == 8688213 and provenance['selected_count'] == 3
assert provenance['full_original_plan_retained_on_server'] and not provenance['full_original_plan_raw_in_this_core_archive']
assert provenance['full_evidence_archive_sha256'] == '6d922f4c5d5fd3acbbf13aab7f4858343eb23876a4ed2ca4e233c63e5cacbd41'
assert provenance['selected_sha256'] == sha(f['frozen-original-plan-selected.json'])
full_manifest = json.loads(f['full-evidence-manifest.json'])
assert all(sha(f[name]) == digest for name,digest in full_manifest.items() if name != 'frozen-original-plan.json')
assert full_manifest['frozen-original-plan.json'] == provenance['full_original_plan_sha256']
assert sha(f['frozen-original-samples.jsonl']) == state['old_samples_sha256'] == '3b0f46db9ff11a369427cedfbe8650fb580763b251866f7fbbe5657d09c85ebf'
rows = [json.loads(s) for s in f['publish-samples.jsonl'].splitlines()]
prior = [json.loads(s) for s in f['frozen-original-samples.jsonl'].splitlines()]
plan = json.loads(f['frozen-original-plan-selected.json'])['selected']
assert len(rows) == 60 and len(prior) == 30 and [p['Index'] for p in plan] == [458,2309,4147]
oracle = {(r['Pair']['Index'],r['Format']):r for r in prior}
expected_order = []
for group,pair in enumerate(plan):
    arms = [('old','2'),('old','5'),('new','2'),('new','5')]
    for round_index in range(5):
        offset = (group+round_index)%4
        order = arms[offset:]+arms[:offset]
        if round_index%2:
            order = list(reversed(order))
        expected_order += [(pair['Index'],arm,fmt,'pair-%d-round-%d-format-%s'%(group,round_index,fmt),round_index>0) for arm,fmt in order]
assert [(r['Pair']['Index'],r['Implementation'],r['Format'],r['Label'],r['Timed']) for r in rows] == expected_order
closure = json.loads(f['physical-output-closure.json'])
assert closure['complete'] and closure['references'] == len(closure['files']) == 180
files = {(r['implementation'],r['label'],r['path']):r for r in closure['files']}
assert len(files) == 180
refs = set()
groups = collections.defaultdict(list)
for r in rows:
    o = oracle[r['Pair']['Index'],r['Format']]
    for name in ['Pair','OutputBytes','CanonicalAndCompanionSHA','ColumnKeyModes','PlainPages']:
        assert r[name] == o[name]
    assert r['Seconds'] > 0 and r['CPUSeconds'] > 0 and r['AllocBytes'] > 0
    assert len(r['OutputRefs']) == 3
    assert {x['kind']:x['checksum'] for x in r['OutputRefs']} == {x['kind']:x['checksum'] for x in o['OutputRefs']}
    assert sum(x['size'] for x in r['OutputRefs']) == r['OutputBytes']
    for ref in r['OutputRefs']:
        key = (r['Implementation'],r['Label'],ref['path'])
        assert key not in refs
        refs.add(key)
        c = files[key]
        assert c['bytes'] == ref['size'] and c['kind'] == ref['kind'] and 'sha256:'+c['sha256'] == ref['checksum']
    if r['Timed']:
        groups[r['Pair']['Index'],r['Implementation'],r['Format']].append(r)
assert set(files) == refs and len(groups) == 12
summary = []
for (pair,arm,fmt),items in sorted(groups.items()):
    assert len(items) == 4
    summary.append(dict(pair_index=pair, implementation=arm, format=fmt, n=4,
                        median_seconds=statistics.median(r['Seconds'] for r in items),
                        min_seconds=min(r['Seconds'] for r in items), max_seconds=max(r['Seconds'] for r in items),
                        median_cpu_seconds=statistics.median(r['CPUSeconds'] for r in items),
                        median_alloc_bytes=statistics.median(r['AllocBytes'] for r in items),
                        median_gc=statistics.median(r['GCs'] for r in items), output_bytes=items[0]['OutputBytes'],
                        canonical_and_companion_sha=items[0]['CanonicalAndCompanionSHA'],
                        physical_output_sha={r['kind']:r['checksum'] for r in items[0]['OutputRefs']}))
assert summary == json.loads(f['publish-summary.json'])
bykey = {(r['pair_index'],r['implementation'],r['format']):r for r in summary}
effects = []
for pair in [458,2309,4147]:
    a,b,c,d = [bykey[pair,arm,fmt] for arm,fmt in [('old','2'),('old','5'),('new','2'),('new','5')]]
    effects.append(dict(pair_index=pair, serial_v2_seconds=a['median_seconds'], serial_v5_seconds=b['median_seconds'],
                        ordered_v2_seconds=c['median_seconds'], ordered_v5_seconds=d['median_seconds'],
                        ordered_v5_vs_serial_v5_percent=(d['median_seconds']/b['median_seconds']-1)*100,
                        ordered_v5_vs_ordered_v2_percent=(d['median_seconds']/c['median_seconds']-1)*100,
                        ordered_v2_vs_serial_v2_percent=(c['median_seconds']/a['median_seconds']-1)*100,
                        ordered_v5_cpu_vs_ordered_v2_percent=(d['median_cpu_seconds']/c['median_cpu_seconds']-1)*100,
                        ordered_v5_alloc_bytes=d['median_alloc_bytes']))
package_lines = [row.split() for row in n['native-packages.log'].splitlines()]
assert [row[:2] for row in package_lines] == [[b'ok',('github.com/tronprotocol/go-tron/'+pkg).encode()] for pkg in ['core/rawdb','core/state','core/state/snapshots','core','cmd/gtron']]
assert b'--- PASS: TestOfflineColumnsOrderedLargeFixture' in n['native-large-publication-fixture.log']
print(json.dumps(dict(native_checks_verified=10, native_raw_logs_retrieved=n is not None, full_original_plan_raw_retrieved=False, real_publications=60, timed_publications=48,
                      physical_outputs_verified=180, all_outputs_equal_original_frozen_oracle=True,
                      effects=effects, full_fixed_copy_acceptance_complete=False, production_mutations=False),indent=2))
