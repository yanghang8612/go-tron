"""Independently verify full final-binary correctness and complete HTTP matrix."""
import collections
import hashlib
import io
import json
import pathlib
import statistics
import tarfile
import zipfile

P = pathlib.Path(__file__).resolve().parent
sha = lambda b: hashlib.sha256(b).hexdigest()
# Both producer receipts are retained; the solid archive has all 704 exact members.
original = json.loads((P/'server-final-acceptance-seal.json').read_text())
seal = json.loads((P/'server-final-acceptance-solid-seal.json').read_text())
assert original['complete'] and not original['production_mutations'] and len(original['seals']) == 1
assert seal['complete'] and not seal['production_mutations'] and seal['all_members_exactly_preserved']
assert seal['archive'] == 'final-acceptance-core.tar.xz' and seal['members'] == 704
assert seal['original_zip_bytes'] == original['seals'][0]['bytes'] == 1242954
assert seal['original_zip_sha256'] == original['seals'][0]['sha256'] == '26641ef303359d752ee8ce4399828a9f91ec211d025b6133a5aacfc95e7890dd'
b = (P/seal['archive']).read_bytes()
assert len(b) == seal['bytes'] == 467028
assert sha(b) == seal['sha256'] == '80f824b2c934e80861630356bc97829230c079b7dcec5e5fcfc1f063cfe00f3d'
with tarfile.open(fileobj=io.BytesIO(b),mode='r:xz') as t:
    members = t.getmembers()
    names = [x.name for x in members]
    assert len(names) == len(set(names)) == seal['members']
    assert all(x.isfile() and not pathlib.PurePosixPath(x.name).is_absolute() and '..' not in pathlib.PurePosixPath(x.name).parts for x in members)
    all_files = {x.name:t.extractfile(x).read() for x in members}
    m = json.loads(all_files.pop('evidence-manifest.json'))
    assert set(all_files) == set(m)
    assert all(sha(all_files[n]) == h for n,h in m.items())
    f = all_files
# Recreate the exact original ZIP framing, then require its original whole-file SHA.
original_files = dict(f)
with tarfile.open(fileobj=io.BytesIO(b),mode='r:xz') as t:
    original_files['evidence-manifest.json'] = t.extractfile('evidence-manifest.json').read()
zip_out = io.BytesIO()
with zipfile.ZipFile(zip_out,'w',zipfile.ZIP_DEFLATED) as z:
    for name,data in sorted(original_files.items()):
        info = zipfile.ZipInfo(name,date_time=(2026,10,26,0,0,0))
        info.compress_type = zipfile.ZIP_DEFLATED
        info.external_attr = 0o100644 << 16
        z.writestr(info,data)
original_zip = zip_out.getvalue()
assert len(original_zip) == 1242954 and sha(original_zip) == '26641ef303359d752ee8ce4399828a9f91ec211d025b6133a5aacfc95e7890dd'
with zipfile.ZipFile(io.BytesIO(original_zip)) as z:
    assert z.testzip() is None and len(z.namelist()) == 704
    assert all(z.read(name) == data for name,data in original_files.items())
s = json.loads(f['acceptance-status.json'])
n = json.loads(f['native-status.json'])
assert s['complete'] and s['phase'] == 'complete' and not s.get('error') and not s['production_mutations']
assert not s['full_latest_snapshot_export_import_performed']
assert s['before_identity'] == s['after_identity']
assert len(s['checks']) == 3 and {x['label'] for x in s['checks']} == {'full-muhash','history-restore','full-http-comparison'}
assert all(x['returncode'] == 0 and sha(f[x['label']+'.log']) == x['log_sha256'] for x in s['checks'])
assert s['baseline_binary_sha256'] == 'fff5e1dd9f51fdaa3fb25f5a93d73147178403890e95bdc5a0fb32d9b741d4bf'
assert s['candidate_binary_sha256'] == n['binaries']['ordered-http.test'] == '1258ec43a1500101193cc79bb40996cd488bac8169c1cb7fa9bd1529063c54f2'
assert n['binaries']['ordered-snapshots.test'] == '541fd9e1be5d88932e744d02397673406c2809183f7033cb2b6bc9c8dc7e5f0d'
assert sha(f['source-inventory.json']) == '3281b8fcd833df0066efba23ac68c6ed329f9a1534ea5be5c75002655b6e9150'
assert len(json.loads(f['source-inventory.json'])) == 2149 and len(json.loads(f['inherited-extra-go-inventory.json'])) == 24
extra = json.loads(f['inherited-extra-go-inventory.json'])
for name in ['core/offline_fixed_http_test.go','core/offline_request_muhash_test.go','core/state/snapshots/offline_routing_test.go']:
    assert sha(f['harness-source/'+name]) == extra[name]
assert sha(f['harness-source/core/offline_fixed_http_test.go']) == s['http_harness_sha256'] == '10e6015af7666bf83b5c405543681c368b7537f428b8278a3344f0312aacd3bf'
assert s['source_before'] == s['source_after']
root = '8fbf149684146a59b1650b318a768f0edbae0c8c1c3ceb2c5c610129c10f76a5'
census = json.loads(f['muhash-census.json'])
assert census['complete'] and census['muhash_verified'] and bytes(census['muhash_root']).hex() == root
assert census['block_num'] == 11818192 and census['tx_num'] == 601386177
assert census['source_rows'] == [3484117,481,141418493]
assert f['chaindata-before.json'] == f['chaindata-after.json']
after = json.loads(f['chaindata-after.json'])
assert set(after) == {'baseline','candidate'} and all(len(rows) == 803 for rows in after.values())
assert s['chaindata_full_sha_unchanged'] and s['fingerprint_files_per_mode'] == 803
history = json.loads(f['history-restore.json'])
assert history['complete'] and len(history['windows']) == 3
assert [w['from_block'] for w in history['windows']] == [3144020,5000000,9000000]
assert [w['rows'] for w in history['windows']] == [387,11845,10264]
for w in history['windows']:
    assert w['to_block'] == w['from_block']+19 and w['range_rows_verified'] == 20
    assert w['full_row_sha256'] == w['candidate_row_sha256'] == w['restored_row_sha256']
    assert w['result']['ChangesRestored'] == w['rows'] and w['result']['TxRangesRestored'] == 20
    assert w['result']['FromTxNum'] == w['from_tx'] and w['result']['ToTxNum'] == w['to_tx']
Q = 'tail-read-comparison/'
cs = json.loads(f[Q+'comparison-status.json'])
assert cs['complete'] and cs['requests'] == 286 and cs['all_responses_correct'] and not cs['environment_identity_changed']
assert cs['before_identity'] == cs['after_identity']
assert cs['baseline_binary_sha256'] == s['baseline_binary_sha256'] and cs['candidate_binary_sha256'] == s['candidate_binary_sha256']
expected = {hex(h):'0x%064x'%v for h,v in [(3144020,6),(3144021,7),(5000000,63),(9000000,207),(11817936,225),(11818191,225)]}
expected['latest'] = '0x%064x'%225
specs = json.loads(f[Q+'specs.json'])
assert len(specs) == 7 and {x['tag']:x['expected'] for x in specs} == expected
rows = [json.loads(x) for x in f[Q+'requests.jsonl'].splitlines()]
assert len(rows) == 286 and [x['id'] for x in rows] == list(range(1,287))
for row in rows:
    assert row['mode'] in ('baseline','candidate') and row['tag'] in expected
    assert row['correct'] and row['expected'] == expected[row['tag']]
    assert row['response']['jsonrpc'] == '2.0' and row['response']['id'] == row['id']
    assert 'error' not in row['response'] and row['response']['result'] == row['expected']
    assert row['height'] == (11818192 if row['tag'] == 'latest' else int(row['tag'],16))
    assert row['milliseconds'] >= 0
server_metrics = {}
profile_metric_counts = collections.Counter()
metric_files = [name for name in f if name.startswith(Q) and name.endswith('/server-requests.jsonl')]
assert len(metric_files) == 122
for name in metric_files:
    prefix = name[:-len('server-requests.jsonl')]
    for line in f[name].splitlines():
        metric = json.loads(line)
        assert metric['method'] == 'eth_call' and len(metric['params']) == 2
        assert metric['params'][0]['to'] == '0xa4997124a328a4615eab437cede31da47a31b375'
        assert metric['params'][0]['data'] == '0x9fa6a6e3'
        assert metric['elapsed_ns'] >= 0 and metric['alloc_bytes'] >= 0
        if metric['id'] == 999999:
            profile_metric_counts[prefix] += 1
            continue
        assert metric['id'] not in server_metrics
        server_metrics[metric['id']] = metric
assert set(server_metrics) == set(range(1,287))
for row in rows:
    metric = server_metrics[row['id']]
    assert metric['params'][1] == row['tag']
    assert row['milliseconds']+0.01 >= metric['elapsed_ns']/1e6
    metric['cpu_ms'] = sum((metric['usage_after'][k]['Sec']-metric['usage_before'][k]['Sec'])*1000+(metric['usage_after'][k]['Usec']-metric['usage_before'][k]['Usec'])/1000 for k in ['Utime','Stime'])
    io_before = {k:int(v) for k,v in (line.split(':',1) for line in metric['io_before'].splitlines())}
    io_after = {k:int(v) for k,v in (line.split(':',1) for line in metric['io_after'].splitlines())}
    metric['physical_read_bytes'] = io_after['read_bytes']-io_before['read_bytes']
    metric['read_chars'] = io_after['rchar']-io_before['rchar']
    assert metric['cpu_ms'] >= 0 and metric['physical_read_bytes'] >= 0 and metric['read_chars'] >= 0
counts = collections.Counter(x['cache'] for x in rows)
assert counts == {'discovery-excluded':14,'warmup-excluded':28,'warm':140,'fresh-process-os-uncontrolled':42,'private-os-cold-fresh-process':56,'trace-excluded':4,'profile-warmup-excluded':2}
groups = collections.defaultdict(list)
for row in rows:
    if row['cache'] in ('warm','fresh-process-os-uncontrolled','private-os-cold-fresh-process'):
        groups[row['mode'],row['cache'],row['tag']].append(row['milliseconds'])
summary = [dict(mode=a,cache=b,tag=c,n=len(v),median_ms=statistics.median(v),min_ms=min(v),max_ms=max(v)) for (a,b,c),v in sorted(groups.items())]
assert len(summary) == 42 and summary == json.loads(f[Q+'summary.json'])
for row in summary:
    assert row['n'] == {'warm':10,'fresh-process-os-uncontrolled':3,'private-os-cold-fresh-process':4}[row['cache']]
ready_files = [name for name in f if name.startswith(Q) and name.endswith('/ready.json')]
assert len(ready_files) == 122
for name in ready_files:
    ready = json.loads(f[name]);prefix = name[:-len('ready.json')]
    env = json.loads(f[prefix+'environment.json'])
    resources = json.loads(f[prefix+'process-resources.json'])
    assert ready['read_only'] and ready['gomaxprocs'] == 2 and ready['head'] == 11818192 and ready['head_tx'] == 601386177
    assert ready['hot_cache_mib'] == ready['history_cache_mib'] == 512
    assert bytes.fromhex(ready['root']).decode() == root
    assert env['production_start'] == env['production_end'] == cs['before_identity']
    assert resources['pid'] == ready['pid'] and not resources.get('process_disappeared')
proofs = [json.loads(v) for k,v in f.items() if k.startswith(Q+'os-cold-') and k.endswith('-residency.json')]
assert len(proofs) == 56
for proof in proofs:
    assert proof['pages'] > 0 and 0 <= proof['resident_pages'] <= proof['pages']
    assert proof['fraction'] == proof['resident_pages']/proof['pages'] and proof['fraction'] < .01
    assert 1 <= proof['attempt'] <= 8
for mode in ['baseline','candidate']:
    readset = json.loads(f[Q+mode+'-read-files.json'])
    private = json.loads(f[Q+mode+'-private-files.json'])
    assert len(readset) == len(set(readset)) == len(private)
    assert set(readset) == {x['path'] for x in private}
    assert all(x['source_inode'] != x['private_inode'] and len(x['sha256']) == 64 and x['bytes'] >= 0 for x in private)
    assert all(x['source'].startswith('/data/gtron/history-columns-ordered-pages-20261026-01/'+mode+'-input/') for x in private)
    workload = json.loads(f[Q+'nine-million-profile-'+mode+'/workload.json'])
    assert workload['responses_correct'] and workload['requests'] > 0
    assert profile_metric_counts[Q+'nine-million-profile-'+mode+'/'] == workload['requests']
closure = json.loads(f['final-source-and-evidence-closure.json'])
assert closure['complete'] and not closure['production_mutations'] and not closure['full_latest_snapshot_export_import_performed']
assert closure['acceptance_unit']['MainPID'] == '0' and closure['acceptance_unit']['ExecMainStatus'] == '0'
assert closure['source_files'] == 2149 and closure['inherited_go_files'] == 24
assert closure['binaries'] == n['binaries']
assert closure['chaindata_before_sha256'] == closure['chaindata_after_sha256'] == sha(f['chaindata-before.json'])
assert len(closure['traces_retained_on_server']) == 18 and all(x['retained_on_server'] for x in closure['traces_retained_on_server'])
effects = []
for cache in ['warm','fresh-process-os-uncontrolled','private-os-cold-fresh-process']:
    a,b = [next(x for x in summary if x['mode'] == mode and x['cache'] == cache and x['tag'] == hex(9000000)) for mode in ['baseline','candidate']]
    resource = {}
    for mode in ['baseline','candidate']:
        selected = [server_metrics[row['id']] for row in rows if row['mode'] == mode and row['cache'] == cache and row['tag'] == hex(9000000)]
        resource[mode] = {key:statistics.median(v[key] for v in selected) for key in ['cpu_ms','alloc_bytes','physical_read_bytes','read_chars']}
    effects.append(dict(cache=cache,baseline_median_ms=a['median_ms'],candidate_median_ms=b['median_ms'],percent=(b['median_ms']/a['median_ms']-1)*100,request_resources=resource))
print(json.dumps(dict(full_muhash_verified=True,muhash_root=root,source_rows=census['source_rows'],unchanged_fingerprints=1606,
                      historical_restored_blocks=60,historical_restored_rows=22496,all_http_responses_correct=286,
                      timed_http_requests=238,server_metric_rows_verified=286,summary_groups=42,private_cold_proofs=56,
                      max_pre_startup_resident_fraction=max(x['fraction'] for x in proofs),hosts=122,
                      nine_million_http_effects=effects,full_latest_snapshot_export_import_performed=False,
                      raw_traces_retained_on_server=18,all_704_original_members_retrieved=True,original_zip_bytes_reconstructed_exactly=True,production_mutations=False),indent=2))
