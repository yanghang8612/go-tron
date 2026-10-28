"""Reconcile authenticated native census and projections, without scanning state.

Target membership/key union counts are native-study measurements, not an
independent replay of source dictionaries. Layout formulas, pair identities and
frozen coverage are recomputed here; no projection is an HTTP benchmark.
"""
import collections,hashlib,json,pathlib,statistics,struct,sys,zipfile

archive=pathlib.Path(sys.argv[1]);out=pathlib.Path(sys.argv[2]);out.mkdir(parents=True,exist_ok=True)
z=zipfile.ZipFile(archive);assert z.testzip() is None
s=json.loads(z.read('candidate-status.json'));d=json.loads(z.read('distribution.json'))
assert s['complete'] and not s['production_mutations'] and not s['index_outputs']
assert s['production_before']==s['production_after']
assert d['complete'] and d['manifest_unchanged'] and d['projections_only_not_built_or_benchmarked']
raw=z.read('input-manifest.json');assert hashlib.sha256(raw).hexdigest()==d['manifest_sha256'];m=json.loads(raw)
ref,=[r for r in m['segments'] if r['kind']=='account-history-routing']
b=z.read('account-base-metadata.bin')
assert 'sha256:'+hashlib.sha256(b).hexdigest()==ref['lookupRoot'] and b[:8]==b'GTACRT01'
n=struct.unpack('>I',b[12:16])[0];dirs=struct.unpack('>Q',b[32:40])[0]
assert len(b)==64+n*32+dirs*53
covered={b[64+32*i:96+32*i] for i in range(n)};assert len(covered)==n
def pair(h,a):
    digest=hashlib.sha256(b'go-tron/system-history-routing/pair/v1\0')
    for r in [h,a]:
        digest.update(struct.pack('>QQQQQH',r['fromTxNum'],r['toTxNum'],r.get('aggregationSteps',0) or 1,r['size'],r.get('rows',0),r.get('domain',0)))
        for v in [r.get('dataset',''),r['kind'],r['path'],r.get('checksum','')]:
            v=v.encode();digest.update(struct.pack('>Q',len(v)));digest.update(v)
    return digest.digest()
by_path=collections.defaultdict(list)
for r in m['segments']:by_path[r['path']].append(r)
unknown=[]
for h in m['segments']:
    if h['kind']!='history' or h.get('dataset')!='state-domain-change':continue
    a,=[r for r in by_path[str(pathlib.PurePosixPath(h['path']).with_suffix('.kv'))] if r['kind']=='accessor' and r.get('dataset')==h['dataset'] and r['fromTxNum']==h['fromTxNum'] and r['toTxNum']==h['toTxNum'] and (r.get('aggregationSteps',0) or 1)==(h.get('aggregationSteps',0) or 1)]
    if pair(h,a) not in covered:unknown.append((h,a))
rows=d['rows'];assert len(rows)==len(unknown)==d['sources']==1521
for row,(h,a) in zip(rows,unknown):
    assert row['source']==h and row['accessor']==a and row['pair_id']==pair(h,a).hex()
    count=row['account_keys']+row['generation_keys']
    if not row['unsupported'] and not row['hash_budget_excluded']:
        size=0 if not count else 1<<(max(64,count*2)-1).bit_length()
        assert size==row['projected_filter_bytes_16bits']
        assert all(not yes or maybe for yes,maybe in zip(row['target_present'],row['target_may_contain']))
def io(text):return {k:int(v) for k,v in (line.split(':') for line in text.splitlines())}
for row in rows:
    a,b=io(row['io_before']),io(row['io_after']);row['io_delta']={k:b[k]-a[k] for k in a}
groups=d['groups'];assert len(groups)==24
for i,g in enumerate(groups):
    chunk=rows[i*64:(i+1)*64];assert g['sources']==len(chunk)
    assert (g['from'],g['to'])==(chunk[0]['source']['fromTxNum'],chunk[-1]['source']['toTxNum'])
    assert g['incomplete_due_to_unsupported']==any(r['unsupported'] for r in chunk)
    n=g['unique_account_keys'];pages=(n+140)//141;dirs=(pages+76)//77;meta=64+32*len(chunk)+53*dirs
    assert g['root_metadata_bytes']==meta and g['projected_file_bytes']==meta+(dirs+pages)*4096
    assert n<=sum(r['account_keys']+r['generation_keys'] for r in chunk)
    if not g['overflow_lower_bound']:assert n>=max(r['account_keys']+r['generation_keys'] for r in chunk)
    needs=[any(not r['target_skipped_by_existing_routes'][j] for r in chunk) for j in range(4)]
    for j,q in enumerate(g['lookup_projections']):
        assert q['target']==j and q['needed_after_existing_routes']==needs[j]
        assert q['present_in_group']==any(r['target_present'][j] for r in chunk)
        assert q['max_source_to_if_present']==max([r['source']['toTxNum'] for r in chunk if r['target_present'][j]] or [0])
    read=g['projected_actual_two_key_batch_read_bytes'];assert read==0 if not any(needs[:2]) else read>=64+meta
valid=[r for r in rows if not r['unsupported'] and not r['hash_budget_excluded']]
exact_groups=[g for g in groups if not g['overflow_lower_bound'] and not g['incomplete_due_to_unsupported']]
counts=sorted(r['account_keys']+r['generation_keys'] for r in rows)
targets=[]
for j,q in enumerate(d['queries']):
    unresolved=[r for r in rows if not r['target_skipped_by_existing_routes'][j]]
    filtered=[r for r in valid if not r['target_skipped_by_existing_routes'][j] and not r['target_may_contain'][j]]
    targets.append(dict(query=q,present_sources=sum(r['target_present'][j] for r in rows),existing_route_skips=len(rows)-len(unresolved),unresolved_sources=len(unresolved),projected_bloom_negative_sources=len(filtered),projected_bloom_false_positive_sources=sum(not r['target_present'][j] and r['target_may_contain'][j] for r in valid),filter_bytes_for_unresolved_valid_sources=sum(r['projected_filter_bytes_16bits'] for r in valid if not r['target_skipped_by_existing_routes'][j])))
result=dict(complete=True,source=s['source'],sources=len(rows),seconds=d['seconds'],production_identity=s['production_after'],unsupported=sum(r['unsupported'] for r in rows),hash_budget_excluded=sum(r['hash_budget_excluded'] for r in rows),account_keys=sum(r['account_keys'] for r in rows),generation_keys=sum(r['generation_keys'] for r in rows),key_count_min=counts[0],key_count_median=statistics.median(counts),key_count_p90=counts[int(.9*(len(counts)-1))],key_count_max=counts[-1],projected_bloom_total_bytes=sum(r['projected_filter_bytes_16bits'] for r in valid),projected_delta_exact_groups=len(exact_groups),projected_delta_total_file_bytes=sum(g['projected_file_bytes'] for g in exact_groups),projected_delta_actual_batch_read_bytes=sum(g['projected_actual_two_key_batch_read_bytes'] for g in exact_groups),delta_incomplete_groups=len(groups)-len(exact_groups),study_io_totals={k:sum(r['io_delta'][k] for r in rows) for k in rows[0]['io_delta']},targets=targets,archive=dict(bytes=archive.stat().st_size,sha256=hashlib.sha256(archive.read_bytes()).hexdigest()),scope='native authenticated key counts and membership; local identity/coverage/formula checks; projections only, no built-index HTTP claim')
for name in ['candidate-status.json','distribution.json','study.log']:(out/name).write_bytes(z.read(name))
(out/'verification.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
