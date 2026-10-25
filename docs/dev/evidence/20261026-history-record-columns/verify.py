#!/usr/bin/env python3
"""Verify the complete page-screening evidence; this is not whole-node acceptance."""
import pathlib,json,hashlib,zipfile,gzip,statistics,collections,io
E=pathlib.Path(__file__).resolve().parent;W=E.parents[3]
def sha(b):return hashlib.sha256(b).hexdigest()
def read(n):return json.loads((E/n).read_text())
checks=read('SHA256SUMS.json');actual={str(p.relative_to(E))for p in E.rglob('*')if p.is_file()and p.name!='SHA256SUMS.json'};assert actual==set(checks)
for n,d in checks.items():assert sha((E/n).read_bytes())==d,n
archives={};members=0
for label,receipt in read('transfer-receipts.json').items():
 b=(E/'archives'/(label+'.zip')).read_bytes();assert sha(b)==receipt['sha256']and len(b)==receipt['bytes']
 with zipfile.ZipFile(io.BytesIO(b))as z:
  names=z.namelist();assert len(names)==len(set(names))and z.testzip()is None
  assert all(not pathlib.PurePosixPath(n).is_absolute()and'..'not in pathlib.PurePosixPath(n).parts for n in names)
  m=json.loads(z.read('evidence-sha256.json'));assert set(names)==set(m)|{'evidence-sha256.json'}
  archives[label]={n:z.read(n)for n in m}
  for n,d in m.items():assert sha(archives[label][n])==d,n
  members+=len(m)
def j(label,n):return json.loads(archives[label][n])
def lines(label,n):return [json.loads(s)for s in archives[label][n].splitlines()if s.strip()]
base=gzip.decompress((E/'baseline-source-inventory.json.gz').read_bytes());assert sha(base)==j('probe','driver-status.json')['baseline_inventory_sha256'];inventory=json.loads(base)
for n,d in inventory.items():assert sha((W/n).read_bytes())==d,n
state=j('round3','round3-status.json');assert state['complete']and state['canonical_sample_identity_equal']and not state['production_mutations']
for n,d in state['inputs'].items():assert sha((W/'core/state/snapshots'/n).read_bytes())==d,n
assert sha(archives['round3']['history_record_columns.v3.go'])==state['inputs']['history_record_columns.go']
assert sha(archives['round3']['history_record_columns_test.v3.go'])==state['inputs']['history_record_columns_test.go']
base_rows=lines('probe','probe-rows.jsonl');rows=lines('round3','probe-rows.jsonl');adaptive=lines('adaptive','probe-rows.jsonl');assert len(base_rows)==len(rows)==len(adaptive)==14
for a,b,c in zip(base_rows,rows,adaptive):
 for k in ['Path','SourceBytes','LogicalBytes','Records','FullHeaders','CrossingHeaders','Pages','SamePagePlainBytes','CanonicalSHA256']:assert a[k]==b[k]==c[k],k
 assert b['FullHeaders']+b['CrossingHeaders']==b['Records']and b['SamePagePlainBytes']==b['SourceBytes']
 for u,v in zip(b['Variants'],c['Variants']):assert u['EncodedBytes']==v['EncodedBytes']and u['AfterFallbackBytes']==v['AfterFallbackBytes']
 assert c['AdaptiveAfterFallbackBytes']==min(c['AdaptiveEncodedBytes'],c['SourceBytes'])<=min(v['AfterFallbackBytes']for v in c['Variants'])
assert j('adaptive','adaptive-status.json')['complete']and j('adaptive','adaptive-status.json')['canonical_and_uniform_controls_equal']
cohorts={}
for label,rs in [('all14',adaptive),('fixed9',adaptive[:9]),('mature5',adaptive[9:])]:
 before=sum(r['SourceBytes']for r in rs);after=sum(r['AdaptiveAfterFallbackBytes']for r in rs)
 cohorts[label]=dict(files=len(rs),source_bytes=before,projected_adaptive_bytes=after,projected_saved_bytes=before-after,projected_saved_percent=(before-after)/before*100)
costs={}
for label in ['probe','round2','round3']:
 d=j(label,'page-cost.json');assert d['complete']and d['all_canonical_oracles_equal']and len(d['samples'])==8
 groups=collections.defaultdict(list)
 for r in d['rows']:assert r['ns_per_decode']==r['nanoseconds']/r['decodes']and r['decodes']==256;groups[r['arm']].append(r['ns_per_decode'])
 assert len(groups)==2 and all(len(v)==10 for v in groups.values())
 costs[label]={k:statistics.median(v)for k,v in groups.items()}
 for a,b in zip(j('probe','page-cost.json')['samples'],d['samples']):
  for k in ['Path','Page','CanonicalBytes','PlainBytes','SHA256']:assert a[k]==b[k],k
result=dict(phase='page-screening-only',whole_node_acceptance_complete=False,original_members_verified=members,source_files_verified=len(inventory)+len(state['inputs']),cohorts=cohorts,cost_medians_ns=costs,records=sum(r['Records']for r in rows),canonical_bytes=sum(r['LogicalBytes']for r in rows),production_mutations=False)
assert result==read('analysis.json'),'derived analysis differs'
print(json.dumps(result,indent=2))
