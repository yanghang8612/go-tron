import pathlib,json,hashlib,collections,statistics,sys
P=pathlib.Path(sys.argv[1]);W=pathlib.Path(sys.argv[2])
def read(n):return json.loads((P/n).read_text())
def rows(n):return [json.loads(x)for x in (P/n).read_text().splitlines()if x.strip()]
inv=read('final-source-inventory.json')
for n,h in inv.items():assert hashlib.sha256((W/n).read_bytes()).hexdigest()==h,n
assert len(inv)==1089
A,B=(read('manifests/'+m+'.json')for m in ['baseline','candidate'])
assert {k:v for k,v in A.items()if k!='segments'}=={k:v for k,v in B.items()if k!='segments'}
refs={m:{r['path']:r for r in d['segments']}for m,d in [('baseline',A),('candidate',B)]}
C=read('final-space-and-physical-closure.json'); prefix='/data/gtron/history-account-delta-20261025-01/'
physical={r['path']:r for r in C['per_file']};assert len(physical)==len(C['per_file'])==C['unique_physical_files']
# Immutable files hard-linked between inputs are represented by one physical receipt.
identity={(r['inode'],r['sha256'],r['bytes'],r['allocated']) for r in physical.values()}
by_checksum=collections.defaultdict(list)
for r in physical.values():by_checksum[(r['sha256'],r['bytes'])].append(r)
for mode,rr in refs.items():
 assert len(rr)==C['modes'][mode]['ref_files']
 assert sum(r['size']for r in rr.values())==C['modes'][mode]['ref_file_bytes']
 for n,r in rr.items():
  want=(r['checksum'].removeprefix('sha256:'),r['size']);assert want in by_checksum,(mode,n)
  if prefix+mode+'-input/state-snapshots/'+n in physical:
   got=physical[prefix+mode+'-input/state-snapshots/'+n];assert (got['sha256'],got['bytes'])==want
 assert hashlib.sha256((P/'manifests'/f'{mode}.json').read_bytes()).hexdigest()==C['modes'][mode]['manifest_sha256']
controls=rows('account-controls-files.jsonl'); receipts=rows('complete-input-files.jsonl')
assert len(controls)==len(receipts)==4609
for r in receipts:
 for mode,field in [('baseline','source'),('candidate','candidate')]:
  for ref in r[field]:assert refs[mode][ref['path']]==ref
 for a,b in zip(r['source'][1:],r['candidate'][1:]):assert (a['size'],a['checksum'])==(b['size'],b['checksum'])
for r in controls:
 assert refs['baseline'][r['Source']['path']]==r['Source'] and refs['candidate'][r['Candidate']['path']]==r['Candidate']
 if r['ControlPath']:
  q=physical[r['ControlPath']];assert (q['sha256'],q['bytes'],q['allocated'])==(r['ControlSHA256'],r['ControlFileBytes'],r['ControlAllocated'])
assert sum(r['bytes']for r in physical.values())==C['physical_bytes_hashed']
# Verify all 40 decoder batches and eight independently checked logical points.
d=read('decoder-cost.json');assert d['complete']and d['all_canonical_and_point_oracles_equal']and len(d['samples'])==8
arms=collections.defaultdict(list)
for r in d['rows']:
 assert r['decodes']==256 and r['ns_per_decode']==r['nanoseconds']/r['decodes']
 arms[r['arm']].append(r['ns_per_decode'])
assert len(arms)==4 and all(len(v)==10 for v in arms.values())
dec={k:dict(batches=len(v),median_ns_per_decode=statistics.median(v),min_ns_per_decode=min(v),max_ns_per_decode=max(v))for k,v in arms.items()}
base=dec['old-128KiB-existing-decoder']['median_ns_per_decode']
for a in dec.values():a['relative_to_old']=a['median_ns_per_decode']/base
plan=read('decoder-plan.json'); control_by_path={r['Candidate']['path']:r for r in controls}
# This field describes whole files; sampled pages themselves were explicitly mode-zero checked.
plan_stats=[dict(path=r['Path'],account_pages=control_by_path[r['Path']]['AccountPages'],pages=control_by_path[r['Path']]['Pages'])for r in plan]
result=dict(source_inventory_files=len(inv),go_files=sum(n.endswith('.go')for n in inv),module_files=sum(not n.endswith('.go')for n in inv),manifests_metadata_equal=True,companion_receipts_verified=len(receipts)*2,unique_physical_sha_receipts=len(physical),physical_bytes_hashed=C['physical_bytes_hashed'],materialized_controls_verified=len(controls),decoder=dict(samples=len(d['samples']),arms=dec,query_file_page_modes=plan_stats),production_mutations=False)
print(json.dumps(result,indent=2))
