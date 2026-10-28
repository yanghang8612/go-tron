import pathlib,json,hashlib,gzip,statistics,collections,sys
r=pathlib.Path(sys.argv[1]);qs=json.loads((r/'probe/queries.json').read_text());oracle=json.loads((r/'probe/baseline-first/rows.json').read_text());assert len(qs)==len(oracle)==141
meta=qs[-1]['Domain'];assert qs[-1]['Key']=='bWV0YQ=='
for p in r.rglob('rows.json'):
 if p.parent.name.startswith('scope-metadata-'):want=[v for q,v in zip(qs,oracle) if q['Domain']==meta]
 elif p.parent.name.startswith('scope-system-'):want=[v for q,v in zip(qs,oracle) if q['Domain']!=meta]
 else:want=oracle
 assert json.loads(p.read_text())==want,str(p)
 for line in (p.parent/'queries.jsonl').read_text().splitlines():
  sample=json.loads(line);assert sample['digest']==hashlib.sha256(p.read_bytes()).hexdigest(),str(p)
final=json.loads((r/'final/status.json').read_text());assert final['complete'] and 'error' not in final
strict=json.loads((r/'full-route/strict/strict.json').read_text());assert strict['passed'] and strict['sources']==29658 and strict['keys']==61
manifest=json.loads(gzip.decompress((r/'source-proof/before-manifest.json.gz').read_bytes()));refs={x['path']:x for x in manifest['segments']};proof=json.loads((r/'final/private-files-proof.json').read_text());assert len(proof)==8153
for x in proof:
 assert x['bytes']==refs[x['path']]['size'] and 'sha256:'+x['sha256']==refs[x['path']]['checksum']
 assert (x['source_dev'],x['source_ino'])!=(x['private_dev'],x['private_ino'])
for p in (r/'final').glob('os-cold-*-residency.json'):
 v=json.loads(p.read_text());assert v['resident_pages']==sum(x['resident_pages'] for x in v['per_file']);assert v['total_pages']==sum(x['pages'] for x in v['per_file']);assert v['resident_fraction']<.01 and all(x['resident_pages']/x['pages']<.01 for x in v['per_file']);assert v['verified_independent_files']==len(v['per_file'])
restore=json.loads((r/'final/restoration/history-restore.json').read_text());assert restore['complete'] and len(restore['windows'])==3
for w in restore['windows']:assert w['full_row_sha256']==w['candidate_row_sha256']==w['restored_row_sha256'] and w['range_rows_verified']==20
samples=json.loads((r/'recomputed-samples.json').read_text());groups=collections.defaultdict(list)
for s in samples:
 name=s['path'].split('/')[-1]
 if name.startswith('repeat-'):
  mode=name.split('-')[-1];groups[(mode,'process-first' if s['iteration']==0 else 'process-subsequent')].append(s)
  if s['iteration']>0 and s['physical_read_bytes']==0:groups[(mode,'subsequent-zero-physical-read')].append(s)
 elif name.startswith('os-cold-'):groups[(name.split('-')[-1],'private-OS-cold')].append(s)
 elif name.startswith('scope-'):groups[(name.split('-')[-1],name.split('-')[1]+'-scope-'+('first' if s['iteration']==0 else 'warm'))].append(s)
summary=[]
for (mode,kind),items in sorted(groups.items()):
 out={'mode':mode,'kind':kind,'n':len(items),'seconds_median':statistics.median(x['seconds'] for x in items),'seconds_range':[min(x['seconds'] for x in items),max(x['seconds'] for x in items)]}
 for k in ['cpu_seconds','allocated_bytes','num_gc','logical_bytes','physical_read_bytes','read_syscalls']:out[k+'_median']=statistics.median(x[k] for x in items)
 summary.append(out)
result={'complete':True,'samples':summary,'rows_verified':len(list(r.rglob('rows.json'))),'cold_sources_strictly_verified':strict['sources'],'small_keys':strict['keys'],'restore_rows':sum(w['rows'] for w in restore['windows']),'restored_block_ranges':60,'private_bytes':sum(x['bytes'] for x in proof),'product_sha256':final['product_sha256'],'limits':['Cold Manager query only; no complete state snapshot / new-old HTTP comparison / latest-state full MuHash recomputation.','Process-first samples do not imply cold OS cache.','Small n; no p95 claim; concurrent production load.']}
(r/'independent-verification.json').write_text(json.dumps(result,indent=2));print(json.dumps(result,indent=2))
