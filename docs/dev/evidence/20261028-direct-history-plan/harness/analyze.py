import pathlib,json,statistics,hashlib,sys
D=pathlib.Path(sys.argv[1]);phase=sys.argv[2] if len(sys.argv)>2 else 'final'
status=json.loads((D/phase/'candidate-status.json').read_text());assert status['complete']
assert status['production_before']==status['production_after']
build=json.loads((D/'final/index-build.json').read_text());assert build['result']['FromBlock']==build['index_before']['BlockNum']+1
assert build['result']['ToBlock']==45836827 and build['canonical_head_unchanged']
def io(v):return {k:int(n) for k,n in (line.split(':') for line in v.strip().splitlines())}
def usage(v):return sum(v[k]['Sec']+v[k]['Usec']/1e6 for k in ['Utime','Stime'])
requests=json.loads((D/phase/'requests.json').read_text());rows=[];expected={}
for q in requests:
 case=D/phase/q['case'];ready=json.loads((case/'ready.json').read_text())
 assert ready['head']==45836827 and ready['head_tx']==4240407293 and ready['root']=='4bb19b7a938ba6c865820ea86f5239235172f84923c350b04c4b0eb75743b627'
 logs=[json.loads(x) for x in (case/'server-requests.jsonl').read_text().splitlines()]
 matched=[x for x in logs if x['id']==q['response']['id']];assert len(matched)==1
 s=matched[0];height=int(s['params'][1],16);value=q['response']['result'];assert value==expected.setdefault(height,value)
 if height==9000000:assert int(value,16)==207
 a,b=io(s['io_before']),io(s['io_after'])
 row=dict(q,height=height,server_elapsed_ns=s['elapsed_ns'],cpu_seconds=usage(s['usage_after'])-usage(s['usage_before']),alloc_bytes=s['alloc_bytes'],read_bytes=b['read_bytes']-a['read_bytes'],rchar=b['rchar']-a['rchar'],read_syscalls=b['syscr']-a['syscr'],maxrss_kib=s['usage_after']['Maxrss'])
 row['admitted']=q['cache']!='discovery-excluded';rows.append(row)
for case in sorted((D/phase).glob('cold-*')):
 proof=json.loads((case/'dispatch-residency.json').read_text());assert all(x['resident_pages']==0 for x in proof)
 cover=json.loads((case/'actual-read-coverage.json').read_text());assert not cover['missing'] and not cover['overflow']
 for row in rows:
  if row['case']==case.name:row['actual_cold_files']=len(cover['paths'])
groups=[]
for family in ['before-indexed','final-indexed','final-original-full','old-partial-cold','final-indexed-cold']:
 for height in sorted(expected):
  if family.endswith('-cold'):
   rs=[r for r in rows if r['case'].startswith('cold-') and ('old-partial' in r['case'])==(family=='old-partial-cold') and r['cache']=='private-cold-history-pages-zero-at-dispatch' and r['height']==height]
  else:
   rs=[r for r in rows if r['case'].startswith(family) and r['cache']=='process-warm' and r['read_bytes']==0 and r['height']==height]
  if not rs:continue
  g={'family':family,'height':height,'n':len(rs),'seconds':[r['seconds_loopback_http'] for r in rs]}
  for field in ['seconds_loopback_http','cpu_seconds','alloc_bytes','read_bytes','rchar','read_syscalls']:
   g['median_'+field]=statistics.median(r[field] for r in rs)
  if 'actual_cold_files' in rs[0]:g['files']=[r['actual_cold_files'] for r in rs]
  groups.append(g)
output={'final_source':status['source'],'requests':rows,'formal_requests':sum(r['admitted'] for r in rows),'all_returns_match':True,'groups':groups,'index_build':build,'production_before':status['production_before'],'production_after':status['production_after']}
(D.parent/('summary-'+phase+'.json')).write_text(json.dumps(output,indent=2))
print(json.dumps({'formal_requests':output['formal_requests'],'groups':groups},indent=2))
