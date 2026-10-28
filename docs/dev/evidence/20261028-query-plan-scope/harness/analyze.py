import pathlib,json,statistics,sys
D=pathlib.Path(sys.argv[1]);s=json.loads((D/'candidate-status.json').read_text());assert s['complete'] and s['production_before']==s['production_after']
requests=json.loads((D/'requests.json').read_text());assert len(requests)==36
expected={};rows=[]
def io(text):return {k:int(v) for k,v in (line.split(':') for line in text.strip().splitlines())}
def cpu(v):return sum(v[k]['Sec']+v[k]['Usec']/1e6 for k in ['Utime','Stime'])
for q in requests:
 case=D/q['case'];ready=json.loads((case/'ready.json').read_text());assert ready['head']==45836827 and ready['head_tx']==4240407293 and ready['root']=='4bb19b7a938ba6c865820ea86f5239235172f84923c350b04c4b0eb75743b627'
 events=[json.loads(line) for line in (case/'server-requests.jsonl').read_text().splitlines()];e=[e for e in events if e['id']==q['response']['id']];assert len(e)==1;e=e[0]
 h=int(e['params'][1],16);value=q['response']['result'];assert value==expected.setdefault(h,value)
 if h==9000000:assert int(value,16)==207
 a,b=io(e['io_before']),io(e['io_after']);v=dict(q,height=h,cpu_seconds=cpu(e['usage_after'])-cpu(e['usage_before']),alloc_bytes=e['alloc_bytes'],read_bytes=b['read_bytes']-a['read_bytes'],read_syscalls=b['syscr']-a['syscr'],rchar=b['rchar']-a['rchar']);rows.append(v)
 if q['cache']=='private-cold-history-pages-zero-at-dispatch':
  r=json.loads((case/'dispatch-residency.json').read_text());assert len(r)==636 and all(x['resident_pages']==0 for x in r)
  coverage=json.loads((case/'actual-read-coverage.json').read_text());assert not coverage['overflow'] and not coverage['missing'];v['actual_files']=len(coverage['paths'])
groups=[]
for version in ['before-indexed','final-indexed']:
 for cache in ['private-cold-history-pages-zero-at-dispatch','second-call-process-warm','process-warm']:
  for h in sorted(expected):
   rs=[r for r in rows if version in r['case'] and r['cache']==cache and r['height']==h and (cache!='process-warm' or r['read_bytes']==0)]
   if not rs:continue
   g={'version':version,'cache':cache,'height':h,'n':len(rs),'seconds':[r['seconds_loopback_http'] for r in rs]}
   for f in ['seconds_loopback_http','cpu_seconds','alloc_bytes','read_bytes','read_syscalls','rchar']:g['median_'+f]=statistics.median(r[f] for r in rs)
   if 'actual_files' in rs[0]:g['files']=[r['actual_files'] for r in rs]
   groups.append(g)
summary={'source':s['source'],'baseline_product_source':'9a9de233c9fd4def2acca00fdf22f6b26aa5cc76','formal_requests':len(rows),'all_returns_match':True,'production_before':s['production_before'],'production_after':s['production_after'],'requests':rows,'groups':groups}
(D.parent/'query-plan-scope-summary.json').write_text(json.dumps(summary,indent=2))
print(json.dumps(groups,indent=2))
