"""Recompute complete HTTP samples and server resources from admitted raw rows."""
import pathlib,json,statistics,collections,sys
root=pathlib.Path(sys.argv[1]);R=root/'http-final';s=json.loads((R/'status.json').read_text());assert s['complete'] and s['all_results_correct'] and s['production_identity_unchanged'];rows=[json.loads(x) for x in (R/'requests.jsonl').read_text().splitlines()];assert len(rows)==s['requests'] and all(r['correct'] and 'error' not in r['response'] and r['response']['result']==r['expected'] for r in rows)
checks={}
for d in R.iterdir():
 p=d/'server-requests.jsonl'
 if not d.is_dir() or not p.exists():continue
 for line in p.read_text().splitlines():
  q=json.loads(line)
  if q['method']=='eth_call':checks[q['id']]=(d.name,q)
def io(v):return {k:int(n) for k,n in (x.split(':',1) for x in v.strip().splitlines())}
def tv(t):return t['Sec']+t['Usec']/1e6
out=[];groups=collections.defaultdict(list)
for r in rows:
 assert r['id'] in checks;rdir,q=checks[r['id']];assert q['params']==[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},r['tag']];a,b=io(q['io_before']),io(q['io_after']);x={**r,'server_directory':rdir,'server_seconds':q['elapsed_ns']/1e9,'cpu_seconds':sum(tv(q['usage_after'][k])-tv(q['usage_before'][k]) for k in ['Utime','Stime']),'allocated_bytes':q['alloc_bytes'],'gc_cycles':q['num_gc'],'rchar':b['rchar']-a['rchar'],'read_bytes':b['read_bytes']-a['read_bytes'],'read_syscalls':b['syscr']-a['syscr']};out.append(x)
 if r['cache'] in ['warm','private-zero-residency-new-process-first-http']:
  groups[r['mode'],r['cache'],r['tag']].append(x)
  if r['cache'].startswith('private-'):
   proof=json.loads((R/(rdir+'-residency.json')).read_text());assert proof and all(x['resident_pages']==0 for x in proof);proved={x['path'] for x in proof};watch=json.loads((R/(rdir+'-actual-inotify-open-coverage.json')).read_text());assert not watch['overflow'] and not watch['missing_from_eviction_proof'] and watch['timed_host_including_startup'] and set(watch['actual_paths'])<=proved
summary=[]
metrics=['seconds_complete_loopback_http','server_seconds','cpu_seconds','allocated_bytes','gc_cycles','rchar','read_bytes','read_syscalls']
for (mode,cache,tag),v in sorted(groups.items()):
 assert len(v)==(4 if cache=='warm' else 2),(mode,cache,tag,len(v));summary.append({'mode':mode,'cache':cache,'tag':tag,'height':v[0]['height'],'n':len(v),'min_seconds':min(r['seconds_complete_loopback_http'] for r in v),'max_seconds':max(r['seconds_complete_loopback_http'] for r in v),'medians':{k:statistics.median(r[k] for r in v) for k in metrics},'all_zero_physical_read':all(r['read_bytes']==0 for r in v),'physical_read_sample_count':sum(r['read_bytes']>0 for r in v)})
assert len(summary)==32
for mode in ['baseline','candidate']:
 private={q['path']:q for q in json.loads((R/(mode+'-private-proof.json')).read_text())}
 for p,q in private.items():assert (q['private_dev'],q['private_ino'])!=(q['source_dev'],q['source_ino'])
 for r in out:
  if r['mode']==mode and r['cache'].startswith('private-'):
   for w in json.loads((R/(r['server_directory']+'-residency.json')).read_text()):assert w['path'] in private
coverage=json.loads((R/'actual-read-coverage.json').read_text());assert len(coverage)==16 and all(not x['missing_from_cold_proof'] for x in coverage)
(R/'locally-recomputed-requests.json').write_text(json.dumps(out,indent=2));(R/'locally-recomputed-summary.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary,indent=2))
