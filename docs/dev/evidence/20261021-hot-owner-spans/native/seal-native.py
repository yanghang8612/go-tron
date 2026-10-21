import pathlib,json,subprocess,time,hashlib,zipfile,os,datetime,traceback,statistics,collections
ROOT=pathlib.Path('/data/gtron/cold-query-layout-20261020-01/hot-owner-spans-20261021-03')
state={'complete':False,'phase':'waiting-for-validation','production_mutations':False}
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();p=ROOT/'seal-status.json';tmp=p.with_suffix('.tmp');tmp.write_text(json.dumps(state,indent=2));tmp.replace(p)
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
try:
 save();start=time.monotonic()
 units=[('gtron-owner-blake-prepare-20261021','preparation-status.json'),('gtron-owner-blake-layout-20261021','layout-status.json'),('gtron-owner-blake-compare-20261021','tail-read-comparison/comparison-status.json'),('gtron-owner-blake-audit-20261021','audit-status.json'),('gtron-owner-blake-diagnostic-20261021','nine-diagnostic/status.json'),('gtron-owner-blake-resources-20261021','resource-status.json')]
 while True:
  ready=True
  for unit,filename in units:
   p=ROOT/filename
   if not p.exists():ready=False;continue
   try:x=json.loads(p.read_text())
   except json.JSONDecodeError:ready=False;continue
   assert x.get('phase')!='failed',(unit,x)
   active=subprocess.check_output(['systemctl','show',unit+'.service','-p','ActiveState']).decode().strip()
   if not x.get('complete') or active!='ActiveState=inactive':ready=False
  if ready:break
  if time.monotonic()-start>8000:raise TimeoutError('bounded archive wait')
  time.sleep(2)
 state['phase']='aggregate-and-seal';save()
 comparison=ROOT/'tail-read-comparison';requests=[json.loads(x) for x in (comparison/'requests.jsonl').read_text().splitlines()];assert len(requests)==154 and all(x['correct'] for x in requests)
 metrics={}
 def usage(r):
  def us(x):return x['Sec']*1000000+x['Usec']
  return sum(us(r['usage_after'][k])-us(r['usage_before'][k]) for k in ('Utime','Stime'))/1000
 def io(s):return {k:int(v) for k,v in (line.split(':') for line in s.splitlines())}
 for row in requests:
  if row['cache'] not in ('warm','fresh-process-os-uncontrolled','private-os-cold-fresh-process'):continue
  matches=[]
  for p in comparison.glob('*/server-requests.jsonl'):
   for line in p.read_text().splitlines():
    r=json.loads(line)
    if r['id']==row['id']:matches.append(r)
  assert len(matches)==1,(row,len(matches));r=matches[0];before=io(r['io_before']);after=io(r['io_after'])
  values={'client_ms':row['milliseconds'],'handler_ms':r['elapsed_ns']/1000000,'cpu_ms':usage(r),'alloc_bytes':r['alloc_bytes'],'num_gc':r['num_gc'],**{k:after[k]-before[k] for k in ['syscr','rchar','read_bytes']}}
  key=(row['mode'],row['cache'],row['tag']);metrics.setdefault(key,[]).append(values)
 aggregate=[{'mode':k[0],'cache':k[1],'tag':k[2],'n':len(v),**{n:statistics.median(r[n] for r in v) for n in v[0]}} for k,v in sorted(metrics.items())]
 (ROOT/'aggregate.json').write_text(json.dumps(aggregate,indent=2))
 harness=ROOT/'harness';harness.mkdir()
 for rel in ['core/offline_fixed_http_test.go','core/offline_request_muhash_test.go','core/state/snapshots/offline_routing_test.go','core/rawdb/offline_owner_spans_test.go']:
  p=harness/(pathlib.Path(rel).name+'.txt');p.write_bytes((ROOT/'source'/rel).read_bytes())
 pid=int(subprocess.check_output(['systemctl','show','gtron','-p','MainPID']).decode().split('=')[1]);live={'pid':pid,'sha256':sha(pathlib.Path('/proc/%d/exe'%pid)),'old_database_exists':pathlib.Path('/data/gtron/main/datadir-pre-compact-v3-20261019-659ab078').is_dir(),'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
 assert live['pid']==28737 and live['sha256']=='5848de48f2f24a128054ff0967cddfd7256c54029f674b317d37f98ac23c63c4' and live['old_database_exists'];(ROOT/'final-production-identity.json').write_text(json.dumps(live,indent=2))
 selected=[]
 for p in ROOT.rglob('*'):
  rel=p.relative_to(ROOT)
  if any(x=='source' or x.endswith('-cache-input') or x=='owner-index-input' for x in rel.parts):continue
  if not p.is_file() or p.name in ['candidate.test','snapshots.test','rawdb.test','gtron-candidate','seal-status.json','seal-receipt.json']:continue
  if p.suffix not in ('.json','.jsonl','.py','.log','.txt','.gz','.trace'):continue
  selected.append(('native/'+str(rel),p))
 inv={name:{'bytes':p.stat().st_size,'sha256':sha(p)} for name,p in sorted(selected)}
 archive=pathlib.Path('/tmp/gtron-hot-owner-spans-20261021-03.zip');assert not archive.exists()
 with zipfile.ZipFile(archive,'x',zipfile.ZIP_DEFLATED,6) as out:
  for name,p in sorted(selected):out.write(p,name)
  out.writestr('evidence-inventory.json',json.dumps(inv,indent=2))
 os.chmod(archive,0o644);receipt={'archive':str(archive),'bytes':archive.stat().st_size,'sha256':sha(archive),'files':len(inv),'uncompressed_bytes':sum(r['bytes'] for r in inv.values()),'complete':True};(ROOT/'seal-receipt.json').write_text(json.dumps(receipt,indent=2));state.update(complete=True,phase='complete',receipt=receipt);save();print(json.dumps(receipt))
except BaseException as exc:state.update(phase='failed',error=repr(exc),traceback=traceback.format_exc());save();raise
