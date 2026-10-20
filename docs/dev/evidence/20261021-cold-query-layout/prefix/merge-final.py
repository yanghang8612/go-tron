import datetime,hashlib,json,os,pathlib,shutil,subprocess,time,traceback
ROOT=pathlib.Path('/data/gtron/cold-query-layout-20261020-01')
state={'complete':False,'production_mutations':False,'phase':'waiting-for-final-preparation'}
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(ROOT/'merge-final-driver-status.json').write_text(json.dumps(state,indent=2))
def identity():
 pid=int(subprocess.check_output(['systemctl','show','gtron','-p','MainPID']).decode().split('=')[1]);return {'pid':pid,'sha256':hashlib.sha256(pathlib.Path('/proc/%d/exe'%pid).read_bytes()).hexdigest()}
try:
 state['before_identity']=identity();save();started=time.monotonic()
 while True:
  p=ROOT/'final-status.json'
  if p.exists():
   status=json.loads(p.read_text())
   if status.get('phase')=='failed':raise RuntimeError('final native preparation failed')
   if status.get('complete'):break
  if time.monotonic()-started>7200:raise TimeoutError('final native preparation wait')
  time.sleep(5)
 state['phase']='isolating-merge-input';save()
 source=ROOT/'prefix-input';target=ROOT/'merged-final-input';target.mkdir(exist_ok=False)
 for directory,dirs,files in os.walk(source):
  rel=pathlib.Path(directory).relative_to(source);(target/rel).mkdir(exist_ok=True)
  for name in files:
   if name in ('LOCK','FLOCK'):continue
   a=pathlib.Path(directory)/name;b=target/rel/name
   if a.suffix in ('.sst','.seg','.idx','.kv') or str(rel).startswith('ancient'):os.link(a,b)
   else:shutil.copy2(a,b)
 env=dict(os.environ,GTRON_LAYOUT_ROOT=str(ROOT),GTRON_LAYOUT_PHASE='merged',GTRON_HISTORY_COMPRESSION_FORMAT='2',GOMAXPROCS='2',GOMEMLIMIT='3GiB')
 state['phase']='canonical-64-step-merges';save()
 with (ROOT/'merge-final.log').open('wb') as out:subprocess.run([str(ROOT/'layout-final.test'),'-test.run=^TestOfflineColdQueryLayout$','-test.v','-test.timeout=2h'],env=env,stdout=out,stderr=subprocess.STDOUT,check=True,timeout=7300)
 state['after_identity']=identity();assert state['after_identity']==state['before_identity'];state.update(complete=True,phase='ready');save()
except BaseException as exc:
 state.update(phase='failed',error=repr(exc),traceback=traceback.format_exc());save();raise
