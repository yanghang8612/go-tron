import datetime,hashlib,json,os,pathlib,subprocess,time,traceback
ROOT=pathlib.Path('/data/gtron/cold-query-layout-20261020-01')
state={'complete':False,'production_mutations':False,'phase':'waiting-for-comparison'}
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(ROOT/'routing-verification-status.json').write_text(json.dumps(state,indent=2))
def identity():
 pid=int(subprocess.check_output(['systemctl','show','gtron','-p','MainPID']).decode().split('=')[1]);return {'pid':pid,'sha256':hashlib.sha256(pathlib.Path('/proc/%d/exe'%pid).read_bytes()).hexdigest()}
try:
 state['before_identity']=identity();save();started=time.monotonic()
 while True:
  p=ROOT/'routing-comparison/comparison-status.json'
  if p.exists():
   s=json.loads(p.read_text())
   if s.get('phase')=='failed':raise RuntimeError('routing comparison failed')
   if s.get('complete'):break
  if time.monotonic()-started>14400:raise TimeoutError('routing comparison wait')
  time.sleep(5)
 env=dict(os.environ,GTRON_ROUTING_ROOT=str(ROOT),GTRON_FIXED_INPUT=str(ROOT/'routing-input'),GTRON_FIXED_CENSUS=str(ROOT/'routing-muhash-census.json'),GOMAXPROCS='2',GOMEMLIMIT='3GiB',GTRON_HISTORY_COMPRESSION_FORMAT='2')
 state['phase']='full-current-source-muhash-fold';save()
 with (ROOT/'routing-muhash.log').open('wb') as out:subprocess.run([str(ROOT/'routing-candidate.test'),'-test.run=^TestOfflineRequestFixedMuHash$','-test.v','-test.timeout=35m'],env=env,stdout=out,stderr=subprocess.STDOUT,check=True,timeout=2200)
 state['phase']='mainnet-cold-history-restore-windows';save()
 with (ROOT/'routing-history-restore.log').open('wb') as out:subprocess.run([str(ROOT/'routing-layout.test'),'-test.run=^TestOfflineRoutingHistoryRestore$','-test.v','-test.timeout=25m'],env=env,stdout=out,stderr=subprocess.STDOUT,check=True,timeout=1600)
 state['after_identity']=identity();assert state['after_identity']==state['before_identity'];state.update(complete=True,phase='complete');save()
except BaseException as exc:
 state.update(phase='failed',error=repr(exc),traceback=traceback.format_exc());save();raise
