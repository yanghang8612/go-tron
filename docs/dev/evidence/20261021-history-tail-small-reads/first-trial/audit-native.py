import pathlib,json,subprocess,os,time,datetime,hashlib,traceback
OLD=pathlib.Path('/data/gtron/cold-query-layout-20261020-01');ROOT=OLD/'tail-read-20261021-01';state={'complete':False,'production_mutations':False,'phase':'waiting-for-comparison'}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def identity():
 pid=int(subprocess.check_output(['systemctl','show','gtron','-p','MainPID']).decode().split('=')[1]);return {'pid':pid,'sha256':sha(pathlib.Path('/proc/%d/exe'%pid))}
def save():state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(ROOT/'audit-status.json').write_text(json.dumps(state,indent=2))
try:
 state['before_identity']=identity();save();begin=time.monotonic()
 while True:
  f=ROOT/'tail-read-comparison/comparison-status.json';s=json.loads(f.read_text()) if f.exists() else {}
  if s.get('phase')=='failed':raise RuntimeError('HTTP comparison failed')
  if s.get('complete') and subprocess.check_output(['systemctl','show','gtron-tail-read-compare-20261021.service','-p','ActiveState']).decode().strip()=='ActiveState=inactive':break
  if time.monotonic()-begin>7200:raise TimeoutError('comparison wait')
  time.sleep(3)
 env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='3GiB',GTRON_FIXED_INPUT=str(OLD/'account-gc-input'),GTRON_FIXED_CENSUS=str(ROOT/'muhash-census.json'),GTRON_ROUTING_ROOT=str(ROOT))
 for phase,command in [('muhash',[str(ROOT/'candidate.test'),'-test.run=^TestOfflineRequestFixedMuHash$','-test.v','-test.timeout=35m']),('history-restore',[str(ROOT/'snapshots.test'),'-test.run=^TestOfflineAccountHistoryRestore$','-test.v','-test.timeout=20m'])]:
  state['phase']=phase;save()
  with (ROOT/(phase+'.log')).open('wb') as f:subprocess.run(command,env=env,stdout=f,stderr=subprocess.STDOUT,timeout=2300,check=True)
 src=json.loads((ROOT/'source-identity.json').read_text());source=ROOT/'source'
 for name,digest in src['production_sources'].items():assert sha(source/name)==digest,name
 census=json.loads((ROOT/'muhash-census.json').read_text());assert census['complete'] and census['muhash_verified'];assert bytes(census['muhash_root']).hex()=='8fbf149684146a59b1650b318a768f0edbae0c8c1c3ceb2c5c610129c10f76a5';assert census['source_rows']==[3484117,481,141418493]
 restored=json.loads((ROOT/'history-restore.json').read_text());assert restored['complete'] and sum(w['rows'] for w in restored['windows'])==22496
 state['after_identity']=identity();assert state['before_identity']==state['after_identity'];state.update(complete=True,phase='complete');save()
except BaseException as exc:state.update(phase='failed',error=repr(exc),traceback=traceback.format_exc());save();raise
