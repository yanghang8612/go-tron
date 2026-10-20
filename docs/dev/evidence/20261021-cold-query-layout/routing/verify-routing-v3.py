import datetime,hashlib,json,os,pathlib,subprocess,time,traceback
ROOT=pathlib.Path('/data/gtron/cold-query-layout-20261020-01')
state={'complete':False,'production_mutations':False,'phase':'waiting-for-comparison'}
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(ROOT/'routing-verification-v3-status.json').write_text(json.dumps(state,indent=2))
def identity():
 pid=int(subprocess.check_output(['systemctl','show','gtron','-p','MainPID']).decode().split('=')[1]);return {'pid':pid,'sha256':hashlib.sha256(pathlib.Path('/proc/%d/exe'%pid).read_bytes()).hexdigest()}
try:
 state['before_identity']=identity();save();started=time.monotonic()
 while True:
  p=ROOT/'routing-diagnostic-status.json'
  if p.exists():
   s=json.loads(p.read_text())
   if s.get('phase')=='failed':raise RuntimeError('routing comparison failed')
   if s.get('complete'):break
  if time.monotonic()-started>14400:raise TimeoutError('routing comparison wait')
  time.sleep(5)
 env=dict(os.environ,GTRON_ROUTING_ROOT=str(ROOT),GTRON_FIXED_INPUT=str(ROOT/'routing-input'),GTRON_FIXED_CENSUS=str(ROOT/'routing-muhash-census.json'),GOMAXPROCS='2',GOMEMLIMIT='3GiB',GTRON_HISTORY_COMPRESSION_FORMAT='2')
 state['phase']='full-current-source-muhash-fold';save()
 with (ROOT/'routing-muhash.log').open('wb') as out:subprocess.run([str(ROOT/'routing-candidate.test'),'-test.run=^TestOfflineRequestFixedMuHash$','-test.v','-test.timeout=35m'],env=env,stdout=out,stderr=subprocess.STDOUT,check=True,timeout=2200)
 state['phase']='build-restore-diagnostic-context-adapter';save()
 source=ROOT/'routing-source';overlay=ROOT/'routing-restore-overlay.json'
 overlay.write_text(json.dumps({'Replace':{str(source/'core/state/snapshots/offline_routing_test.go'):str(ROOT/'routing-restore-v2.go')}}))
 buildenv=dict(env,PATH='/data/go/bin:'+os.environ.get('PATH',''),GOTOOLCHAIN='local',CGO_ENABLED='1',GOCACHE='/root/.cache/go-build',GOPATH='/root/go')
 with (ROOT/'routing-restore-v2-build.log').open('wb') as out:subprocess.run(['/data/go/bin/go','test','-p=2','-tags=sapling','-overlay='+str(overlay),'-c','./core/state/snapshots','-o',str(ROOT/'routing-restore-v2.test')],cwd=source,env=buildenv,stdout=out,stderr=subprocess.STDOUT,check=True,timeout=900)
 record={'binary_sha256':hashlib.sha256((ROOT/'routing-restore-v2.test').read_bytes()).hexdigest(),'harness_sha256':hashlib.sha256((ROOT/'routing-restore-v2.go').read_bytes()).hexdigest(),'overlay_sha256':hashlib.sha256(overlay.read_bytes()).hexdigest(),'production_sources_unchanged':True}
 expected=json.loads((ROOT/'routing-source-identity.json').read_text())['production_sources']
 for name,digest in expected.items():assert hashlib.sha256((source/name).read_bytes()).hexdigest()==digest,name
 (ROOT/'routing-restore-v2-identity.json').write_text(json.dumps(record,indent=2))
 state['phase']='mainnet-cold-history-restore-windows';save()
 with (ROOT/'routing-history-restore.log').open('wb') as out:subprocess.run([str(ROOT/'routing-restore-v2.test'),'-test.run=^TestOfflineRoutingHistoryRestore$','-test.v','-test.timeout=25m'],env=env,stdout=out,stderr=subprocess.STDOUT,check=True,timeout=1600)
 state['after_identity']=identity();assert state['after_identity']==state['before_identity'];state.update(complete=True,phase='complete');save()
except BaseException as exc:
 state.update(phase='failed',error=repr(exc),traceback=traceback.format_exc());save();raise
