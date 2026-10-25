import pathlib,json,subprocess,os,time,datetime,traceback,hashlib,collections
P=pathlib.Path('/data/gtron/history-account-delta-20261025-01');S=P/'candidate-source';state={'complete':False,'production_mutations':False}
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();x=P/'acceptance-status.json';x.with_suffix('.tmp').write_text(json.dumps(state,indent=2));x.with_suffix('.tmp').replace(x)
def waitready():
 start=time.monotonic()
 while True:
  a=json.loads((P/'layout-status.json').read_text());b=json.loads((P/'native-status.json').read_text())
  if a.get('phase')=='failed' or b.get('phase')=='failed':raise RuntimeError('preparation failed: '+str((a.get('error'),b.get('error'))))
  if a.get('complete') and b.get('complete'):return
  if time.monotonic()-start>21600:raise TimeoutError('preparation')
  time.sleep(4)
def run(label,args,env,timeout):
 state['phase']=label;save()
 with (P/(label+'.log')).open('wb') as out:subprocess.run(args,cwd=str(S),env=env,stdout=out,stderr=subprocess.STDOUT,check=True,timeout=timeout)
try:
 state['phase']='waiting-layout-and-native';save();waitready()
 env=dict(os.environ,PATH='/data/go/bin:'+os.environ.get('PATH',''),GOCACHE='/root/.cache/go-build',GOPATH='/root/go',GOTOOLCHAIN='local',GOMAXPROCS='2',GOMEMLIMIT='3GiB',GTRON_FIXED_INPUT=str(P/'candidate-input'),GTRON_FIXED_CENSUS=str(P/'muhash-census.json'),GTRON_ROUTING_ROOT=str(P),GTRON_ACCOUNT_DELTA_ROOT=str(P))
 baseline_go=subprocess.check_output(['/data/go/bin/go','version','-m',str(P/'baseline.test')]).decode();candidate_go=subprocess.check_output(['/data/go/bin/go','version','-m',str(P/'candidate.test')]).decode();(P/'baseline-build-info.txt').write_text(baseline_go);(P/'candidate-build-info.txt').write_text(candidate_go);assert baseline_go.splitlines()[0].split()[-1]==candidate_go.splitlines()[0].split()[-1];state['toolchain_go_version']=candidate_go.splitlines()[0].split()[-1];save()
 run('candidate-retention',['/usr/bin/python3',str(P/'candidate-retention.py')],env,180)
 state['phase']='wait-full-muhash';save();start=time.monotonic()
 while True:
  f=P/'muhash-driver-status.json';proof=json.loads(f.read_text()) if f.exists() else {}
  if proof.get('phase')=='failed':raise RuntimeError('full MuHash failed: '+str(proof.get('error')))
  if proof.get('complete'):break
  if time.monotonic()-start>3600:raise TimeoutError('full MuHash')
  time.sleep(3)
 assert proof['binary_sha256']==hashlib.sha256((P/'candidate.test').read_bytes()).hexdigest()
 state['muhash_verified']=True;save()
 run('history-restore',[str(P/'bounded-snapshots.test'),'-test.run=^TestOfflineAccountHistoryRestore$','-test.v','-test.timeout=30m'],env,1850)
 run('comparison',['/usr/bin/python3',str(P/'compare.py'),'tail-read'],env,7200)
 assert json.loads((P/'tail-read-comparison/comparison-status.json').read_text())['complete']
 state.update(complete=True,phase='complete');save()
except BaseException as e:state.update(phase='failed',error=repr(e),traceback=traceback.format_exc());save();raise
