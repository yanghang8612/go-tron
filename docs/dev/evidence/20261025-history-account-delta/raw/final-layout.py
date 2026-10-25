import pathlib,json,hashlib,shutil,os,subprocess,datetime,traceback,time
P=pathlib.Path('/data/gtron/history-account-delta-20261025-01');SOURCE=pathlib.Path('/data/gtron/history-tx-runs-20261025-01/candidate-input');state={'complete':False,'production_mutations':False,'parallel_workers':4}
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();p=P/'layout-status.json';p.with_suffix('.tmp').write_text(json.dumps(state,indent=2));p.with_suffix('.tmp').replace(p)
def identity():
 x=dict(l.split('=',1) for l in subprocess.check_output(['systemctl','show','gtron','-p','MainPID','-p','ActiveState']).decode().splitlines());pid=int(x['MainPID']);x['sha256']=hashlib.sha256(pathlib.Path('/proc/%d/exe'%pid).read_bytes()).hexdigest() if pid else None;return x
def replica(dst):
 dst.mkdir()
 for d,dirs,files in os.walk(SOURCE):
  rel=pathlib.Path(d).relative_to(SOURCE);(dst/rel).mkdir(exist_ok=True)
  for n in files:
   if n in ('LOCK','FLOCK'):continue
   a=pathlib.Path(d)/n;b=dst/rel/n
   if a.suffix in ('.sst','.seg','.idx','.kv','.route','.gtv2') or str(rel).startswith('ancient'):os.link(a,b)
   else:shutil.copy2(a,b)
try:
 state['before_identity']=identity();state['phase']='wait-final-native';save();start=time.monotonic()
 while True:
  a=json.loads((P/'native-status.json').read_text())
  if a.get('phase')=='failed':raise RuntimeError('native preparation failed')
  if a.get('candidate') and a.get('experiment_binaries_ready'):break
  if time.monotonic()-start>7200:raise TimeoutError('final native build')
  time.sleep(3)
 assert shutil.disk_usage('/data').free>150*(1<<30)
 for n in ['candidate-input','complete-input-files.jsonl','complete-input-status.json','complete-input.log']:
  p=P/n;to=P/('byte-'+n+'-preserved');assert not to.exists();p.rename(to)
 replica(P/'candidate-input');(P/'complete-input-files.jsonl').write_bytes(b'')
 (P/'candidate-source/core/state/snapshots/offline_account_delta_parallel_test.go').write_bytes((P/'offline_account_delta_parallel_test.go').read_bytes())
 manifest=SOURCE/'state-snapshots/manifest.json';digest=hashlib.sha256(manifest.read_bytes()).hexdigest();assert hashlib.sha256((P/'baseline-input/state-snapshots/manifest.json').read_bytes()).hexdigest()==digest;state['input_manifest_sha256']=digest
 env=dict(os.environ,GOMAXPROCS='4',GOMEMLIMIT='3GiB',GOTOOLCHAIN='local',GOCACHE='/root/.cache/go-build',GOPATH='/root/go',CGO_ENABLED='1',GTRON_ACCOUNT_DELTA_ROOT=str(P))
 state['phase']='final-convert-and-strictly-check';save()
 with (P/'complete-input.log').open('wb') as out:subprocess.run([str(P/'snapshots.test'),'-test.run=^TestOfflineAccountDeltaCaptureParallel$','-test.v','-test.timeout=180m'],env=env,stdout=out,stderr=subprocess.STDOUT,timeout=10850,check=True)
 assert json.loads((P/'complete-input-status.json').read_text())['complete'];assert json.loads((P/'complete-input-routing.json').read_text())['complete'];assert hashlib.sha256(manifest.read_bytes()).hexdigest()==digest
 state['after_identity']=identity();state['environment_identity_changed']=state['before_identity']!=state['after_identity'];state.update(complete=True,phase='complete');save()
except BaseException as e:state.update(phase='failed',error=repr(e),traceback=traceback.format_exc());save();raise
