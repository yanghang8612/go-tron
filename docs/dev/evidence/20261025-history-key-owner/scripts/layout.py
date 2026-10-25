import pathlib,json,hashlib,shutil,os,subprocess,datetime,traceback,time
P=pathlib.Path('/data/gtron/history-key-owner-20261025-01');SOURCE=pathlib.Path('/data/gtron/history-offset-pages-20261024-01/candidate-input');state={'complete':False,'production_mutations':False}
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
 state['before_identity']=identity();state['phase']='wait-preparation';save();start=time.monotonic()
 while True:
  p=P/'native-status.json';s=json.loads(p.read_text()) if p.exists() else {}
  if s.get('phase')=='failed':raise RuntimeError('preparation failed')
  if s.get('experiment_binaries_ready'):break
  if time.monotonic()-start>7200:raise TimeoutError('prepare')
  time.sleep(3)
 state['phase']='clone-private-inputs';save();replica(P/'baseline-input');replica(P/'candidate-input')
 manifest=SOURCE/'state-snapshots/manifest.json';digest=hashlib.sha256(manifest.read_bytes()).hexdigest();state['input_manifest_sha256']=digest
 env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='3GiB',GTRON_V10_ROOT=str(P),GTRON_V10_SOURCE=str(P/'baseline-input/state-snapshots'),GTRON_V10_TARGET=str(P/'candidate-input/state-snapshots'),GTRON_V10_MANIFEST=str(P/'baseline-input/state-snapshots/manifest.json'),GTRON_V10_LABEL='complete-input',GTRON_V10_COMPLETE_INPUT='1')
 state['phase']='convert-and-strictly-check';save()
 with (P/'complete-input.log').open('wb') as out:subprocess.run([str(P/'snapshots.test'),'-test.run=^TestOfflineV10Corpus$','-test.v','-test.timeout=60m'],env=env,stdout=out,stderr=subprocess.STDOUT,timeout=3650,check=True)
 assert json.loads((P/'complete-input-status.json').read_text())['complete'];assert json.loads((P/'complete-input-routing.json').read_text())['complete']
 assert hashlib.sha256(manifest.read_bytes()).hexdigest()==digest
 state['after_identity']=identity();state['environment_identity_changed']=state['after_identity']!=state['before_identity'];state.update(complete=True,phase='complete');save()
except BaseException as e:state.update(phase='failed',error=repr(e),traceback=traceback.format_exc());save();raise
