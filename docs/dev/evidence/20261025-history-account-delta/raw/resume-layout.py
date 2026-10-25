import pathlib,json,hashlib,os,subprocess,datetime,traceback,time,shutil
P=pathlib.Path('/data/gtron/history-account-delta-20261025-01');S=P/'candidate-source';state={'complete':False,'production_mutations':False,'parallel_workers':4}
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();p=P/'layout-status.json';p.with_suffix('.tmp').write_text(json.dumps(state,indent=2));p.with_suffix('.tmp').replace(p)
try:
 start=time.monotonic()
 while True:
  a=json.loads((P/'native-status.json').read_text())
  if a.get('phase')=='failed':raise RuntimeError('native tests failed')
  if a.get('complete'):break
  if time.monotonic()-start>3600:raise TimeoutError('native tests');
  time.sleep(3)
 assert int(subprocess.check_output(['nproc']))>=12
 mem=dict((x.split(':')[0],int(x.split()[1])*1024)for x in pathlib.Path('/proc/meminfo').read_text().splitlines());assert mem['MemAvailable']>16*(1<<30)
 state['original_snapshot_binary_sha256']=hashlib.sha256((P/'snapshots.test').read_bytes()).hexdigest()
 subprocess.run(['systemctl','stop','gtron-account-delta-layout-20261025-01'],check=True)
 for n in ['layout-status.json','complete-input-status.json','complete-input.log','complete-input-files.jsonl']:
  a=P/n;b=P/(n+'.sequential');assert not b.exists();a.rename(b)
 src=(P/'complete-input-files.jsonl.sequential').read_bytes();lines=src.splitlines(keepends=True);valid=[]
 for i,line in enumerate(lines):
  try:json.loads(line)
  except ValueError:
   assert i==len(lines)-1 and not line.endswith(b'\n');(P/'interrupted-receipt-tail.bin').write_bytes(line);break
  valid.append(line if line.endswith(b'\n') else line+b'\n')
 with (P/'complete-input-files.jsonl').open('xb')as f:
  f.writelines(valid);f.flush();os.fsync(f.fileno())
 state['resumed_receipts']=len(valid);state['phase']='resume-build';save()
 (S/'core/state/snapshots/offline_account_delta_parallel_test.go').write_bytes((P/'offline_account_delta_parallel_test.go').read_bytes())
 env=dict(os.environ,PATH='/data/go/bin:'+os.environ.get('PATH',''),GOCACHE='/root/.cache/go-build',GOPATH='/root/go',GOTOOLCHAIN='local',CGO_ENABLED='1',GOMAXPROCS='4',GOMEMLIMIT='3GiB',GTRON_ACCOUNT_DELTA_ROOT=str(P))
 with (P/'resume-build.log').open('wb')as f:subprocess.run(['/data/go/bin/go','test','-c','./core/state/snapshots','-o',str(P/'parallel-snapshots.test')],cwd=str(S),env=env,stdout=f,stderr=subprocess.STDOUT,check=True,timeout=600)
 state['parallel_binary_sha256']=hashlib.sha256((P/'parallel-snapshots.test').read_bytes()).hexdigest();state['phase']='parallel-convert-and-strictly-check';save()
 with (P/'complete-input.log').open('wb')as f:subprocess.run([str(P/'parallel-snapshots.test'),'-test.run=^TestOfflineAccountDeltaCaptureParallel$','-test.v','-test.timeout=180m'],cwd=str(S),env=env,stdout=f,stderr=subprocess.STDOUT,check=True,timeout=10850)
 assert json.loads((P/'complete-input-status.json').read_text())['complete'];assert json.loads((P/'complete-input-routing.json').read_text())['complete']
 for n,d in json.loads((P/'source-inventory.json').read_text()).items():assert hashlib.sha256((S/n).read_bytes()).hexdigest()==d,n
 state.update(complete=True,phase='complete');save()
except BaseException as e:state.update(phase='failed',error=repr(e),traceback=traceback.format_exc());save();raise
