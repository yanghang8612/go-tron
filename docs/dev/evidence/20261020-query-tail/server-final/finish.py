from pathlib import Path
import os,json,subprocess,hashlib,datetime,traceback
p=Path('/data/gtron/cold-query-tail-20261020');s=json.loads((p/'status.json').read_text());(p/'server-race-timeout-status.json').write_text(json.dumps(s,indent=2));s.pop('error',None);s.pop('traceback',None);s.update(complete=False,server_race_verified=False,server_race_note='900s build timeout before test execution; bounded -x probe reached go-ethereum crypto/secp256k1 cgo compilation. Local Go 1.25.5 race passed. Preserve raw diagnostics; do not count server race as passed.')
def identity():
 pid=subprocess.check_output(['systemctl','show','gtron','-p','MainPID']).decode().strip().split('=')[1]
 return {'pid':int(pid),'binary_sha256':hashlib.sha256(Path('/proc/'+pid+'/exe').read_bytes()).hexdigest(),'service':subprocess.check_output(['systemctl','show','gtron','-p','ActiveState']).decode().strip()}
def save():
 s['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(p/'status.json').write_text(json.dumps(s,indent=2))
def run(cmd,name,env):
 s['phase']=name;save()
 with (p/name).open('wb') as f:r=subprocess.run(['nice','-n','15','ionice','-c3']+cmd,cwd=str(p/'candidate'),env=env,stdout=f,stderr=subprocess.STDOUT,timeout=600)
 if r.returncode:raise RuntimeError(name+' '+(p/name).read_text()[-3000:])
try:
 assert identity()==s['before_identity'];save()
 env=dict(os.environ,PATH='/data/go/bin:'+os.environ.get('PATH',''),GOTOOLCHAIN='local',GOPATH='/root/go',GOCACHE='/root/.cache/go-build',GOMAXPROCS='2',GOMEMLIMIT='2GiB',CGO_ENABLED='1')
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','./core','./cmd/gtron','./core/state','-run','TestJSONRPCLogAddressFilterTVMAndNative|TestArchiveQuery_|TestMuHash|Test.*Restore|TestHistoryStorageMetadataAcrossRecreate','-count=1','-timeout','300s'],'final-native-correctness.log',env)
 run(['/data/go/bin/go','build','-p=2','-tags=sapling','-o',str(p/'gtron-candidate-final'),'./cmd/gtron'],'final-native-build.log',env)
 s['candidate_binary_sha256']=hashlib.sha256((p/'gtron-candidate-final').read_bytes()).hexdigest();s['after_identity']=identity();assert s['after_identity']==s['before_identity']
 s.update(complete=True,phase='verified-final',early_positive_stop_verified=True);save()
except BaseException as e:
 s.update(phase='failed-native-final',error=repr(e),traceback=traceback.format_exc());save()
