from pathlib import Path
import os,json,subprocess,hashlib,datetime,traceback,shutil
p=Path('/data/gtron/cold-query-tail-20261020');q=Path('/data/gtron/compact-v3-rollout-20261019-01/cold-query-opt-20261020')
s=json.loads((p/'status.json').read_text());(p/'status-v1.json').write_text(json.dumps(s,indent=2));s.update(complete=False,phase='early-hit-amendment')
def identity():
 pid=subprocess.check_output(['systemctl','show','gtron','-p','MainPID']).decode().strip().split('=')[1]
 return {'pid':int(pid),'binary_sha256':hashlib.sha256(Path('/proc/'+pid+'/exe').read_bytes()).hexdigest(),'service':subprocess.check_output(['systemctl','show','gtron','-p','ActiveState']).decode().strip()}
def save():
 s['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(p/'status.json').write_text(json.dumps(s,indent=2))
def run(cmd,name,env):
 s['phase']=name;save()
 with (p/name).open('wb') as f:r=subprocess.run(['nice','-n','15','ionice','-c3']+cmd,cwd=str(p/'candidate'),env=env,stdout=f,stderr=subprocess.STDOUT,timeout=900)
 if r.returncode:raise RuntimeError(name+' '+(p/name).read_text()[-3000:])
try:
 assert identity()==s['before_identity'];save()
 env=dict(os.environ,PATH='/data/go/bin:'+os.environ.get('PATH',''),GOTOOLCHAIN='local',GOPATH='/root/go',GOCACHE='/root/.cache/go-build',GOMAXPROCS='2',GOMEMLIMIT='2GiB',CGO_ENABLED='0')
 run(['/data/go/bin/go','test','-p=2','./core/rawdb','./core/state','-count=1','-timeout','300s'],'final-packages.log',env)
 run(['/data/go/bin/go','test','-p=2','./core/rawdb','-run','^$','-bench','^BenchmarkQueryTailDensePointReads$','-benchtime=1x','-count=3'],'final-dense-bench.log',env)
 env.update(GTRON_CHECKPOINT_DB=str(q/'checkpoint-227222/chaindata'),GTRON_COLD_QUERY_CORPUS=str(q/'corpus'),GTRON_CHECKPOINT_OUT=str(p/'final-balance-result.json'))
 run(['/data/go/bin/go','test','-p=2','./core/state','-run','^TestOfflineColdBalanceCheckpoint$','-v','-count=1'],'final-balance.log',env)
 run(['/data/go/bin/go','run','./cmd/generation-census','--db-dir',str(q/'checkpoint-227222/chaindata'),'--out',str(p/'final-census.json'),'--groups',str(p/'final-census-groups.jsonl'),'--verify-muhash=true'],'final-census.log',env)
 env['CGO_ENABLED']='1'
 run(['/data/go/bin/go','test','-p=2','-race','./core/rawdb','-run','^TestQueryTail','-count=1','-timeout','180s'],'final-race.log',env)
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','./core','./cmd/gtron','./core/state','-run','TestJSONRPCLogAddressFilterTVMAndNative|TestArchiveQuery_|TestMuHash|Test.*Restore|TestHistoryStorageMetadataAcrossRecreate','-count=1','-timeout','600s'],'final-native-correctness.log',env)
 run(['/data/go/bin/go','build','-p=2','-tags=sapling','-o',str(p/'gtron-candidate-final'),'./cmd/gtron'],'final-native-build.log',env)
 s['candidate_binary_sha256']=hashlib.sha256((p/'gtron-candidate-final').read_bytes()).hexdigest();s['after_identity']=identity();assert s['after_identity']==s['before_identity']
 s.update(complete=True,phase='verified-final',early_positive_stop_verified=True);save()
except BaseException as e:
 s.update(phase='failed-final',error=repr(e),traceback=traceback.format_exc());save()
