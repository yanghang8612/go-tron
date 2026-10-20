from pathlib import Path
import os,json,subprocess,traceback,hashlib,datetime,urllib.request
p=Path('/data/gtron/compact-v3-rollout-20261019-01/cold-query-opt-20261020');c=p/'candidate-final';s={'complete':False,'phase':'history-and-muhash'}
def save():
 s['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(p/'accepted-checkpoint-status.json').write_text(json.dumps(s,indent=2))
def run(cmd,env,log,timeout=600):
 with (p/log).open('wb') as f:
  r=subprocess.run(['nice','-n','15','ionice','-c3']+cmd,cwd=str(c),env=env,stdout=f,stderr=subprocess.STDOUT,timeout=timeout)
  if r.returncode:raise RuntimeError(log+' rc='+str(r.returncode)+' '+(p/log).read_text()[-2500:])
try:
 save();env=dict(os.environ,GOTOOLCHAIN='local',GOMAXPROCS='2',GOMEMLIMIT='4GiB',CGO_ENABLED='0',GOPATH='/root/go');env['PATH']='/data/go/bin:'+env.get('PATH','');env['GTRON_COLD_QUERY_CORPUS']=str(p/'corpus');env['GTRON_CHECKPOINT_DB']=str(p/'checkpoint-227222/chaindata');env['GTRON_CHECKPOINT_OUT']=str(p/'candidate-final-balance.json')
 run(['/data/go/bin/go','test','-p=2','./core/state','-run=^TestOfflineColdBalanceCheckpoint$','-v','-count=1','-timeout=5m'],env,'candidate-final-balance.log')
 run(['/data/go/bin/go','build','-p=2','-o',str(p/'generation-census-final'),'./cmd/generation-census'],env,'census-final-build.log')
 run([str(p/'generation-census-final'),'--db-dir',env['GTRON_CHECKPOINT_DB'],'--out',str(p/'candidate-final-census.json'),'--groups',str(p/'candidate-final-census-groups.jsonl'),'--max-duration','5m'],env,'candidate-final-census.log')
 report=json.loads((p/'candidate-final-census.json').read_text());prior=json.loads((p.parent/'census.json').read_text());assert report['complete'] and report['muhash_verified'] and report['muhash_root']==prior['muhash_root']
 s['muhash_verified']=True;s['block_num']=report['block_num'];s['phase']='native-build-and-regression';save()
 target=c/'third_party/librustzcash/target';target.parent.mkdir(parents=True,exist_ok=True);os.symlink('/data/gtron/go-tron/third_party/librustzcash/target',str(target))
 env['CGO_ENABLED']='1'
 run(['/data/go/bin/go','build','-p=2','-tags=sapling','-o',str(p/'gtron-candidate-final'),'./cmd/gtron'],env,'native-final-build.log')
 regex='Test(History(Genesis|ColdBound|ColdFirst|PointSeek|Header|StorageMetadata)|FirstStateDomainChangePostingMiss|PersistentHistoryReader|ContractAt|MuHash|SnapshotRestoreMuHash)'
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','./core/state','./core/rawdb','./core/state/snapshots','./core','./cmd/gtron','-run='+regex,'-count=1','-timeout=6m'],env,'native-final-tests.log',900)
 b=json.loads((p/'transfer-final.json').read_text());names=set(b['new_files'])
 for line in b['patch'].splitlines():
  if line.startswith('+++ b/'):names.add(line[6:])
 s['source_files_sha256']={f:hashlib.sha256((c/f).read_bytes()).hexdigest() for f in sorted(names)}
 s['candidate_binary_sha256']=hashlib.sha256((p/'gtron-candidate-final').read_bytes()).hexdigest()
 s['service']=subprocess.check_output(['systemctl','show','gtron','-p','MainPID','-p','ActiveState']).decode();s['production_binary_sha256']=hashlib.sha256(Path('/proc/10692/exe').read_bytes()).hexdigest()
 with urllib.request.urlopen('http://127.0.0.1:8090/wallet/getnodeinfo',timeout=8) as f:s['wallet']=json.load(f)
 s.update({'complete':True,'phase':'verified'});save()
except BaseException as e:s.update({'phase':'failed','error':repr(e),'traceback':traceback.format_exc()});save()
