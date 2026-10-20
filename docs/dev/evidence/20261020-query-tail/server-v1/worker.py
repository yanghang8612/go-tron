from pathlib import Path
import os,json,time,subprocess,hashlib,datetime,traceback,statistics
p=Path('/data/gtron/cold-query-tail-20261020');q=Path('/data/gtron/compact-v3-rollout-20261019-01/cold-query-opt-20261020')
s={'complete':False,'production_mutations':False}
def identity():
 pid=subprocess.check_output(['systemctl','show','gtron','-p','MainPID']).decode().strip().split('=')[1]
 return {'pid':int(pid),'binary_sha256':hashlib.sha256(Path('/proc/'+pid+'/exe').read_bytes()).hexdigest(),'service':subprocess.check_output(['systemctl','show','gtron','-p','ActiveState']).decode().strip()}
def save():
 s['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(p/'status.json').write_text(json.dumps(s,indent=2))
def run(cmd,log,cwd=None,env=None,expected=0):
 s['phase']=log;save()
 with (p/log).open('wb') as f:r=subprocess.run(['nice','-n','15','ionice','-c3']+cmd,cwd=str(cwd or p/'candidate'),env=env,stdout=f,stderr=subprocess.STDOUT,timeout=900)
 if r.returncode!=expected:raise RuntimeError(log+' rc='+str(r.returncode)+' '+(p/log).read_text()[-3000:])
try:
 s['before_identity']=identity();save()
 # Retain exactly the immutable event segment containing the canonical witness.
 source=Path('/data/gtron/main/datadir/gtron/state-snapshots')
 manifest_bytes=(p/'source-manifest.json').read_bytes() if (p/'source-manifest.json').exists() else (source/'manifest.json').read_bytes();manifest=json.loads(manifest_bytes)
 (p/'source-manifest.json').write_bytes(manifest_bytes)
 refs=[r for r in manifest['segments'] if r['kind']=='event-log' and r['fromTxNum']<=3144021<=r['toTxNum']]
 assert len(refs)==1,refs
 ref=refs[0];d=p/'event-copy';d.mkdir(exist_ok=True)
 dst=d/ref['path'];dst.parent.mkdir(parents=True,exist_ok=True);os.link(str(source/ref['path']),str(dst)) if not dst.exists() else None
 (d/'source-ref.json').write_text(json.dumps(ref))
 st=dst.stat();s['event_copy']={'ref':ref,'inode':st.st_ino,'bytes':st.st_size,'mtime_ns':st.st_mtime_ns,'manifest_sha256':hashlib.sha256(manifest_bytes).hexdigest()};save()
 env=dict(os.environ,PATH='/data/go/bin:'+os.environ.get('PATH',''),GOTOOLCHAIN='local',GOPATH='/root/go',GOCACHE='/root/.cache/go-build',CGO_ENABLED='0',GOMAXPROCS='2',GOMEMLIMIT='2GiB',GTRON_EVENT_COPY=str(d),GTRON_EVENT_OUT=str(p/'event-result.json'),GTRON_CHECKPOINT_DB=str(q/'checkpoint-227222/chaindata'),GTRON_COLD_QUERY_CORPUS=str(q/'corpus'),GTRON_CHECKPOINT_OUT=str(p/'balance-result.json'))
 run(['/data/go/bin/go','test','-p=2','./core/state/snapshots','-run','^TestOfflineCounterLogAddressFilter$','-v','-count=1'], 'baseline-event.log',p/'baseline',env,expected=1)
 assert 'filtered 0 vs expected 1' in (p/'baseline-event.log').read_text()
 run(['/data/go/bin/go','test','-p=2','./core/state/snapshots','-run','^TestOfflineCounterLogAddressFilter$','-v','-count=1'], 'candidate-event.log',env=env)
 run(['/data/go/bin/go','test','-p=2','./core/state','-run','^TestOfflineColdBalanceCheckpoint$','-v','-count=1'], 'candidate-balance.log',env=env)
 run(['/data/go/bin/go','test','-p=2','./core/rawdb','-run','^TestQueryTail|^TestFirstStateDomainChangePosting','-count=1','-timeout','180s'], 'candidate-tail.log',env=env)
 run(['/data/go/bin/go','test','-p=2','./core/rawdb','-run','^$','-bench','^BenchmarkQueryTailDensePointReads$','-benchtime=1x','-count=3'], 'dense-bench.log',env=env)
 run(['/data/go/bin/go','run','./cmd/generation-census','--db-dir',str(q/'checkpoint-227222/chaindata'),'--out',str(p/'census.json'),'--groups',str(p/'census-groups.jsonl'),'--verify-muhash=true'], 'census.log',env=env)
 env['CGO_ENABLED']='1'
 rust=p/'candidate/third_party/librustzcash/target';rust.parent.mkdir(parents=True,exist_ok=True);rust.symlink_to('/data/gtron/go-tron/third_party/librustzcash/target',target_is_directory=True)
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','./core','./cmd/gtron','./core/state','-run','TestJSONRPCLogAddressFilterTVMAndNative|TestArchiveQuery_|TestMuHash|Test.*Restore|TestHistoryStorageMetadataAcrossRecreate','-count=1','-timeout','600s'],'native-correctness.log',env=env)
 run(['/data/go/bin/go','build','-p=2','-tags=sapling','-o',str(p/'gtron-candidate'),'./cmd/gtron'],'native-build.log',env=env)
 s['candidate_binary_sha256']=hashlib.sha256((p/'gtron-candidate').read_bytes()).hexdigest()
 st2=dst.stat();assert (st.st_ino,st.st_size,st.st_mtime_ns)==(st2.st_ino,st2.st_size,st2.st_mtime_ns)
 s['after_identity']=identity();assert s['before_identity']==s['after_identity']
 s.update(complete=True,phase='verified');save()
except BaseException as e:
 s.update(phase='failed',error=repr(e),traceback=traceback.format_exc());save()
