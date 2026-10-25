import pathlib,json,subprocess,os,hashlib,traceback,datetime
P=pathlib.Path('/data/gtron/history-key-owner-20261025-01');S=P/'candidate-source';state={'complete':False,'production_mutations':False}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(P/'native-status.json').write_text(json.dumps(state,indent=2))
env=dict(os.environ,PATH='/data/go/bin:'+os.environ.get('PATH',''),GOCACHE='/root/.cache/go-build',GOPATH='/root/go',GOTOOLCHAIN='local',CGO_ENABLED='1',GOMAXPROCS='2',GOMEMLIMIT='3GiB')
def run(label,args,timeout=1500):
 state['phase']=label;save()
 with (P/(label+'.log')).open('wb') as out:subprocess.run(args,cwd=str(S),env=env,stdout=out,stderr=subprocess.STDOUT,check=True,timeout=timeout)
try:
 assert json.loads((P/'prepare-status.json').read_text())['complete']
 inv=json.loads((P/'prepare-status.json').read_text())['production_source_inventory']
 state['production_source_inventory']=inv;state['http_harness_sha256']=sha(S/'core/offline_fixed_http_test.go');assert state['http_harness_sha256']=='10e6015af7666bf83b5c405543681c368b7537f428b8278a3344f0312aacd3bf'
 run('native-reader-tests',['/data/go/bin/go','test','./core/state/snapshots','-run','Test(HistoryOwner|HistoryOffsetPages|StateDomainChangeV[789]|HistoryPostingWriter)','-count=1','-timeout=180s'],240)
 run('snapshots-build',['/data/go/bin/go','test','-c','./core/state/snapshots','-o',str(P/'snapshots.test')],300)
 run('candidate-build',['/data/go/bin/go','test','-c','./core','-o',str(P/'candidate.test')],300)
 # Independent bounded experiment units may start after immutable binaries exist.
 state['experiment_binaries_ready']=True;save()
 run('native-packages',['/data/go/bin/go','test','./core/rawdb','./core/state','./core/state/snapshots','./core','./cmd/gtron','-count=1','-timeout=1200s'],1400)
 run('native-race',['/data/go/bin/go','test','-race','./core/state/snapshots','-run','Test(HistoryOwner|HistoryOffsetPages|StateDomainChangeV[789]|HistoryPostingWriter)','-count=1','-timeout=240s'],300)
 run('native-vet',['/data/go/bin/go','vet','./core/state/snapshots','./core/state','./core/rawdb','./core','./cmd/gtron'],300)
 run('native-gtron-build',['/data/go/bin/go','build','-o',str(P/'gtron-candidate'),'./cmd/gtron'],300)
 for n,v in inv.items():assert sha(S/n)==v,n
 state['binaries']={n:sha(P/n) for n in ['baseline.test','candidate.test','snapshots.test','gtron-candidate']};state.update(complete=True,phase='complete');save()
except BaseException as e:state.update(phase='failed',error=repr(e),traceback=traceback.format_exc());save();raise
