import pathlib,json,subprocess,os,hashlib,traceback,datetime,time
P=pathlib.Path('/data/gtron/history-account-delta-20261025-01');S=P/'candidate-source';state={'complete':False,'production_mutations':False,'candidate':'owner-columns-bounded-numeric-deltas-1MiB-depth4-cache-byte-budget'}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();p=P/'native-status.json';p.with_suffix('.tmp').write_text(json.dumps(state,indent=2));p.with_suffix('.tmp').replace(p)
env=dict(os.environ,PATH='/data/go/bin:'+os.environ.get('PATH',''),GOCACHE='/root/.cache/go-build',GOPATH='/root/go',GOTOOLCHAIN='local',CGO_ENABLED='1',GOMAXPROCS='2',GOMEMLIMIT='3GiB')
def run(label,args,timeout=1500):
 state['phase']=label;save()
 with (P/(label+'.log')).open('wb') as out:subprocess.run(args,cwd=str(S),env=env,stdout=out,stderr=subprocess.STDOUT,check=True,timeout=timeout)
try:
 assert json.loads((P/'probe-v4-driver-status.json').read_text())['complete']
 (S/'core/state/snapshots/offline_account_delta_parallel_test.go').write_bytes((P/'offline_account_delta_parallel_test.go').read_bytes())
 (S/'core/state/snapshots/history_account_delta_numeric_test.go').write_bytes((P/'history_account_delta_numeric_test.go').read_bytes())
 inv=json.loads((P/'final-source-inventory.json').read_text());state['production_source_inventory']=inv
 for n,d in inv.items():assert sha(S/n)==d,n
 state['offline_harness_inventory']={str(x.relative_to(S)):sha(x) for x in S.glob('core/**/offline*_test.go')};state['http_harness_sha256']=sha(S/'core/offline_fixed_http_test.go');assert state['http_harness_sha256']=='10e6015af7666bf83b5c405543681c368b7537f428b8278a3344f0312aacd3bf'
 run('bounded-reader-tests',['/data/go/bin/go','test','./core/state/snapshots','-run','Test(AccountDelta|HistoryReferenceTranscodeAllFormatsFullTrio|HistoryTxRuns)','-count=1','-timeout=180s'],240)
 run('bounded-snapshots-build',['/data/go/bin/go','test','-c','./core/state/snapshots','-o',str(P/'bounded-snapshots.test')],300)
 run('bounded-candidate-build',['/data/go/bin/go','test','-c','./core','-o',str(P/'bounded-candidate.test')],300)
 for a,b in [('bounded-candidate.test','candidate.test')]:
  src=P/a;tmp=P/(b+'.numeric-next');os.link(str(src),str(tmp));tmp.replace(P/b)
 state['experiment_binaries_ready']=True;save()
 run('bounded-native-packages',['/data/go/bin/go','test','./core/rawdb','./core/state','./core/state/snapshots','./core','./cmd/gtron','-count=1','-timeout=1200s'],1400)
 run('bounded-native-race',['/data/go/bin/go','test','-race','./core/state/snapshots','-run','Test(AccountDelta|HistoryReferenceTranscodeAllFormatsFullTrio|HistoryTxRuns)','-count=1','-timeout=240s'],300)
 run('bounded-native-vet',['/data/go/bin/go','vet','./core/state/snapshots','./core/state','./core/rawdb','./core','./cmd/gtron'],300)
 run('bounded-native-gtron-build',['/data/go/bin/go','build','-o',str(P/'gtron-numeric-bounded'),'./cmd/gtron'],300)
 for n,d in inv.items():assert sha(S/n)==d,n
 state['binaries']={n:sha(P/n) for n in ['baseline.test','candidate.test','snapshots.test','bounded-snapshots.test','gtron-numeric-bounded']};state.update(complete=True,phase='complete');save()
except BaseException as e:state.update(phase='failed',error=repr(e),traceback=traceback.format_exc());save();raise
