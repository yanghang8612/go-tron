import pathlib,shutil,os,subprocess,json,hashlib,traceback,datetime
P=pathlib.Path('/data/gtron/history-tx-runs-20261025-01');B=pathlib.Path('/data/gtron/history-key-owner-20261025-01');S=P/'census-source';state={'complete':False,'production_mutations':False}
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(P/'census-native-status.json').write_text(json.dumps(state))
try:
 assert (P/'census-pin-receipt.json').exists()
 state['phase']='prepare';save()
 inv=json.loads((B/'native-status.json').read_text())['production_source_inventory']
 for n,v in inv.items():assert hashlib.sha256((B/'candidate-source'/n).read_bytes()).hexdigest()==v,n
 def ignore(path,names):return ['target'] if path==str(B/'candidate-source/third_party/librustzcash') else (['build','.git'] if path==str(B/'candidate-source') else [])
 shutil.copytree(B/'candidate-source',S,symlinks=True,ignore=ignore);lib=S/'third_party/librustzcash/target/release/librustzcash.a';lib.parent.mkdir(parents=True);os.link(B/'candidate-source/third_party/librustzcash/target/release/librustzcash.a',lib)
 shutil.copy2(P/'offline_v11_census_test.go',S/'core/state/snapshots/offline_v11_census_test.go')
 env=dict(os.environ,PATH='/data/go/bin:'+os.environ.get('PATH',''),GOCACHE='/root/.cache/go-build',GOPATH='/root/go',GOTOOLCHAIN='local',CGO_ENABLED='1',GOMAXPROCS='2',GOMEMLIMIT='3GiB',GTRON_TX_RUN_ROOT=str(P))
 state['phase']='build';save()
 with (P/'census-build.log').open('wb') as o:subprocess.run(['/data/go/bin/go','test','-c','./core/state/snapshots','-o',str(P/'census.test')],cwd=S,env=env,stdout=o,stderr=subprocess.STDOUT,check=True,timeout=600)
 state['phase']='scan';state['source_inventory']=inv;state['binary_sha256']=hashlib.sha256((P/'census.test').read_bytes()).hexdigest();save()
 with (P/'census.log').open('wb') as o:subprocess.run([str(P/'census.test'),'-test.run=^TestOfflineV11MatureCensus$','-test.v','-test.timeout=6h'],cwd=S,env=env,stdout=o,stderr=subprocess.STDOUT,check=True,timeout=21660)
 assert json.loads((P/'census-status.json').read_text())['complete'];state.update(complete=True,phase='complete');save()
except BaseException as e:state.update(phase='failed',error=repr(e),traceback=traceback.format_exc());save();raise
