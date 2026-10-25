import pathlib,json,hashlib,shutil,subprocess,os,datetime,traceback
P=pathlib.Path('/data/gtron/history-account-delta-20261025-01');OLD=pathlib.Path('/data/gtron/history-tx-runs-20261025-01');S=P/'candidate-source'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
s={'complete':False,'production_mutations':False}
try:
 inv=json.loads((OLD/'native-status.json').read_text())['production_source_inventory']
 for n,d in inv.items():assert sha(OLD/'candidate-source'/n)==d,n
 def ignore(path,names):return ['target'] if path==str(OLD/'candidate-source/third_party/librustzcash') else (['build','.git'] if path==str(OLD/'candidate-source') else [])
 shutil.copytree(str(OLD/'candidate-source'),str(S),symlinks=True,ignore=ignore)
 lib=S/'third_party/librustzcash/target/release/librustzcash.a';lib.parent.mkdir(parents=True,exist_ok=True);os.link(str(OLD/'candidate-source/third_party/librustzcash/target/release/librustzcash.a'),str(lib))
 for n in ['history_account_delta.go','history_account_delta_test.go','offline_account_delta_probe_test.go']:
  assert not (S/'core/state/snapshots'/n).exists();shutil.copyfile(str(P/n),str(S/'core/state/snapshots'/n))
 m=json.loads((OLD/'candidate-input/state-snapshots/manifest.json').read_text());refs=m['segments'];hist=sorted([r for r in refs if r['kind']=='history'],key=lambda r:r['fromTxNum']);access={r['path']:r for r in refs if r['kind']=='accessor'}
 indices=sorted(set([0,1,len(hist)//10,len(hist)//3,len(hist)//2,len(hist)*2//3,len(hist)*9//10,len(hist)-2,len(hist)-1]));plan=[]
 for i in indices:
  h=hist[i];plan.append(dict(Dir=str(OLD/'candidate-input/state-snapshots'),History=h,Accessor=access[h['path'][:-4]+'.kv']))
 cp=json.loads((OLD/'census-plan.json').read_text())
 for height in [25000000,27500000,30000000,32500000,35000000]:
  x=min(cp,key=lambda x:abs(x['first_block']-height));plan.append(dict(Dir=str(OLD/'census-input'),History=x['history_ref'],Accessor=x['accessor_ref']))
 (P/'probe-plan.json').write_text(json.dumps(plan,indent=2));env=dict(os.environ,PATH='/data/go/bin:'+os.environ.get('PATH',''),GOCACHE='/root/.cache/go-build',GOPATH='/root/go',GOTOOLCHAIN='local',CGO_ENABLED='1',GOMAXPROCS='2',GOMEMLIMIT='3GiB',GTRON_ACCOUNT_DELTA_ROOT=str(P))
 for label,cmd in [('probe-unit',['/data/go/bin/go','test','./core/state/snapshots','-run','^TestAccountDeltaPage','-count=1','-timeout=120s']),('probe-build',['/data/go/bin/go','test','-c','./core/state/snapshots','-o',str(P/'probe.test')]),('probe',['%s/probe.test'%P,'-test.run=^TestOfflineAccountDeltaProbe$','-test.v','-test.timeout=30m'])]:
  s['phase']=label;(P/'probe-driver-status.json').write_text(json.dumps(s))
  with (P/(label+'.log')).open('wb') as out:subprocess.run(cmd,cwd=str(S),env=env,stdout=out,stderr=subprocess.STDOUT,timeout=1900,check=True)
 s.update(complete=True,phase='complete',utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
except BaseException as e:s.update(phase='failed',error=repr(e),traceback=traceback.format_exc());raise
finally:(P/'probe-driver-status.json').write_text(json.dumps(s,indent=2))
