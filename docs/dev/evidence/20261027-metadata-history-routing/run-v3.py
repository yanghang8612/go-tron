import pathlib,subprocess,os,shutil,json,traceback,datetime,hashlib,base64,zlib,time
A=pathlib.Path('/data/gtron/maintenance/9m-query-20261027-01');P=A/'metadata-v3';P.mkdir(mode=0o700,exist_ok=True)
s={'complete':False,'stage':'setup','production_mutations':False}
def save():
 s['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(P/'status.json').write_text(json.dumps(s,indent=2))
def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def put(p,data):p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data)
env=dict(os.environ,GOCACHE='/data/gtron/deploy-cache/go-build',GOPATH='/root/go',GOMODCACHE='/root/go/pkg/mod',GOTOOLCHAIN='local',CGO_ENABLED='1',CC='/opt/rh/devtoolset-11/root/usr/bin/gcc',CXX='/opt/rh/devtoolset-11/root/usr/bin/g++',GOMAXPROCS='2',GOMEMLIMIT='3GiB')
def command(cmd,log,ev=env,cwd=None):
 with (P/log).open('wb') as f:subprocess.check_call(cmd,env=ev,cwd=cwd,stdout=f,stderr=subprocess.STDOUT)
def clone(a,b):
 if pathlib.Path(a).name=='manifest.json':shutil.copyfile(a,b)
 else:os.link(a,b)
 return b
try:
 save();packet=json.loads(zlib.decompress((A/'metadata-packet-v3.zlib').read_bytes()));src=P/'source';shutil.copytree(str(A/'final/candidate-source'),str(src))
 assert {n:digest(src/n) for n in packet['before_sha256']}==packet['before_sha256']
 result=subprocess.run(['patch','-p1','--forward'],input=packet['patch'].encode(),cwd=str(src),stdout=subprocess.PIPE,stderr=subprocess.STDOUT);(P/'patch.log').write_bytes(result.stdout);assert result.returncode==0
 for name,b in packet['files'].items():put(src/name,base64.b64decode(b))
 s['product_sha256']={n:digest(src/n) for n in packet['product_sha256']};assert s['product_sha256']==packet['product_sha256'];save()
 s.update(stage='compile');save();command(['/data/go/bin/go','test','-c','./core/state/snapshots','-o',str(P/'candidate.test')],'build.log',cwd=str(src))
 s['binary_sha256']=digest(P/'candidate.test');s.update(stage='native correctness');save();command([str(P/'candidate.test'),'-test.run=^TestMetadataHistoryRouting','-test.v','-test.timeout=10m'],'native-routing-tests.log');command(['/data/go/bin/go','test','./core/state','-run=^TestSetContract|^TestTransactionVersionedMetadata','-count=1','-timeout=10m'],'native-state-tests.log',cwd=str(src));s.update(stage='clone fixed cold catalog');save()
 shutil.copytree(str(A/'full-route/catalog'),str(P/'catalog'),copy_function=clone)
 ev=dict(env,GTRON_NINEM_CATALOG=str(P/'catalog'),GTRON_NINEM_OUTPUT=str(P),GTRON_NINEM_QUERIES=str(A/'probe/queries.json'))
 s.update(stage='build complete metadata pages');save();command([str(P/'candidate.test'),'-test.run=^TestOfflineBuildMetadata$','-test.v','-test.timeout=120m'],'index-build.log',ev)
 s.update(stage='interleaved fixed queries before independent strict proof');save()
 modes={'baseline':(A/'final/baseline.test',A/'baseline-catalog'),'previous':(A/'final/candidate.test',A/'full-route/catalog'),'candidate':(P/'candidate.test',P/'catalog')}
 oracle=json.loads((A/'probe/baseline-first/rows.json').read_text())
 for round_ in range(2):
  for index,mode in enumerate(('baseline','previous','candidate','candidate','previous','baseline')):
   out=P/('repeat-%d-%d-%s'%(round_,index,mode));out.mkdir();binary,cat=modes[mode];e=dict(ev,GTRON_NINEM_CATALOG=str(cat),GTRON_NINEM_OUTPUT=str(out),GTRON_NINEM_ITERATIONS='3')
   command([str(binary),'-test.run=^TestOfflineNineMColdQuery$','-test.v','-test.timeout=20m'],str(out.relative_to(P)/'run.log'),e)
   assert json.loads((out/'rows.json').read_text())==oracle
 for mode in ('baseline','previous','candidate'):
  out=P/('metadata-'+mode);out.mkdir();binary,cat=modes[mode];e=dict(ev,GTRON_NINEM_CATALOG=str(cat),GTRON_NINEM_OUTPUT=str(out),GTRON_NINEM_ITERATIONS='3',GTRON_NINEM_SCOPE='metadata')
  command([str(binary),'-test.run=^TestOfflineNineMColdQuery$','-test.v','-test.timeout=20m'],str(out.relative_to(P)/'run.log'),e)
  assert json.loads((out/'rows.json').read_text())==[None]
 s.update(stage='independent strict full dictionary verification');save();command([str(P/'candidate.test'),'-test.run=^TestOfflineStrictMetadata$','-test.v','-test.timeout=120m'],'strict.log',ev)
 s.update(stage='historical full-row restoration');save();out=P/'restoration';out.mkdir();e=dict(ev,GTRON_NINEM_OUTPUT=str(out),GTRON_NINEM_ORIGINAL=str(A/'baseline-catalog'))
 command([str(P/'candidate.test'),'-test.run=^TestOfflineNineMRestore$','-test.v','-test.timeout=30m'],'restoration/run.log',e)
 s.update(complete=True,stage='done');save()
except Exception:
 s['error']=traceback.format_exc();save();raise
