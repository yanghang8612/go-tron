import datetime,hashlib,json,os,pathlib,subprocess,time,traceback
ROOT=pathlib.Path('/data/gtron/cold-query-layout-20261020-01');BASE=pathlib.Path('/data/gtron/eth-call-request-20261020-04');SOURCE=ROOT/'source'
state={'complete':False,'production_mutations':False,'phase':'waiting-for-prefix-comparison'}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(ROOT/'final-status.json').write_text(json.dumps(state,indent=2))
def identity():
 pid=int(subprocess.check_output(['systemctl','show','gtron','-p','MainPID']).decode().split('=')[1]);return {'pid':pid,'sha256':sha(pathlib.Path('/proc/%d/exe'%pid))}
def inventory():
 return {str(p.relative_to(SOURCE)):sha(p) for p in SOURCE.rglob('*') if p.is_file() and (p.suffix in ('.go','.rs') or p.name in ('go.mod','go.sum','Cargo.toml','Cargo.lock')) and not p.name.endswith('_test.go') and '/target/' not in str(p) and '/build/' not in str(p)}
try:
 state['before_identity']=identity();save();started=time.monotonic()
 while True:
  p=ROOT/'prefix-comparison/comparison-status.json'
  if p.exists():
   s=json.loads(p.read_text())
   if s.get('phase')=='failed':raise RuntimeError('prefix comparison failed')
   if s.get('complete'):break
  if time.monotonic()-started>7200:raise TimeoutError('prefix comparison wait')
  time.sleep(5)
 (ROOT/'source-prefix-writer-sha256.json').write_text(json.dumps(inventory(),indent=2))
 patches=json.loads((ROOT/'final-delta.json').read_text())['patches'];prepared=[]
 for entry in patches:
  p=SOURCE/entry['path'];assert sha(p)==entry['before'],entry['path'];lines=p.read_text().splitlines(keepends=True)
  for edit in reversed(entry['edits']):lines[edit['start']:edit['end']]=edit['text'].splitlines(keepends=True)
  data=''.join(lines).encode();assert hashlib.sha256(data).hexdigest()==entry['after'];prepared.append((p,data))
 for p,data in prepared:
  temp=p.with_name(p.name+'.prepared');temp.write_bytes(data);os.replace(temp,p)
 env=dict(os.environ,PATH='/data/go/bin:'+os.environ.get('PATH',''),GOTOOLCHAIN='local',CGO_ENABLED='1',GOCACHE='/root/.cache/go-build',GOPATH='/root/go',GOMAXPROCS='2',GOMEMLIMIT='3GiB',GTRON_HISTORY_COMPRESSION_FORMAT='2')
 for name in ('GTRON_LAYOUT_ROOT','GTRON_LAYOUT_PHASE','GTRON_FIXED_INPUT','GTRON_FIXED_OUTPUT'):env.pop(name,None)
 def run(cmd,name,timeout=1800):
  state['phase']=name;save()
  with (ROOT/name).open('wb') as out:subprocess.run(cmd,cwd=SOURCE,env=env,stdout=out,stderr=subprocess.STDOUT,timeout=timeout,check=True)
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','./core/state/snapshots','-count=1','-timeout=15m'],'native-final-snapshots.log')
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','./core/state','./core','./cmd/gtron','-run','^(TestHistoryRequest|TestHistoryExecution|TestHistoryStorage|TestMuHash|TestArchiveQuery_|TestSnapshot|TestRestore)','-count=1','-timeout=15m'],'native-final-correctness.log')
 run(['/data/go/bin/go','vet','-p=2','-tags=sapling','./core/state/snapshots','./core/state'],'native-final-vet.log')
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','-c','./core','-o',str(ROOT/'candidate.test')],'http-final-build.log',900)
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','-c','./core/state/snapshots','-o',str(ROOT/'layout-final.test')],'layout-final-build.log',900)
 env['GTRON_LAYOUT_ROOT']=str(ROOT)
 run([str(ROOT/'layout-final.test'),'-test.run=^TestOfflinePrefixWriterEquivalence$','-test.v','-test.timeout=10m'],'writer-equivalence.log',700)
 env.pop('GTRON_LAYOUT_ROOT',None)
 run(['/data/go/bin/go','build','-p=2','-tags=sapling','-o',str(ROOT/'gtron'),'./cmd/gtron'],'native-final-build.log',900)
 record={'production_sources':inventory(),'binaries':{name:sha(ROOT/name) for name in ('candidate.test','layout-final.test','gtron')},'http_harness_sha256':sha(SOURCE/'core/offline_fixed_http_test.go'),'sapling_sha256':sha(SOURCE/'third_party/librustzcash/target/release/librustzcash.a'),'go_version':subprocess.check_output(['/data/go/bin/go','version']).decode().strip()}
 assert record['http_harness_sha256']==json.loads((BASE/'source-identity.json').read_text())['harness_sha256']
 (ROOT/'source-final-identity.json').write_text(json.dumps(record,indent=2));state['after_identity']=identity();assert state['after_identity']==state['before_identity'];state.update(phase='ready',complete=True);save()
except BaseException as exc:
 state.update(phase='failed',error=repr(exc),traceback=traceback.format_exc());save();raise
