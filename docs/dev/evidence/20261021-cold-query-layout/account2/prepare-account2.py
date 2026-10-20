import datetime,hashlib,json,os,pathlib,shutil,subprocess,time,traceback
ROOT=pathlib.Path('/data/gtron/cold-query-layout-20261020-01');SOURCE=ROOT/'account2-source';BASE=ROOT/'routing-source'
state={'complete':False,'production_mutations':False,'phase':'preflight'}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(ROOT/'account2-preparation-status.json').write_text(json.dumps(state,indent=2))
def identity():
 pid=int(subprocess.check_output(['systemctl','show','gtron','-p','MainPID']).decode().split('=')[1]);return {'pid':pid,'sha256':sha(pathlib.Path('/proc/%d/exe'%pid))}
def inventory():
 return {str(p.relative_to(SOURCE)):sha(p) for p in SOURCE.rglob('*') if p.is_file() and (p.suffix in ('.go','.rs') or p.name in ('go.mod','go.sum','Cargo.toml','Cargo.lock')) and not p.name.endswith('_test.go') and '/target/' not in str(p) and '/build/' not in str(p)}
try:
 state['before_identity']=identity();save()
 assert json.loads((ROOT/'routing-verification-v3-status.json').read_text())['complete']
 assert subprocess.check_output(['systemctl','show','gtron-cold-routing-verify-v3-20261020.service','-p','ActiveState']).decode().strip()=='ActiveState=inactive'
 state['phase']='copy-frozen-source';save();shutil.copytree(BASE,SOURCE,ignore=shutil.ignore_patterns('target','build','.git'))
 target=SOURCE/'third_party/librustzcash/target';target.symlink_to((BASE/'third_party/librustzcash/target').resolve(),target_is_directory=True)
 baseline=json.loads((ROOT/'routing-source-identity.json').read_text())['production_sources'];assert inventory()==baseline,'frozen source changed'
 (ROOT/'account2-source-before.json').write_text(json.dumps(baseline,indent=2))
 delta=json.loads((ROOT/'account2-delta.json').read_text());prepared=[]
 for entry in delta['files']:
  path=SOURCE/entry['path'];assert (sha(path) if path.exists() else None)==entry['before'],entry['path']
  if 'source' in entry:data=entry['source'].encode()
  else:
   lines=path.read_text().splitlines(keepends=True)
   for edit in reversed(entry['edits']):lines[edit['start']:edit['end']]=edit['text'].splitlines(keepends=True)
   data=''.join(lines).encode()
  assert hashlib.sha256(data).hexdigest()==entry['after'];prepared.append((path,data))
 for path,data in prepared:
  path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
 (SOURCE/'core/state/snapshots/offline_routing_test.go').write_text((ROOT/'offline_account2_test.go').read_text())
 env=dict(os.environ,PATH='/data/go/bin:'+os.environ.get('PATH',''),GOTOOLCHAIN='local',CGO_ENABLED='1',GOCACHE='/root/.cache/go-build',GOPATH='/root/go',GOMAXPROCS='2',GOMEMLIMIT='3GiB',GTRON_HISTORY_COMPRESSION_FORMAT='2')
 for name in ('GTRON_LAYOUT_ROOT','GTRON_LAYOUT_PHASE','GTRON_FIXED_INPUT','GTRON_FIXED_OUTPUT','GTRON_ROUTING_ROOT'):env.pop(name,None)
 def run(cmd,name,timeout=1800):
  state['phase']=name;save()
  with (ROOT/name).open('wb') as out:subprocess.run(cmd,cwd=SOURCE,env=env,stdout=out,stderr=subprocess.STDOUT,timeout=timeout,check=True)
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','./core/state/snapshots','-count=1','-timeout=15m'],'account2-native-snapshots.log')
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','./core/state','./core','./cmd/gtron','-run','^(TestHistoryRequest|TestHistoryExecution|TestHistoryStorage|TestMuHash|TestArchiveQuery_|TestSnapshot|TestRestore)','-count=1','-timeout=15m'],'account2-native-correctness.log')
 run(['/data/go/bin/go','vet','-p=2','-tags=sapling','./core/state/snapshots','./core/state'],'account2-native-vet.log')
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','-c','./core','-o',str(ROOT/'account2-candidate.test')],'account2-http-build.log',900)
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','-c','./core/state/snapshots','-o',str(ROOT/'account2-layout.test')],'account2-layout-build.log',900)
 run(['/data/go/bin/go','build','-p=2','-tags=sapling','-o',str(ROOT/'gtron-account2'),'./cmd/gtron'],'account2-native-build.log',900)
 record={'production_sources':inventory(),'binaries':{name:sha(ROOT/name) for name in ('account2-candidate.test','account2-layout.test','gtron-account2')},'http_harness_sha256':sha(SOURCE/'core/offline_fixed_http_test.go'),'muhash_harness_sha256':sha(SOURCE/'core/offline_request_muhash_test.go'),'routing_harness_sha256':sha(SOURCE/'core/state/snapshots/offline_routing_test.go'),'sapling_sha256':sha(SOURCE/'third_party/librustzcash/target/release/librustzcash.a'),'go_version':subprocess.check_output(['/data/go/bin/go','version']).decode().strip(),'delta_sha256':sha(ROOT/'account2-delta.json')}
 assert record['http_harness_sha256']==json.loads((ROOT/'routing-source-identity.json').read_text())['http_harness_sha256']
 (ROOT/'account2-source-identity.json').write_text(json.dumps(record,indent=2))
 state['phase']='isolate-account2-input';save();source=ROOT/'routing-input';target=ROOT/'account2-input';target.mkdir(exist_ok=False)
 for directory,dirs,files in os.walk(source):
  rel=pathlib.Path(directory).relative_to(source);(target/rel).mkdir(exist_ok=True)
  for name in files:
   if name in ('LOCK','FLOCK'):continue
   a=pathlib.Path(directory)/name;b=target/rel/name
   if a.suffix in ('.sst','.seg','.idx','.kv') or str(rel).startswith('ancient'):os.link(a,b)
   else:shutil.copy2(a,b)
 env['GTRON_ROUTING_ROOT']=str(ROOT)
 run([str(ROOT/'account2-layout.test'),'-test.run=^TestOfflineAccountHistoryRouting$','-test.v','-test.timeout=2h'],'account2-build.log',7300)
 state['after_identity']=identity();assert state['after_identity']==state['before_identity'];state.update(phase='ready',complete=True);save()
except BaseException as exc:
 state.update(phase='failed',error=repr(exc),traceback=traceback.format_exc());save();raise
