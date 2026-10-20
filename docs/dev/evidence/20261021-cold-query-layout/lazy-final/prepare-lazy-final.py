import datetime,hashlib,json,os,pathlib,shutil,subprocess,time,traceback
ROOT=pathlib.Path('/data/gtron/cold-query-layout-20261020-01');SOURCE=ROOT/'lazy-final-source';BASE=ROOT/'account-gc-source'
state={'complete':False,'production_mutations':False}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(ROOT/'lazy-final-preparation-status.json').write_text(json.dumps(state,indent=2))
def inventory():
 return {str(p.relative_to(SOURCE)):sha(p) for p in SOURCE.rglob('*') if p.is_file() and (p.suffix in ('.go','.rs') or p.name in ('go.mod','go.sum','Cargo.toml','Cargo.lock')) and not p.name.endswith('_test.go') and '/target/' not in str(p) and '/build/' not in str(p)}
try:
 assert json.loads((ROOT/'account-gc-verification-status.json').read_text())['complete'];assert json.loads((ROOT/'lazy-caller-preflight-status.json').read_text())['complete']
 state['phase']='freeze-final-source';save();shutil.copytree(BASE,SOURCE,ignore=shutil.ignore_patterns('target','build','.git'));(SOURCE/'third_party/librustzcash/target').symlink_to((BASE/'third_party/librustzcash/target').resolve(),target_is_directory=True)
 expected=json.loads((ROOT/'account-gc-source-identity.json').read_text())['production_sources'];assert inventory()==expected
 delta=json.loads((ROOT/'lazy-caller-delta.json').read_text());p=SOURCE/'core/state/history_request_cache.go';assert sha(p)==delta['before'];p.write_text(delta['source']);assert sha(p)==delta['after'];expected['core/state/history_request_cache.go']=delta['after'];assert inventory()==expected
 env=dict(os.environ,PATH='/data/go/bin:'+os.environ.get('PATH',''),GOTOOLCHAIN='local',CGO_ENABLED='1',GOCACHE='/root/.cache/go-build',GOPATH='/root/go',GOMAXPROCS='2',GOMEMLIMIT='3GiB')
 for name in tuple(env):
  if name.startswith('GTRON_'):env.pop(name)
 def run(cmd,name,timeout=1200):
  state['phase']=name;save()
  with (ROOT/name).open('wb') as out:subprocess.run(cmd,cwd=SOURCE,env=env,stdout=out,stderr=subprocess.STDOUT,check=True,timeout=timeout)
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','./core/state','./core','./cmd/gtron','-run','^(TestHistoryRequest|TestHistoryExecution|TestHistoryStorage|TestMuHash|TestArchiveQuery_|TestSnapshot|TestRestore)','-count=1','-timeout=15m'],'lazy-final-native-correctness.log')
 run(['/data/go/bin/go','vet','-p=2','-tags=sapling','./core/state/snapshots','./core/state','./core','./cmd/gtron'],'lazy-final-native-vet.log')
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','-c','./core','-o',str(ROOT/'lazy-final-candidate.test')],'lazy-final-http-build.log')
 run(['/data/go/bin/go','build','-p=2','-tags=sapling','-o',str(ROOT/'gtron-lazy-final'),'./cmd/gtron'],'lazy-final-native-build.log')
 record={'commit':'0b370448','production_sources':inventory(),'candidate_sha256':sha(ROOT/'lazy-final-candidate.test'),'gtron_sha256':sha(ROOT/'gtron-lazy-final'),'http_harness_sha256':sha(SOURCE/'core/offline_fixed_http_test.go'),'muhash_harness_sha256':sha(SOURCE/'core/offline_request_muhash_test.go'),'sapling_sha256':sha(SOURCE/'third_party/librustzcash/target/release/librustzcash.a')};assert record['http_harness_sha256']=='10e6015af7666bf83b5c405543681c368b7537f428b8278a3344f0312aacd3bf';(ROOT/'lazy-final-source-identity.json').write_text(json.dumps(record,indent=2))
 state.update(complete=True,phase='complete');save()
except BaseException as exc:
 state.update(phase='failed',error=repr(exc),traceback=traceback.format_exc());save();raise
