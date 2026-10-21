import pathlib,json,hashlib,shutil,os,subprocess,datetime,traceback
OLD=pathlib.Path('/data/gtron/cold-query-layout-20261020-01');PREV=OLD/'tail-read-final-20261021-02';ROOT=OLD/'hot-owner-spans-20261021-02';BASE=OLD/'hot-owner-spans-20261021-01/source';SOURCE=ROOT/'source'
state={'complete':False,'production_mutations':False}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save():state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();p=ROOT/'preparation-status.json';tmp=p.with_suffix('.tmp');tmp.write_text(json.dumps(state,indent=2));tmp.replace(p)
def identity():
 pid=int(subprocess.check_output(['systemctl','show','gtron','-p','MainPID']).decode().split('=')[1]);return {'pid':pid,'sha256':sha(pathlib.Path('/proc/%d/exe'%pid))}
def inventory():
 return {str(p.relative_to(SOURCE)):sha(p) for p in SOURCE.rglob('*') if p.is_file() and (p.suffix in ('.go','.rs') or p.name in ('go.mod','go.sum','Cargo.toml','Cargo.lock')) and not p.name.endswith('_test.go') and '/target/' not in str(p) and '/build/' not in str(p)}
try:
 assert json.loads((PREV/'tail-read-comparison/comparison-status.json').read_text())['complete'];state['before_identity']=identity();state['phase']='freeze-source';save();assert sha(PREV/'candidate.test')=='ac0900c54a12e3a8b685abe6f154b5ad15972f9ef6c7d4fa24f86985d0eea41f'
 shutil.copytree(BASE,SOURCE,ignore=shutil.ignore_patterns('target','build','.git'));(SOURCE/'third_party/librustzcash/target').symlink_to((BASE/'third_party/librustzcash/target').resolve(),target_is_directory=True)
 expected=json.loads((OLD/'hot-owner-spans-20261021-01/source-identity.json').read_text())['production_sources'];assert inventory()==expected
 delta=json.loads((ROOT/'source-delta.json').read_text())
 prepared=[]
 for row in delta['files']:
  p=SOURCE/row['path'];assert (sha(p) if p.exists() else None)==row['before'],row['path']
  if 'source' in row:data=row['source'].encode()
  else:
   lines=p.read_text().splitlines(keepends=True)
   for edit in reversed(row['edits']):lines[edit['start']:edit['end']]=edit['text'].splitlines(keepends=True)
   data=''.join(lines).encode()
  assert hashlib.sha256(data).hexdigest()==row['after'];prepared.append((p,data))
  if not row['path'].endswith('_test.go'):expected[row['path']]=row['after']
 for p,data in prepared:p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data)
 assert inventory()==expected
 restore=SOURCE/'core/state/snapshots/offline_routing_test.go';s=restore.read_text().replace(str(OLD/'hot-owner-spans-20261021-01/owner-index-input/state-snapshots'),str(ROOT/'owner-index-input/state-snapshots'));assert str(ROOT/'owner-index-input/state-snapshots') in s;restore.write_text(s);(SOURCE/'core/rawdb/offline_owner_spans_test.go').write_text((ROOT/'offline_owner_spans_test.go.txt').read_text())
 env=dict(os.environ,PATH='/data/go/bin:'+os.environ.get('PATH',''),GOTOOLCHAIN='local',CGO_ENABLED='1',GOCACHE='/root/.cache/go-build',GOPATH='/root/go',GOMAXPROCS='2',GOMEMLIMIT='3GiB')
 for name in tuple(env):
  if name.startswith('GTRON_'):env.pop(name)
 def run(cmd,name,timeout=1800):
  state['phase']=name;save()
  with (ROOT/name).open('wb') as out:subprocess.run(cmd,cwd=SOURCE,env=env,stdout=out,stderr=subprocess.STDOUT,check=True,timeout=timeout)
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','./core/rawdb','./core/state','./core/state/snapshots','-count=1','-timeout=20m'],'native-packages.log')
 run(['/data/go/bin/go','test','-p=2','-race','-tags=sapling','./core/rawdb','./core/state','./core/state/snapshots','./core','-run','TestHistoryOwnerSpans|TestQueryTail|TestQueryFirstTail|TestHistoryColdFirst|TestDeleteStateDomainChangeRange|TestDirectHistory|TestStateHistoryIndex','-count=1','-timeout=10m'],'native-race.log')
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','./core','./cmd/gtron','-run','^(TestStateHistoryIndex|TestMuHash|TestArchiveQuery_|TestSnapshot|TestRestore)','-count=1','-timeout=15m'],'native-core-correctness.log')
 run(['/data/go/bin/go','vet','-p=2','-tags=sapling','./core/rawdb','./core/state','./core/state/snapshots','./core','./cmd/gtron'],'native-vet.log')
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','-c','./core','-o',str(ROOT/'candidate.test')],'http-build.log')
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','-c','./core/rawdb','-o',str(ROOT/'rawdb.test')],'rawdb-build.log')
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','-c','./core/state/snapshots','-o',str(ROOT/'snapshots.test')],'snapshots-build.log')
 run(['/data/go/bin/go','build','-p=2','-tags=sapling','-o',str(ROOT/'gtron-candidate'),'./cmd/gtron'],'native-build.log')
 record={'commit':delta['commit'],'production_sources':inventory(),'tests':{r['path']:sha(SOURCE/r['path']) for r in delta['files'] if r['path'].endswith('_test.go')},'binaries':{n:sha(ROOT/n) for n in ['candidate.test','snapshots.test','rawdb.test','gtron-candidate']},'http_harness_sha256':sha(SOURCE/'core/offline_fixed_http_test.go'),'muhash_harness_sha256':sha(SOURCE/'core/offline_request_muhash_test.go'),'restore_harness_sha256':sha(restore),'owner_spans_harness_sha256':sha(SOURCE/'core/rawdb/offline_owner_spans_test.go'),'sapling_sha256':sha(SOURCE/'third_party/librustzcash/target/release/librustzcash.a'),'go_version':subprocess.check_output(['/data/go/bin/go','version']).decode().strip()};assert record['http_harness_sha256']=='10e6015af7666bf83b5c405543681c368b7537f428b8278a3344f0312aacd3bf';(ROOT/'source-identity.json').write_text(json.dumps(record,indent=2))
 state['after_identity']=identity();assert state['before_identity']==state['after_identity'];state.update(complete=True,phase='complete');save()
except BaseException as exc:state.update(phase='failed',error=repr(exc),traceback=traceback.format_exc());save();raise
