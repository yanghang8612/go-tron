import datetime,hashlib,json,os,pathlib,shutil,subprocess,tarfile,traceback
ROOT=pathlib.Path('/data/gtron/eth-call-request-20261020-04')
OLD=pathlib.Path('/data/gtron/eth-call-fixed-20261020-01')
SOURCE=ROOT/'source'
state={'complete':False,'phase':'preparing-isolated-source','production_mutations':False}
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(ROOT/'prepare-status.json').write_text(json.dumps(state,indent=2))
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def identity():
 pid=int(subprocess.check_output(['systemctl','show','gtron','-p','MainPID']).decode().split('=')[1]);return {'pid':pid,'sha256':sha(pathlib.Path('/proc/'+str(pid)+'/exe'))}
def run(command,name,timeout=1200):
 state['phase']=name;save()
 with (ROOT/name).open('wb') as output:subprocess.run(command,cwd=SOURCE,env=env,stdout=output,stderr=subprocess.STDOUT,timeout=timeout,check=True)
try:
 state['before_identity']=identity();save()
 old=json.loads((OLD/'final-proof.json').read_text());assert old['fixed_head']==11818192 and old['muhash_verified']
 assert pathlib.Path('/data/gtron/main/datadir').stat().st_ino==old['data_dir_inode']
 assert sha(OLD/'candidate.test')=='7a5b487b0b016526c8f0391778d36049c895a9183aabc93def9be81c98854363'
 if not SOURCE.exists():shutil.copytree('/data/gtron/cold-query-tail-20261020/candidate',SOURCE,symlinks=True)
 with tarfile.open(ROOT/'source.tar') as archive:
  for member in archive:
   if member.isdir():continue
   assert member.isfile() and not pathlib.Path(member.name).is_absolute() and '..' not in pathlib.Path(member.name).parts
   target=SOURCE/member.name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(archive.extractfile(member).read())
 (SOURCE/'core/offline_request_muhash_test.go').write_bytes((ROOT/'offline_request_muhash_test.go.txt').read_bytes())
 assert sha(SOURCE/'core/offline_fixed_http_test.go')==json.loads((OLD/'source-identity-final.json').read_text())['harness_sha256']
 if not (ROOT/'baseline.test').exists():os.link(OLD/'candidate.test',ROOT/'baseline.test')
 for name in ['census.json','oracle.json','specs.json']:
  shutil.copy2(OLD/name,ROOT/name)
 env=dict(os.environ,PATH='/data/go/bin:'+os.environ.get('PATH',''),GOTOOLCHAIN='local',GOCACHE='/root/.cache/go-build',GOPATH='/root/go',CGO_ENABLED='1',GOMAXPROCS='2',GOMEMLIMIT='3GiB')
 pattern='^(TestHistoryRequest|TestHistoryExecution|TestHistoryStorage|TestHistoryMixed|TestHistoryDirectory|TestMuHash|TestArchiveQuery_|TestSnapshot|TestRestore)'
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','./core/state','./core/state/snapshots','./core','./cmd/gtron','-run',pattern,'-count=1','-timeout=15m'],'native-correctness.log',1500)
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','-c','./core','-o',str(ROOT/'candidate.test')],'http-build.log',900)
 run(['/data/go/bin/go','build','-p=2','-tags=sapling','-o',str(ROOT/'gtron'),'./cmd/gtron'],'native-build.log',900)
 hashes={}
 for file in SOURCE.rglob('*'):
  if file.is_file() and (file.suffix in ('.go','.rs') or file.name in ('go.mod','go.sum','Cargo.toml','Cargo.lock')) and not file.name.endswith('_test.go') and '/target/' not in str(file) and '/build/' not in str(file):hashes[str(file.relative_to(SOURCE))]=sha(file)
 record={'candidate':'b6db66fcbde2d17f4b8fca099e5fd642e4435308','baseline':'4e6978dd','source_sha256':hashes,'harness_sha256':sha(SOURCE/'core/offline_fixed_http_test.go'),'muhash_harness_sha256':sha(SOURCE/'core/offline_request_muhash_test.go'),'binaries':{n:sha(ROOT/n) for n in ['baseline.test','candidate.test','gtron']},'go_version':subprocess.check_output(['/data/go/bin/go','version']).decode().strip(),'sapling_sha256':sha(SOURCE/'third_party/librustzcash/target/release/librustzcash.a')}
 (ROOT/'source-identity.json').write_text(json.dumps(record,indent=2));state['after_identity']=identity();assert state['after_identity']==state['before_identity']
 state.update(complete=True,phase='ready');save()
except BaseException as exc:
 state.update(phase='failed',error=repr(exc),traceback=traceback.format_exc());save();raise
