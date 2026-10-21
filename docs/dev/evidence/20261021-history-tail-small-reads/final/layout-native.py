import pathlib,json,subprocess,os,time,datetime,hashlib,shutil,traceback
OLD=pathlib.Path('/data/gtron/cold-query-layout-20261020-01');ROOT=OLD/'tail-read-final-20261021-02';SOURCE=OLD/'account-gc-input';TARGET=ROOT/'storage-route-input';state={'complete':False,'production_mutations':False,'phase':'waiting-for-preparation'}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def identity():
 pid=int(subprocess.check_output(['systemctl','show','gtron','-p','MainPID']).decode().split('=')[1]);return {'pid':pid,'sha256':sha(pathlib.Path('/proc/%d/exe'%pid))}
def save():state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(ROOT/'layout-status.json').write_text(json.dumps(state,indent=2))
try:
 state['before_identity']=identity();save();begin=time.monotonic()
 while True:
  f=ROOT/'preparation-status.json';s=json.loads(f.read_text()) if f.exists() else {}
  if s.get('phase')=='failed':raise RuntimeError('native preparation failed')
  if s.get('complete') and subprocess.check_output(['systemctl','show','gtron-tail-final-prepare-20261021.service','-p','ActiveState']).decode().strip()=='ActiveState=inactive':break
  if time.monotonic()-begin>3600:raise TimeoutError('preparation wait')
  time.sleep(2)
 state['phase']='derive-immutable-input';save();TARGET.mkdir();proof=[]
 for directory,dirs,files in os.walk(SOURCE):
  rel=pathlib.Path(directory).relative_to(SOURCE);(TARGET/rel).mkdir(exist_ok=True)
  for name in files:
   if name in ('LOCK','FLOCK'):continue
   a=pathlib.Path(directory)/name;b=TARGET/rel/name
   immutable=a.suffix in ('.sst','.seg','.idx','.kv','.route','.gtv2') or str(rel).startswith('ancient')
   if immutable:os.link(a,b)
   else:shutil.copy2(a,b)
   if immutable and a.suffix!='.route':
    x=a.stat();y=b.stat();assert (x.st_dev,x.st_ino,x.st_size)==(y.st_dev,y.st_ino,y.st_size);proof.append({'path':str(b.relative_to(TARGET)),'device':x.st_dev,'inode':x.st_ino,'bytes':x.st_size})
 (ROOT/'canonical-inodes.json').write_text(json.dumps(proof,indent=2));state['phase']='build-and-strict-check-storage-route';save()
 env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='3GiB',GTRON_LAYOUT_EVIDENCE_DIR=str(ROOT))
 with (ROOT/'storage-route-build.log').open('wb') as out:subprocess.run([str(ROOT/'snapshots.test'),'-test.run=^TestOfflineStorageHistoryRoute$','-test.v','-test.timeout=35m'],env=env,stdout=out,stderr=subprocess.STDOUT,check=True,timeout=2160)
 for r in proof:
  a=(SOURCE/r['path']).stat();b=(TARGET/r['path']).stat();assert (a.st_dev,a.st_ino,a.st_size)==(b.st_dev,b.st_ino,b.st_size)==(r['device'],r['inode'],r['bytes']),r['path']
 build=json.loads((ROOT/'storage-route-build.json').read_text());assert build['complete'] and build['all_pairs_covered'] and build['covered_pairs']==4609 and build['canonical_references_unchanged'] and build['progress_unchanged']
 state['after_identity']=identity();assert state['before_identity']==state['after_identity'];state.update(complete=True,phase='complete',canonical_files_unchanged=len(proof),build=build);save()
except BaseException as exc:state.update(phase='failed',error=repr(exc),traceback=traceback.format_exc());save();raise
