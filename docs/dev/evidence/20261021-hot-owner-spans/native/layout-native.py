import pathlib,json,subprocess,os,time,datetime,hashlib,shutil,traceback
OLD=pathlib.Path('/data/gtron/cold-query-layout-20261020-01');ROOT=OLD/'hot-owner-spans-20261021-03';SOURCE=OLD/'tail-read-final-20261021-02/storage-route-input';TARGET=ROOT/'owner-index-input';state={'complete':False,'production_mutations':False,'phase':'waiting-for-preparation'}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def identity():
 pid=int(subprocess.check_output(['systemctl','show','gtron','-p','MainPID']).decode().split('=')[1]);return {'pid':pid,'sha256':sha(pathlib.Path('/proc/%d/exe'%pid))}
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();p=ROOT/'layout-status.json';tmp=p.with_suffix('.tmp');tmp.write_text(json.dumps(state,indent=2));tmp.replace(p)
try:
 state['before_identity']=identity();save();begin=time.monotonic()
 while True:
  f=ROOT/'preparation-status.json';s=json.loads(f.read_text()) if f.exists() else {}
  if s.get('phase')=='failed':raise RuntimeError('native preparation failed')
  if s.get('complete') and subprocess.check_output(['systemctl','show','gtron-owner-blake-prepare-20261021.service','-p','ActiveState']).decode().strip()=='ActiveState=inactive':break
  if time.monotonic()-begin>3600:raise TimeoutError('preparation wait')
  time.sleep(2)
 state['phase']='derive-immutable-input';save();TARGET.mkdir();proof=[];cold_metadata={}
 for directory,dirs,files in os.walk(SOURCE):
  rel=pathlib.Path(directory).relative_to(SOURCE);(TARGET/rel).mkdir(exist_ok=True)
  for name in files:
   if name in ('LOCK','FLOCK'):continue
   a=pathlib.Path(directory)/name;b=TARGET/rel/name
   immutable=a.suffix in ('.sst','.seg','.idx','.kv','.route','.gtv2') or str(rel).startswith('ancient')
   if immutable:os.link(a,b)
   else:shutil.copy2(a,b)
   if immutable:
    x=a.stat();y=b.stat();assert (x.st_dev,x.st_ino,x.st_size)==(y.st_dev,y.st_ino,y.st_size);proof.append({'path':str(b.relative_to(TARGET)),'device':x.st_dev,'inode':x.st_ino,'bytes':x.st_size})
   if str(rel).startswith('state-snapshots') and not immutable:cold_metadata[str(b.relative_to(TARGET))]=sha(a)
 (ROOT/'canonical-inodes.json').write_text(json.dumps(proof,indent=2));(ROOT/'cold-metadata-sha256.json').write_text(json.dumps(cold_metadata,indent=2));state['phase']='build-and-strict-check-owner-spans';save()
 env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='3GiB',GTRON_OWNER_INDEX_ROOT=str(ROOT))
 with (ROOT/'owner-spans-build.log').open('wb') as out:subprocess.run([str(ROOT/'rawdb.test'),'-test.run=^TestOfflineOwnerSpansBuild$','-test.v','-test.timeout=35m'],env=env,stdout=out,stderr=subprocess.STDOUT,check=True,timeout=2160)
 retired=[]
 for record in proof:
  a=(SOURCE/record['path']).stat();assert (a.st_dev,a.st_ino,a.st_size)==(record['device'],record['inode'],record['bytes'])
  p=TARGET/record['path']
  if p.exists():
   b=p.stat();assert (b.st_dev,b.st_ino,b.st_size)==(a.st_dev,a.st_ino,a.st_size),record['path']
  else:
   assert record['path'].startswith('history/') and record['path'].endswith('.sst'),record['path'];retired.append(record['path'])
 for name,digest in cold_metadata.items():assert sha(SOURCE/name)==sha(TARGET/name)==digest,name
 src_inodes={r['inode'] for r in proof};added=[]
 for p in (TARGET/'history').iterdir():
  if p.is_file() and p.stat().st_ino not in src_inodes:added.append({'path':str(p.relative_to(TARGET)),'bytes':p.stat().st_size,'allocated_bytes':p.stat().st_blocks*512})
 (ROOT/'derived-physical-files.json').write_text(json.dumps({'note':'New private history files include WAL recovery/metadata and owner-index output. File bytes are not exclusively attributable to the index; exact auxiliary logical key/value bytes are separately counted. Both compared readers use this same final physical input.','files':added,'retired_derived_sst_paths':retired},indent=2))
 build=json.loads((ROOT/'owner-spans-build.json').read_text());assert build['complete'] and build['built']==build['strictly_checked'] and build['canonical_unchanged'] and build['only_auxiliary_writes'];assert json.loads((ROOT/'writer-replay.json').read_text())['complete']
 state['after_identity']=identity();assert state['before_identity']==state['after_identity'];state.update(complete=True,phase='complete',source_immutable_files_unchanged=len(proof),retired_derived_sst_paths=retired,build=build);save()
except BaseException as exc:state.update(phase='failed',error=repr(exc),traceback=traceback.format_exc());save();raise
