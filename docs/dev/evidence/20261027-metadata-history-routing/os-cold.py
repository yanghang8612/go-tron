import pathlib,subprocess,os,shutil,json,traceback,datetime,hashlib,time,ctypes,mmap
A=pathlib.Path('/data/gtron/maintenance/9m-query-20261027-01');M=A/'metadata-v3';P=A/'metadata-cold-v3';P.mkdir(mode=0o700,exist_ok=True)
s={'complete':False,'stage':'private copy','production_mutations':False}
def save():s['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(P/'status.json').write_text(json.dumps(s,indent=2))
def digest(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(4<<20),b''):h.update(b)
 return h.hexdigest()
def clone(a,b):
 if pathlib.Path(a).name=='manifest.json':shutil.copyfile(a,b)
 else:os.link(a,b)
 return b
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='3GiB')
try:
 save();opened=json.loads((A/'final/baseline-opened-files.json').read_text());paths=opened['files'];total=opened['bytes'];s['copy_input_bytes']=total;save()
 assert total<180_000_000_000 and shutil.disk_usage(str(P)).free>total+50_000_000_000
 target=P/'baseline';shutil.copytree(str(A/'baseline-catalog'),str(target),copy_function=clone)
 refs={r['path']:r for r in json.loads((target/'manifest.json').read_text())['segments']};proof=[]
 for i,name in enumerate(paths):
  source=A/'baseline-catalog'/name;dest=target/name;temp=dest.with_name(dest.name+'.private-copy');shutil.copyfile(str(source),str(temp))
  with temp.open('rb') as f:os.fsync(f.fileno())
  sha=digest(temp);assert 'sha256:'+sha==refs[name]['checksum'];v=temp.stat();o=source.stat();assert (v.st_dev,v.st_ino)!=(o.st_dev,o.st_ino);temp.replace(dest)
  proof.append({'path':name,'bytes':v.st_size,'sha256':sha,'source_dev':o.st_dev,'source_ino':o.st_ino,'private_dev':v.st_dev,'private_ino':v.st_ino})
  if i%200==0:s.update(copied_files=i+1,total_files=len(paths));save()
 (P/'private-files-proof.json').write_text(json.dumps(proof,indent=2));s.update(stage='wait independent strict proof');save()
 while True:
  st=json.loads((M/'status.json').read_text())
  if st.get('error'):raise RuntimeError(st)
  if st.get('complete'):break
  time.sleep(10)
 assert json.loads((M/'strict.json').read_text())['passed']
 candidate=P/'candidate';shutil.copytree(str(target),str(candidate),copy_function=clone);shutil.copyfile(str(M/'catalog/manifest.json'),str(candidate/'manifest.json'))
 extra=[]
 oldpaths={r['path'] for r in refs.values()}
 for r in json.loads((candidate/'manifest.json').read_text())['segments']:
  if r['path'] in oldpaths:continue
  source=M/'catalog'/r['path'];dest=candidate/r['path'];dest.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(str(source),str(dest))
  with dest.open('rb') as f:os.fsync(f.fileno())
  sha=digest(dest);assert 'sha256:'+sha==r['checksum'];v=dest.stat();o=source.stat();assert (v.st_dev,v.st_ino)!=(o.st_dev,o.st_ino)
  extra.append({'path':r['path'],'bytes':v.st_size,'sha256':sha,'source_dev':o.st_dev,'source_ino':o.st_ino,'private_dev':v.st_dev,'private_ino':v.st_ino})
 (P/'private-extra-proof.json').write_text(json.dumps(extra,indent=2))
 libc=ctypes.CDLL(None,use_errno=True)
 def cold(mode,name):
  cat=target if mode=='baseline' else candidate;entries=proof+(extra if mode!='baseline' else [])
  for r in entries:
   f=os.open(str(cat/r['path']),os.O_RDONLY)
   try:os.posix_fadvise(f,0,0,os.POSIX_FADV_DONTNEED)
   finally:os.close(f)
  resident,totalpages=0,0;per=[]
  for r in entries:
   dest=cat/r['path'];v=dest.stat();assert (v.st_dev,v.st_ino)==(r['private_dev'],r['private_ino'])
   f=os.open(str(dest),os.O_RDONLY)
   try:
    if not v.st_size:continue
    mm=mmap.mmap(f,v.st_size,access=mmap.ACCESS_COPY);buf=(ctypes.c_char*1).from_buffer(mm);pages=(v.st_size+4095)//4096;vec=(ctypes.c_ubyte*pages)();assert libc.mincore(ctypes.c_void_p(ctypes.addressof(buf)),ctypes.c_size_t(v.st_size),vec)==0
    nr=sum(int(x)&1 for x in vec);assert nr/max(1,pages)<.01,(r['path'],nr,pages);per.append({'path':r['path'],'resident_pages':nr,'pages':pages});resident+=nr;totalpages+=pages;del buf;mm.close()
   finally:os.close(f)
  assert resident/max(1,totalpages)<.01
  (P/(name+'-residency.json')).write_text(json.dumps({'per_file':per,'resident_pages':resident,'total_pages':totalpages,'verified_independent_files':len(entries),'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()},indent=2))
  out=P/name;out.mkdir();e=dict(env,GTRON_NINEM_CATALOG=str(cat),GTRON_NINEM_OUTPUT=str(out),GTRON_NINEM_QUERIES=str(A/'probe/queries.json'),GTRON_NINEM_ITERATIONS='1');binary=A/'final/baseline.test' if mode=='baseline' else M/'candidate.test'
  (out/'environment.json').write_text(json.dumps({'loadavg':pathlib.Path('/proc/loadavg').read_text(),'meminfo':pathlib.Path('/proc/meminfo').read_text(),'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'binary_sha256':digest(binary),'mode':mode},indent=2))
  with (out/'run.log').open('wb') as log:subprocess.check_call([str(binary),'-test.run=^TestOfflineNineMColdQuery$','-test.v','-test.timeout=20m'],env=e,stdout=log,stderr=subprocess.STDOUT)
  assert json.loads((out/'rows.json').read_text())==json.loads((A/'probe/baseline-first/rows.json').read_text())
 s.update(stage='matched OS-cold batches');save()
 for i,mode in enumerate(('baseline','candidate','candidate','baseline')):cold(mode,'os-cold-%d-%s'%(i,mode))
 s.update(complete=True,stage='done');save()
except Exception:
 s['error']=traceback.format_exc();save();raise
