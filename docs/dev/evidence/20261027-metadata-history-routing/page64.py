import pathlib,json,os,shutil,time,hashlib,struct,subprocess,gzip,datetime,ctypes,mmap,re,zipfile,pwd,traceback
A=pathlib.Path('/data/gtron/maintenance/9m-query-20261027-01');M=A/'metadata-v3';Q=A/'metadata-cold-v3';F=A/'metadata-final';R=A/'metadata-page64';R.mkdir(mode=0o700,exist_ok=True);s={'complete':False,'stage':'wait prior acceptance','production_mutations':False}
def save():s['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(R/'status.json').write_text(json.dumps(s,indent=2))
def sha(b):return hashlib.sha256(b).hexdigest()
def clone(a,b):
 if pathlib.Path(a).name=='manifest.json':shutil.copyfile(a,b)
 else:os.link(a,b)
 return b
def pair(h,a):
 s=hashlib.sha256(b'go-tron/system-history-routing/pair/v1\0')
 for r in [h,a]:
  s.update(struct.pack('>QQQQQH',r['fromTxNum'],r['toTxNum'],r.get('aggregationSteps',0) or 1,r['size'],r.get('rows',0),r.get('domain',0)))
  for k in ['dataset','kind','path','checksum']:
   b=r.get(k,'').encode();s.update(struct.pack('>Q',len(b))+b)
 return s.digest()
try:
 save()
 for i in range(360):
  v=json.loads((F/'status.json').read_text())
  if v.get('error'):raise RuntimeError(v)
  if v.get('complete'):break
  time.sleep(10)
 else:raise RuntimeError('prior acceptance timeout')
 assert json.loads((Q/'status.json').read_text())['complete'] and json.loads((M/'strict.json').read_text())['passed']
 s.update(stage='equivalent metadata page layout with 64 sources');save();cat=R/'catalog';shutil.copytree(str(Q/'candidate'),str(cat),copy_function=clone);manifest=json.loads((cat/'manifest.json').read_text());refs={r['path']:r for r in manifest['segments']};filters={}
 for r in refs.values():
  if r['kind']!='metadata-history-routing':continue
  b=(cat/r['path']).read_bytes();assert len(b)==r['size'] and 'sha256:'+sha(b)==r['checksum'];assert b[:8]==b'GTMETR01';n=struct.unpack_from('>I',b,12)[0];pos=32
  for i in range(n):
   key=b[pos:pos+32];count=struct.unpack_from('>Q',b,pos+32)[0];pos+=40;bits=b[pos:pos+1024] if count else b'';pos+=len(bits);assert key not in filters;filters[key]=(count,bits)
  assert pos==len(b)
 sources=[r for r in manifest['segments'] if r.get('dataset')=='state-domain-change' and r['kind']=='history'];assert len(sources)==len(filters)==29658
 new=[];newproof=[];seen=set()
 for start in range(0,len(sources),64):
  chunk=sources[start:start+64];rows=[]
  for h in chunk:
   accessor=refs[str(pathlib.PurePosixPath(h['path']).with_suffix('.kv'))];key=pair(h,accessor);count,bits=filters[key];assert key not in seen;seen.add(key);rows.append(key+struct.pack('>Q',count)+bits)
  b=b'GTMETR01'+struct.pack('>IIQQ',1,len(rows),chunk[0]['fromTxNum'],chunk[-1]['toTxNum'])+b''.join(sorted(rows,key=lambda x:x[:32]));checksum=sha(b);name='history/metadata-routing-%d-%d-%s.route'%(chunk[0]['fromTxNum'],chunk[-1]['toTxNum'],checksum);p=cat/name;assert not p.exists();p.write_bytes(b)
  with p.open('rb') as f:os.fsync(f.fileno())
  st=p.stat();new.append({'dataset':'state-domain-change','kind':'metadata-history-routing','fromTxNum':chunk[0]['fromTxNum'],'toTxNum':chunk[-1]['toTxNum'],'rows':len(rows),'path':name,'size':len(b),'checksum':'sha256:'+checksum});newproof.append({'path':name,'bytes':len(b),'sha256':checksum,'private_dev':st.st_dev,'private_ino':st.st_ino,'generated_on_isolated_copy':True})
 assert seen==filters.keys() and len(new)==464
 manifest['segments']=[r for r in manifest['segments'] if r['kind']!='metadata-history-routing']+new;(cat/'manifest.json').write_text(json.dumps(manifest,separators=(',',':')));(R/'manifest.json.gz').write_bytes(gzip.compress(json.dumps(manifest).encode()));(R/'new-private-files.json').write_text(json.dumps(newproof,indent=2));(R/'layout-proof.json').write_text(json.dumps({'sources':len(seen),'pages':len(new),'max_sources_per_page':64,'filter_tuples_equal_strictly_verified_256_layout':True,'bytes':sum(r['size'] for r in new),'refs':new},indent=2))
 proof=json.loads((Q/'private-files-proof.json').read_text())+json.loads((Q/'private-extra-proof.json').read_text());oracle=json.loads((A/'probe/baseline-first/rows.json').read_text());env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='3GiB');libc=ctypes.CDLL(None,use_errno=True)
 def run(mode,name,cold=True,trace=False):
  catalog=Q/'baseline' if mode=='baseline' else cat;entries=proof if mode=='baseline' else proof+newproof
  if mode=='baseline':entries=[r for r in proof if not r.get('path','').startswith('history/metadata-routing-') and (catalog/r['path']).exists()]
  if cold:
   for r in entries:
    p=catalog/r['path'];st=p.stat();assert (st.st_dev,st.st_ino)==(r['private_dev'],r['private_ino']);fd=os.open(str(p),os.O_RDONLY)
    try:os.posix_fadvise(fd,0,0,os.POSIX_FADV_DONTNEED)
    finally:os.close(fd)
   res=[]
   for r in entries:
    p=catalog/r['path'];size=p.stat().st_size
    if not size:continue
    with p.open('rb') as f:
     mm=mmap.mmap(f.fileno(),size,access=mmap.ACCESS_COPY);buf=(ctypes.c_char*1).from_buffer(mm);pages=(size+4095)//4096;vec=(ctypes.c_ubyte*pages)();assert libc.mincore(ctypes.c_void_p(ctypes.addressof(buf)),ctypes.c_size_t(size),vec)==0;nr=sum(int(x)&1 for x in vec);assert nr/pages<.01;res.append({'path':r['path'],'resident_pages':nr,'pages':pages});del buf;mm.close()
   (R/(name+'-residency.json')).write_text(json.dumps(res))
  out=R/name;out.mkdir();e=dict(env,GTRON_NINEM_CATALOG=str(catalog),GTRON_NINEM_OUTPUT=str(out),GTRON_NINEM_QUERIES=str(A/'probe/queries.json'),GTRON_NINEM_ITERATIONS='1' if cold or trace else '3');binary=A/'final/baseline.test' if mode=='baseline' else M/'candidate.test';cmd=[str(binary),'-test.run=^TestOfflineNineMColdQuery$','-test.v','-test.timeout=20m']
  if trace:cmd=['strace','-f','-e','trace=openat','-o',str(out/'openat.log')]+cmd
  with (out/'run.log').open('wb') as log:subprocess.check_call(cmd,env=e,stdout=log,stderr=subprocess.STDOUT)
  assert json.loads((out/'rows.json').read_text())==oracle
 (R/'runtime-limits.json').write_text(json.dumps({'unit':subprocess.check_output(['systemctl','show','gtron-offline-metadata-page64-20261027-01','--property=MemoryLimit','--property=CPUQuotaPerSecUSec','--property=Nice']).decode(),'candidate_binary_sha256':sha((M/'candidate.test').read_bytes()),'baseline_binary_sha256':sha((A/'final/baseline.test').read_bytes())},indent=2))
 s.update(stage='matched OS-cold production-shaped layout');save()
 for i,mode in enumerate(['baseline','candidate','candidate','baseline']):run(mode,'os-cold-%d-%s'%(i,mode))
 run('candidate','warm',cold=False);run('candidate','trace',cold=False,trace=True);paths=set();prefix=str(cat)+'/';manifest_paths={r['path'] for r in manifest['segments']}
 for line in (R/'trace/openat.log').read_text().splitlines():
  m=re.search(r'openat\([^,]+, "([^"]+)"',line)
  if m and m.group(1).startswith(prefix):
   rel=m.group(1)[len(prefix):]
   if rel in manifest_paths:paths.add(rel)
 private={r['path']:r for r in proof+newproof};assert paths<=private.keys();(R/'trace-coverage.json').write_text(json.dumps({'paths':sorted(paths),'all_open_attempts_covered':True,'timing_excluded':True},indent=2));s.update(stage='seal evidence');save()
 files=set()
 for pattern in ['*.json','*.gz','os-cold-*/*','warm/*','trace/*']:files.update(p for p in R.glob(pattern) if p.is_file())
 files.update(cat/r['path'] for r in new);files.add(A/'run-metadata-page64.py');dest=pathlib.Path('/tmp/AAA-gtron-metadata-page64-20261027.zip');members={}
 with zipfile.ZipFile(str(dest),'w',zipfile.ZIP_DEFLATED) as z:
  for p in sorted(files):
   name=str(p.relative_to(A));b=p.read_bytes();members[name]={'bytes':len(b),'sha256':sha(b)};z.writestr(name,b)
  z.writestr('files-sha256.json',json.dumps(members,indent=2))
 u=pwd.getpwnam('java-tron');os.chown(str(dest),u.pw_uid,u.pw_gid);os.chmod(str(dest),0o644);seal={'file':str(dest),'bytes':dest.stat().st_size,'sha256':sha(dest.read_bytes()),'members':len(members)};(A/'metadata-page64-seal.json').write_text(json.dumps(seal,indent=2));s.update(complete=True,stage='done',seal=seal);save()
except Exception:s['error']=traceback.format_exc();save();raise
