"""Full HTTP cold-start read-set closure over all four database directories.
Never evict an inode shared with an original or production database.
"""
import pathlib,sys,json,os,time,traceback,hashlib,ctypes,mmap,struct,threading,select,shutil,zipfile
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'cold.py').read_text().split('\ntry:\n until=',1)[0],'fixed-host-definitions','exec'))
R=pathlib.Path('/data/gtron/maintenance/whole-query-cold-20261029-01')
input_=R/'input';kinds=('chaindata','history','ancient','state-snapshots')
def save():
 s['utc']=time.time();(R/'whole-query-status.json').write_text(json.dumps(s,indent=2))
s={'complete':False,'stage':'wait for private index preparation','source':SOURCE,'production_mutations':False,'cache':'fresh engine; all opened database files independently copied, zero file pages before startup; startup warming recorded'};requests=[]
libc=ctypes.CDLL(None,use_errno=True)
class AllDatabaseOpens:
 def __init__(self):
  self.fd=libc.inotify_init1(os.O_NONBLOCK|os.O_CLOEXEC);assert self.fd>=0;self.paths={};self.seen=set();self.overflow=False;self.stop=threading.Event()
  for kind in kinds:
   for directory,dirs,files in os.walk(str(input_/kind),followlinks=False):
    assert not any((pathlib.Path(directory)/p).is_symlink() for p in dirs+files),'escaping DB symlink'
    wd=libc.inotify_add_watch(self.fd,os.fsencode(directory),0x21);assert wd>=0;self.paths[wd]=str(pathlib.Path(directory).relative_to(input_))
  self.t=threading.Thread(target=self.run);self.t.start()
 def drain(self):
  while True:
   try:b=os.read(self.fd,1<<20)
   except BlockingIOError:return
   if not b:return
   p=0
   while p+16<=len(b):
    wd,mask,cookie,n=struct.unpack_from('iIII',b,p);p+=16;name=os.fsdecode(b[p:p+n].split(b'\0',1)[0]);p+=n
    if mask&0x4000:self.overflow=True
    if mask&0x21 and not mask&0x40000000 and wd in self.paths and name:self.seen.add(self.paths[wd]+'/'+name)
 def run(self):
  while not self.stop.is_set():
   if select.select([self.fd],[],[],.01)[0]:self.drain()
 def close(self):
  self.stop.set();self.t.join();self.drain();os.close(self.fd);assert not self.overflow,'inotify queue overflow';return self.seen

def dump(name,obj):(R/name).write_text(json.dumps(obj,indent=2))
def request(ready,name,height,want,repeat,scope):
 q={'jsonrpc':'2.0','id':len(requests)+1,'method':'eth_call','params':[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},hex(height)]}
 began=time.perf_counter();req=urllib.request.Request(ready['url'],data=json.dumps(q).encode(),headers={'Content-Type':'application/json'})
 with urllib.request.urlopen(req,timeout=180) as f:body=json.loads(f.read())
 row={'case':name,'height':height,'repeat':repeat,'cache':scope,'seconds_loopback_http':time.perf_counter()-began,'request':q,'response':body};requests.append(row);dump('requests.json',requests);assert int(body['result'],16)==want,row;save()
 return row
proof={};readset=set();copy_started=time.monotonic();copied=0
proof_file=R/'whole-file-private-proof.json'
if proof_file.exists():proof={x['path']:x for x in json.loads(proof_file.read_text())};readset=set(proof)
def isolate(paths):
 global copied
 for rel in sorted(paths):
  p=input_/rel;st=p.stat();assert p.is_file() and input_.resolve() in p.resolve().parents
  if rel in proof:
   item=proof[rel];assert st.st_nlink==1 and (st.st_dev,st.st_ino)==(item['private_dev'],item['private_ino']);continue
  if st.st_nlink==1:method='independent mutable/new derived file';digest=None;original=(st.st_dev,st.st_ino)
  else:
   assert shutil.disk_usage(str(R)).free>st.st_size+400_000_000_000,'copy reserve'
   target=p.with_name(p.name+'.whole-query-private-next');original=(st.st_dev,st.st_ino);h=hashlib.sha256();n=0
   with p.open('rb',buffering=0) as source,target.open('xb',buffering=0) as dest:
    for chunk in iter(lambda:source.read(8<<20),b''):
     n+=dest.write(chunk);h.update(chunk);copied+=len(chunk)
     delay=copied/(256<<20)-(time.monotonic()-copy_started)
     if delay>0:time.sleep(min(delay,.1))
    os.fsync(dest.fileno())
   assert n==st.st_size and (p.stat().st_dev,p.stat().st_ino)==original
   os.replace(str(target),str(p));st=p.stat();method='full independent whole-file copy';digest=h.hexdigest();assert (st.st_dev,st.st_ino)!=original
  assert st.st_nlink==1
  proof[rel]={'path':rel,'bytes':st.st_size,'source_dev':original[0],'source_ino':original[1],'private_dev':st.st_dev,'private_ino':st.st_ino,'method':method,'copy_sha256':digest}
  if len(proof)%50==0:dump('whole-file-private-proof.json',list(proof.values()));s.update(private_files=len(proof),copied_bytes=copied);save()
 dump('whole-file-private-proof.json',list(proof.values()));readset.update(paths)
def residency(evict):
 result=[]
 for rel,item in sorted(proof.items()):
  p=input_/rel;st=p.stat();assert st.st_nlink==1 and (st.st_dev,st.st_ino)==(item['private_dev'],item['private_ino'])
  count=0;pages=(st.st_size+4095)//4096
  if st.st_size:
   with p.open('rb') as f:
    if evict:os.fsync(f.fileno());os.posix_fadvise(f.fileno(),0,0,os.POSIX_FADV_DONTNEED)
    mm=mmap.mmap(f.fileno(),st.st_size,access=mmap.ACCESS_COPY);buf=(ctypes.c_char*1).from_buffer(mm);vec=(ctypes.c_ubyte*pages)();assert libc.mincore(ctypes.c_void_p(ctypes.addressof(buf)),ctypes.c_size_t(st.st_size),vec)==0;count=sum(x&1 for x in vec);del buf;mm.close()
  result.append({'path':rel,'pages':pages,'resident_pages':count})
 return result
try:
 until=time.monotonic()+2700
 while True:
  status=json.loads((R/'candidate-status.json').read_text())
  if status.get('complete'):break
  assert status['stage']!='failed',status
  assert time.monotonic()<until,'prepare timeout';time.sleep(2)
 s['production_before']=identity();s['prepared_source']=status['source'];save()
 versions=[('baseline',pathlib.Path('/data/gtron/maintenance/first-cold-followup-20261028-01/account-runtime/http.test')),('candidate',R/'http.test')]
 s['binary_sha256']={n:hashlib.sha256(p.read_bytes()).hexdigest() for n,p in versions};save()
 heights=[(9000000,207),(10000000,213),(15000000,238),(20000000,249)]
 for version,exe in versions:
  name='whole-discovery-'+version;s['stage']=name;save();observer=AllDatabaseOpens()
  try:
   with host(name,exe,input_) as ready:
    for height,want in heights:request(ready,name,height,want,0,'discovery excluded from cold comparisons')
  finally:seen=observer.close()
  dump(name+'/all-database-opens.json',sorted(seen));readset.update(seen)
 s.update(stage='copy whole files for complete read-set isolation',files=len(readset),whole_file_bytes=sum((input_/p).stat().st_size for p in readset));save();isolate(readset)
 accepted=[];rejected=[]
 for round_ in range(2):
  order=versions if round_==0 else list(reversed(versions))
  for height,want in heights:
   for version,exe in order:
    for attempt in range(5):
     name='whole-cold-%d-%s-%d-%d'%(round_,version,height,attempt);s['stage']=name;save()
     before=residency(True);assert sum(x['resident_pages'] for x in before)==0, 'private pages not evicted before startup'
     observer=AllDatabaseOpens();rows=[]
     try:
      with host(name,exe,input_) as ready:
       dump(name+'/pre-start-residency.json',before)
       startup=residency(False);dump(name+'/startup-residency.json',startup)
       # This second eviction affects only independent private inodes. Fresh
       # engine startup may already have cached a few mandatory metadata reads.
       dispatch=residency(True);dump(name+'/dispatch-residency.json',dispatch)
       rows.append(request(ready,name,height,want,0,'whole database opened-file pages cold before startup; dispatch proof attached'))
       rows.append(request(ready,name,height,want,1,'same process repeat'))
     finally:seen=observer.close()
     missing=seen-readset;dump(name+'/all-database-opens.json',{'paths':sorted(seen),'missing':sorted(missing),'overflow':False})
     if missing:
      rejected.append({'case':name,'reason':'new non-isolated opened files','files':sorted(missing)});dump('rejected.json',rejected);s['stage']='extend whole-file closure';save();isolate(missing);continue
     accepted.append({'case':name,'round':round_,'version':version,'height':height,'first_seconds':rows[0]['seconds_loopback_http'],'repeat_seconds':rows[1]['seconds_loopback_http'],'pre_start_resident_pages':0,'dispatch_resident_pages':sum(x['resident_pages'] for x in dispatch),'all_opened_files_independent':True});dump('accepted.json',accepted);break
    else:raise AssertionError('read-set did not close')
 s['production_after']=identity();s['production_identity_changed']=s['production_before']!=s['production_after'];s.update(complete=True,stage='whole-query cold HTTP matrix complete',accepted=len(accepted),rejected=len(rejected),all_returns_match=True);save()
 target=pathlib.Path('/tmp/AAA-gtron-whole-query-cold-20261029-final.zip');assert not target.exists()
 with zipfile.ZipFile(str(target),'x',zipfile.ZIP_DEFLATED) as z:
  for p in sorted(R.rglob('*')):
   if p.is_file() and not p.is_symlink() and p.suffix in ['.json','.jsonl','.log'] and 'source-candidate' not in p.parts and 'input' not in p.parts:z.write(str(p),str(p.relative_to(R)))
 receipt={'path':str(target),'bytes':target.stat().st_size,'sha256':hashlib.sha256(target.read_bytes()).hexdigest()};dump('archive-receipt.json',receipt);print(json.dumps(receipt),flush=True)
except BaseException:s.update(stage='failed',error=traceback.format_exc());dump('whole-query-status.json',s);raise
