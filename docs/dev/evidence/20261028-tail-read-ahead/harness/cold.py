# Target only immutable cold-history files; never evict production/shared inodes.
import pathlib,sys,json,os,time,datetime,subprocess,signal,urllib.request,traceback,shutil,contextlib,ctypes,struct,threading,select,mmap,fcntl
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'run.py').read_text().split('\ntry:\n s[',1)[0],'fixed-host-definitions','exec'))
R=W/'cold';R.mkdir(exist_ok=True)
s={'complete':False,'stage':'discover cold-history reads','source':SOURCE,'production_mutations':False,'cache_scope':'private cold-history file pages zero resident at dispatch; startup/process and hot Pebble caches retained'};requests=[]
partial=O/'candidate-maintenance-input';baseline=O/'candidate-http.test';candidate=W/'candidate-http.test'
def dump(name,obj):
 p=R/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(obj,indent=2))
@contextlib.contextmanager
def host(name,exe,input_):
 out=R/name;out.mkdir();env=dict(E,GTRON_FIXED_INPUT=str(input_),GTRON_FIXED_OUTPUT=str(out));log=(out/'host.log').open('wb');p=subprocess.Popen([str(exe),'-test.run=^TestOfflineFullEthCallHTTP$','-test.v','-test.timeout=10m'],env=env,cwd=str(W),stdout=log,stderr=subprocess.STDOUT,start_new_session=True);ready=None
 try:
  until=time.monotonic()+180
  while not (out/'ready.json').exists():
   assert p.poll() is None,name
   assert time.monotonic()<until,name
   time.sleep(.1)
  ready=json.loads((out/'ready.json').read_text());assert ready['head']==45836827 and ready['head_tx']==4240407293 and ready['root']=='4bb19b7a938ba6c865820ea86f5239235172f84923c350b04c4b0eb75743b627'
  yield ready
 finally:
  if p.poll() is None:
   os.kill(ready['pid'] if ready else p.pid,signal.SIGTERM)
   try:p.wait(20)
   except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait()
  log.close();assert p.returncode==0,(name,p.returncode)
def call(ready,name,cache):
 q={'jsonrpc':'2.0','id':len(requests)+1,'method':'eth_call','params':[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},hex(9000000)]};start=time.perf_counter()
 req=urllib.request.Request(ready['url'],data=json.dumps(q).encode(),headers={'Content-Type':'application/json'})
 with urllib.request.urlopen(req,timeout=180) as f:body=json.loads(f.read())
 row={'case':name,'cache':cache,'seconds_loopback_http':time.perf_counter()-start,'response':body};requests.append(row);dump('requests.json',requests);assert int(body['result'],16)==207,row
 return row
class Observer:
 def __init__(self,root):
  self.libc=ctypes.CDLL(None,use_errno=True);self.fd=self.libc.inotify_init1(os.O_NONBLOCK|os.O_CLOEXEC);assert self.fd>=0;self.paths={};self.seen=set();self.overflow=False;self.stop=threading.Event()
  for directory,dirs,files in os.walk(str(root/'state-snapshots')):
   wd=self.libc.inotify_add_watch(self.fd,os.fsencode(directory),1);assert wd>=0;self.paths[wd]=str(pathlib.Path(directory).relative_to(root))
  self.t=threading.Thread(target=self.run);self.t.start()
 def read(self):
  while True:
   try:b=os.read(self.fd,1<<20)
   except BlockingIOError:return
   if not b:return
   pos=0
   while pos+16<=len(b):
    wd,mask,cookie,n=struct.unpack_from('iIII',b,pos);pos+=16;name=os.fsdecode(b[pos:pos+n].split(b'\0',1)[0]);pos+=n
    if mask&0x4000:self.overflow=True
    if mask&1 and wd in self.paths and pathlib.Path(name).suffix in ['.seg','.kv','.idx','.route']:
     self.seen.add(self.paths[wd]+'/'+name)
 def run(self):
  while not self.stop.is_set():
   if select.select([self.fd],[],[],.02)[0]:self.read()
 def close(self):
  self.stop.set();self.t.join();self.read();os.close(self.fd);assert not self.overflow,'inotify overflow';return self.seen
try:
 s['production_before']=identity();save();readset=set()
 for name,exe in [('discovery-baseline',baseline),('discovery-candidate',candidate)]:
  with host(name,exe,partial) as ready:
   observer=Observer(partial)
   try:call(ready,name,'discovery-excluded')
   finally:seen=observer.close()
  dump(name+'/readset.json',sorted(seen));readset.update(seen)
 assert readset;dump('readset.json',sorted(readset));s.update(stage='prepare private cold-history inodes',files=len(readset),whole_file_bytes=sum((partial/p).stat().st_size for p in readset));save()
 dest=R/'private-input';dest.mkdir()
 for p in partial.iterdir():
  if p.name in ['state-snapshots','ancient']:shutil.copytree(str(p),str(dest/p.name),copy_function=os.link,symlinks=True)
  else:os.symlink(str(p),str(dest/p.name),target_is_directory=p.is_dir())
 oldinput=A/'http-filepage-cold-9m/baseline-private-input';oldsource=A/'http-performance-priority/baseline-input';proof=[];copied=reused=0;copy_started=time.monotonic()
 for index,rel in enumerate(sorted(readset)):
  source=partial/rel;st=source.stat();key=(st.st_dev,st.st_ino,st.st_size,st.st_mtime_ns);old=oldinput/rel;original=oldsource/rel;target=(dest/rel).with_name((dest/rel).name+'.private-next');target.parent.mkdir(parents=True,exist_ok=True)
  if old.is_file() and original.is_file() and (original.stat().st_dev,original.stat().st_ino)==key[:2] and old.stat().st_size==st.st_size and (old.stat().st_dev,old.stat().st_ino)!=key[:2]:
   os.link(str(old),str(target));reused+=st.st_size;method='reuse-completed-private-copy'
  else:
   assert shutil.disk_usage(str(R)).free>st.st_size+200_000_000_000
   n=0
   with source.open('rb',buffering=0) as src,target.open('xb',buffering=0) as out:
    try:fcntl.ioctl(out.fileno(),0x40049409,src.fileno());n=st.st_size;method='reflink'
    except OSError:
     method='full-copy'
     for b in iter(lambda:src.read(8<<20),b''):n+=out.write(b)
    os.fsync(out.fileno())
   assert n==st.st_size;copied+=n
  after=source.stat();assert key==(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns)
  actual=target.stat();assert actual.st_size==st.st_size and (actual.st_dev,actual.st_ino)!=key[:2]
  os.replace(str(target),str(dest/rel));proof.append({'path':rel,'bytes':st.st_size,'source_dev':st.st_dev,'source_ino':st.st_ino,'private_dev':actual.st_dev,'private_ino':actual.st_ino,'method':method})
  if index%100==0:s.update(private_files=index+1,copied_bytes=copied,reused_bytes=reused,copy_seconds=time.monotonic()-copy_started);save()
 dump('private-proof.json',proof);s.update(stage='controlled cold-history HTTP',copied_bytes=copied,reused_bytes=reused,copy_seconds=time.monotonic()-copy_started);save()
 libc=ctypes.CDLL(None,use_errno=True)
 for index,(version,exe) in enumerate([('baseline',baseline),('candidate',candidate),('candidate',candidate),('baseline',baseline)]):
  name='cold-%d-%s'%(index,version);s['stage']=name;save()
  with host(name,exe,dest) as ready:
   residency=[]
   for rel in sorted(readset):
    p=dest/rel;st=p.stat();origin=(partial/rel).stat();assert (st.st_dev,st.st_ino)!=(origin.st_dev,origin.st_ino)
    with p.open('rb') as f:
     os.posix_fadvise(f.fileno(),0,0,os.POSIX_FADV_DONTNEED)
     mm=mmap.mmap(f.fileno(),st.st_size,access=mmap.ACCESS_COPY);buf=(ctypes.c_char*1).from_buffer(mm);pages=(st.st_size+4095)//4096;vec=(ctypes.c_ubyte*pages)();assert libc.mincore(ctypes.c_void_p(ctypes.addressof(buf)),ctypes.c_size_t(st.st_size),vec)==0;nr=sum(int(x)&1 for x in vec);del buf;mm.close();assert nr==0,(rel,nr,pages);residency.append({'path':rel,'pages':pages,'resident_pages':nr})
   dump(name+'/dispatch-residency.json',residency)
   observer=Observer(dest)
   try:call(ready,name,'private-cold-history-pages-zero-at-dispatch')
   finally:seen=observer.close()
   dump(name+'/actual-read-coverage.json',{'paths':sorted(seen),'missing':sorted(seen-readset),'overflow':False});assert not seen-readset,seen-readset
   call(ready,name,'second-call-process-warm')
 s['production_after']=identity();assert s['production_before']==s['production_after'];s.update(complete=True,stage='controlled cold-history HTTP complete',all_returns_match=True);save()
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
