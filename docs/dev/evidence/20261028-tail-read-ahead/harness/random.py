# Compare descriptor-local random I/O advice after the original cold matrix.
import pathlib,sys,json,os,time,ctypes,mmap,traceback
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
code=(W/'cold.py').read_text();prefix=code.split('\ntry:\n until=',1)[0];assert len(prefix)<len(code)
exec(compile(prefix,'cold-host-definitions','exec'))
R=W/'random';R.mkdir(exist_ok=True);s={'complete':False,'stage':'build random-I/O variant','source':SOURCE,'base':'80293e10cbc9ff983fe112dad2b9dafa08a29ed0','production_mutations':False};requests=[]
try:
 s['production_before']=identity();save();src=archive_source('source-candidate',SOURCE)
 command(['/data/go/bin/go','test','-tags=sapling','./core/state/snapshots','-run','Test(HistoryOrderedReads|HistoryPhysicalPages|HistoryMixedBatch|HistoryAccessorPages|ManagerBatchesFirst)','-count=1','-timeout=120s'],src,R/'focused.log',180)
 command(['/data/go/bin/go','test','-tags=sapling','-c','./core','-o',str(R/'candidate-http.test')],src,R/'build.log')
 until=time.monotonic()+1800;s['stage']='wait for original controlled-cold matrix';save()
 while True:
  prior=json.loads((W/'cold/candidate-status.json').read_text())
  if prior.get('complete'):break
  assert prior.get('stage')!='failed',prior
  assert time.monotonic()<until,'waiting for original cold matrix'
  time.sleep(2)
 dest=W/'cold/private-input';readset=set(json.loads((W/'cold/readset.json').read_text()));libc=ctypes.CDLL(None,use_errno=True)
 for index,(version,exe) in enumerate([('random',R/'candidate-http.test'),('lookahead',W/'candidate-http.test'),('lookahead',W/'candidate-http.test'),('random',R/'candidate-http.test')]):
  name='cold-%d-%s'%(index,version);s['stage']=name;save()
  with host(name,exe,dest) as ready:
   residency=[]
   for rel in sorted(readset):
    p=dest/rel;st=p.stat();origin=(partial/rel).stat();assert (st.st_dev,st.st_ino)!=(origin.st_dev,origin.st_ino)
    with p.open('rb') as f:
     os.posix_fadvise(f.fileno(),0,0,os.POSIX_FADV_DONTNEED)
     mm=mmap.mmap(f.fileno(),st.st_size,access=mmap.ACCESS_COPY);buf=(ctypes.c_char*1).from_buffer(mm);pages=(st.st_size+4095)//4096;vec=(ctypes.c_ubyte*pages)();assert libc.mincore(ctypes.c_void_p(ctypes.addressof(buf)),ctypes.c_size_t(st.st_size),vec)==0;nr=sum(int(x)&1 for x in vec);del buf;mm.close();assert nr==0,(rel,nr,pages);residency.append({'path':rel,'pages':pages,'resident_pages':nr})
   dump(name+'/dispatch-residency.json',residency);observer=Observer(dest)
   try:call(ready,name,'private-cold-history-pages-zero-at-dispatch')
   finally:seen=observer.close()
   dump(name+'/actual-read-coverage.json',{'paths':sorted(seen),'missing':sorted(seen-readset),'overflow':False});assert not seen-readset,seen-readset
   call(ready,name,'second-call-process-warm')
 # Other heights and full routing stay in separate fresh hosts, same fixed cut.
 expected={q['height']:int(q['response']['result'],16) for q in json.loads((W/'candidate-requests.json').read_text())}
 for name,input_ in [('random-partial',partial),('random-complete',A/'http-performance-priority/candidate-input')]:http_case(name,R/'candidate-http.test',input_)
 # http_case also persists the shared request array; seal the complete sequence.
 dump('requests.json',requests);s['production_after']=identity();assert s['production_before']==s['production_after'];s.update(complete=True,stage='random-I/O cold and warm HTTP complete',all_returns_match=True,requests=len(requests));save()
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
