import pathlib,sys,json,os,time,traceback,shutil,subprocess,hashlib
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'cold.py').read_text().split('\ntry:\n until=',1)[0],'fixed-cut-host-definitions','exec'))
P=pathlib.Path('/data/gtron/maintenance/direct-history-plan-20261028-01');R=P/'candidate';R.mkdir()
s={'complete':False,'stage':'native candidate build','source':SOURCE,'production_mutations':False,'full_validation_repeated':False};requests=[]
try:
 s['production_before']=identity();save();src=archive_source('source',SOURCE)
 command(['/data/go/bin/go','test','-tags=sapling','./core/state/snapshots','-run','Test(HistoryCandidates|HistoryMixedBatch|HistoryOrderedReads|MetadataHistoryRoutingEmptyProof)','-count=1','-timeout=120s'],src,R/'focused.log',180)
 command(['/data/go/bin/go','test','-tags=sapling','-c','./core','-o',str(R/'http.test')],src,R/'build.log')
 s['candidate_binary_sha256']=hashlib.sha256((R/'http.test').read_bytes()).hexdigest();s['stage']='wait for independent tail-index ablation';save()
 until=time.monotonic()+900
 while True:
  previous=json.loads((P/'tail-ablation-resumed/candidate-status.json').read_text())
  if previous.get('complete'):break
  assert previous.get('stage')!='failed',previous
  assert time.monotonic()<until,'tail ablation wait limit'
  time.sleep(2)
 full=A/'http-performance-priority/candidate-input';indexed=P/'tail-ablation/indexed-input';partial=O/'candidate-maintenance-input';baseline=W/'candidate-http.test';candidate=R/'http.test'
 for name,exe,input_ in [('old-full-1',baseline,full),('new-full-1',candidate,full),('new-full-2',candidate,full),('old-full-2',baseline,full),('new-indexed',candidate,indexed),('new-partial',candidate,partial)]:http_case(name,exe,input_)
 s['stage']='discover direct-query cold-history set';save();readset=set(json.loads((P/'full-readset.json').read_text()))
 with host('discovery-indexed',candidate,indexed) as ready:
  observer=Observer(indexed)
  try:call(ready,'discovery-indexed','discovery-excluded')
  finally:readset.update(observer.close())
 dump('readset.json',sorted(readset))
 # Reuse completed private canonical files; only auxiliary route differences or
 # additional speculative files require new copies. Never evict shared inodes.
 dest=R/'private-indexed-input';dest.mkdir();oldprivate=W/'cold/private-input'
 for p in indexed.iterdir():
  if p.name=='state-snapshots':shutil.copytree(str(oldprivate/p.name),str(dest/p.name),copy_function=os.link,symlinks=True)
  elif p.name=='ancient':shutil.copytree(str(indexed/p.name),str(dest/p.name),copy_function=os.link,symlinks=True)
  elif p.name=='ancient-original-link':continue
  else:os.symlink(str(p),str(dest/p.name),target_is_directory=p.is_dir())
 manifest=full/'state-snapshots/manifest.json';target=dest/'state-snapshots/manifest.json'
 # Do not copy over a hard link to any source manifest.
 tmp=target.with_name('manifest.private-next');shutil.copy2(str(manifest),str(tmp));os.replace(str(tmp),str(target))
 copied=0;proof=[];started=time.monotonic()
 for rel in sorted(readset):
  source=indexed/rel;st=source.stat();target=dest/rel;old=partial/rel
  reusable=target.is_file() and target.stat().st_size==st.st_size and (target.stat().st_dev,target.stat().st_ino)!=(st.st_dev,st.st_ino) and old.is_file() and (old.stat().st_dev,old.stat().st_ino)==(st.st_dev,st.st_ino)
  if not reusable:
   copied+=st.st_size;assert copied<=8<<30,'8 GiB extra-copy budget';assert shutil.disk_usage(str(R)).free>st.st_size+100_000_000_000
   tmp=target.with_name(target.name+'.private-next');tmp.parent.mkdir(parents=True,exist_ok=True)
   shutil.copy2(str(source),str(tmp));os.replace(str(tmp),str(target))
  after=source.stat();actual=target.stat();assert (st.st_dev,st.st_ino,st.st_size,st.st_mtime_ns)==(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns)
  assert actual.st_size==st.st_size and (actual.st_dev,actual.st_ino)!=(st.st_dev,st.st_ino)
  proof.append({'path':rel,'bytes':st.st_size,'source_ino':st.st_ino,'private_ino':actual.st_ino,'reused':reusable})
 dump('private-proof.json',proof);s.update(stage='controlled cold HTTP',extra_copied_bytes=copied,private_prepare_seconds=time.monotonic()-started,readset_files=len(readset));save()
 libc=ctypes.CDLL(None,use_errno=True)
 oldset=set(json.loads((W/'cold/readset.json').read_text()))
 for index,version in enumerate(['old-partial','direct-indexed','direct-indexed','old-partial']):
  name='cold-%d-%s'%(index,version);input_=oldprivate if version=='old-partial' else dest;exe=baseline if version=='old-partial' else candidate;controlled=oldset if version=='old-partial' else readset;s['stage']=name;save()
  with host(name,exe,input_) as ready:
   residency=[]
   for rel in sorted(controlled):
    file=input_/rel;st=file.stat();origin=(partial if version=='old-partial' else indexed)/rel;original=origin.stat();assert (st.st_dev,st.st_ino)!=(original.st_dev,original.st_ino)
    with file.open('rb') as f:
     os.posix_fadvise(f.fileno(),0,0,os.POSIX_FADV_DONTNEED);mm=mmap.mmap(f.fileno(),st.st_size,access=mmap.ACCESS_COPY);buf=(ctypes.c_char*1).from_buffer(mm);pages=(st.st_size+4095)//4096;vec=(ctypes.c_ubyte*pages)();assert libc.mincore(ctypes.c_void_p(ctypes.addressof(buf)),ctypes.c_size_t(st.st_size),vec)==0;nr=sum(int(x)&1 for x in vec);del buf;mm.close();assert nr==0,(rel,nr);residency.append({'path':rel,'pages':pages,'resident_pages':nr})
   dump(name+'/dispatch-residency.json',residency);observer=Observer(input_)
   try:call(ready,name,'private-cold-history-pages-zero-at-dispatch')
   finally:seen=observer.close()
   dump(name+'/actual-read-coverage.json',{'paths':sorted(seen),'missing':sorted(seen-controlled),'overflow':False});assert not seen-controlled,seen-controlled
   call(ready,name,'second-call-process-warm')
 dump('requests.json',requests);s['production_after']=identity();assert s['production_before']==s['production_after'];s.update(complete=True,stage='direct candidate and tail-index HTTP complete',requests=len(requests),all_returns_match=True);save()
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
