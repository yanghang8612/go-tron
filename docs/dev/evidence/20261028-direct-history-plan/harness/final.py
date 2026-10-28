# Final matched validation, with non-overlapping tail append and block batching.
import pathlib,sys,json,os,time,traceback,shutil,subprocess,hashlib
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'cold.py').read_text().split('\ntry:\n until=',1)[0],'fixed-cut-host-definitions','exec'))
P=pathlib.Path('/data/gtron/maintenance/direct-history-plan-20261028-01');R=P/'final';R.mkdir()
s={'complete':False,'stage':'native final build','source':SOURCE,'production_mutations':False,'full_validation_repeated':False,'early_overlapping_tail_trial_not_admitted':True};requests=[]
try:
 s['production_before']=identity();save();src=archive_source('source',SOURCE)
 (src/'core/offline_tail_index_test.go').write_bytes(git('show',SOURCE+':docs/dev/evidence/20261028-direct-history-plan/harness/tail_index_test.go.txt'))
 command(['/data/go/bin/go','test','-tags=sapling','./core/rawdb','-run','Test(IndexedKVBatch|ReadFirstState|QueryFirstTail|StateHistoryReadView)','-count=1','-timeout=120s'],src,R/'focused-rawdb.log',180)
 command(['/data/go/bin/go','test','-tags=sapling','-c','./core','-o',str(R/'http.test')],src,R/'build.log')
 s['binary_sha256']=hashlib.sha256((R/'http.test').read_bytes()).hexdigest()
 prior=json.loads((P/'candidate/candidate-status.json').read_text());assert prior['complete'],prior
 full=A/'http-performance-priority/candidate-input';dest=R/'indexed-input';dest.mkdir();copied=linked=0
 for p in full.iterdir():
  if p.name=='history':
   for directory,dirs,files in os.walk(str(p)):
    target=dest/'history'/pathlib.Path(directory).relative_to(p);target.mkdir(parents=True,exist_ok=True)
    for name in files:
     source=pathlib.Path(directory)/name;to=target/name
     if name.endswith('.sst'):os.link(str(source),str(to));linked+=source.stat().st_size
     else:
      copied+=source.stat().st_size;assert copied<=1<<30
      shutil.copy2(str(source),str(to));assert source.stat().st_ino!=to.stat().st_ino
  elif p.name=='ancient':shutil.copytree(str(p),str(dest/p.name),copy_function=os.link,symlinks=True)
  else:os.symlink(str(p),str(dest/p.name),target_is_directory=p.is_dir())
 s.update(stage='append only unpublished tail postings',copied_metadata_bytes=copied,linked_sst_bytes=linked);save()
 env=dict(E,GTRON_TAIL_INDEX_INPUT=str(dest),GTRON_TAIL_INDEX_OUTPUT=str(R/'index-build.json'),GTRON_TAIL_INDEX_WRITE='1')
 with (R/'index-build.log').open('wb') as f:subprocess.check_call([str(R/'http.test'),'-test.run=^TestOfflineTailIndexAblation$','-test.v','-test.timeout=5m'],env=env,stdout=f,stderr=subprocess.STDOUT,timeout=330)
 build=json.loads((R/'index-build.json').read_text());assert build['result']['FromBlock']==build['index_before']['BlockNum']+1
 # Same correct auxiliary data in both versions: isolate the batch-reader fix.
 before=P/'candidate/http.test';candidate=R/'http.test'
 for name,exe in [('before-indexed-1',before),('final-indexed-1',candidate),('final-indexed-2',candidate),('before-indexed-2',before)]:http_case(name,exe,dest)
 # New implementation with the untouched original auxiliary state as a control.
 http_case('final-original-full',candidate,full)
 s['stage']='discover final cold read set';save()
 readset=set(json.loads((P/'candidate/readset.json').read_text()))
 with host('discovery-final',candidate,dest) as ready:
  observer=Observer(dest)
  try:call(ready,'discovery-final','discovery-excluded')
  finally:readset.update(observer.close())
 dump('readset.json',sorted(readset))
 private=R/'private-indexed-input';private.mkdir();earlier=P/'candidate/private-indexed-input'
 for p in dest.iterdir():
  if p.name=='state-snapshots':os.symlink(str(earlier/p.name),str(private/p.name),target_is_directory=True)
  elif p.name=='ancient':shutil.copytree(str(p),str(private/p.name),copy_function=os.link,symlinks=True)
  else:os.symlink(str(p),str(private/p.name),target_is_directory=p.is_dir())
 extra=0;proof=[]
 for rel in sorted(readset):
  origin=dest/rel;original=origin.stat();target=private/rel
  if not target.is_file() or (target.stat().st_dev,target.stat().st_ino)==(original.st_dev,original.st_ino):
   extra+=original.st_size;assert extra<=1<<30
   tmp=target.with_name(target.name+'.final-private');tmp.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(str(origin),str(tmp));os.replace(str(tmp),str(target))
  actual=target.stat();assert actual.st_size==original.st_size and (actual.st_dev,actual.st_ino)!=(original.st_dev,original.st_ino)
  proof.append({'path':rel,'bytes':actual.st_size,'source_ino':original.st_ino,'private_ino':actual.st_ino})
 dump('private-proof.json',proof);s.update(stage='final controlled cold HTTP',extra_copied_bytes=extra,readset_files=len(readset));save()
 oldprivate=W/'cold/private-input';partial=O/'candidate-maintenance-input';oldset=set(json.loads((W/'cold/readset.json').read_text()));libc=ctypes.CDLL(None,use_errno=True)
 for index,version in enumerate(['old-partial','final-indexed','final-indexed','old-partial']):
  name='cold-%d-%s'%(index,version);input_=oldprivate if version=='old-partial' else private;exe=W/'candidate-http.test' if version=='old-partial' else candidate;controlled=oldset if version=='old-partial' else readset;source=partial if version=='old-partial' else dest;s['stage']=name;save()
  with host(name,exe,input_) as ready:
   residency=[]
   for rel in sorted(controlled):
    file=input_/rel;st=file.stat();original=(source/rel).stat();assert (st.st_dev,st.st_ino)!=(original.st_dev,original.st_ino)
    with file.open('rb') as f:
     os.posix_fadvise(f.fileno(),0,0,os.POSIX_FADV_DONTNEED);mm=mmap.mmap(f.fileno(),st.st_size,access=mmap.ACCESS_COPY);buf=(ctypes.c_char*1).from_buffer(mm);pages=(st.st_size+4095)//4096;vec=(ctypes.c_ubyte*pages)();assert libc.mincore(ctypes.c_void_p(ctypes.addressof(buf)),ctypes.c_size_t(st.st_size),vec)==0;nr=sum(int(x)&1 for x in vec);del buf;mm.close();assert nr==0,(rel,nr);residency.append({'path':rel,'pages':pages,'resident_pages':nr})
   dump(name+'/dispatch-residency.json',residency);observer=Observer(input_)
   try:call(ready,name,'private-cold-history-pages-zero-at-dispatch')
   finally:seen=observer.close()
   dump(name+'/actual-read-coverage.json',{'paths':sorted(seen),'missing':sorted(seen-controlled),'overflow':False});assert not seen-controlled,seen-controlled
   call(ready,name,'second-call-process-warm')
 dump('requests.json',requests);s['production_after']=identity();assert s['production_before']==s['production_after'];s.update(complete=True,stage='final matched HTTP complete',requests=len(requests),all_returns_match=True);save()
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
