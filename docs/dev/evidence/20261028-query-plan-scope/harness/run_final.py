# Reuse the corrected auxiliary input; no further index rebuild or data copy.
import pathlib,sys,json,os,time,traceback,hashlib
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'cold.py').read_text().split('\ntry:\n until=',1)[0],'fixed-cut-host-definitions','exec'))
P=pathlib.Path('/data/gtron/maintenance/direct-history-plan-20261028-01');R=P/'query-plan-scope-final';R.mkdir()
s={'complete':False,'stage':'native request routing and posting frame build','source':SOURCE,'production_mutations':False,'full_validation_repeated':False,'index_input_reused_without_mutation':True};requests=[]
try:
 s['production_before']=identity();save();src=archive_source('source',SOURCE)
 command(['/data/go/bin/go','test','-tags=sapling','./core/rawdb','./core/state','./core/state/snapshots','-run','Test(HistoryCandidate|HistoryRoutingReadScope|StateChangePostingScratch|IndexedKVShortWindow|HistoryExecutionPrefetch|HistoryRequest)','-count=1','-timeout=120s'],src,R/'focused-state.log',180)
 command(['/data/go/bin/go','test','-tags=sapling','-c','./core','-o',str(R/'http.test')],src,R/'build.log')
 s['binary_sha256']=hashlib.sha256((R/'http.test').read_bytes()).hexdigest();save()
 assert json.loads((P/'final/candidate-status.json').read_text())['complete']
 dest=P/'final/indexed-input';private=P/'final/private-indexed-input';candidate=R/'http.test'
 for name,exe in [('before-indexed-1',P/'final/http.test'),('final-indexed-1',candidate),('final-indexed-2',candidate),('before-indexed-2',P/'final/http.test')]:http_case(name,exe,dest)
 readset=set(json.loads((P/'final/readset.json').read_text()));libc=ctypes.CDLL(None,use_errno=True)
 for index,version in enumerate(['before-indexed','final-indexed','final-indexed','before-indexed']):
  name='cold-%d-%s'%(index,version);input_=private;exe=P/'final/http.test' if version=='before-indexed' else candidate;controlled=readset;source=dest;s['stage']=name;save()
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
 dump('requests.json',requests);s['production_after']=identity();assert s['production_before']==s['production_after'];s.update(complete=True,stage='request routing and posting frame HTTP complete',requests=len(requests),all_returns_match=True);save()
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise

# Package only this experiment; immutable inputs, source archives and binaries stay on server.
import zipfile,datetime
paths=[p for p in R.rglob('*') if p.is_file() and not p.is_symlink() and 'source' not in p.relative_to(R).parts and p.suffix in {'.json','.jsonl','.log','.py','.pprof','.txt'} and p.name not in {'archive-receipt.json','member-seal.json'}]
seal={str(p.relative_to(R)):{'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(paths)}
raw=json.dumps(seal,indent=2).encode();(R/'member-seal.json').write_bytes(raw)
archive=pathlib.Path('/tmp/AAA-gtron-query-plan-scope-final-20261028-01.zip')
with zipfile.ZipFile(str(archive),'w',zipfile.ZIP_DEFLATED,6) as z:
 for p in sorted(paths):z.write(str(p),str(p.relative_to(R)))
 z.writestr('member-seal.json',raw)
os.chmod(str(archive),0o644)
receipt={'utc':datetime.datetime.utcnow().isoformat()+'Z','path':str(archive),'bytes':archive.stat().st_size,'sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),'members':len(paths),'source':SOURCE}
(R/'archive-receipt.json').write_text(json.dumps(receipt,indent=2));print(json.dumps(receipt))
