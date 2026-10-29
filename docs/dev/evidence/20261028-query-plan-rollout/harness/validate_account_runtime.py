"""No production mutations. Reuse the sealed fixed cut and private cold inodes."""
import pathlib,sys,json,os,shutil,time,urllib.request,traceback,hashlib,zipfile,ctypes,mmap
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'cold.py').read_text().split('\ntry:\n until=',1)[0],'fixed-host-definitions','exec'))
R=pathlib.Path('/data/gtron/maintenance/first-cold-followup-20261028-01/account-runtime');R.mkdir()
P=R.parent/'account-delta'
s={'complete':False,'stage':'native runtime tests and build','source':SOURCE,'base_source':'78c48bd020c11dbde226cb47245ae6d3f97aaf6e','production_mutations':False,'cache':'fresh processes; shared canonical cache uncontrolled; separate private account-index cold phase'};requests=[]
def request(ready,name,height,want,repeat):
 q={'jsonrpc':'2.0','id':len(requests)+1,'method':'eth_call','params':[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},hex(height)]}
 began=time.perf_counter();req=urllib.request.Request(ready['url'],data=json.dumps(q).encode(),headers={'Content-Type':'application/json'})
 with urllib.request.urlopen(req,timeout=150) as f:body=json.loads(f.read())
 requests.append({'case':name,'height':height,'repeat':repeat,'seconds_loopback_http':time.perf_counter()-began,'request':q,'response':body})
 (R/'requests.json').write_text(json.dumps(requests,indent=2));assert int(body['result'],16)==want;save()
try:
 s['production_before']=identity();save();src=archive_source('source-candidate',SOURCE)
 fixture=git('show',SOURCE+':docs/dev/evidence/20261028-query-plan-rollout/harness/offline_account_runtime_test.go.txt');(src/'core/state/snapshots/offline_account_runtime_test.go').write_bytes(fixture)
 command(['/data/go/bin/go','test','-tags=sapling','./core/state/snapshots','-run','TestAccountRoutingRuntime|TestAccountRoutingDelta|TestAccountDeltaRanges|TestHistoryRoutingMaintenance','-count=1','-timeout=180s'],src,R/'focused.log',240)
 command(['/data/go/bin/go','test','-tags=sapling','-c','./core/state/snapshots','-o',str(R/'snapshots.test')],src,R/'build-snapshots.log',300)
 command(['/data/go/bin/go','test','-tags=sapling','-c','./core','-o',str(R/'http.test')],src,R/'build-http.log',300)
 s['binary_sha256']={name:hashlib.sha256(p.read_bytes()).hexdigest() for name,p in [('before',P/'http.test'),('candidate',R/'http.test')]}
 assert s['binary_sha256']['before']=='12827bd067d3632ac1bd10852cffbc3a06bec9846931846e1561a74197cff9c0'
 dest=R/'input-runtime';dest.mkdir()
 for p in (P/'input-gaps').iterdir():
  if p.name in ['state-snapshots','ancient']:shutil.copytree(str(p),str(dest/p.name),copy_function=os.link,symlinks=True)
  else:os.symlink(str(p),str(dest/p.name),target_is_directory=p.is_dir())
 s['stage']='three bounded policy build/publication passes';save()
 env=dict(E,GTRON_ACCOUNT_RUNTIME_INPUT=str(dest/'state-snapshots'),GTRON_ACCOUNT_RUNTIME_OUTPUT=str(R))
 with (R/'runtime.log').open('wb') as log:subprocess.check_call([str(R/'snapshots.test'),'-test.run=^TestOfflineAccountRuntimeBoundedPasses$','-test.v','-test.timeout=5m'],cwd=str(R),env=env,stdout=log,stderr=subprocess.STDOUT,timeout=360)
 compact=P/'input-compact'
 s['compact_manifest_sha256_before']=hashlib.sha256((compact/'state-snapshots/manifest.json').read_bytes()).hexdigest()
 for name,exe in [('before-1',P/'http.test'),('candidate-1',R/'http.test'),('candidate-2',R/'http.test'),('before-2',P/'http.test')]:
  s['stage']=name+' same compact input HTTP';save()
  with host(name,exe,compact) as ready:
   for height,want in [(9000000,207),(10000000,213),(11000000,220),(15000000,238),(20000000,249)]:
    for repeat in range(2):request(ready,name,height,want,repeat)
 cold=R.parent/'account-delta-cold';private=cold/'input-private';proof=json.loads((cold/'private-proof.json').read_text());libc=ctypes.CDLL(None,use_errno=True)
 (R/'private-inode-proof.json').write_text(json.dumps(proof,indent=2))
 for index,(height,want) in enumerate([(9000000,207),(10000000,213),(15000000,238),(20000000,249)]*2):
  name='private-cold-'+str(index);s['stage']=name;save()
  with host(name,R/'http.test',private) as ready:
   residency=[]
   for item in proof:
    p=private/item['path'];st=p.stat();origin=(compact/item['path']).stat()
    assert st.st_nlink==1 and (st.st_dev,st.st_ino)==(item['private_dev'],item['private_ino']) and (st.st_dev,st.st_ino)!=(origin.st_dev,origin.st_ino)
    with p.open('rb') as f:
     os.posix_fadvise(f.fileno(),0,0,os.POSIX_FADV_DONTNEED)
     mm=mmap.mmap(f.fileno(),st.st_size,access=mmap.ACCESS_COPY);buf=(ctypes.c_char*1).from_buffer(mm);pages=(st.st_size+4095)//4096;vec=(ctypes.c_ubyte*pages)();assert libc.mincore(ctypes.c_void_p(ctypes.addressof(buf)),ctypes.c_size_t(st.st_size),vec)==0;nr=sum(int(x)&1 for x in vec);del buf;mm.close();assert nr==0,(str(p),nr,pages)
    residency.append({'path':item['path'],'pages':pages,'resident_pages':nr})
   (R/name/'dispatch-residency.json').write_text(json.dumps(residency,indent=2))
   for repeat in range(2):request(ready,name,height,want,repeat)
 s['compact_manifest_sha256_after']=hashlib.sha256((compact/'state-snapshots/manifest.json').read_bytes()).hexdigest();assert s['compact_manifest_sha256_before']==s['compact_manifest_sha256_after']
 s['production_after']=identity();assert s['production_before']==s['production_after'];s.update(complete=True,stage='bounded runtime and same-input HTTP complete',requests=len(requests),all_returns_match=True);save()
 target=pathlib.Path('/tmp/AAA-gtron-account-runtime-20261029-01.zip')
 assert not target.exists()
 with zipfile.ZipFile(str(target),'x',zipfile.ZIP_DEFLATED) as z:
  for p in sorted(R.rglob('*')):
   if p.is_file() and not p.is_symlink() and p.suffix in ['.json','.jsonl','.log'] and 'source-candidate' not in p.parts and 'input-runtime' not in p.parts:z.write(str(p),str(p.relative_to(R)))
 receipt={'bytes':target.stat().st_size,'sha256':hashlib.sha256(target.read_bytes()).hexdigest(),'path':str(target)}
 (R/'archive-receipt.json').write_text(json.dumps(receipt,indent=2));print(json.dumps(receipt),flush=True)
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
