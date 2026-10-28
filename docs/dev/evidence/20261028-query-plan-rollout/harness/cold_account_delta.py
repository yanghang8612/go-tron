# Isolate the NEW account delta auxiliary files only, without production eviction.
# This is not a whole-node or all-canonical-files cold-cache benchmark.
import pathlib,sys,json,os,time,urllib.request,traceback,shutil,ctypes,mmap
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'cold.py').read_text().split('\ntry:\n until=',1)[0],'fixed-host-definitions','exec'))
R=pathlib.Path('/data/gtron/maintenance/first-cold-followup-20261028-01/account-delta-cold');R.mkdir()
P=R.parent/'account-delta';s={'complete':False,'stage':'wait for completed account delta matrix','source':SOURCE,'production_mutations':False,'cache':'new account delta files are private inodes, zero resident at dispatch; canonical and older routing caches uncontrolled'};requests=[]
try:
 until=time.monotonic()+1800
 while True:
  p=P/'candidate-status.json';state=json.loads(p.read_text()) if p.exists() else {}
  if state.get('complete'):
   s['binary_source']=state['source'];break
  live=subprocess.check_output(['systemctl','show','gtron-account-delta-20261029-01','-p','ActiveState']).decode()
  assert 'ActiveState=active' in live and state.get('stage')!='failed',state
  assert time.monotonic()<until,'observation wait deadline';time.sleep(5)
 s['production_before']=identity();s['stage']='private new auxiliary inodes';save()
 source=P/'input-compact';dest=R/'input-private';dest.mkdir()
 for p in source.iterdir():
  if p.name in ['state-snapshots','ancient']:shutil.copytree(str(p),str(dest/p.name),copy_function=os.link,symlinks=True)
  else:os.symlink(str(p),str(dest/p.name),target_is_directory=p.is_dir())
 m=json.loads((source/'state-snapshots/manifest.json').read_text());refs=[r for r in m['segments'] if r['kind']=='account-history-routing-delta'];assert 0<sum(r['size'] for r in refs)<8<<30
 proof=[]
 for copy_index,ref in enumerate(refs):
  s['stage']='copy private account delta '+str(copy_index+1)+'/'+str(len(refs));save()
  rel='state-snapshots/'+ref['path'];original=source/rel;target=dest/rel;temp=target.with_name(target.name+'.private-next');st=original.stat()
  with original.open('rb') as src,temp.open('xb') as out:shutil.copyfileobj(src,out,1<<20);out.flush();os.fsync(out.fileno())
  private=temp.stat();assert private.st_nlink==1 and private.st_size==st.st_size and (private.st_dev,private.st_ino)!=(st.st_dev,st.st_ino)
  os.replace(str(temp),str(target));proof.append({'path':rel,'bytes':st.st_size,'source_dev':st.st_dev,'source_ino':st.st_ino,'private_dev':private.st_dev,'private_ino':private.st_ino})
 (R/'private-proof.json').write_text(json.dumps(proof,indent=2));libc=ctypes.CDLL(None,use_errno=True)
 for index,(height,want) in enumerate([(9000000,207),(10000000,213),(15000000,238),(20000000,249)]*2):
  name='private-account-cold-'+str(index);s['stage']=name;save()
  with host(name,P/'http.test',dest) as ready:
   residency=[]
   for item in proof:
    p=dest/item['path'];st=p.stat();assert st.st_nlink==1 and (st.st_dev,st.st_ino)==(item['private_dev'],item['private_ino'])
    with p.open('rb') as f:
     os.posix_fadvise(f.fileno(),0,0,os.POSIX_FADV_DONTNEED)
     mm=mmap.mmap(f.fileno(),st.st_size,access=mmap.ACCESS_COPY);buf=(ctypes.c_char*1).from_buffer(mm);pages=(st.st_size+4095)//4096;vec=(ctypes.c_ubyte*pages)();assert libc.mincore(ctypes.c_void_p(ctypes.addressof(buf)),ctypes.c_size_t(st.st_size),vec)==0;nr=sum(int(x)&1 for x in vec);del buf;mm.close();assert nr==0,(str(p),nr,pages)
    residency.append({'path':item['path'],'pages':pages,'resident_pages':nr})
   (R/name/'dispatch-residency.json').write_text(json.dumps(residency,indent=2))
   for repeat in range(2):
    q={'jsonrpc':'2.0','id':len(requests)+1,'method':'eth_call','params':[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},hex(height)]};began=time.perf_counter()
    req=urllib.request.Request(ready['url'],data=json.dumps(q).encode(),headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(req,timeout=150) as f:body=json.loads(f.read())
    row={'case':name,'height':height,'repeat':repeat,'seconds_loopback_http':time.perf_counter()-began,'response':body};requests.append(row);(R/'requests.json').write_text(json.dumps(requests,indent=2));assert int(body['result'],16)==want;save()
 s['production_after']=identity();assert s['production_before']==s['production_after'];s.update(complete=True,stage='new auxiliary cold-page HTTP complete',all_returns_match=True);save()
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
