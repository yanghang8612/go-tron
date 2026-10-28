# Isolate account coverage without changing any canonical data or source code.
import pathlib,sys,json,os,shutil,hashlib,struct,time,urllib.request,traceback
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'cold.py').read_text().split('\ntry:\n until=',1)[0],'fixed-host-definitions','exec'))
R=pathlib.Path('/data/gtron/maintenance/first-cold-followup-20261028-01/account-gap-ablation');R.mkdir()
s={'complete':False,'stage':'derive private account coverage gap','source':'52aea62ef56799b7e6c1365293123dd029286935','production_mutations':False,'canonical_data_mutations':False,'cache':'fresh processes; shared OS file caches uncontrolled; no eviction'};requests=[]
original=pathlib.Path('/data/gtron/maintenance/direct-history-plan-20261028-01/final/indexed-input');exe=pathlib.Path('/data/gtron/query-plan-rollout-20261028-01/merged-http.test')
def pid_io(pid):
 try:return dict((k,int(v)) for k,v in (line.split(':') for line in pathlib.Path('/proc/'+str(pid)+'/io').read_text().splitlines()))
 except FileNotFoundError:return {}
try:
 s['production_before']=identity();save();dest=R/'input-1521-gaps';dest.mkdir()
 for p in original.iterdir():
  if p.name=='state-snapshots':shutil.copytree(str(p),str(dest/p.name),copy_function=os.link,symlinks=True)
  else:os.symlink(str(p),str(dest/p.name),target_is_directory=p.is_dir())
 snap=dest/'state-snapshots';m=json.loads((snap/'manifest.json').read_text());refs=m['segments'];route=next(r for r in refs if r['kind']=='account-history-routing');file=snap/route['path']
 with file.open('rb') as f:
  header=bytearray(f.read(64));assert header[:8]==b'GTACRT01';n=int.from_bytes(header[12:16],'big');dirs=int.from_bytes(header[32:40],'big');source_bytes=f.read(n*32);directory=f.read(dirs*53);old_meta=bytes(header)+source_bytes+directory;assert 'sha256:'+hashlib.sha256(old_meta).hexdigest()==route['lookupRoot'];ids=[source_bytes[i:i+32] for i in range(0,len(source_bytes),32)];assert len(set(ids))==n
  # Header source IDs were appended in manifest history order. Retain every
  # existing account upper bound: removing proof IDs can only add candidates.
  census=json.loads((R.parent/'fixed-census/summary.json').read_text());old_gaps=next(x['uncovered_current_pairs'] for x in census['routes'] if x['kind']=='account-history-routing');drop=1521-old_gaps;assert 0<drop<n;kept=ids[:-drop];header[12:16]=struct.pack('>I',len(kept));metadata=bytes(header)+b''.join(kept)+directory;newfile=snap/'history/account-gap-ablation.route';digest=hashlib.sha256()
  with newfile.open('xb') as target:
   target.write(metadata);digest.update(metadata)
   for chunk in iter(lambda:f.read(4<<20),b''):target.write(chunk);digest.update(chunk)
   target.flush();os.fsync(target.fileno())
  new=dict(route,path='history/account-gap-ablation.route',size=newfile.stat().st_size,lookupRoot='sha256:'+hashlib.sha256(metadata).hexdigest(),checksum='sha256:'+digest.hexdigest());assert new['size']==route['size']-drop*32
 refs[refs.index(route)]=new;m['generation']+=1;temp=snap/'manifest.ablation-next.json';temp.write_text(json.dumps(m));os.replace(str(temp),str(snap/'manifest.json'));s.update(original_account_uncovered=old_gaps,ablation_account_uncovered=1521,removed_proof_sources=drop,canonical_root_unchanged=True,source_route=route,derived_route=new);save()
 matrix=[('gaps-1',dest),('original-1',original),('original-2',original),('gaps-2',dest)]
 for name,input_ in matrix:
  s['stage']=name;save()
  with host(name,exe,input_) as ready:
   for height,want in [(9000000,207),(10000000,213),(11000000,220),(15000000,238),(20000000,249)]:
    for repeat in range(2):
     q={'jsonrpc':'2.0','id':len(requests)+1,'method':'eth_call','params':[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},hex(height)]};before=pid_io(ready['pid']);start=time.perf_counter();req=urllib.request.Request(ready['url'],data=json.dumps(q).encode(),headers={'Content-Type':'application/json'})
     with urllib.request.urlopen(req,timeout=150) as f:body=json.loads(f.read())
     elapsed=time.perf_counter()-start;after=pid_io(ready['pid']);row={'case':name,'height':height,'repeat':repeat,'seconds_loopback_http':elapsed,'response':body,'io_delta':{k:after[k]-before[k] for k in before if k in after}};requests.append(row);(R/'requests.json').write_text(json.dumps(requests,indent=2));assert int(body['result'],16)==want,row;save()
 s['production_after']=identity();assert s['production_before']==s['production_after'];s.update(complete=True,stage='account coverage ablation complete',all_returns_match=True,requests=len(requests));save()
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
