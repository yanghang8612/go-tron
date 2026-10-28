import pathlib,sys,json,os,shutil,time,urllib.request,traceback
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'cold.py').read_text().split('\ntry:\n until=',1)[0],'fixed-host-definitions','exec'))
R=pathlib.Path('/data/gtron/maintenance/first-cold-followup-20261028-01/storage-pages');R.mkdir()
s={'complete':False,'stage':'native storage-page build and tests','source':SOURCE,'base':'52aea62ef56799b7e6c1365293123dd029286935','production_mutations':False,'canonical_data_mutations':False,'cache':'fresh processes; shared OS caches uncontrolled; no eviction'};requests=[]
original=R.parent/'metadata-repair-ablation/input-candidate'
try:
 s['production_before']=identity();save();src=archive_source('source-candidate',SOURCE);fixture=git('show',SOURCE+':docs/dev/evidence/20261028-query-plan-rollout/harness/offline_storage_build_test.go.txt');(src/'core/state/snapshots/offline_storage_build_test.go').write_bytes(fixture)
 command(['/data/go/bin/go','test','-tags=sapling','./core/state/snapshots','-run','TestStoragePrefix|TestMetadataHistoryRoutingRepair','-count=1','-timeout=150s'],src,R/'focused.log',210)
 command(['/data/go/bin/go','test','-tags=sapling','-c','./core/state/snapshots','-o',str(R/'snapshots.test')],src,R/'build-snapshots.log',300)
 command(['/data/go/bin/go','test','-tags=sapling','-c','./core','-o',str(R/'http.test')],src,R/'build-http.log',300)
 dest=R/'input-storage';dest.mkdir()
 for p in original.iterdir():
  if p.name in ['state-snapshots','ancient']:shutil.copytree(str(p),str(dest/p.name),copy_function=os.link,symlinks=True)
  else:os.symlink(str(p),str(dest/p.name),target_is_directory=p.is_dir())
 env=dict(E,GTRON_STORAGE_BUILD_INPUT=str(dest/'state-snapshots'),GTRON_STORAGE_BUILD_OUTPUT=str(R));s['stage']='bounded dictionaries and one private bulk publication';save()
 with (R/'build-pages.log').open('wb') as log:subprocess.check_call([str(R/'snapshots.test'),'-test.run=^TestOfflineStorageRoutingBuild$','-test.v','-test.timeout=30m'],env=env,cwd=str(R),stdout=log,stderr=subprocess.STDOUT,timeout=1860)
 before=pathlib.Path('/data/gtron/query-plan-rollout-20261028-01/merged-http.test');candidate=R/'http.test'
 matrix=[('before-1',before,original),('control-1',candidate,original),('indexed-1',candidate,dest),('indexed-2',candidate,dest),('control-2',candidate,original),('before-2',before,original)]
 for name,exe,input_ in matrix:
  s['stage']=name+' complete HTTP';save()
  with host(name,exe,input_) as ready:
   for height,want in [(9000000,207),(10000000,213),(11000000,220),(15000000,238),(20000000,249)]:
    for repeat in range(2):
     q={'jsonrpc':'2.0','id':len(requests)+1,'method':'eth_call','params':[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},hex(height)]};start=time.perf_counter();req=urllib.request.Request(ready['url'],data=json.dumps(q).encode(),headers={'Content-Type':'application/json'})
     with urllib.request.urlopen(req,timeout=150) as f:body=json.loads(f.read())
     row={'case':name,'height':height,'repeat':repeat,'seconds_loopback_http':time.perf_counter()-start,'response':body};requests.append(row);(R/'requests.json').write_text(json.dumps(requests,indent=2));assert int(body['result'],16)==want,row;save()
 s['production_after']=identity();assert s['production_before']==s['production_after'];s.update(complete=True,stage='storage pages build and HTTP complete',all_returns_match=True,requests=len(requests));save()
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
