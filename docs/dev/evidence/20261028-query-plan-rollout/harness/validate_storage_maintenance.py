import pathlib,sys,json,os,shutil,time,urllib.request,traceback
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'cold.py').read_text().split('\ntry:\n until=',1)[0],'fixed-host-definitions','exec'))
R=pathlib.Path('/data/gtron/maintenance/first-cold-followup-20261028-01')/(sys.argv[2] if len(sys.argv)>2 else 'storage-maintenance');R.mkdir()
P=R.parent/'storage-pages';original=P/'input-storage';s={'complete':False,'stage':'native runtime maintenance tests and build','source':SOURCE,'production_mutations':False,'cache':'fresh HTTP processes; shared canonical/OS caches uncontrolled; no eviction'};requests=[]
try:
 s['production_before']=identity();save();src=archive_source('source-candidate',SOURCE)
 fixture=git('show',SOURCE+':docs/dev/evidence/20261028-query-plan-rollout/harness/offline_storage_maintenance_test.go.txt');(src/'core/state/snapshots/offline_storage_maintenance_test.go').write_bytes(fixture)
 command(['/data/go/bin/go','test','-tags=sapling','./core/state/snapshots','-run','TestStorageRoutingRuntime|TestHistoryRoutingMaintenance','-count=1','-timeout=150s'],src,R/'focused.log',210)
 command(['/data/go/bin/go','test','-tags=sapling','-c','./core/state/snapshots','-o',str(R/'snapshots.test')],src,R/'build-snapshots.log',300)
 command(['/data/go/bin/go','test','-tags=sapling','-c','./core','-o',str(R/'http.test')],src,R/'build-http.log',300)
 dest=R/'input-runtime';dest.mkdir()
 for p in original.iterdir():
  if p.name in ['state-snapshots','ancient']:shutil.copytree(str(p),str(dest/p.name),copy_function=os.link,symlinks=True)
  else:os.symlink(str(p),str(dest/p.name),target_is_directory=p.is_dir())
 s['stage']='repair eight absent pages through guarded runtime publication';save()
 env=dict(E,GTRON_STORAGE_MAINTENANCE_INPUT=str(dest/'state-snapshots'),GTRON_STORAGE_MAINTENANCE_OUTPUT=str(R))
 with (R/'runtime.log').open('wb') as log:subprocess.check_call([str(R/'snapshots.test'),'-test.run=^TestOfflineStorageRuntimeRepair$','-test.v','-test.timeout=10m'],env=env,cwd=str(R),stdout=log,stderr=subprocess.STDOUT,timeout=660)
 matrix=[('control-1',P/'http.test',original),('runtime-1',R/'http.test',dest),('runtime-2',R/'http.test',dest),('control-2',P/'http.test',original)]
 for name,exe,input_ in matrix:
  s['stage']=name+' full HTTP after runtime repair';save()
  with host(name,exe,input_) as ready:
   for height,want in [(9000000,207),(10000000,213),(15000000,238),(20000000,249)]:
    for repeat in range(2):
     q={'jsonrpc':'2.0','id':len(requests)+1,'method':'eth_call','params':[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},hex(height)]};began=time.perf_counter()
     req=urllib.request.Request(ready['url'],data=json.dumps(q).encode(),headers={'Content-Type':'application/json'})
     with urllib.request.urlopen(req,timeout=150) as f:body=json.loads(f.read())
     row={'case':name,'height':height,'repeat':repeat,'seconds_loopback_http':time.perf_counter()-began,'response':body};requests.append(row);(R/'requests.json').write_text(json.dumps(requests,indent=2));assert int(body['result'],16)==want;save()
 s['production_after']=identity();assert s['production_before']==s['production_after'];s.update(complete=True,stage='runtime page repair and HTTP matrix complete',requests=len(requests),all_returns_match=True);save()
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
