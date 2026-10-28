import pathlib,sys,json,os,time,traceback,shutil,subprocess
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'cold.py').read_text().split('\ntry:\n until=',1)[0],'fixed-cut-host-definitions','exec'))
R=pathlib.Path('/data/gtron/maintenance/direct-history-plan-20261028-01/tail-ablation');R.mkdir()
s={'complete':False,'stage':'build index ablation host','source':SOURCE,'production_mutations':False};requests=[]
try:
 s['production_before']=identity();save();src=archive_source('source',SOURCE)
 fixture=git('show',SOURCE+':docs/dev/evidence/20261028-direct-history-plan/harness/tail_index_test.go.txt')
 (src/'core/offline_tail_index_test.go').write_bytes(fixture)
 command(['/data/go/bin/go','test','-tags=sapling','-c','./core','-o',str(R/'http.test')],src,R/'build.log')
 full=A/'http-performance-priority/candidate-input'
 def prepare(input_,name,write):
  env=dict(E,GTRON_TAIL_INDEX_INPUT=str(input_),GTRON_TAIL_INDEX_OUTPUT=str(R/(name+'.json')),GTRON_TAIL_INDEX_WRITE='1' if write else '0')
  with (R/(name+'.log')).open('wb') as f:subprocess.check_call([str(R/'http.test'),'-test.run=^TestOfflineTailIndexAblation$','-test.v','-test.timeout=5m'],env=env,stdout=f,stderr=subprocess.STDOUT,timeout=330)
 prepare(full,'tail-info',False);info=json.loads((R/'tail-info.json').read_text());s['tail_info']=info;save()
 assert 0<info['lag_blocks']<=4096,info
 dest=R/'indexed-input';dest.mkdir();copied=linked=0
 for p in full.iterdir():
  if p.name!='history':os.symlink(str(p),str(dest/p.name),target_is_directory=p.is_dir());continue
  for directory,dirs,files in os.walk(str(p)):
   target=dest/'history'/pathlib.Path(directory).relative_to(p);target.mkdir(parents=True,exist_ok=True)
   for name in files:
    source=pathlib.Path(directory)/name;to=target/name
    if name.endswith('.sst'):os.link(str(source),str(to));linked+=source.stat().st_size
    else:
     copied+=source.stat().st_size;assert copied<=1<<30,'mutable metadata copy limit'
     shutil.copy2(str(source),str(to));assert source.stat().st_ino!=to.stat().st_ino
 s.update(stage='rebuild auxiliary tail on independent history copy',copied_metadata_bytes=copied,linked_sst_bytes=linked);save()
 prepare(dest,'index-build',True)
 for name,exe,input_ in [('original-full-1',W/'candidate-http.test',full),('indexed-full-1',R/'http.test',dest),('indexed-full-2',R/'http.test',dest),('original-full-2',W/'candidate-http.test',full)]:http_case(name,exe,input_)
 s['production_after']=identity();assert s['production_before']==s['production_after'];s.update(complete=True,stage='tail-index HTTP ablation complete',requests=len(requests));save()
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
