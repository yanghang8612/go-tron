# Continue the same compiled ablation after giving the freezer a real directory.
import pathlib,sys,json,os,time,traceback,shutil,subprocess
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'cold.py').read_text().split('\ntry:\n until=',1)[0],'fixed-cut-host-definitions','exec'))
P=pathlib.Path('/data/gtron/maintenance/direct-history-plan-20261028-01')
old=P/'tail-ablation';R=P/'tail-ablation-resumed';R.mkdir()
s={'complete':False,'stage':'repair offline freezer path','source':SOURCE,'binary_source':'a9cca24e4c59b0e4381923ecb07af581666d8726','production_mutations':False};requests=[]
try:
 s['production_before']=identity();save();dest=old/'indexed-input';full=A/'http-performance-priority/candidate-input'
 assert (dest/'ancient').is_symlink()
 os.rename(str(dest/'ancient'),str(dest/'ancient-original-link'))
 shutil.copytree(str(full/'ancient'),str(dest/'ancient'),copy_function=os.link,symlinks=True)
 exe=old/'http.test'
 env=dict(E,GTRON_TAIL_INDEX_INPUT=str(dest),GTRON_TAIL_INDEX_OUTPUT=str(R/'index-build.json'),GTRON_TAIL_INDEX_WRITE='1')
 s['stage']='rebuild auxiliary tail';save()
 with (R/'index-build.log').open('wb') as f:subprocess.check_call([str(exe),'-test.run=^TestOfflineTailIndexAblation$','-test.v','-test.timeout=5m'],env=env,stdout=f,stderr=subprocess.STDOUT,timeout=330)
 for name,binary,input_ in [('original-full-1',W/'candidate-http.test',full),('indexed-full-1',exe,dest),('indexed-full-2',exe,dest),('original-full-2',W/'candidate-http.test',full)]:http_case(name,binary,input_)
 s['production_after']=identity();assert s['production_before']==s['production_after'];s.update(complete=True,stage='tail-index HTTP ablation complete',requests=len(requests));save()
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
