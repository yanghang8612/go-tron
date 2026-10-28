# Count accessed immutable files after the main matrix; no production eviction.
import pathlib,sys,json,time,urllib.request,traceback
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'cold.py').read_text().split('\ntry:\n until=',1)[0],'fixed-host-definitions','exec'))
R=pathlib.Path('/data/gtron/maintenance/first-cold-followup-20261028-01/storage-observe');R.mkdir()
P=R.parent/'storage-pages';s={'complete':False,'stage':'wait for specific main comparison unit','production_mutations':False,'source':SOURCE,'cache':'uncontrolled OS caches; diagnostics excluded from primary matrix'};requests=[]
try:
 until=time.monotonic()+2100
 while True:
  status=json.loads((P/'candidate-status.json').read_text())
  if status['complete']:break
  live=subprocess.check_output(['systemctl','show','gtron-cold-storage-pages-20261028-01','-p','ActiveState','-p','SubState']).decode()
  assert 'ActiveState=active' in live and status['stage']!='failed',status
  assert time.monotonic()<until,'main comparison deadline';time.sleep(5)
 s['production_before']=identity();save()
 matrix=[('control-1',R.parent/'metadata-repair-ablation/input-candidate'),('indexed-1',P/'input-storage'),('indexed-2',P/'input-storage'),('control-2',R.parent/'metadata-repair-ablation/input-candidate')]
 for name,input_ in matrix:
  s['stage']=name+' file-access observation';save()
  with host(name,P/'http.test',input_) as ready:
   for repeat in range(2):
    observer=Observer(input_);q={'jsonrpc':'2.0','id':len(requests)+1,'method':'eth_call','params':[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},hex(20000000)]};began=time.perf_counter()
    try:
     req=urllib.request.Request(ready['url'],data=json.dumps(q).encode(),headers={'Content-Type':'application/json'})
     with urllib.request.urlopen(req,timeout=150) as f:body=json.loads(f.read())
     seconds=time.perf_counter()-began
    finally:seen=observer.close()
    row={'case':name,'repeat':repeat,'seconds_diagnostic_http':seconds,'response':body,'paths':sorted(seen),'overflow':observer.overflow};requests.append(row);(R/'requests.json').write_text(json.dumps(requests,indent=2));assert int(body['result'],16)==249;save()
 s['production_after']=identity();assert s['production_before']==s['production_after'];s.update(complete=True,stage='file-access observation complete',all_returns_match=True);save()
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
