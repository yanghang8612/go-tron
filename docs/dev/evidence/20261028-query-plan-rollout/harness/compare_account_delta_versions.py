import pathlib,sys,json,time,urllib.request,traceback,hashlib
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'cold.py').read_text().split('\ntry:\n until=',1)[0],'fixed-host-definitions','exec'))
R=pathlib.Path('/data/gtron/maintenance/first-cold-followup-20261028-01/account-delta-code-ab');R.mkdir()
P=R.parent/'account-delta';old=R.parent/'storage-maintenance-r2';old_status=json.loads((old/'candidate-status.json').read_text())
assert old_status['complete'] and old_status['source']=='69d74ae09d953a40a65b98b0a43eb22d115e7f70'
assert json.loads((P/'candidate-status.json').read_text())['complete']
s={'complete':False,'stage':'old/new compiled HTTP comparison','source':SOURCE,'base_source':old_status['source'],'candidate_source':json.loads((P/'candidate-status.json').read_text())['source'],'production_mutations':False,'cache':'fresh processes, same fixed canonical cut, shared OS caches uncontrolled; no eviction'};requests=[]
try:
 s['production_before']=identity();s['binary_sha256']={name:hashlib.sha256(path.read_bytes()).hexdigest() for name,path in [('before',old/'http.test'),('candidate',P/'http.test')]};save()
 matrix=[('before-1',old/'http.test',P/'input-gaps'),('candidate-1',P/'http.test',P/'input-compact'),('candidate-2',P/'http.test',P/'input-compact'),('before-2',old/'http.test',P/'input-gaps')]
 for name,exe,input_ in matrix:
  s['stage']=name;save()
  with host(name,exe,input_) as ready:
   for height,want in [(9000000,207),(10000000,213),(11000000,220),(15000000,238),(20000000,249)]:
    for repeat in range(2):
     q={'jsonrpc':'2.0','id':len(requests)+1,'method':'eth_call','params':[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},hex(height)]};began=time.perf_counter();req=urllib.request.Request(ready['url'],data=json.dumps(q).encode(),headers={'Content-Type':'application/json'})
     with urllib.request.urlopen(req,timeout=150) as f:body=json.loads(f.read())
     row={'case':name,'height':height,'repeat':repeat,'seconds_loopback_http':time.perf_counter()-began,'response':body};requests.append(row);(R/'requests.json').write_text(json.dumps(requests,indent=2));assert int(body['result'],16)==want;save()
 s['production_after']=identity();assert s['production_before']==s['production_after'];s.update(complete=True,stage='compiled version HTTP comparison complete',requests=len(requests));save()
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
