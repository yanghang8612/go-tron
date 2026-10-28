# Finish evidence collection; do not modify production or the fixed canonical cut.
import pathlib,sys,json,os,time,datetime,traceback,subprocess,threading,urllib.request,hashlib
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'cold.py').read_text().split('\ntry:\n until=',1)[0],'fixed-cut-host-definitions','exec'))
P=pathlib.Path('/data/gtron/maintenance/direct-history-plan-20261028-01')
R=P/'selected-profile';R.mkdir()
s={'complete':False,'stage':'wait for final diagnostic','source':'9a9de233c9fd4def2acca00fdf22f6b26aa5cc76','production_mutations':False};requests=[]
try:
 end=time.monotonic()+900
 while True:
  diagnostic=json.loads((P/'block-cache-final/candidate-status.json').read_text())
  if diagnostic.get('complete') or diagnostic.get('stage')=='failed':break
  assert time.monotonic()<end
  time.sleep(2)
 rows=json.loads((P/'block-cache-final/requests.json').read_text());assert len(rows)==36,len(rows)
 values={}
 for q in rows:
  response=q['response'];assert 'error' not in response
  f=P/'block-cache-final'/q['case'];events=[json.loads(x) for x in (f/'server-requests.jsonl').read_text().splitlines()]
  e=next(x for x in events if x['id']==response['id']);height=int(e['params'][1],16)
  assert response['result']==values.setdefault(height,response['result'])
  if height==9000000:assert int(response['result'],16)==207
 timeline={x.name:{'ready_utc':datetime.datetime.utcfromtimestamp((x/'ready.json').stat().st_mtime).isoformat()+'Z','last_request_utc':datetime.datetime.utcfromtimestamp((x/'server-requests.jsonl').stat().st_mtime).isoformat()+'Z'} for x in (P/'block-cache-final').iterdir() if x.is_dir() and (x/'ready.json').exists()}
 (P/'block-cache-final/post-run-assessment.json').write_text(json.dumps({'requests':len(rows),'all_returns_match':True,'diagnostic_status_preserved':True,'external_production_identity_changed':diagnostic.get('production_before')!=diagnostic.get('production_after'),'admitted_to_selected_results':False,'reason':'bounded block cache did not remove the near-HEAD regression; production changed during this diagnostic; use earlier stable-identity 9a9de matched experiment','case_timeline':timeline},indent=2))
 s['production_before']=identity();s['stage']='selected reader CPU profile outside admitted timings';save()
 exe=P/'final/http.test';fixed=P/'final/indexed-input'
 with host('selected-reader-cpu',exe,fixed) as ready:
  def capture():
   with urllib.request.urlopen(ready['url']+'debug/pprof/profile?seconds=15',timeout=30) as f:(R/'selected-reader-cpu/cpu.pprof').write_bytes(f.read())
  t=threading.Thread(target=capture);t.start();end=time.monotonic()+16
  while time.monotonic()<end:call(ready,'selected-reader-cpu','profile-excluded')
  t.join()
 with (R/'selected-reader-cpu/cpu-top.txt').open('wb') as f:subprocess.check_call(['/data/go/bin/go','tool','pprof','-top','-cum','-nodecount=55','-tagfocus=rpc=eth_call',str(exe),str(R/'selected-reader-cpu/cpu.pprof')],stdout=f,stderr=subprocess.STDOUT,env=E)
 s['production_after']=identity();assert s['production_before']==s['production_after'];s.update(complete=True,stage='selected profile complete');save()
 v=identity();v['binary_sha256']=subprocess.check_output(['sha256sum','/proc/'+v['MainPID']+'/exe']).decode().split()[0];v['observed_utc']=datetime.datetime.utcnow().isoformat()+'Z';(P/'production-final.json').write_text(json.dumps(v,indent=2))
 exec(compile((P/'package.py').read_text(),'package','exec'))
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
