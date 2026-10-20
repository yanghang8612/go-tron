import contextlib,datetime,hashlib,json,os,pathlib,signal,subprocess,time,traceback,urllib.request
ROOT=pathlib.Path('/data/gtron/cold-query-layout-20261020-01');SOURCE=ROOT/'account-gc-source'
state={'complete':False,'production_mutations':False,'phase':'waiting-for-final-muhash-and-restore'}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(ROOT/'lazy-caller-preflight-status.json').write_text(json.dumps(state,indent=2))
try:
 save();begin=time.monotonic()
 while True:
  p=ROOT/'account-gc-verification-status.json';s=json.loads(p.read_text()) if p.exists() else {}
  if s.get('phase')=='failed':raise RuntimeError('final audit failed')
  done=ROOT/'account-gc-resource-status.json';r=json.loads(done.read_text()) if done.exists() else {}
  if s.get('complete') and r.get('complete') and r.get('phase')=='unit-finished':break
  if time.monotonic()-begin>14400:raise TimeoutError('final audit wait')
  time.sleep(5)
 overlay=ROOT/'lazy-caller-overlay.json';base=SOURCE/'core/state/history_request_cache.go';delta=json.loads((ROOT/'lazy-caller-delta.json').read_text());assert sha(base)==delta['before'];replacement=ROOT/'lazy-history-request-cache.go';replacement.write_text(delta['source']);assert sha(replacement)==delta['after']
 overlay.write_text(json.dumps({'Replace':{str(base):str(replacement)}}))
 env=dict(os.environ,PATH='/data/go/bin:'+os.environ.get('PATH',''),GOTOOLCHAIN='local',CGO_ENABLED='1',GOCACHE='/root/.cache/go-build',GOPATH='/root/go',GOMAXPROCS='2',GOMEMLIMIT='3GiB')
 for n in ('GTRON_FIXED_INPUT','GTRON_FIXED_OUTPUT','GTRON_FIXED_CENSUS','GTRON_GC_ROOT','GTRON_ROUTING_ROOT'):env.pop(n,None)
 def run(cmd,name,timeout=1000):
  state['phase']=name;save()
  with (ROOT/name).open('wb') as out:subprocess.run(cmd,cwd=SOURCE,env=env,stdout=out,stderr=subprocess.STDOUT,check=True,timeout=timeout)
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','-overlay='+str(overlay),'./core/state','-count=1','-timeout=15m'],'lazy-caller-native-state.log')
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','-overlay='+str(overlay),'-c','./core','-o',str(ROOT/'lazy-caller-candidate.test')],'lazy-caller-http-build.log')
 expected=json.loads((ROOT/'account-gc-source-identity.json').read_text())
 for name,digest in expected['production_sources'].items():assert sha(SOURCE/name)==digest,name
 effective=dict(expected['production_sources']);effective['core/state/history_request_cache.go']=delta['after'];(ROOT/'lazy-caller-source-identity.json').write_text(json.dumps({'production_sources_effective':effective,'base_sources_unchanged':True,'base_commit':'c7ef42cf5a6821ce6afc5d8f06d56c3fa23e7d30','overlay_sha256':sha(overlay),'replacement_sha256':sha(replacement),'candidate_sha256':sha(ROOT/'lazy-caller-candidate.test'),'http_harness_sha256':sha(SOURCE/'core/offline_fixed_http_test.go')},indent=2))
 output=ROOT/'lazy-caller-preflight';output.mkdir();specs=json.loads((ROOT/'account2-comparison/specs.json').read_text());responses=[]
 for mode in ('baseline','candidate'):
  dest=output/mode;dest.mkdir();hostenv=dict(env,GTRON_FIXED_INPUT=str(ROOT/'account-gc-input'),GTRON_FIXED_OUTPUT=str(dest))
  state['phase']='quick-http-'+mode;save()
  binary=ROOT/('account-gc-candidate.test' if mode=='baseline' else 'lazy-caller-candidate.test')
  with (dest/'host.log').open('wb') as log:
   process=subprocess.Popen([str(binary),'-test.run=^TestOfflineFixedEthCallHTTP$','-test.v','-test.timeout=10m'],env=hostenv,stdout=log,stderr=subprocess.STDOUT)
   try:
    begin=time.monotonic()
    while not (dest/'ready.json').exists():
     if process.poll() is not None:raise RuntimeError('host startup')
     if time.monotonic()-begin>120:raise TimeoutError('startup')
     time.sleep(.1)
    ready=json.loads((dest/'ready.json').read_text());old=json.loads((ROOT/'account2-comparison/comparison-status.json').read_text())['head']
    for key in ('head','head_tx','head_hash','root'):assert ready[key]==old[key],key
    for spec in specs+[specs[0]]*4:
     start=time.perf_counter();body=json.dumps({'jsonrpc':'2.0','id':len(responses)+1,'method':'eth_call','params':[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},spec['tag']]}).encode()
     with urllib.request.urlopen(urllib.request.Request(ready['url'],body,{'Content-Type':'application/json'}),timeout=150) as r:response=json.loads(r.read())
     assert response.get('result')==spec['expected'],response
     responses.append({'mode':mode,**spec,'response':response,'milliseconds':(time.perf_counter()-start)*1000,'diagnostic_excluded_from_primary_comparison':True})
   finally:
    if process.poll() is None:process.send_signal(signal.SIGTERM);process.wait(timeout=20)
    assert process.returncode==0,process.returncode
 (output/'responses.json').write_text(json.dumps(responses,indent=2))
 state.update(complete=True,phase='complete',responses=len(responses));save()
except BaseException as exc:
 state.update(phase='failed',error=repr(exc),traceback=traceback.format_exc());save();raise
