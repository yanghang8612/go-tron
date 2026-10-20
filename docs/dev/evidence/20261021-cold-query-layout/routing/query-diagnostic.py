import datetime,hashlib,json,os,pathlib,signal,subprocess,time,traceback,urllib.request
ROOT=pathlib.Path('/data/gtron/cold-query-layout-20261020-01');SOURCE=ROOT/'routing-source'
state={'complete':False,'production_mutations':False,'phase':'waiting-for-comparison'}
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(ROOT/'routing-diagnostic-status.json').write_text(json.dumps(state,indent=2))
try:
 save();start=time.monotonic()
 while True:
  p=ROOT/'routing-comparison/comparison-status.json';s=json.loads(p.read_text()) if p.exists() else {}
  if s.get('phase')=='failed':raise RuntimeError('comparison failed')
  if s.get('complete'):break
  if time.monotonic()-start>7200:raise TimeoutError('comparison wait')
  time.sleep(5)
 d=json.loads((ROOT/'routing-query-diagnostic-delta.json').read_text());base=SOURCE/'core/state/snapshots/history_segment.go';assert hashlib.sha256(base.read_bytes()).hexdigest()==d['base_sha256'];lines=base.read_text().splitlines(keepends=True)
 for edit in reversed(d['edits']):lines[edit['start']:edit['end']]=edit['text'].splitlines(keepends=True)
 data=''.join(lines).encode();assert hashlib.sha256(data).hexdigest()==d['after_sha256'];overlaySource=ROOT/'diagnostic-history-segment.go';overlaySource.write_bytes(data)
 overlay=ROOT/'routing-query-diagnostic-overlay.json';overlay.write_text(json.dumps({'Replace':{str(base):str(overlaySource)}}))
 env=dict(os.environ,PATH='/data/go/bin:'+os.environ.get('PATH',''),GOTOOLCHAIN='local',CGO_ENABLED='1',GOCACHE='/root/.cache/go-build',GOPATH='/root/go',GOMAXPROCS='2',GOMEMLIMIT='3GiB')
 state['phase']='build-diagnostic-overlay';save()
 with (ROOT/'routing-query-diagnostic-build.log').open('wb') as out:subprocess.run(['/data/go/bin/go','test','-p=2','-tags=sapling','-overlay='+str(overlay),'-c','./core','-o',str(ROOT/'routing-query-diagnostic.test')],cwd=SOURCE,env=env,stdout=out,stderr=subprocess.STDOUT,check=True,timeout=900)
 output=ROOT/'routing-query-diagnostic';output.mkdir();env.update(GTRON_FIXED_INPUT=str(ROOT/'routing-input'),GTRON_FIXED_OUTPUT=str(output),GTRON_ROUTING_QUERY_TRACE=str(output/'cold-batches.jsonl'))
 state['phase']='two-full-http-diagnostic-calls';save()
 with (output/'host.log').open('wb') as log:
  process=subprocess.Popen([str(ROOT/'routing-query-diagnostic.test'),'-test.run=^TestOfflineFixedEthCallHTTP$','-test.v','-test.timeout=10m'],env=env,stdout=log,stderr=subprocess.STDOUT)
  try:
   start=time.monotonic()
   while not (output/'ready.json').exists():
    if process.poll() is not None:raise RuntimeError('diagnostic host startup failed')
    if time.monotonic()-start>120:raise TimeoutError('diagnostic host startup')
    time.sleep(.1)
   ready=json.loads((output/'ready.json').read_text());spec=json.loads((ROOT/'routing-comparison/specs.json').read_text())[0];responses=[]
   for repeat in range(2):
    body=json.dumps({'jsonrpc':'2.0','id':repeat+1,'method':'eth_call','params':[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},spec['tag']]}).encode();before=time.perf_counter()
    with urllib.request.urlopen(urllib.request.Request(ready['url'],body,{'Content-Type':'application/json'}),timeout=150) as response:row=json.loads(response.read())
    assert row.get('result')==spec['expected'],row;responses.append({'response':row,'milliseconds':(time.perf_counter()-before)*1000,'diagnostic_excluded_from_performance':True})
   (output/'responses.json').write_text(json.dumps(responses,indent=2))
  finally:
   if process.poll() is None:process.send_signal(signal.SIGTERM);process.wait(timeout=20)
   assert process.returncode==0,process.returncode
 state.update(complete=True,phase='complete',diagnostic_binary_sha256=hashlib.sha256((ROOT/'routing-query-diagnostic.test').read_bytes()).hexdigest(),overlay_source_sha256=hashlib.sha256(overlaySource.read_bytes()).hexdigest());save()
except BaseException as exc:
 state.update(phase='failed',error=repr(exc),traceback=traceback.format_exc());save();raise
