"""Causal cache-capacity experiment; an overlay-only, undeployed variant."""
import contextlib,datetime,json,os,pathlib,signal,statistics,subprocess,time,traceback,urllib.request
ROOT=pathlib.Path('/data/gtron/eth-call-fixed-20261020-01')
SOURCE=pathlib.Path('/data/gtron/cold-query-tail-20261020/candidate')
status={'complete':False,'phase':'waiting-for-diagnostics','production_mutations':False}
def save():
 status['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(ROOT/'capacity-status.json').write_text(json.dumps(status,indent=2))
def wait(predicate,limit):
 start=time.monotonic()
 while not predicate():
  if time.monotonic()-start>limit:raise TimeoutError('bounded capacity experiment wait')
  time.sleep(2)
def read_json(path):
 try:return json.loads(path.read_text())
 except (FileNotFoundError,json.JSONDecodeError):return {}
@contextlib.contextmanager
def host(mode,name):
 out=ROOT/name;out.mkdir()
 env=dict(os.environ,GTRON_FIXED_INPUT=str(ROOT/'cache-input'),GTRON_FIXED_OUTPUT=str(out),GOMAXPROCS='2',GOMEMLIMIT='3GiB')
 with (out/'host.log').open('wb') as log:
  p=subprocess.Popen([str(ROOT/(mode+'.test')),'-test.run=^TestOfflineFixedEthCallHTTP$','-test.v','-test.timeout=20m'],env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
  try:
   wait(lambda:(out/'ready.json').exists() or p.poll() is not None,120)
   assert p.poll() is None,(out/'host.log').read_text()
   ready=read_json(out/'ready.json');assert ready['head']==11818192
   yield ready,out
  finally:
   if p.poll() is None:
    os.kill(ready['pid'] if 'ready' in locals() else p.pid,signal.SIGTERM)
    try:p.wait(timeout=20)
    except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait()
   assert p.returncode==0,(out/'host.log').read_text()[-2000:]
try:
 save();wait(lambda:read_json(ROOT/'diagnostic-status.json').get('phase') in ('diagnosed','failed'),6000)
 assert read_json(ROOT/'diagnostic-status.json').get('complete')
 directory=ROOT/'capacity-overlay';directory.mkdir()
 source=SOURCE/'core/state/snapshots/history_header_cache.go'
 text=source.read_text();assert 'const historyHeaderCacheEntries = 8192' in text
 overlay=directory/'history_header_cache.go';overlay.write_text(text.replace('const historyHeaderCacheEntries = 8192','const historyHeaderCacheEntries = 32768',1))
 mapping=directory/'overlay.json';mapping.write_text(json.dumps({'Replace':{str(source):str(overlay)}},indent=2))
 env=dict(os.environ,PATH='/data/go/bin:'+os.environ.get('PATH',''),GOTOOLCHAIN='local',GOCACHE='/root/.cache/go-build',GOPATH='/root/go',CGO_ENABLED='1',GOMAXPROCS='2',GOMEMLIMIT='3GiB')
 status['phase']='building-overlay-variant';save()
 with (ROOT/'capacity-build.log').open('wb') as log:subprocess.run(['/data/go/bin/go','test','-p=2','-overlay',str(mapping),'-c','-tags=sapling','./core','-o',str(ROOT/'candidate-header32768.test')],cwd=SOURCE,env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=900)
 specs=[s for s in read_json(ROOT/'specs.json') if s['height'] in (3144020,9000000,11817936) or s['tag']=='latest']
 oracle={r['height']:r['expected'] for r in read_json(ROOT/'oracle.json')['expected']}
 rows=[]
 # Reverse both variant order and process order; identical original HTTP host,
 # original candidate binary vs one changed constant, without debug counters.
 for block,(mode,count) in enumerate((('candidate',2),('candidate-header32768',2),('candidate-header32768',1),('candidate',1))):
  status.update(phase='capacity-'+str(block)+'-'+mode);save()
  with host(mode,'capacity-'+str(block)+'-'+mode) as (ready,out):
   for repeat in range(-1,count):
    for spec in specs:
     body=json.dumps({'jsonrpc':'2.0','id':len(rows)+1,'method':'eth_call','params':[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},spec['tag']]}).encode();start=time.perf_counter()
     with urllib.request.urlopen(urllib.request.Request(ready['url'],body,{'Content-Type':'application/json'}),timeout=90) as response:result=json.loads(response.read())
     row={'id':len(rows)+1,'mode':mode,'block':block,'repeat':repeat,'height':spec['height'],'tag':spec['tag'],'milliseconds':(time.perf_counter()-start)*1000,'result':result,'correct':result.get('result')==oracle[spec['height']]}
     rows.append(row);assert row['correct'],row
     with (ROOT/'capacity-requests.jsonl').open('a') as log:log.write(json.dumps(row)+'\n')
 summary=[]
 for mode in ('candidate','candidate-header32768'):
  for spec in specs:
   values=[r['milliseconds'] for r in rows if r['mode']==mode and r['height']==spec['height'] and r['repeat']>=0]
   summary.append({'mode':mode,'height':spec['height'],'tag':spec['tag'],'n':len(values),'median_ms':statistics.median(values),'min_ms':min(values),'max_ms':max(values)})
 (ROOT/'capacity-summary.json').write_text(json.dumps(summary,indent=2));status.update(complete=True,phase='capacity-compared',requests=len(rows),all_correct=True);save()
except BaseException as exc:
 status.update(phase='failed',error=repr(exc),traceback=traceback.format_exc());save();raise
