"""Additional diagnostic counters, never used for latency measurements."""
import contextlib, datetime, hashlib, json, os, pathlib, signal, subprocess, time, traceback, urllib.request

ROOT=pathlib.Path('/data/gtron/eth-call-fixed-20261020-01')
SOURCE=pathlib.Path('/data/gtron/cold-query-tail-20261020/candidate')
state={'complete':False,'phase':'waiting-for-comparison'}
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(ROOT/'diagnostic-status.json').write_text(json.dumps(state,indent=2))
def waitfor(predicate,seconds):
 start=time.monotonic()
 while not predicate():
  if time.monotonic()-start>seconds:raise TimeoutError('bounded diagnostic wait')
  time.sleep(2)
def identity():
 pid=int(subprocess.check_output(['systemctl','show','gtron','-p','MainPID']).decode().split('=')[1]);return {'pid':pid,'sha256':hashlib.sha256(pathlib.Path(f'/proc/{pid}/exe').read_bytes()).hexdigest()}
def run(command,name,env=None,cwd=SOURCE,timeout=900):
 state['phase']=name;save()
 with (ROOT/name).open('wb') as output:
  subprocess.run(command,cwd=cwd,env=env,stdout=output,stderr=subprocess.STDOUT,check=True,timeout=timeout)
def get(url):
 with urllib.request.urlopen(url,timeout=120) as response:return json.loads(response.read())
try:
 state['before_identity']=identity();save()
 waitfor(lambda:json.loads((ROOT/'comparison-status.json').read_text())['phase'] in ('compared','failed'),5400)
 compared=json.loads((ROOT/'comparison-status.json').read_text());assert compared['complete'],compared
 env=dict(os.environ,PATH='/data/go/bin:'+os.environ.get('PATH',''),GOTOOLCHAIN='local',GOCACHE='/root/.cache/go-build',GOPATH='/root/go',CGO_ENABLED='1',GOMAXPROCS='2',GOMEMLIMIT='3GiB',GTRON_FIXED_INPUT=str(ROOT/'capture-1'),GTRON_ORACLE_OUTPUT=str(ROOT/'oracle.json'))
 run([str(ROOT/'oracle.test'),'-test.run=^TestOfflineFixedCounterBeforeImageOracle$','-test.v','-test.timeout=15m'],'oracle.log',env)
 oracle=json.loads((ROOT/'oracle.json').read_text());expected={row['height']:row['expected'] for row in oracle['expected']}
 requests=[json.loads(line) for line in (ROOT/'requests.jsonl').read_text().splitlines()]
 assert all(row['response']['result']==expected[row['height']] for row in requests)
 state['oracle_verified_requests']=len(requests);save()
 overlaydir=ROOT/'diagnostic-overlay';overlaydir.mkdir()
 replace={}
 def overlay(relative,transform):
  source=SOURCE/relative;old=source.read_text();new=transform(old);assert new!=old
  dest=overlaydir/(source.parent.name+'-'+source.name);dest.write_text(new);replace[str(source)]=str(dest)
 def header(old):
  out=old.replace('"sync"','"sync"\n "sync/atomic"',1)
  out=out.replace('v, ok := c.entries[key]\n\treturn v, ok','v, ok := c.entries[key]\n diagnosticObserveHeaderKey(key); if ok { diagnosticHeaderHits.Add(1) } else { diagnosticHeaderMisses.Add(1) }; return v, ok',1)
  out=out.replace('delete(c.entries, c.fifo[c.next])','diagnosticHeaderEvictions.Add(1); delete(c.entries, c.fifo[c.next])',1)
  out=out.replace('key := historyHeaderKey{digest: blake2b.Sum256(comp), decodedSize: expected}','diagnosticHeaderBytes.Add(uint64(len(comp))); key := historyHeaderKey{digest: blake2b.Sum256(comp), decodedSize: expected}',1)
  out+='''
var diagnosticHeaderHits, diagnosticHeaderMisses, diagnosticHeaderEvictions, diagnosticHeaderBytes atomic.Uint64
var diagnosticHeaderKeyMu sync.Mutex
var diagnosticHeaderKeys=map[historyHeaderKey]struct{}{}
func diagnosticObserveHeaderKey(key historyHeaderKey){diagnosticHeaderKeyMu.Lock();defer diagnosticHeaderKeyMu.Unlock();diagnosticHeaderKeys[key]=struct{}{}}
func diagnosticHeaderKeyCount()uint64{diagnosticHeaderKeyMu.Lock();defer diagnosticHeaderKeyMu.Unlock();return uint64(len(diagnosticHeaderKeys))}
func DiagnosticFixedHeaderReset(){ diagnosticHeaderHits.Store(0);diagnosticHeaderMisses.Store(0);diagnosticHeaderEvictions.Store(0);diagnosticHeaderBytes.Store(0);diagnosticHeaderKeyMu.Lock();clear(diagnosticHeaderKeys);diagnosticHeaderKeyMu.Unlock() }
func DiagnosticFixedHeaderStats()map[string]uint64{return map[string]uint64{"hits":diagnosticHeaderHits.Load(),"misses":diagnosticHeaderMisses.Load(),"evictions":diagnosticHeaderEvictions.Load(),"distinct_keys":diagnosticHeaderKeyCount(),"compressed_bytes_revalidated":diagnosticHeaderBytes.Load(),"limit":historyHeaderCacheEntries}}
'''
  for text in ('diagnosticHeaderHits.Add(1)','diagnosticHeaderEvictions.Add(1)','diagnosticHeaderBytes.Add(uint64(len(comp)))'):assert text in out
  return out
 def tail(old):
  out=old.replace('if v.rejected[owner] {','if v.rejected[owner] {\n diagnosticTailCount(owner,"rejected_fallback")',1)
  out=out.replace('reject := func() {','reject := func() {\n diagnosticTailCount(owner,"budget_rejection")',1)
  out=out.replace('// Continue this same scan without a cache.', '''if v.used-oldBytes+next.bytes+size > v.budget {diagnosticTailCount(owner,"bytes_limit")}
 if v.rows-oldRows+len(next.rows)+1 > v.maxRows {diagnosticTailCount(owner,"rows_limit")}
 diagnosticTailValue(owner,"rows_at_rejection",uint64(v.rows-oldRows+len(next.rows)+1))
 diagnosticTailValue(owner,"bytes_at_rejection",uint64(v.used-oldBytes+next.bytes+size))
 // Continue this same scan without a cache.''',1)
  out=out.replace('v.entries[owner] = next','v.entries[owner] = next\n diagnosticTailCount(owner,"complete_scan_cached")',1)
  out=out.replace('for _, row := range entry.rows {','diagnosticTailCount(owner,"cache_hit")\n for _, row := range entry.rows {',1)
  out+='''
var diagnosticTailMu sync.Mutex
var diagnosticTailRows=map[string]map[string]uint64{}
func diagnosticTailCount(owner common.Address,event string){diagnosticTailMu.Lock();defer diagnosticTailMu.Unlock();key:=owner.Hex();if diagnosticTailRows[key]==nil{diagnosticTailRows[key]=map[string]uint64{}};diagnosticTailRows[key][event]++}
func diagnosticTailValue(owner common.Address,event string,n uint64){diagnosticTailMu.Lock();defer diagnosticTailMu.Unlock();key:=owner.Hex();if diagnosticTailRows[key]==nil{diagnosticTailRows[key]=map[string]uint64{}};diagnosticTailRows[key][event]=n}
func DiagnosticFixedTailReset(){diagnosticTailMu.Lock();defer diagnosticTailMu.Unlock();diagnosticTailRows=map[string]map[string]uint64{}}
func DiagnosticFixedTailStats()map[string]map[string]uint64{diagnosticTailMu.Lock();defer diagnosticTailMu.Unlock();out:=map[string]map[string]uint64{};for owner,events:=range diagnosticTailRows{row:=map[string]uint64{};for event,n:=range events{row[event]=n};out[owner]=row};return out}
'''
  return out
 def host(old):
  needle='mux := http.NewServeMux()'
  assert needle in old
  return old.replace(needle,needle+'''
 mux.HandleFunc("/diag/reset",func(w http.ResponseWriter,r *http.Request){snapshots.DiagnosticFixedHeaderReset();rawdb.DiagnosticFixedTailReset();w.Write([]byte("{}"))})
 mux.HandleFunc("/diag/cache",func(w http.ResponseWriter,r *http.Request){json.NewEncoder(w).Encode(map[string]any{"header":snapshots.DiagnosticFixedHeaderStats(),"tail":rawdb.DiagnosticFixedTailStats()})})
''',1)
 overlay('core/state/snapshots/history_header_cache.go',header)
 overlay('core/rawdb/state_history_query_tail.go',tail)
 overlay('core/offline_fixed_http_test.go',host)
 overlayfile=overlaydir/'overlay.json';overlayfile.write_text(json.dumps({'Replace':replace},indent=2))
 run(['/data/go/bin/go','test','-p=2','-overlay',str(overlayfile),'-c','-tags=sapling','./core','-o',str(ROOT/'diagnostic.test')],'diagnostic-build.log',env)
 out=ROOT/'diagnostic-host';out.mkdir();env['GTRON_FIXED_OUTPUT']=str(out)
 with (out/'host.log').open('wb') as logfile:
  process=subprocess.Popen([str(ROOT/'diagnostic.test'),'-test.run=^TestOfflineFixedEthCallHTTP$','-test.v','-test.timeout=25m'],env=env,stdout=logfile,stderr=subprocess.STDOUT,start_new_session=True)
  try:
   waitfor(lambda:(out/'ready.json').exists() or process.poll() is not None,120)
   assert process.poll() is None,(out/'host.log').read_text()
   ready=json.loads((out/'ready.json').read_text());results=[]
   specs=json.loads((ROOT/'specs.json').read_text())
   state['phase']='diagnostic-counts';save()
   for repeat in range(2):
    for spec in specs:
     get(ready['url']+'diag/reset')
     body=json.dumps({'jsonrpc':'2.0','id':len(results)+1,'method':'eth_call','params':[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},spec['tag']]}).encode()
     with urllib.request.urlopen(urllib.request.Request(ready['url'],body,{'Content-Type':'application/json'}),timeout=120) as response:result=json.loads(response.read())
     assert result.get('result')==expected[spec['height']],result
     results.append({'repeat':repeat,'height':spec['height'],'tag':spec['tag'],'result':result,'stats':get(ready['url']+'diag/cache')})
     (ROOT/'diagnostic-counts.json').write_text(json.dumps(results,indent=2))
  finally:
   if process.poll() is None:
    os.kill(ready['pid'] if 'ready' in locals() else process.pid,signal.SIGTERM)
    try:process.wait(timeout=20)
    except subprocess.TimeoutExpired:os.killpg(process.pid,signal.SIGKILL);process.wait()
   assert process.returncode==0,(out/'host.log').read_text()[-3000:]
 for mode in ('baseline','candidate'):
  for name,args in [('cpu-top.txt',['-top','-nodecount=50']),('cpu-history.txt',['-top','-cum','-nodecount=60','-focus=TronBackend|PersistentHistoryReader|iterateStateHistory|historyHeader|compressedBlockReader|decodePersisted|rlp']),('cpu-list.txt',['-list=readHeaderPrefix|iterateOwner|executionDynamicPropertiesStrict'])]:
   run(['/data/go/bin/go','tool','pprof']+args+[str(ROOT/(mode+'.test')),str(ROOT/('profile-'+mode)/'cpu.pb.gz')],mode+'-'+name,env)
 state['after_identity']=identity();assert state['after_identity']==state['before_identity']
 state.update(complete=True,phase='diagnosed',diagnostic_queries=len(results));save()
except BaseException as exc:
 state.update(phase='failed',error=repr(exc),traceback=traceback.format_exc());save();raise
