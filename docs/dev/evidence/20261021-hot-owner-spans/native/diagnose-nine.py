import pathlib,json,hashlib,datetime,os,subprocess,signal,urllib.request,time,ctypes,mmap,collections,re,zipfile,traceback,threading
OLDLAB=pathlib.Path('/data/gtron/cold-query-layout-20261020-01');LAB=OLDLAB/'hot-owner-spans-20261021-03';ROOT=LAB/'nine-diagnostic';ROOT.mkdir(exist_ok=False);INPUT=LAB/'tail-read-comparison/candidate-cache-input';state={'complete':False,'excluded_from_primary_performance':True,'production_mutations':False}
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();p=ROOT/'status.json';tmp=p.with_suffix('.tmp');tmp.write_text(json.dumps(state,indent=2));tmp.replace(p)
try:
 state['phase']='waiting-for-serial-audit';save();wait_start=time.monotonic()
 while True:
  q=LAB/'audit-status.json'
  if q.exists():
   x=json.loads(q.read_text());assert x.get('phase')!='failed',x
   inactive=subprocess.check_output(['systemctl','show','gtron-owner-blake-audit-20261021.service','-p','ActiveState']).decode().strip()=='ActiveState=inactive'
   if x.get('complete') and inactive:break
  if time.monotonic()-wait_start>7000:raise TimeoutError('bounded serial audit wait')
  time.sleep(2)
 assert json.loads((LAB/'tail-read-comparison/comparison-status.json').read_text())['complete'];proof=json.loads((LAB/'tail-read-comparison/candidate-private-files.json').read_text())
 private=set()
 for r in proof:
  f=INPUT/r['path'];s=f.stat();assert s.st_ino==r['private_inode'] and s.st_ino!=r['source_inode'] and s.st_size==r['bytes'];private.add(r['path'])
 code=(LAB/'compare-native.py').read_text();exec(code[code.index('def evict_private('):code.index('def replica(')],globals());state['phase']='evict-proven-private-inodes';save();res=evict_private(INPUT,private);(ROOT/'residency.json').write_text(json.dumps(res))
 env=dict(os.environ,GTRON_FIXED_INPUT=str(INPUT),GTRON_FIXED_OUTPUT=str(ROOT),GOMAXPROCS='2',GOMEMLIMIT='3GiB');binary=LAB/'candidate.test';state.update(phase='single-os-cold-full-http-strace',candidate_sha256=hashlib.sha256(binary.read_bytes()).hexdigest());save()
 with (ROOT/'host.log').open('wb') as out:
  process=subprocess.Popen(['strace','-f','-ttt','-T','-yy','-e','trace=read,pread64','-o',str(ROOT/'read.trace'),str(binary),'-test.run=^TestOfflineFixedEthCallHTTP$','-test.v','-test.timeout=6m'],env=env,stdout=out,stderr=subprocess.STDOUT,start_new_session=True)
  try:
   begin=time.monotonic()
   while not (ROOT/'ready.json').exists():
    assert process.poll() is None
    if time.monotonic()-begin>150:raise TimeoutError('trace host')
    time.sleep(.1)
   ready=json.loads((ROOT/'ready.json').read_text());expected=json.loads((LAB/'tail-read-comparison/comparison-status.json').read_text())['head']
   for key in ['head','head_tx','head_hash','root']:assert ready[key]==expected[key]
   request_start=time.time();begin=time.perf_counter();body=json.dumps({'jsonrpc':'2.0','id':1,'method':'eth_call','params':[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},'0x895440']}).encode()
   with urllib.request.urlopen(urllib.request.Request(ready['url'],body,{'Content-Type':'application/json'}),timeout=150) as r:response=json.loads(r.read())
   request_end=time.time();assert response.get('result')=='0x'+format(207,'064x'),response
   (ROOT/'response.json').write_text(json.dumps({'response':response,'correct':True,'start_unix':request_start,'end_unix':request_end,'milliseconds':(time.perf_counter()-begin)*1000,'excluded_from_primary_performance':True},indent=2))
  finally:
   if process.poll() is None:os.kill(ready['pid'] if 'ready' in locals() else process.pid,signal.SIGTERM);process.wait(timeout=20)
   assert process.returncode==0,process.returncode
 rows=[];unparsed=[]
 for line in (ROOT/'read.trace').read_text().splitlines():
  ts=re.match(r'\s*\d+\s+(\d+\.\d+)\s+',line)
  if not ts or not request_start<=float(ts[1])<=request_end:continue
  match=re.search(r'\b(read|pread64)\(\d+<([^>]+)>.*=\s+(\d+)\s+<([0-9.]+)>$',line)
  if not match:
   unparsed.append(line);continue
  call,path,size,duration=match.groups()
  if str(INPUT) in path:
   file=pathlib.Path(path);relative=str(file.relative_to(INPUT));family='cold-history' if relative.startswith('state-snapshots/history/') else ('hot-history-pebble' if relative.startswith('history/') else ('state-pebble' if relative.startswith('chaindata/') else 'other-input'))
   kind=file.suffix;rows.append({'family':family,'suffix':kind,'path':relative,'calls':1,'returned_bytes':int(size),'syscall_elapsed_seconds':float(duration),'timestamp':float(ts[1])})
 groups={}
 for row in rows:row['read_size']=row['returned_bytes']
 for fields in [('family',),('family','suffix'),('path',),('family','suffix','read_size')]:
  grouped=collections.defaultdict(lambda:{'calls':0,'returned_bytes':0,'syscall_elapsed_seconds':0.0})
  for r in rows:
   group=grouped[tuple(r[k] for k in fields)];group['calls']+=1;group['returned_bytes']+=r['returned_bytes'];group['syscall_elapsed_seconds']+=r['syscall_elapsed_seconds']
  groups['+'.join(fields)]=[{**dict(zip(fields,key)),**value} for key,value in sorted(grouped.items(),key=lambda kv:-kv[1]['syscall_elapsed_seconds'])]
 (ROOT/'read-breakdown.json').write_text(json.dumps({'complete':True,'excluded_from_primary_performance':True,'notes':'Completed read/pread64 syscalls starting in the client request window; syscall elapsed sums can overlap across threads and are not a disjoint wall-clock profile. Unparsed lines are retained. Startup reads excluded.','groups':groups,'attributed_calls':len(rows),'unparsed_window_lines':len(unparsed)},indent=2));(ROOT/'unparsed-window.txt').write_text('\n'.join(unparsed)+'\n');state.update(phase='trace-complete');save()

 # An additional warmed CPU profile is diagnostic-only and serial with the trace.
 state['phase']='diagnostic-warm-cpu-profile';save();prof=ROOT/'profile';prof.mkdir()
 env=dict(env,GTRON_FIXED_OUTPUT=str(prof));errors=[];count=0
 with (prof/'host.log').open('wb') as out:
  process=subprocess.Popen([str(binary),'-test.run=^TestOfflineFixedEthCallHTTP$','-test.v','-test.timeout=4m'],env=env,stdout=out,stderr=subprocess.STDOUT,start_new_session=True)
  try:
   start=time.monotonic()
   while not (prof/'ready.json').exists():
    assert process.poll() is None
    if time.monotonic()-start>120:raise TimeoutError('profile host')
    time.sleep(.1)
   ready=json.loads((prof/'ready.json').read_text())
   def call():
    with urllib.request.urlopen(urllib.request.Request(ready['url'],body,{'Content-Type':'application/json'}),timeout=150) as r:result=json.loads(r.read())
    assert result.get('result')=='0x'+format(207,'064x'),result
   call()
   def profile():
    try:
     with urllib.request.urlopen(ready['url']+'debug/pprof/profile?seconds=20',timeout=40) as r:(prof/'cpu.pb.gz').write_bytes(r.read())
    except BaseException as exc:errors.append(repr(exc))
   worker=threading.Thread(target=profile);worker.start()
   while worker.is_alive():call();count+=1
   worker.join();assert not errors,errors
   (prof/'profile-receipt.json').write_text(json.dumps({'excluded_from_primary_performance':True,'all_results_correct':True,'calls':count,'seconds_requested':20},indent=2))
  finally:
   if process.poll() is None:os.kill(ready['pid'] if 'ready' in locals() else process.pid,signal.SIGTERM);process.wait(timeout=20)
   assert process.returncode==0,process.returncode
 env['PATH']='/data/go/bin:'+env.get('PATH','');env['GOTOOLCHAIN']='local';env['GOCACHE']='/root/.cache/go-build';env['GOPATH']='/root/go'
 for args,name in [(['-top','-nodecount=40'],'cpu-top.txt'),(['-top','-cum','-nodecount=40'],'cpu-cumulative.txt')]:
  with (prof/name).open('wb') as out:subprocess.run(['/data/go/bin/go','tool','pprof',*args,str(binary),str(prof/'cpu.pb.gz')],stdout=out,stderr=subprocess.STDOUT,check=True,env=env,timeout=120)
 state.update(complete=True,phase='complete');save()
except BaseException as exc:
 state.update(phase='failed',error=repr(exc),traceback=traceback.format_exc());save();raise
