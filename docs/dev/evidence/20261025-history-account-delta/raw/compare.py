import collections, contextlib, ctypes, datetime, hashlib, json, mmap, os, pathlib, re, shutil, signal, statistics, subprocess, threading, time, traceback, urllib.request

import sys
OLDLAB=pathlib.Path('/data/gtron/cold-query-layout-20261020-01')
LAB=pathlib.Path('/data/gtron/history-account-delta-20261025-01')
BASE=pathlib.Path('/data/gtron/eth-call-request-20261020-04')
PHASE=sys.argv[1]
assert PHASE == 'tail-read'
ROOT=LAB/(PHASE+'-comparison');ROOT.mkdir(exist_ok=False)

OLD=pathlib.Path('/data/gtron/eth-call-fixed-20261020-01')
INPUT=OLD/'capture-1'
ADDR='0xa4997124a328a4615eab437cede31da47a31b375'
state={'complete':False,'production_mutations':False,'phase':'waiting-for-audit'}
results=[]; sources={}; specs=[]; production_binary_cache={}
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat()
 p=ROOT/'comparison-status.json';tmp=p.with_suffix('.tmp');tmp.write_text(json.dumps(state,indent=2));tmp.replace(p)
def identity():
 fields=dict(line.split('=',1) for line in subprocess.check_output(['systemctl','show','gtron','-p','MainPID','-p','ActiveState']).decode().splitlines())
 pid=int(fields['MainPID']);start=pathlib.Path('/proc/%d/stat'%pid).read_text().split(') ',1)[1].split()[19] if pid else None
 key=(pid,start)
 if pid and key not in production_binary_cache:production_binary_cache[key]=hashlib.sha256(pathlib.Path('/proc/%d/exe'%pid).read_bytes()).hexdigest()
 return {'pid':pid,'sha256':production_binary_cache.get(key),'active_state':fields['ActiveState'],'process_start_ticks':start}
def waitfor(predicate,seconds):
 start=time.monotonic()
 while not predicate():
  if time.monotonic()-start>seconds:raise TimeoutError('bounded wait exhausted')
  time.sleep(1)
def request(url,method,params,id_,timeout=150):
 body=json.dumps({'jsonrpc':'2.0','id':id_,'method':method,'params':params}).encode()
 req=urllib.request.Request(url,body,{'Content-Type':'application/json'})
 with urllib.request.urlopen(req,timeout=timeout) as r:return json.loads(r.read())
@contextlib.contextmanager
def host(mode,name,input_=INPUT,trace=False):
 out=ROOT/name;out.mkdir();environment={'production_start':identity(),'loadavg_start':pathlib.Path('/proc/loadavg').read_text(),'meminfo_start':pathlib.Path('/proc/meminfo').read_text(),'utc_start':datetime.datetime.now(datetime.timezone.utc).isoformat()}
 env=dict(os.environ,GOCACHE='/root/.cache/go-build',GTRON_FIXED_INPUT=str(input_),GTRON_FIXED_OUTPUT=str(out),GOMAXPROCS='2',GOMEMLIMIT='3GiB')
 command=[str(LAB/(mode+'.test')),'-test.run=^TestOfflineFixedEthCallHTTP$','-test.v','-test.timeout=32m']
 if trace:command=['strace','-f','-yy','-e','trace=read,pread64,mmap','-o',str(out/'read.trace')]+command
 with (out/'host.log').open('wb') as log:
  process=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT,env=env,start_new_session=True)
  try:
   waitfor(lambda:(out/'ready.json').exists() or process.poll() is not None,120)
   if process.poll() is not None:raise RuntimeError('host startup '+name+' '+(out/'host.log').read_text()[-4000:])
   ready=json.loads((out/'ready.json').read_text())
   if state.get('head') is None:state['head']=ready;save()
   for key in ('head','head_tx','head_hash','root'):
    assert ready[key]==state['head'][key],(name,key,ready[key],state['head'][key])
   yield ready,out
  finally:
   if 'ready' in locals():
    proc=pathlib.Path('/proc/%d'%ready['pid']);resources={'pid':ready['pid'],'source':'procfs after workload, before stopping this isolated host'}
    try:
     resources['status']={line.split(':')[0]:line.split(':',1)[1].strip() for line in (proc/'status').read_text().splitlines() if line.startswith(('VmRSS:','VmHWM:','VmSwap:','Threads:'))}
     resources['io']=(proc/'io').read_text();resources['stat']=(proc/'stat').read_text()
    except FileNotFoundError:resources['process_disappeared']=True
    (out/'process-resources.json').write_text(json.dumps(resources,indent=2))
    environment.update(production_end=identity(),loadavg_end=pathlib.Path('/proc/loadavg').read_text(),meminfo_end=pathlib.Path('/proc/meminfo').read_text(),utc_end=datetime.datetime.now(datetime.timezone.utc).isoformat());(out/'environment.json').write_text(json.dumps(environment,indent=2))
   if process.poll() is None:
    os.kill(ready['pid'] if 'ready' in locals() else process.pid,signal.SIGTERM)
    try:process.wait(timeout=20)
    except subprocess.TimeoutExpired:os.killpg(process.pid,signal.SIGKILL);process.wait()
   if process.returncode!=0:raise RuntimeError('host exit '+name+' '+str(process.returncode)+' '+(out/'host.log').read_text()[-4000:])
def call(mode,ready,spec,cache,repeat):
 id_=len(results)+1;start=time.perf_counter()
 row={'id':id_,'mode':mode,'cache':cache,'repeat':repeat,**spec}
 try:
  response=request(ready['url'],'eth_call',[{'to':ADDR,'data':'0x9fa6a6e3'},spec['tag']],id_)
  row['milliseconds']=(time.perf_counter()-start)*1000;row['response']=response
  row['correct']='error' not in response and response.get('result')==spec['expected']
 except Exception as exc:row.update(milliseconds=(time.perf_counter()-start)*1000,error=repr(exc),correct=False)
 results.append(row)
 with (ROOT/'requests.jsonl').open('a') as file:file.write(json.dumps(row)+'\n')
 state['last_result']={k:v for k,v in row.items() if k!='response'};save()
 if not row['correct']:raise RuntimeError('wrong result or incomplete HTTP request '+json.dumps(row))
 return row
def traced_paths(path):
 found=set()
 with path.open(errors='replace') as file:
  for line in file:
   if not re.search(r'= [1-9][0-9]*\s*$',line):continue
   for item in re.findall(r'<('+re.escape(str(INPUT))+r'/[^>]+)>',line):
    file_=pathlib.Path(item)
    if file_.is_file():found.add(str(file_.relative_to(INPUT)))
 return found
def independent_copy(read_set):
 # Reuse only prior lab-private inodes; hard-linking these avoids copying
 # another 157 GB. New read paths still get independent copies before advice.
 dest=ROOT/'cache-input';dest.mkdir()
 for directory,dirs,files in os.walk(OLD/'cache-input'):
  rel=pathlib.Path(directory).relative_to(OLD/'cache-input');(dest/rel).mkdir(exist_ok=True)
  for name in files:
   if name not in ('LOCK','FLOCK'):os.link(pathlib.Path(directory)/name,dest/rel/name)
 manifest=json.loads((OLD/'private-files.json').read_text())
 private={item['path'] for item in manifest}
 for relative in sorted(read_set-private):
  source=INPUT/relative;target=dest/relative;target.parent.mkdir(parents=True,exist_ok=True)
  temp=target.with_name(target.name+'.private-copy');digest=hashlib.sha256()
  with source.open('rb') as inp,temp.open('xb') as out:
   while True:
    data=inp.read(4<<20)
    if not data:break
    out.write(data);digest.update(data)
   out.flush();os.fsync(out.fileno())
  os.replace(temp,target)
  manifest.append({'path':relative,'bytes':target.stat().st_size,'sha256':digest.hexdigest()})
 for row in manifest:
  a=(INPUT/row['path']).stat();b=(dest/row['path']).stat()
  assert (a.st_dev,a.st_ino)!=(b.st_dev,b.st_ino) and a.st_size==b.st_size==row['bytes']
 (ROOT/'private-files.json').write_text(json.dumps(manifest,indent=2))
 return dest,{item['path'] for item in manifest}
def evict_private_once(dest,private):
 # Never use drop_caches; every advised inode was independently copied.
 for relative in sorted(private):
  file=dest/relative
  with file.open('rb') as inp:os.posix_fadvise(inp.fileno(),0,0,os.POSIX_FADV_DONTNEED)
 libc=ctypes.CDLL(None,use_errno=True);resident=pages=0
 for relative in sorted(private):
  file=dest/relative;size=file.stat().st_size
  if not size:continue
  with file.open('rb') as inp:
   mapping=mmap.mmap(inp.fileno(),0,flags=mmap.MAP_PRIVATE,prot=mmap.PROT_READ|mmap.PROT_WRITE)
   address=ctypes.addressof(ctypes.c_char.from_buffer(mapping));count=(size+4095)//4096
   vector=(ctypes.c_ubyte*count)()
   rc=libc.mincore(ctypes.c_void_p(address),ctypes.c_size_t(size),vector)
   if rc:raise OSError(ctypes.get_errno(),'mincore')
   resident+=sum(value&1 for value in vector);pages+=count;mapping.close()
 fraction=resident/pages if pages else 0
 proof={'pages':pages,'resident_pages':resident,'fraction':fraction}
 return proof
def evict_private(dest,private):
 for attempt in range(8):
  proof=evict_private_once(dest,private);proof['attempt']=attempt+1
  if proof['fraction']<0.01:return proof
  time.sleep(0.1)
 raise RuntimeError('private cache residency above threshold '+json.dumps(proof))
def replica(source,target):
 target.mkdir()
 for directory,dirs,files in os.walk(source):
  rel=pathlib.Path(directory).relative_to(source);(target/rel).mkdir(exist_ok=True)
  for name in files:
   if name in ('LOCK','FLOCK'):continue
   a=pathlib.Path(directory)/name;b=target/rel/name
   if a.suffix in ('.sst','.seg','.idx','.kv','.route','.gtv2') or str(rel).startswith('ancient'):os.link(a,b)
   else:shutil.copy2(a,b)
def paths_from_trace(trace,input_):
 found=set()
 with trace.open(errors='replace') as f:
  for line in f:
   # Conservative union includes unfinished reads and file-backed mmap.
   for item in re.findall(r'<('+re.escape(str(input_))+r'/[^>]+)>',line):
    p=pathlib.Path(item).resolve()
    try:relative=p.relative_to(input_.resolve())
    except ValueError:continue
    if p.is_file():found.add(str(relative))
 return found

private_sha_cache={}
def stream_sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(4<<20),b''):h.update(b)
 return h.hexdigest()

def private_input(mode,readset):
 source=inputs[mode];target=ROOT/(mode+'-cache-input');replica(source,target)
 previous=pathlib.Path('/data/gtron/history-tx-runs-20261025-01/tail-read-comparison')
 prior={r['path']:r for r in json.loads((previous/'candidate-private-files.json').read_text())}
 prior_source=pathlib.Path('/data/gtron/history-tx-runs-20261025-01/candidate-input')
 prior_cache=previous/'candidate-cache-input'
 manifest=[]
 for relative in sorted(readset):
  current=source/relative;dst=target/relative;a=current.stat();old=prior.get(relative)
  reused=False
  if old and old['bytes']==a.st_size and (prior_source/relative).exists():
   original=(prior_source/relative).stat();private=(prior_cache/relative).stat()
   if (a.st_dev,a.st_ino)==(original.st_dev,original.st_ino) and a.st_ino==old['source_inode'] and private.st_ino==old['private_inode'] and private.st_size==a.st_size:
    assert (private.st_dev,private.st_ino)!=(a.st_dev,a.st_ino)
    temp=dst.with_name(dst.name+'.private-link');os.link(prior_cache/relative,temp);os.replace(temp,dst);reused=True
  if not reused:
   tmp=dst.with_name(dst.name+'.independent');tmp.parent.mkdir(parents=True,exist_ok=True)
   with current.open('rb') as inp,tmp.open('xb') as out:
    shutil.copyfileobj(inp,out,4<<20);out.flush();os.fsync(out.fileno())
   os.replace(tmp,dst)
  b=dst.stat();assert a.st_size==b.st_size and (a.st_dev,a.st_ino)!=(b.st_dev,b.st_ino)
  original=INPUT/relative
  if original.exists():assert (b.st_dev,b.st_ino)!=(original.stat().st_dev,original.stat().st_ino)
  key=(a.st_dev,a.st_ino,a.st_size,a.st_mtime_ns,b.st_dev,b.st_ino,b.st_size,b.st_mtime_ns)
  if key not in private_sha_cache:
   source_sha=stream_sha(current);private_sha=stream_sha(dst);assert source_sha==private_sha,(mode,relative,'private bytes differ')
   assert current.stat().st_mtime_ns==a.st_mtime_ns and dst.stat().st_mtime_ns==b.st_mtime_ns
   private_sha_cache[key]=source_sha
  manifest.append({'sha256':private_sha_cache[key],'path':relative,'bytes':b.st_size,'source_inode':a.st_ino,'private_inode':b.st_ino,'source':str(current),'reused_proven_private_inode':reused,'prior_manifest':str(previous/'candidate-private-files.json') if reused else None})
 (ROOT/(mode+'-private-files.json')).write_text(json.dumps(manifest,indent=2));return target,set(readset)

try:
 state['before_identity']=identity();save()
 def prepared():
  p=LAB/'layout-status.json'
  if not p.exists():return False
  x=json.loads(p.read_text())
  if x.get('phase')=='failed':raise RuntimeError('final source preparation failed')
  return x.get('complete') and True
 waitfor(prepared,21600)
 state['baseline_binary_sha256']=hashlib.sha256((LAB/'baseline.test').read_bytes()).hexdigest();state['candidate_binary_sha256']=hashlib.sha256((LAB/'candidate.test').read_bytes()).hexdigest();state['query_code']='V11-versus-account-delta-pages-decoded-byte-cache-budget-identical-canonical-state';save()
 inputs={'baseline':LAB/'baseline-input','candidate':LAB/'candidate-input'}
 specs=json.loads((BASE/'specs.json').read_text());(ROOT/'specs.json').write_text(json.dumps(specs,indent=2))
 readsets={}
 for mode in ('baseline','candidate'):
  readsets[mode]=set()
  for index,spec in enumerate(specs):
   state['phase']='discovery-'+mode+'-'+str(index);save()
   with host(mode,'discovery-'+mode+'-'+str(index),input_=inputs[mode],trace=True) as (ready,directory):call(mode,ready,spec,'discovery-excluded',0)
   readsets[mode].update(paths_from_trace(directory/'read.trace',inputs[mode]))
  assert readsets[mode]
  (ROOT/(mode+'-read-files.json')).write_text(json.dumps(sorted(readsets[mode]),indent=2))
 for block,order in enumerate((('baseline','candidate'),('candidate','baseline'))):
  for mode in order:
   state['phase']='warm-'+str(block)+'-'+mode;save()
   with host(mode,'warm-'+str(block)+'-'+mode,input_=inputs[mode]) as (ready,directory):
    for spec in specs:call(mode,ready,spec,'warmup-excluded',block)
    for repeat in range(5):
     for spec in specs:call(mode,ready,spec,'warm',5*block+repeat)
 for repeat in range(3):
  for index,spec in enumerate(specs):
   for mode in (('baseline','candidate') if (repeat+index)%2==0 else ('candidate','baseline')):
    state['phase']='first-'+str(repeat)+'-'+str(index)+'-'+mode;save()
    with host(mode,'first-'+str(repeat)+'-'+str(index)+'-'+mode,input_=inputs[mode]) as (ready,directory):call(mode,ready,spec,'fresh-process-os-uncontrolled',repeat)
 cache={};private={}
 for mode in ('baseline','candidate'):cache[mode],private[mode]=private_input(mode,readsets[mode])
 for repeat in range(4):
  for index,spec in enumerate(specs):
   for mode in (('baseline','candidate') if (repeat+index)%2==0 else ('candidate','baseline')):
    state['phase']='os-cold-'+str(repeat)+'-'+str(index)+'-'+mode;save()
    proof=evict_private(cache[mode],private[mode])
    name='os-cold-'+str(repeat)+'-'+str(index)+'-'+mode
    (ROOT/(name+'-residency.json')).write_text(json.dumps(proof))
    with host(mode,name,input_=cache[mode]) as (ready,directory):call(mode,ready,spec,'private-os-cold-fresh-process',repeat)
 # Diagnostics are excluded from latency distributions.
 oldest=specs[0]
 for mode in ('baseline','candidate'):
  with host(mode,'oldest-trace-'+mode,input_=inputs[mode],trace=True) as (ready,directory):call(mode,ready,oldest,'trace-excluded',0)
 nine=next(spec for spec in specs if spec['tag']==hex(9000000))
 for mode in ('baseline','candidate'):
  state['phase']='nine-million-trace-'+mode;save()
  with host(mode,'nine-million-trace-'+mode,input_=inputs[mode],trace=True) as (ready,directory):call(mode,ready,nine,'trace-excluded',0)
  state['phase']='nine-million-cpu-profile-'+mode;save()
  with host(mode,'nine-million-profile-'+mode,input_=inputs[mode]) as (ready,directory):
   call(mode,ready,nine,'profile-warmup-excluded',0)
   errors=[];count=[0]
   def workload():
    start=time.monotonic()
    try:
     while time.monotonic()-start<12:
      response=request(ready['url'],'eth_call',[{'to':ADDR,'data':'0x9fa6a6e3'},nine['tag']],999999)
      assert response.get('result')==nine['expected'] and 'error' not in response
      count[0]+=1
    except BaseException as exc:errors.append(repr(exc))
   thread=threading.Thread(target=workload);thread.start()
   with urllib.request.urlopen(ready['url']+'debug/pprof/profile?seconds=10',timeout=30) as response:(directory/'cpu.pprof').write_bytes(response.read())
   thread.join(timeout=30);assert not thread.is_alive() and not errors,(count,errors)
   (directory/'workload.json').write_text(json.dumps({'responses_correct':True,'requests':count[0]}))
   with (directory/'cpu-top.txt').open('wb') as out:subprocess.run(['/data/go/bin/go','tool','pprof','-top','-nodecount=35',str(LAB/(mode+'.test')),str(directory/'cpu.pprof')],stdout=out,stderr=subprocess.STDOUT,check=True,timeout=120,env=dict(os.environ,GOCACHE='/root/.cache/go-build',GOPATH='/root/go',GOTOOLCHAIN='local'))
 grouped=collections.defaultdict(list)
 for row in results:
  if row['cache'] in ('warm','fresh-process-os-uncontrolled','private-os-cold-fresh-process'):grouped[(row['mode'],row['cache'],row['tag'])].append(row['milliseconds'])
 summary=[{'mode':a,'cache':b,'tag':c,'n':len(v),'median_ms':statistics.median(v),'min_ms':min(v),'max_ms':max(v)} for (a,b,c),v in sorted(grouped.items())]
 (ROOT/'summary.json').write_text(json.dumps(summary,indent=2));state['after_identity']=identity();state['environment_identity_changed']=state['before_identity']!=state['after_identity'];state.update(complete=True,phase='complete',requests=len(results),all_responses_correct=all(r['correct'] for r in results));save()
except BaseException as exc:
 state.update(phase='failed',error=repr(exc),traceback=traceback.format_exc());save();raise
