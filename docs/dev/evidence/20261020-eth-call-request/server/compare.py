import collections, contextlib, ctypes, datetime, hashlib, json, mmap, os, pathlib, re, shutil, signal, statistics, subprocess, threading, time, traceback, urllib.request

ROOT=pathlib.Path('/data/gtron/eth-call-request-20261020-04')
OLD=pathlib.Path('/data/gtron/eth-call-fixed-20261020-01')
INPUT=OLD/'capture-1'
ADDR='0xa4997124a328a4615eab437cede31da47a31b375'
state={'complete':False,'production_mutations':False,'phase':'waiting-for-audit'}
results=[]; sources={}; specs=[]
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat()
 (ROOT/'comparison-status.json').write_text(json.dumps(state,indent=2))
def identity():
 pid=int(subprocess.check_output(['systemctl','show','gtron','-p','MainPID']).decode().split('=')[1])
 return {'pid':pid,'sha256':hashlib.sha256(pathlib.Path(f'/proc/{pid}/exe').read_bytes()).hexdigest()}
def waitfor(predicate,seconds):
 start=time.monotonic()
 while not predicate():
  if time.monotonic()-start>seconds:raise TimeoutError('bounded wait exhausted')
  time.sleep(1)
def request(url,method,params,id_,timeout=90):
 body=json.dumps({'jsonrpc':'2.0','id':id_,'method':method,'params':params}).encode()
 req=urllib.request.Request(url,body,{'Content-Type':'application/json'})
 with urllib.request.urlopen(req,timeout=timeout) as r:return json.loads(r.read())
@contextlib.contextmanager
def host(mode,name,input_=INPUT,trace=False):
 out=ROOT/name;out.mkdir()
 env=dict(os.environ,GTRON_FIXED_INPUT=str(input_),GTRON_FIXED_OUTPUT=str(out),GOMAXPROCS='2',GOMEMLIMIT='3GiB')
 command=[str(ROOT/(mode+'.test')),'-test.run=^TestOfflineFixedEthCallHTTP$','-test.v','-test.timeout=32m']
 if trace:command=['strace','-f','-yy','-e','trace=read,pread64','-o',str(out/'read.trace')]+command
 with (out/'host.log').open('wb') as log:
  process=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT,env=env,start_new_session=True)
  try:
   waitfor(lambda:(out/'ready.json').exists() or process.poll() is not None,120)
   if process.poll() is not None:raise RuntimeError('host startup '+name+' '+(out/'host.log').read_text()[-4000:])
   ready=json.loads((out/'ready.json').read_text())
   if state.get('head') is None:state['head']=ready;save()
   for key in ('head','head_tx','head_hash','root','manifest_generation'):
    assert ready[key]==state['head'][key],(name,key,ready[key],state['head'][key])
   yield ready,out
  finally:
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
def evict_private(dest,private):
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
 assert fraction<0.01,proof
 return proof
try:
 state['before_identity']=identity();save()
 waitfor(lambda:(ROOT/'prepare-status.json').exists() and json.loads((ROOT/'prepare-status.json').read_text()).get('phase') in ('ready','failed'),2400)
 assert json.loads((ROOT/'prepare-status.json').read_text())['complete']
 census=json.loads((ROOT/'census.json').read_text());assert census['complete'] and census['muhash_verified'],census
 state['admission']={'complete':True,'muhash_root':census['muhash_root'],'rows':census['source_rows']};save()
 # Discovery is excluded from the performance table. This trace also records
 # the exact input files read by startup and all selected queries.
 read_set=set(json.loads((OLD/'read-files.json').read_text()))
 for mode in ('baseline','candidate'):
  state['phase']='discovery-'+mode;save()
  with host(mode,'discovery-'+mode,trace=(mode=='candidate')) as (ready,out):
   heights=[3144020,3144021,5000000,9000000,ready['head']-256,ready['head']-1]
   if not specs:
    for height in heights:
     tag=hex(height);slot=request(ready['url'],'eth_getStorageAt',[ADDR,'0x0',tag],'reference-'+tag)
     assert 'error' not in slot,slot
     if height in (3144020,3144021):assert int(slot['result'],16)=={3144020:6,3144021:7}[height]
     expected={r['height']:r['expected'] for r in json.loads((ROOT/'oracle.json').read_text())['expected']}
     assert slot['result']==expected[height]
     specs.append({'height':height,'tag':tag,'expected':slot['result'],'reference':'independent fixed before-image oracle, rechecked by baseline storage'})
    slot=request(ready['url'],'eth_getStorageAt',[ADDR,'0x0','latest'],'reference-latest')
    assert 'error' not in slot and slot['result']==expected[ready['head']];specs.append({'height':ready['head'],'tag':'latest','expected':slot['result'],'reference':'root-verified live storage in fixed input'})
    (ROOT/'specs.json').write_text(json.dumps(specs,indent=2))
   for spec in specs:call(mode,ready,spec,'discovery-not-benchmark',0)
  if mode=='candidate':read_set|=traced_paths(out/'read.trace')
 (ROOT/'read-files.json').write_text(json.dumps(sorted(read_set),indent=2))
 assert read_set and any(s.startswith('history/') for s in read_set)
 # Warm A/B then B/A blocks. First pass is excluded warm-up; five measured
 # requests per height/version, not enough samples for a tail percentile.
 for block,(order,count) in enumerate(((('baseline','candidate'),3),(('candidate','baseline'),2))):
  for mode in order:
   state['phase']=f'warm-{block}-{mode}';save()
   with host(mode,f'warm-{block}-{mode}') as (ready,out):
    for spec in specs:call(mode,ready,spec,'warmup-excluded',block)
    for repeat in range(count):
     for spec in specs:call(mode,ready,spec,'warm',block*3+repeat)
 # A fresh host for each height/version; OS page cache left untouched.
 for repeat in range(3):
  for index,spec in enumerate(specs):
   for mode in (('baseline','candidate') if (repeat+index)%2==0 else ('candidate','baseline')):
    state['phase']=f'process-first-{repeat}-{spec["tag"]}-{mode}';save()
    with host(mode,f'first-{repeat}-{index}-{mode}') as (ready,out):call(mode,ready,spec,'fresh-process-os-uncontrolled',repeat)
 dest,private=independent_copy(read_set)
 assert read_set<=private
 # Cold OS-cache runs use fresh processes and only privately copied inodes.
 for repeat in range(2):
  for index,spec in enumerate(specs):
   for mode in (('baseline','candidate') if (repeat+index)%2==0 else ('candidate','baseline')):
    state['phase']=f'private-cold-{repeat}-{spec["tag"]}-{mode}';save()
    proof=evict_private(dest,private)
    name=f'os-cold-{repeat}-{index}-{mode}'
    (ROOT/(name+'-residency.json')).write_text(json.dumps(proof))
    with host(mode,name,input_=dest) as (ready,out):call(mode,ready,spec,'private-os-cold-fresh-process',repeat)
 # Independent profile workload, never pooled with latency observations.
 for mode in ('baseline','candidate'):
  state['phase']='profile-'+mode;save()
  with host(mode,'profile-'+mode,input_=dest) as (ready,out):
   for spec in specs:call(mode,ready,spec,'profile-warmup-excluded',0)
   errors=[]
   def profile():
    try:
     with urllib.request.urlopen(ready['url']+'debug/pprof/profile?seconds=20',timeout=30) as response:(out/'cpu.pb.gz').write_bytes(response.read())
    except Exception as exc:errors.append(repr(exc))
   thread=threading.Thread(target=profile);thread.start()
   for repeat in range(3):
    for spec in specs:call(mode,ready,spec,'profile-workload-excluded',repeat)
   thread.join();assert not errors,errors
 state['after_identity']=identity();assert state['before_identity']==state['after_identity']
 grouped=collections.defaultdict(list)
 for row in results:
  if row['cache'] in ('warm','fresh-process-os-uncontrolled','private-os-cold-fresh-process'):grouped[(row['mode'],row['cache'],row['tag'])].append(row['milliseconds'])
 summary=[{'mode':mode,'cache':cache,'tag':tag,'n':len(values),'median_ms':statistics.median(values),'min_ms':min(values),'max_ms':max(values)} for (mode,cache,tag),values in sorted(grouped.items())]
 (ROOT/'summary.json').write_text(json.dumps(summary,indent=2));state.update(complete=True,phase='compared',all_responses_correct=all(r['correct'] for r in results),requests=len(results));save()
except BaseException as exc:
 state.update(phase='failed',error=repr(exc),traceback=traceback.format_exc());save();raise
