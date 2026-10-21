import collections, contextlib, ctypes, datetime, hashlib, json, mmap, os, pathlib, re, shutil, signal, statistics, subprocess, threading, time, traceback, urllib.request

import sys
OLDLAB=pathlib.Path('/data/gtron/cold-query-layout-20261020-01')
LAB=OLDLAB/'tail-read-final-20261021-02'
BASE=pathlib.Path('/data/gtron/eth-call-request-20261020-04')
PHASE=sys.argv[1]
assert PHASE == 'tail-read'
ROOT=LAB/(PHASE+'-comparison');ROOT.mkdir(exist_ok=False)

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
def request(url,method,params,id_,timeout=150):
 body=json.dumps({'jsonrpc':'2.0','id':id_,'method':method,'params':params}).encode()
 req=urllib.request.Request(url,body,{'Content-Type':'application/json'})
 with urllib.request.urlopen(req,timeout=timeout) as r:return json.loads(r.read())
@contextlib.contextmanager
def host(mode,name,input_=INPUT,trace=False):
 out=ROOT/name;out.mkdir()
 env=dict(os.environ,GTRON_FIXED_INPUT=str(input_),GTRON_FIXED_OUTPUT=str(out),GOMAXPROCS='2',GOMEMLIMIT='3GiB')
 command=[str(LAB/'candidate.test' if mode=='candidate' else OLDLAB/'lazy-final-candidate.test'),'-test.run=^TestOfflineFixedEthCallHTTP$','-test.v','-test.timeout=32m']
 if trace:command=['strace','-f','-yy','-e','trace=read,pread64','-o',str(out/'read.trace')]+command
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
def replica(source,target):
 target.mkdir()
 for directory,dirs,files in os.walk(source):
  rel=pathlib.Path(directory).relative_to(source);(target/rel).mkdir(exist_ok=True)
  for name in files:
   if name in ('LOCK','FLOCK'):continue
   a=pathlib.Path(directory)/name;b=target/rel/name
   if a.suffix in ('.sst','.seg','.idx','.kv','.route') or str(rel).startswith('ancient'):os.link(a,b)
   else:shutil.copy2(a,b)
def paths_from_trace(trace,input_):
 found=set()
 with trace.open(errors='replace') as f:
  for line in f:
   if not re.search(r'= [1-9][0-9]*\s*$',line):continue
   for item in re.findall(r'<('+re.escape(str(input_))+r'/[^>]+)>',line):
    p=pathlib.Path(item)
    if p.is_file():found.add(str(p.relative_to(input_)))
 return found

def private_input(mode,readset):
 source=inputs[mode];target=ROOT/(mode+'-cache-input');replica(source,target)
 oldroot=OLDLAB/'tail-read-20261021-01/tail-read-comparison/candidate-cache-input'
 oldproof={r['path']:r for r in json.loads((OLDLAB/'tail-read-20261021-01/tail-read-comparison/candidate-private-files.json').read_text())}
 manifest=[]
 for relative in sorted(readset):
  current=source/relative;dst=target/relative;old=oldproof.get(relative);oldfile=oldroot/relative
  a=current.stat();reusable=old and oldfile.exists() and old.get('source_inode')==a.st_ino and old['bytes']==a.st_size and old['private_inode']==oldfile.stat().st_ino
  tmp=dst.with_name(dst.name+'.independent');tmp.parent.mkdir(parents=True,exist_ok=True)
  if reusable:os.link(oldfile,tmp)
  else:
   with current.open('rb') as inp,tmp.open('xb') as out:
    shutil.copyfileobj(inp,out,4<<20);out.flush();os.fsync(out.fileno())
  os.replace(tmp,dst);b=dst.stat();assert a.st_size==b.st_size and (a.st_dev,a.st_ino)!=(b.st_dev,b.st_ino)
  original=INPUT/relative
  if original.exists():assert (b.st_dev,b.st_ino)!=(original.stat().st_dev,original.stat().st_ino)
  manifest.append({'path':relative,'bytes':b.st_size,'source_inode':a.st_ino,'private_inode':b.st_ino,'source':str(current),'reused_proven_private_inode':bool(reusable)})
 (ROOT/(mode+'-private-files.json')).write_text(json.dumps(manifest,indent=2));return target,set(readset)

try:
 state['before_identity']=identity();save()
 def prepared():
  p=LAB/'layout-status.json'
  if not p.exists():return False
  x=json.loads(p.read_text())
  if x.get('phase')=='failed':raise RuntimeError('final source preparation failed')
  return x.get('complete') and subprocess.check_output(['systemctl','show','gtron-tail-final-layout-20261021.service','-p','ActiveState']).decode().strip()=='ActiveState=inactive'
 waitfor(prepared,2400)
 state['baseline_binary_sha256']=hashlib.sha256((OLDLAB/'lazy-final-candidate.test').read_bytes()).hexdigest();state['candidate_binary_sha256']=hashlib.sha256((LAB/'candidate.test').read_bytes()).hexdigest();state['query_code']='0b370448-versus-4d34e751-planned-tail-and-v2-storage-route';save()
 inputs={'baseline':OLDLAB/'account-gc-input','candidate':LAB/'storage-route-input'}
 specs=json.loads((BASE/'specs.json').read_text());(ROOT/'specs.json').write_text(json.dumps(specs,indent=2))
 readsets={}
 for mode in ('baseline','candidate'):
  state['phase']='discovery-'+mode;save()
  with host(mode,'discovery-'+mode,input_=inputs[mode],trace=True) as (ready,directory):
   for spec in specs:call(mode,ready,spec,'discovery-excluded',0)
  readsets[mode]=paths_from_trace(directory/'read.trace',inputs[mode]);assert readsets[mode]
  (ROOT/(mode+'-read-files.json')).write_text(json.dumps(sorted(readsets[mode]),indent=2))
 for block,order in enumerate((('baseline','candidate'),('candidate','baseline'))):
  for mode in order:
   state['phase']='warm-'+str(block)+'-'+mode;save()
   with host(mode,'warm-'+str(block)+'-'+mode,input_=inputs[mode]) as (ready,directory):
    for spec in specs:call(mode,ready,spec,'warmup-excluded',block)
    for repeat in range(2):
     for spec in specs:call(mode,ready,spec,'warm',2*block+repeat)
 for repeat in range(2):
  for index,spec in enumerate(specs):
   for mode in (('baseline','candidate') if (repeat+index)%2==0 else ('candidate','baseline')):
    state['phase']='first-'+str(repeat)+'-'+str(index)+'-'+mode;save()
    with host(mode,'first-'+str(repeat)+'-'+str(index)+'-'+mode,input_=inputs[mode]) as (ready,directory):call(mode,ready,spec,'fresh-process-os-uncontrolled',repeat)
 cache={};private={}
 for mode in ('baseline','candidate'):cache[mode],private[mode]=private_input(mode,readsets[mode])
 for repeat in range(2):
  for index,spec in enumerate(specs):
   for mode in (('baseline','candidate') if (repeat+index)%2==0 else ('candidate','baseline')):
    state['phase']='os-cold-'+str(repeat)+'-'+str(index)+'-'+mode;save()
    proof=evict_private(cache[mode],private[mode])
    name='os-cold-'+str(repeat)+'-'+str(index)+'-'+mode
    (ROOT/(name+'-residency.json')).write_text(json.dumps(proof))
    with host(mode,name,input_=cache[mode]) as (ready,directory):call(mode,ready,spec,'private-os-cold-fresh-process',repeat)
 grouped=collections.defaultdict(list)
 for row in results:
  if row['cache'] in ('warm','fresh-process-os-uncontrolled','private-os-cold-fresh-process'):grouped[(row['mode'],row['cache'],row['tag'])].append(row['milliseconds'])
 summary=[{'mode':a,'cache':b,'tag':c,'n':len(v),'median_ms':statistics.median(v),'min_ms':min(v),'max_ms':max(v)} for (a,b,c),v in sorted(grouped.items())]
 (ROOT/'summary.json').write_text(json.dumps(summary,indent=2));state['after_identity']=identity();assert state['before_identity']==state['after_identity'];state.update(complete=True,phase='complete',requests=len(results),all_responses_correct=all(r['correct'] for r in results));save()
except BaseException as exc:
 state.update(phase='failed',error=repr(exc),traceback=traceback.format_exc());save();raise
