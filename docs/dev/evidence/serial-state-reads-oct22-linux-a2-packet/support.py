"""Explicitly invoked inactive Linux evaluation helpers; Python3.6 compatible."""
import os,sys,json,time,hashlib,subprocess,signal,fcntl
from pathlib import Path
PACKET=Path(__file__).resolve().parent
GO='/data/go/bin/go'
PYTHON='/usr/bin/python3'
START_LOCK='/data/gtron/start.lock'
CPU=15
OBSERVER_CPU=0
ENV=dict(os.environ)
ENV.update({'GOMAXPROCS':'1','GOGC':'100','GOMEMLIMIT':'4GiB','CGO_ENABLED':'1','GOOS':'linux','GOARCH':'amd64','GOFLAGS':'','GOWORK':'off','GOPROXY':'off','GOTOOLCHAIN':'local','GOCACHE':'/home/java-tron/.cache/go-build','GOMODCACHE':'/home/java-tron/go/pkg/mod'})
def sha(path):
 h=hashlib.sha256()
 with open(str(path),'rb') as f:
  for b in iter(lambda:f.read(1048576),b''):h.update(b)
 return h.hexdigest()
def write_json(path,data):
 Path(path).write_text(json.dumps(data,indent=2,sort_keys=True)+'\n')
def read_json(path):return json.loads(Path(path).read_text())
def plain(cmd,cwd=None):return subprocess.check_output(cmd,cwd=str(cwd) if cwd else None,stderr=subprocess.STDOUT).decode('utf-8','replace').strip()
def own_start(fd):
 expected=os.stat(START_LOCK);got=os.fstat(fd)
 assert (got.st_dev,got.st_ino)==(expected.st_dev,expected.st_ino),'wrong inherited start-lock FD'
 fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
 # Descriptor is inherited from the audited outer shell; never unlock it here.
def require_linux(fd):
 assert sys.version_info[:3]==(3,6,8) and Path(sys.executable).resolve()==Path('/usr/bin/python3').resolve(),'Python pin mismatch'
 assert sys.platform=='linux' and sorted(os.sched_getaffinity(0)) and CPU in os.sched_getaffinity(0)
 own_start(fd);os.sched_setaffinity(0,{OBSERVER_CPU})
 pins=read_json(PACKET/'toolchain-pins.json')
 for name in ['go','sapling','header']:assert sha(pins[name]['path'])==pins[name]['sha256'],name+' pin mismatch'
 assert plain([GO,'version'])=='go version go1.25.5 linux/amd64'
 return pins

def node_pid():
 s=plain(['systemctl','show','gtron.service','-p','MainPID']);return int(s.split('=',1)[1])
def node_identity():
 pid=node_pid();p=Path('/proc')/str(pid)
 return {'pid':pid,'exe':os.readlink(str(p/'exe')),'start_ticks':(p/'stat').read_text().rsplit(') ',1)[1].split()[19]}
def resources(node):
 mem={l.split(':',1)[0]:int(l.split()[1])*1024 for l in Path('/proc/meminfo').read_text().splitlines() if l.startswith(('MemTotal:','MemAvailable:'))}
 def proc(name):
  try:return (Path('/proc')/str(node['pid'])/name).read_text()
  except OSError:return None
 df={}
 for path in ['/','/data']:
  v=os.statvfs(path);df[path]={'available_bytes':v.f_bavail*v.f_frsize,'total_bytes':v.f_blocks*v.f_frsize}
 try:live=node_identity()
 except Exception as e:live={'error':str(e)}
 cgroups={}
 rawgroup=proc('cgroup') or ''
 for line in rawgroup.splitlines():
  _,controllers,relative=line.split(':',2)
  if 'memory' in controllers.split(','):
   for leaf in ['memory.usage_in_bytes','memory.limit_in_bytes']:
    path=Path('/sys/fs/cgroup/memory')/relative.lstrip('/')/leaf
    try:cgroups[str(path)]=path.read_text()
    except OSError:cgroups[str(path)]=None
 return {'cgroup_memory':cgroups,'utc_unix':time.time(),'monotonic':time.monotonic(),'mem':mem,'df':df,'cpu_stat':Path('/proc/stat').read_text(),'diskstats':Path('/proc/diskstats').read_text(),'loadavg':Path('/proc/loadavg').read_text(),'node_identity':live,'node_status':proc('status'),'node_io':proc('io'),'node_cgroup':proc('cgroup')}
def pressure(row,node,initial=False):
 limit=(12 if initial else 8)*1024**3
 return row['mem']['MemAvailable']<limit or row['df']['/']['available_bytes']<8*1024**3 or row['df']['/data']['available_bytes']<32*1024**3 or row['node_identity']!=node

def source_index(root):
 root=Path(root);result={}
 for base,dirs,files in os.walk(str(root)):
  dirs[:]=[d for d in dirs if d!='.git']
  for name in files:
   p=Path(base)/name;rel=str(p.relative_to(root))
   if rel=='third_party/librustzcash/target/release/librustzcash.a':continue
   result[rel]=sha(p)
 return result

def run_job(out,node,cmd,cwd,label,metadata=None,timeout=900):
 out=Path(out);log=out/(label+'.log');started=time.time();env=dict(ENV);env['TMPDIR']=str(out/'tmp')
 assert not log.exists(),'no retry of '+label
 full=['taskset','-c',str(CPU)]+list(cmd)
 p=None;aborted=None;failure=None;code=None
 def stop_child():
  if p is None:return
  if p.poll() is None:
   try:os.killpg(p.pid,signal.SIGTERM)
   except ProcessLookupError:pass
   try:p.wait(timeout=10)
   except subprocess.TimeoutExpired:
    try:os.killpg(p.pid,signal.SIGKILL)
    except ProcessLookupError:pass
    p.wait()
  else:p.wait()
 try:
  with log.open('wb') as f, (out/'resources.jsonl').open('a') as observations:
   row=resources(node)
   if pressure(row,node):raise RuntimeError('resource/identity precondition failed')
   p=subprocess.Popen(full,cwd=str(cwd),env=env,stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
   while True:
    row=resources(node);row.update({'command_label':label,'child_pid':p.pid,'metadata':metadata or {}})
    try:row['child_status']=(Path('/proc')/str(p.pid)/'status').read_text()
    except OSError:row['child_status']=None
    observations.write(json.dumps(row,sort_keys=True)+'\n');observations.flush()
    if pressure(row,node):aborted='resource/identity change'
    if time.time()-started>timeout:aborted='fixed command timeout'
    if aborted:raise RuntimeError(aborted)
    code=p.poll()
    if code is not None:break
    try:p.wait(timeout=5)
    except subprocess.TimeoutExpired:pass
 except BaseException as e:
  failure=e
  if aborted is None:aborted='runner/observer exception: '+type(e).__name__+': '+str(e)
 finally:
  # Every path after Popen, including observer JSON/IO error and interruption,
  # terminates and reaps only this independently sessioned evaluation child.
  stop_child()
  if p is not None:code=p.returncode
 result={'command':full,'cwd':str(cwd),'label':label,'exit':code,'aborted':aborted,'start_unix':started,'elapsed_sec':time.time()-started,'log':str(log),'sha256':sha(log) if log.exists() else None}
 if metadata:result.update(metadata)
 try:
  with (out/'commands.jsonl').open('a') as f:f.write(json.dumps(result,sort_keys=True)+'\n')
 except BaseException as e:
  if failure is None:failure=e
  print('FAILED to persist command metadata after child cleanup: '+str(e),file=sys.stderr,flush=True)
 print(json.dumps(result,sort_keys=True),flush=True)
 if failure is not None:raise failure
 if code or aborted:raise RuntimeError('one-attempt evaluation failed: '+label)
 return result
