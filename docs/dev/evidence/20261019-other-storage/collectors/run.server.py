import datetime,hashlib,json,os,pathlib,shutil,subprocess,sys
root=pathlib.Path('/data/gtron/other-storage-distribution-20261019-0245')
source=root/'source';capture=pathlib.Path('/data/gtron/generation-census-20261018-1259')
def sha(p):
 h=hashlib.sha256()
 with open(p,'rb') as f:
  for b in iter(lambda:f.read(1<<20),b''):h.update(b)
 return h.hexdigest()
def write(p,v):
 with open(str(p),'x') as f:json.dump(v,f,indent=2);f.flush();os.fsync(f.fileno())
def proof():
 c=json.loads((capture/'capture.json').read_text())
 for row in c['captured']:
  p=capture/'chaindata'/row['name'];st=p.stat();assert st.st_size==row['bytes'],row['name']
  if row['method']=='hardlink-immutable-sst':assert (st.st_dev,st.st_ino)==(row['dev'],row['inode'])
  else:assert sha(p)==row['sha256'],row['name']
 return len(c['captured'])
def service():
 d=dict(x.split('=',1) for x in subprocess.check_output(['systemctl','show','gtron','-p','MainPID','-p','ActiveState'],universal_newlines=True).splitlines());d['exe']=os.readlink('/proc/'+d['MainPID']+'/exe');d['sha256']=sha('/proc/'+d['MainPID']+'/exe');return d
try:
 before=service();checked=proof();env=dict(os.environ,PATH='/data/go/bin:'+os.environ['PATH'],GOMAXPROCS='1',GOTOOLCHAIN='local',CGO_ENABLED='0',GOPATH='/root/go',GOCACHE='/root/.cache/go-build')
 if not source.exists():shutil.copytree('/data/gtron/generation-census-tool-20261018-1259/source',str(source),ignore=shutil.ignore_patterns('.git','build'))

 for f in (root/'cmd'/'other-storage-distribution').glob('*.go'):shutil.copy2(str(f),str(source/'cmd'/'other-storage-distribution'/f.name))
 for args in [['go','test','./cmd/other-storage-distribution','-count=1'],['go','build','-o',str(root/'profiler'),'./cmd/other-storage-distribution']]:subprocess.run(args,cwd=str(source),env=env,check=True)
 subprocess.run([str(root/'profiler'),'--db-dir',str(capture/'chaindata'),'--baseline',str(capture/'census.json'),'--out-dir',str(root/'result')],env=env,check=True)
 checkedAfter=proof();write(root/'consistency.json',{'complete':True,'source_files_verified_before':checked,'source_files_verified_after':checkedAfter,'service_before':before,'service_after':service(),'tool_sha256':sha(root/'profiler'),'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'production_modified':False});write(root/'completed.json',{'complete':True})
except BaseException as e:
 write(root/'failed.json',{'error':repr(e)});raise
