import pathlib,json,os,subprocess,hashlib,datetime,traceback,zipfile,pwd
A=pathlib.Path('/data/gtron/maintenance/9m-query-20261027-01');P=A/'remaining';P.mkdir(mode=0o700,exist_ok=True);s={'complete':False,'stage':'setup'}
def save():s['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(P/'status.json').write_text(json.dumps(s,indent=2))
try:
 save();assert json.loads((A/'final/status.json').read_text())['complete'];proof=json.loads((A/'final/private-files-proof.json').read_text())+json.loads((A/'final/private-extra-proof.json').read_text());cat=A/'final/private-candidate'
 for r in proof:
  p=cat/r['path'];st=p.stat();assert (st.st_dev,st.st_ino)==(r['private_dev'],r['private_ino']);assert (r['private_dev'],r['private_ino'])!=(r['source_dev'],r['source_ino']);f=os.open(str(p),os.O_RDONLY)
  try:os.posix_fadvise(f,0,0,os.POSIX_FADV_DONTNEED)
  finally:os.close(f)
 env=dict(os.environ,GTRON_NINEM_CATALOG=str(cat),GTRON_NINEM_OUTPUT=str(P),GTRON_NINEM_QUERIES=str(A/'probe/queries.json'),GTRON_NINEM_ITERATIONS='1',GTRON_NINEM_SCOPE='metadata',GOMAXPROCS='2',GOMEMLIMIT='3GiB',GOCACHE='/data/gtron/deploy-cache/go-build',GOPATH='/root/go',GOMODCACHE='/root/go/pkg/mod',GOTOOLCHAIN='local')
 cmd=['strace','-f','-ttt','-T','-yy','-e','trace=openat,read,pread64,close','-o',str(P/'strace.log'),str(A/'final/candidate.test'),'-test.run=^TestOfflineNineMColdQuery$','-test.v','-test.timeout=10m','-test.cpuprofile='+str(P/'cpu.pprof')];s['stage']='isolated metadata trace; excluded from performance';save()
 with (P/'run.log').open('wb') as f:subprocess.check_call(cmd,env=env,stdout=f,stderr=subprocess.STDOUT)
 assert json.loads((P/'rows.json').read_text())==[None]
 with (P/'cpu-focused.txt').open('wb') as f:subprocess.check_call(['/data/go/bin/go','tool','pprof','-top','-nodecount=50','-focus=FirstStateDomainChangesContext',str(A/'final/candidate.test'),str(P/'cpu.pprof')],env=env,stdout=f,stderr=subprocess.STDOUT)
 s.update(complete=True,stage='done',limits='Isolated metadata batch after private-file fadvise; residency not remeasured. Tracing/profiling overhead, not a performance sample. Private files only, no global cache drop.');save()
 dest=pathlib.Path('/tmp/AAA-gtron-9m-remaining-profile-20261027.zip');members={}
 with zipfile.ZipFile(str(dest),'w',zipfile.ZIP_DEFLATED) as z:
  for p in sorted(P.iterdir()):
   if p.is_file():b=p.read_bytes();members[p.name]={'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()};z.writestr(p.name,b)
  z.writestr('files-sha256.json',json.dumps(members,indent=2))
 u=pwd.getpwnam('java-tron');os.chown(str(dest),u.pw_uid,u.pw_gid);os.chmod(str(dest),0o644);(A/'remaining-evidence-seal.json').write_text(json.dumps({'file':str(dest),'bytes':dest.stat().st_size,'sha256':hashlib.sha256(dest.read_bytes()).hexdigest()},indent=2))
except BaseException as e:s.update(error=repr(e),traceback=traceback.format_exc());save();raise
