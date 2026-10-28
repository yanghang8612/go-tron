import pathlib,json,subprocess,os,time,datetime,urllib.request,signal,traceback,hashlib,shutil,tarfile,sys
R=pathlib.Path('/data/gtron/maintenance/routing-bootstrap-20261028-01');A=pathlib.Path('/data/gtron/maintenance/full-state-acceptance-20261028-01');REPO='/data/gtron/go-tron';SOURCE=sys.argv[1];BASE='3e456e757cc452347781a167b7c175e317ee6b8d';s={'complete':False,'source':SOURCE,'base':BASE,'production_mutations':False,'stage':'prepare isolated source archives'};requests=[]
def save():s['utc']=datetime.datetime.utcnow().isoformat()+'Z';(R/'candidate-status.json').write_text(json.dumps(s,indent=2))
def git(*args):return subprocess.check_output(['git','-c','safe.directory='+REPO]+list(args),cwd=REPO)
def identity():
 v=dict(x.split('=',1) for x in subprocess.check_output(['systemctl','show','gtron','-p','MainPID','-p','ActiveState','-p','ExecMainStartTimestamp']).decode().splitlines());v['exe']=os.readlink('/proc/'+v['MainPID']+'/exe');return v
E=dict(os.environ,GOCACHE='/data/gtron/deploy-cache/go-build',GOPATH='/root/go',GOMODCACHE='/root/go/pkg/mod',GOTOOLCHAIN='local',CGO_ENABLED='1',CC='/opt/rh/devtoolset-11/root/usr/bin/gcc',CXX='/opt/rh/devtoolset-11/root/usr/bin/g++',GOMAXPROCS='2',GOMEMLIMIT='4GiB',GTRON_SNAPSHOT_MANIFEST_CACHE_BUDGET_BYTES='536870912',GTRON_PEBBLE_BOUNDED_POINT_READ='get',GTRON_ZKSNARK_PARAMS='/data/gtron/params')
def command(args,src,log,timeout=300):
 with log.open('wb') as f:subprocess.check_call(args,cwd=str(src),env=E,stdout=f,stderr=subprocess.STDOUT,timeout=timeout)
def archive_source(name,sha):
 src=R/name;src.mkdir(exist_ok=False);top=[x for x in git('ls-tree','--name-only',sha).decode().splitlines() if x not in ['docs']];p=R/(name+'.tar')
 with p.open('wb') as f:subprocess.check_call(['git','-c','safe.directory='+REPO,'archive',sha,'--']+top,stdout=f,cwd=REPO)
 with tarfile.open(str(p)) as t:t.extractall(str(src))
 fixture=git('show',SOURCE+':docs/dev/evidence/20261028-routing-bootstrap/harness/offline_build_test.go.txt');(src/'core/state/snapshots/offline_bootstrap_build_test.go').write_bytes(fixture)
 # The complete HTTP host is the unchanged fixed-cut harness used previously.
 host=A/'source-candidate/core/offline_full_http_test.go'
 if not host.exists():
  matches=[p for p in (A/'source-candidate/core').glob('*test.go') if b'func TestOfflineFullEthCallHTTP(' in p.read_bytes()];assert len(matches)==1,matches;host=matches[0]
 (src/'core/offline_full_http_test.go').write_bytes(host.read_bytes());return src
base_input=A/'http-performance-priority/baseline-input'
def clone_input(name):
 out=R/name;out.mkdir(exist_ok=False)
 for p in base_input.iterdir():
  if p.name=='state-snapshots':shutil.copytree(str(p),str(out/p.name),copy_function=os.link,symlinks=True)
  else:os.symlink(str(p),str(out/p.name),target_is_directory=p.is_dir())
 return out
expected={}
def http_case(name,exe,input_):
 out=R/name;out.mkdir(exist_ok=False);env=dict(E,GTRON_FIXED_INPUT=str(input_),GTRON_FIXED_OUTPUT=str(out));log=(out/'host.log').open('wb');proc=subprocess.Popen([str(exe),'-test.run=^TestOfflineFullEthCallHTTP$','-test.v','-test.timeout=10m'],cwd=str(R),env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True);ready=None
 try:
  end=time.monotonic()+180
  while not (out/'ready.json').exists():assert proc.poll() is None,(name,'host exited');assert time.monotonic()<end;time.sleep(.2)
  ready=json.loads((out/'ready.json').read_text());assert ready['head']==45836827 and ready['head_tx']==4240407293 and ready['root']=='4bb19b7a938ba6c865820ea86f5239235172f84923c350b04c4b0eb75743b627'
  for height in [9000000,35000000,45836826]:
   for repeat in range(3 if height==9000000 else 2):
    s['stage']=name+' HTTP '+str(height)+' '+str(repeat);save();q={'jsonrpc':'2.0','id':len(requests)+1,'method':'eth_call','params':[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},hex(height)]};start=time.perf_counter();req=urllib.request.Request(ready['url'],data=json.dumps(q).encode(),headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(req,timeout=180) as f:body=json.loads(f.read())
    row={'case':name,'height':height,'repeat':repeat,'cache':'fresh-process-os-cache-uncontrolled' if height==9000000 and repeat==0 else 'process-warm','seconds_loopback_http':time.perf_counter()-start,'response':body};requests.append(row);(R/'candidate-requests.json').write_text(json.dumps(requests,indent=2));assert 'error' not in body,row
    value=int(body['result'],16);assert value==expected.setdefault(height,value),row
    if height==9000000:assert value==207,row
 finally:
  if proc.poll() is None:
   os.kill(ready['pid'] if ready else proc.pid,signal.SIGTERM)
   try:proc.wait(20)
   except subprocess.TimeoutExpired:os.killpg(proc.pid,signal.SIGKILL);proc.wait()
  log.close();assert proc.returncode==0,(name,proc.returncode)
try:
 save();assert len(SOURCE)==40;assert git('rev-parse',SOURCE).decode().strip()==SOURCE;s['production_before']=identity();save()
 current=archive_source('source-current',BASE);candidate=archive_source('source-candidate',SOURCE)
 s['stage']='native source and proof tests';save();command(['/data/go/bin/go','test','-tags=sapling','./core/state/snapshots','-run','Test(HistoryRoutingBootstrap|HistoryAccessorKeySearch|SmallSystemHistoryRouting|MetadataHistoryRouting|SystemHistoryRoutingSelected)','-count=1','-timeout=180s'],candidate,R/'native-focused.log',240)
 for name,src in [('current',current),('candidate',candidate)]:
  s['stage']='compile '+name+' snapshots';save();command(['/data/go/bin/go','test','-tags=sapling','-c','./core/state/snapshots','-o',str(R/(name+'-snapshots.test'))],src,R/(name+'-build-snapshots.log'))
 s['stage']='compile candidate full HTTP';save();command(['/data/go/bin/go','test','-tags=sapling','-c','./core','-o',str(R/'candidate-http.test')],candidate,R/'candidate-build-http.log')
 s['stage']='bounded three-pass maintenance comparison';save()
 for name in ['current','candidate']:
  input_=clone_input(name+'-maintenance-input');out=R/(name+'-maintenance');out.mkdir();env=dict(E,GTRON_BOOTSTRAP_SNAPSHOTS=str(input_/'state-snapshots'),GTRON_BOOTSTRAP_OUTPUT=str(out))
  with (out/'build.log').open('wb') as f:subprocess.check_call([str(R/(name+'-snapshots.test')),'-test.run=^TestOfflineRoutingBootstrapBuild$','-test.v','-test.timeout=5m'],cwd=str(R),env=env,stdout=f,stderr=subprocess.STDOUT,timeout=360)
 # Keep controlled coverage separate from the effect of faster backfill.
 previous=pathlib.Path('/data/gtron/maintenance/history-routing-rollout-20261028-01/bootstrap-offline/bootstrap-http.test')
 http_case('current-partial',previous,base_input)
 http_case('candidate-partial',R/'candidate-http.test',base_input)
 http_case('candidate-bootstrap',R/'candidate-http.test',R/'candidate-maintenance-input')
 http_case('candidate-complete',R/'candidate-http.test',A/'http-performance-priority/candidate-input')
 s['production_after']=identity();assert s['production_before']==s['production_after'];s.update(complete=True,stage='fixed-cut HTTP and bounded maintenance comparison complete',requests=len(requests),all_returns_match=True);save()
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
