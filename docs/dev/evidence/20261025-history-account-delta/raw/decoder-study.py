import pathlib,json,hashlib,subprocess,datetime,traceback,time,os,re,collections
P=pathlib.Path('/data/gtron/history-account-delta-20261025-01');S=P/'candidate-source';state={'complete':False,'production_mutations':False}
def sha(f):
 h=hashlib.sha256()
 with f.open('rb')as x:
  for b in iter(lambda:x.read(4<<20),b''):h.update(b)
 return h.hexdigest()
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();f=P/'decoder-study-status.json';f.with_suffix('.tmp').write_text(json.dumps(state,indent=2));f.with_suffix('.tmp').replace(f)
def run(label,args,env,timeout):
 state['phase']=label;save()
 with (P/(label+'.log')).open('wb')as out:subprocess.run(args,cwd=str(S),env=env,stdout=out,stderr=subprocess.STDOUT,check=True,timeout=timeout)
try:
 state['phase']='waiting-all-heavy-jobs';save();start=time.monotonic()
 while True:
  d=json.loads((P/'postprocess-status.json').read_text())
  if d.get('phase')=='failed':raise RuntimeError(d.get('error'))
  if d.get('complete'):break
  if time.monotonic()-start>10800:raise TimeoutError('postprocess')
  time.sleep(4)
 assert json.loads((P/'acceptance-status.json').read_text())['complete']
 inv=json.loads((P/'final-source-inventory.json').read_text())
 for n,h in inv.items():assert sha(S/n)==h,n
 trace=P/'tail-read-comparison/nine-million-trace-candidate/read.trace';markers=0;pending={};ranges=collections.defaultdict(set)
 for line in trace.open(errors='replace'):
  if re.search(r'read\(\d+</proc/\d+/io>, "rchar:',line):markers+=1;continue
  if markers!=1:continue
  m=re.match(r'\s*(\d+)\s+(.*)',line)
  if not m:continue
  tid,body=m.groups()
  if '<unfinished ...>'in body:pending[tid]=body.split('<unfinished ...>')[0];continue
  if body.startswith('<... ')and' resumed>'in body:
   prefix=pending.pop(tid,None)
   if prefix is None:continue
   body=prefix+body.split(' resumed>',1)[1]
  m=re.match(r'pread64\(\d+<([^>]+)>,.*?,\s*\d+,\s*(\d+)\)\s*=\s*(\d+)\s*$',body)
  if not m:continue
  file,offset,count=m.groups();file=pathlib.Path(file)
  try:relative=file.relative_to(P/'candidate-input/state-snapshots')
  except ValueError:continue
  if file.suffix=='.seg'and int(count)>0:ranges[str(relative)].add((int(offset),int(count)))
 assert markers==2 and ranges,(markers,len(ranges))
 plan=[dict(Path=n,Reads=[dict(Offset=o,Bytes=b)for o,b in sorted(v)])for n,v in sorted(ranges.items())]
 (P/'decoder-plan.json').write_text(json.dumps(plan,indent=2))
 target=S/'core/state/snapshots/offline_account_delta_decoder_test.go';assert not target.exists();target.write_bytes((P/target.name).read_bytes());state['harness_sha256']=sha(target);save()
 env=dict(os.environ,PATH='/data/go/bin:'+os.environ.get('PATH',''),GOTOOLCHAIN='local',GOCACHE='/root/.cache/go-build',GOPATH='/root/go',GOMAXPROCS='2',GOMEMLIMIT='1GiB',CGO_ENABLED='1',GTRON_ACCOUNT_DELTA_ROOT=str(P))
 run('decoder-study-build',['/data/go/bin/go','test','-c','./core/state/snapshots','-o',str(P/'decoder-snapshots.test')],env,600)
 state['binary_sha256']=sha(P/'decoder-snapshots.test');save()
 run('decoder-ownership-fixture',[str(P/'decoder-snapshots.test'),'-test.run=^TestAccountDeltaDecoderStudyFrameOwnership$','-test.v','-test.timeout=60s'],env,80)
 run('decoder-study',[str(P/'decoder-snapshots.test'),'-test.run=^TestOfflineAccountDeltaDecoderCost$','-test.v','-test.timeout=180s'],env,200)
 assert json.loads((P/'decoder-cost.json').read_text())['complete']
 for n,h in inv.items():assert sha(S/n)==h,n
 state.update(complete=True,phase='complete');save()
except BaseException as e:state.update(phase='failed',error=repr(e),traceback=traceback.format_exc());save();raise
