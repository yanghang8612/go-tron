import pathlib,json,subprocess,os,time,datetime,hashlib,traceback,collections,concurrent.futures
P=pathlib.Path('/data/gtron/history-account-delta-20261025-01');S=P/'candidate-source';state={'complete':False,'production_mutations':False}
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();p=P/'postprocess-status.json';p.with_suffix('.tmp').write_text(json.dumps(state,indent=2));p.with_suffix('.tmp').replace(p)
def sha(p):
 h=hashlib.sha256()
 with p.open('rb')as f:
  for b in iter(lambda:f.read(4<<20),b''):h.update(b)
 return h.hexdigest()
def run(label,args,env,timeout):
 state['phase']=label;save()
 with (P/(label+'.log')).open('wb')as f:subprocess.run(args,cwd=str(S),env=env,stdout=f,stderr=subprocess.STDOUT,timeout=timeout,check=True)
try:
 state['phase']='waiting-http-completion';save();start=time.monotonic()
 while True:
  f=P/'acceptance-status.json';a=json.loads(f.read_text())if f.exists()else{}
  if a.get('phase')=='failed':raise RuntimeError('acceptance failed: '+str(a.get('error')))
  if a.get('complete'):break
  if time.monotonic()-start>21600:raise TimeoutError('HTTP completion')
  time.sleep(4)
 inv=json.loads((P/'final-source-inventory.json').read_text())
 for n,d in inv.items():assert sha(S/n)==d,n
 (S/'core/state/snapshots/offline_account_delta_controls_test.go').write_bytes((P/'offline_account_delta_controls_test.go').read_bytes())
 env=dict(os.environ,PATH='/data/go/bin:'+os.environ.get('PATH',''),GOCACHE='/root/.cache/go-build',GOPATH='/root/go',GOTOOLCHAIN='local',CGO_ENABLED='1',GOMAXPROCS='4',GOMEMLIMIT='3GiB',GTRON_ACCOUNT_DELTA_ROOT=str(P))
 run('post-snapshots-build',['/data/go/bin/go','test','-c','./core/state/snapshots','-o',str(P/'post-snapshots.test')],env,600)
 run('controls-fixture',[str(P/'post-snapshots.test'),'-test.run=^TestAccountDeltaActualControlFixture$','-test.v','-test.timeout=180s'],env,200)
 run('account-controls',[str(P/'post-snapshots.test'),'-test.run=^TestOfflineAccountDeltaActualControls$','-test.v','-test.timeout=120m'],env,7250)
 assert json.loads((P/'account-controls-status.json').read_text())['complete']
 state['phase']='final-physical-sha-and-allocation';save()
 roots={'baseline':P/'baseline-input/state-snapshots','candidate':P/'candidate-input/state-snapshots'};manifests={k:json.loads((v/'manifest.json').read_text())for k,v in roots.items()};a,b=manifests.values()
 for k in ['chain','progress','sourceRows','commitmentScheme','elementVersion','visibleTxStart','visibleTxEnd']:assert a.get(k)==b.get(k),k
 tasks={};modes={};refsets={}
 for mode,root in roots.items():
  refs={}
  for r in manifests[mode]['segments']:
   assert r['path'] not in refs or refs[r['path']]==r;refs[r['path']]=r
  refsets[mode]=refs;groups=collections.defaultdict(lambda:dict(files=0,bytes=0,allocated=0));unique={}
  for n,r in refs.items():
   f=root/n;assert os.path.commonpath([str(root.resolve()),str(f.resolve())])==str(root.resolve());st=f.stat();assert st.st_size==r['size'];assert r['checksum'].startswith('sha256:');key=(st.st_dev,st.st_ino,st.st_size,st.st_mtime_ns);groups[r['kind']]['files']+=1;groups[r['kind']]['bytes']+=st.st_size;groups[r['kind']]['allocated']+=st.st_blocks*512;unique[(st.st_dev,st.st_ino)]=st.st_blocks*512
   if key in tasks:assert tasks[key][1]==r['checksum'][7:]
   else:tasks[key]=(f,r['checksum'][7:])
  manifest=root/'manifest.json';ms=manifest.stat();modes[mode]=dict(ref_files=len(refs),ref_file_bytes=sum(r['size']for r in refs.values()),allocated_unique=sum(unique.values()),by_kind=dict(groups),manifest_bytes=ms.st_size,manifest_allocated=ms.st_blocks*512,manifest_sha256=sha(manifest),directory_du_bytes=int(subprocess.check_output(['du','-s','-B1',str(root)]).split()[0]))
 control_rows=[json.loads(s)for s in (P/'account-controls-files.jsonl').read_text().splitlines()]
 for r in control_rows:
  if not r['ControlPath']:continue
  f=pathlib.Path(r['ControlPath']);st=f.stat();assert st.st_size==r['ControlFileBytes'];key=(st.st_dev,st.st_ino,st.st_size,st.st_mtime_ns);tasks[key]=(f,r['ControlSHA256'])
 def check(item):
  key,(f,want)=item;got=sha(f);assert got==want,str(f);st=f.stat();assert (st.st_dev,st.st_ino,st.st_size,st.st_mtime_ns)==key,str(f);return dict(path=str(f),sha256=got,bytes=st.st_size,inode=st.st_ino,allocated=st.st_blocks*512)
 checked=[]
 with concurrent.futures.ThreadPoolExecutor(max_workers=2)as pool:
  for r in pool.map(check,tasks.items()):checked.append(r)
 for n,d in inv.items():assert sha(S/n)==d,n
 result=dict(complete=True,production_mutations=False,utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),canonical_chain_and_frontiers_unchanged=True,all_referenced_physical_sha_verified=True,scope='Every baseline and candidate manifest reference plus every materialized ordinary-Zstd control; shared immutable inodes are hashed once.',modes=modes,unique_physical_files=len(checked),physical_bytes_hashed=sum(x['bytes']for x in checked),per_file=checked,net_active_ref_bytes=modes['baseline']['ref_file_bytes']-modes['candidate']['ref_file_bytes'],net_active_unique_allocated=modes['baseline']['allocated_unique']-modes['candidate']['allocated_unique'],net_directory_du=modes['baseline']['directory_du_bytes']-modes['candidate']['directory_du_bytes'],production_reclaimed_bytes=0,all_old_files_retained=True)
 (P/'final-space-and-physical-closure.json').write_text(json.dumps(result,indent=2));state.update(complete=True,phase='complete');save()
except BaseException as e:state.update(phase='failed',error=repr(e),traceback=traceback.format_exc());save();raise
