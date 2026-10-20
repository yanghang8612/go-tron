import contextlib,datetime,hashlib,json,os,pathlib,shutil,signal,subprocess,time,traceback,urllib.request
ROOT=pathlib.Path('/data/gtron/cold-query-layout-20261020-01');SOURCE=ROOT/'account-gc-source';BASE=ROOT/'account2-source'
state={'complete':False,'production_mutations':False,'phase':'waiting-for-excluded-profile'}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(ROOT/'account-gc-verification-status.json').write_text(json.dumps(state,indent=2))
def identity():
 pid=int(subprocess.check_output(['systemctl','show','gtron','-p','MainPID']).decode().split('=')[1]);return {'pid':pid,'sha256':sha(pathlib.Path('/proc/%d/exe'%pid))}
def inventory():
 return {str(p.relative_to(SOURCE)):sha(p) for p in SOURCE.rglob('*') if p.is_file() and (p.suffix in ('.go','.rs') or p.name in ('go.mod','go.sum','Cargo.toml','Cargo.lock')) and not p.name.endswith('_test.go') and '/target/' not in str(p) and '/build/' not in str(p)}
try:
 state['before_identity']=identity();save();started=time.monotonic()
 while True:
  path=ROOT/'account2-diagnostic-status.json';s=json.loads(path.read_text()) if path.exists() else {}
  if s.get('phase')=='failed':raise RuntimeError('excluded diagnostic failed')
  if s.get('complete'):
   unit=subprocess.check_output(['systemctl','show','gtron-cold-account2-diagnostic-after-compare-20261020.service','-p','ActiveState']).decode().strip()
   if unit=='ActiveState=inactive':break
  if time.monotonic()-started>14400:raise TimeoutError('diagnostic wait')
  time.sleep(5)
 state['phase']='copy-frozen-final-source';save();shutil.copytree(BASE,SOURCE,ignore=shutil.ignore_patterns('target','build','.git'))
 target=SOURCE/'third_party/librustzcash/target';target.symlink_to((BASE/'third_party/librustzcash/target').resolve(),target_is_directory=True)
 baseline=json.loads((ROOT/'account2-source-identity.json').read_text())['production_sources'];assert inventory()==baseline,'frozen measured source changed'
 delta=json.loads((ROOT/'account-gc-delta.json').read_text());prepared=[]
 for entry in delta['files']:
  path=SOURCE/entry['path'];assert (sha(path) if path.exists() else None)==entry['before'],entry['path']
  if 'source' in entry:data=entry['source'].encode()
  else:
   lines=path.read_text().splitlines(keepends=True)
   for edit in reversed(entry['edits']):lines[edit['start']:edit['end']]=edit['text'].splitlines(keepends=True)
   data=''.join(lines).encode()
  assert hashlib.sha256(data).hexdigest()==entry['after'];prepared.append((path,data))
 for path,data in prepared:path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
 (SOURCE/'core/state/snapshots/offline_routing_test.go').write_text((ROOT/'offline_account_gc_test.go').read_text())
 (SOURCE/'core/state/snapshots/offline_retired_routing_test.go').write_text((ROOT/'offline_retired_routing_test.go').read_text())
 env=dict(os.environ,PATH='/data/go/bin:'+os.environ.get('PATH',''),GOTOOLCHAIN='local',CGO_ENABLED='1',GOCACHE='/root/.cache/go-build',GOPATH='/root/go',GOMAXPROCS='2',GOMEMLIMIT='3GiB',GTRON_HISTORY_COMPRESSION_FORMAT='2')
 for name in ('GTRON_LAYOUT_ROOT','GTRON_LAYOUT_PHASE','GTRON_FIXED_INPUT','GTRON_FIXED_OUTPUT','GTRON_FIXED_CENSUS','GTRON_ROUTING_ROOT','GTRON_GC_ROOT'):env.pop(name,None)
 def run(cmd,name,timeout=1800):
  state['phase']=name;save()
  with (ROOT/name).open('wb') as out:subprocess.run(cmd,cwd=SOURCE,env=env,stdout=out,stderr=subprocess.STDOUT,timeout=timeout,check=True)
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','./core/state/snapshots','-count=1','-timeout=15m'],'account-gc-native-snapshots.log')
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','./core/state','./core','./cmd/gtron','-run','^(TestHistoryRequest|TestHistoryExecution|TestHistoryStorage|TestMuHash|TestArchiveQuery_|TestSnapshot|TestRestore)','-count=1','-timeout=15m'],'account-gc-native-correctness.log')
 run(['/data/go/bin/go','vet','-p=2','-tags=sapling','./core/state/snapshots','./core/state','./core','./cmd/gtron'],'account-gc-native-vet.log')
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','-c','./core','-o',str(ROOT/'account-gc-candidate.test')],'account-gc-http-build.log',900)
 run(['/data/go/bin/go','test','-p=2','-tags=sapling','-c','./core/state/snapshots','-o',str(ROOT/'account-gc-layout.test')],'account-gc-layout-build.log',900)
 run(['/data/go/bin/go','build','-p=2','-tags=sapling','-o',str(ROOT/'gtron-account-gc'),'./cmd/gtron'],'account-gc-native-build.log',900)
 record={'production_sources':inventory(),'binaries':{name:sha(ROOT/name) for name in ('account-gc-candidate.test','account-gc-layout.test','gtron-account-gc')},'http_harness_sha256':sha(SOURCE/'core/offline_fixed_http_test.go'),'muhash_harness_sha256':sha(SOURCE/'core/offline_request_muhash_test.go'),'reclamation_harness_sha256':sha(SOURCE/'core/state/snapshots/offline_retired_routing_test.go'),'restore_harness_sha256':sha(SOURCE/'core/state/snapshots/offline_routing_test.go'),'sapling_sha256':sha(SOURCE/'third_party/librustzcash/target/release/librustzcash.a'),'go_version':subprocess.check_output(['/data/go/bin/go','version']).decode().strip(),'delta_sha256':sha(ROOT/'account-gc-delta.json')}
 for key in ('http_harness_sha256','muhash_harness_sha256','sapling_sha256'):assert record[key]==json.loads((ROOT/'account2-source-identity.json').read_text())[key],key
 (ROOT/'account-gc-source-identity.json').write_text(json.dumps(record,indent=2))
 state['phase']='isolate-corpus-and-exclusive-old-auxiliary-inodes';save();source=ROOT/'account2-input';target=ROOT/'account-gc-input';target.mkdir(exist_ok=False)
 m=json.loads((source/'state-snapshots/manifest.json').read_text());private={ref['path']:ref for ref in m.get('retired',[]) if ref['kind'] in ('account-history-routing','system-history-routing')}
 for ref in m['segments']:private.pop(ref['path'],None)
 proof=[]
 for directory,dirs,files in os.walk(source):
  rel=pathlib.Path(directory).relative_to(source);(target/rel).mkdir(exist_ok=True)
  for name in files:
   if name in ('LOCK','FLOCK'):continue
   a=pathlib.Path(directory)/name;b=target/rel/name;relative=str((rel/name).relative_to('state-snapshots')) if str(rel).startswith('state-snapshots') else ''
   if relative in private:
    digest=hashlib.sha256()
    with a.open('rb') as inp,b.open('xb') as out:
     while True:
      data=inp.read(4<<20)
      if not data:break
      digest.update(data);out.write(data)
     out.flush();os.fsync(out.fileno());os.posix_fadvise(out.fileno(),0,0,os.POSIX_FADV_DONTNEED)
    assert 'sha256:'+digest.hexdigest()==private[relative]['checksum'];original=a.stat();copied=b.stat();assert copied.st_nlink==1 and copied.st_ino!=original.st_ino
    proof.append({'path':str(rel/name),'bytes':copied.st_size,'allocated_bytes':copied.st_blocks*512,'source_inode':original.st_ino,'private_inode':copied.st_ino,'private_link_count':copied.st_nlink,'sha256':digest.hexdigest()})
   elif a.suffix in ('.sst','.seg','.idx','.kv','.route') or str(rel).startswith('ancient'):os.link(a,b)
   else:shutil.copy2(a,b)
 (ROOT/'account-gc-private-files.json').write_text(json.dumps(proof,indent=2))
 env['GTRON_GC_ROOT']=str(ROOT)
 run([str(ROOT/'account-gc-layout.test'),'-test.run=^TestOfflineRetiredRoutingReclamation$','-test.v','-test.timeout=15m'],'account-gc-reclamation.log',1000)
 for row in proof:assert (source/row['path']).stat().st_ino==row['source_inode'],'original auxiliary file lost'
 state['phase']='final-exact-binary-http-values-after-reclamation';save();output=ROOT/'account-gc-http';output.mkdir();env.update(GTRON_FIXED_INPUT=str(target),GTRON_FIXED_OUTPUT=str(output));process=None
 with (output/'host.log').open('wb') as log:
  process=subprocess.Popen([str(ROOT/'account-gc-candidate.test'),'-test.run=^TestOfflineFixedEthCallHTTP$','-test.v','-test.timeout=10m'],env=env,stdout=log,stderr=subprocess.STDOUT)
  try:
   begin=time.monotonic()
   while not (output/'ready.json').exists():
    if process.poll() is not None:raise RuntimeError('final HTTP host startup')
    if time.monotonic()-begin>120:raise TimeoutError('final HTTP startup')
    time.sleep(.1)
   ready=json.loads((output/'ready.json').read_text());expected=json.loads((ROOT/'account2-comparison/comparison-status.json').read_text())['head']
   for key in ('head','head_tx','head_hash','root'):assert ready[key]==expected[key],key
   results=[]
   for repeat in range(2):
    for spec in json.loads((ROOT/'account2-comparison/specs.json').read_text()):
     body=json.dumps({'jsonrpc':'2.0','id':len(results)+1,'method':'eth_call','params':[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},spec['tag']]}).encode();begin=time.perf_counter()
     with urllib.request.urlopen(urllib.request.Request(ready['url'],body,{'Content-Type':'application/json'}),timeout=150) as response:row=json.loads(response.read())
     assert row.get('result')==spec['expected'],row;results.append({'repeat':repeat,**spec,'response':row,'milliseconds':(time.perf_counter()-begin)*1000,'excluded_from_primary_performance':True})
   (output/'responses.json').write_text(json.dumps(results,indent=2))
  finally:
   if process.poll() is None:process.send_signal(signal.SIGTERM);process.wait(timeout=20)
   assert process.returncode==0,process.returncode
 env['GTRON_FIXED_CENSUS']=str(ROOT/'account-gc-muhash-census.json')
 run([str(ROOT/'account-gc-candidate.test'),'-test.run=^TestOfflineRequestFixedMuHash$','-test.v','-test.timeout=35m'],'account-gc-muhash.log',2200)
 env['GTRON_ROUTING_ROOT']=str(ROOT)
 run([str(ROOT/'account-gc-layout.test'),'-test.run=^TestOfflineAccountHistoryRestore$','-test.v','-test.timeout=25m'],'account-gc-history-restore.log',1600)
 state['after_identity']=identity();assert state['after_identity']==state['before_identity'];state.update(phase='complete',complete=True);save()
except BaseException as exc:
 state.update(phase='failed',error=repr(exc),traceback=traceback.format_exc());save();raise
