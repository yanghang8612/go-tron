import collections,datetime,hashlib,json,os,pathlib,re,subprocess,time,traceback,urllib.request,zipfile
ROOT=pathlib.Path('/data/gtron/eth-call-fixed-20261020-01')
SOURCE=pathlib.Path('/data/gtron/cold-query-tail-20261020')
state={'complete':False,'phase':'waiting-for-all-experiments'}
def save():
 state['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(ROOT/'seal-status.json').write_text(json.dumps(state,indent=2))
def read(path):
 try:return json.loads(path.read_text())
 except (FileNotFoundError,json.JSONDecodeError):return {}
def h(path):
 result=hashlib.sha256()
 with path.open('rb') as f:
  while True:
   b=f.read(4<<20)
   if not b:break
   result.update(b)
 return result.hexdigest()
def normalized(value):
 if len(value)==128:value=bytes.fromhex(value).decode('ascii')
 assert re.fullmatch('[0-9a-f]{64}',value);return value
def usage(u):return u['Utime']['Sec']+u['Utime']['Usec']/1e6+u['Stime']['Sec']+u['Stime']['Usec']/1e6
def io_values(text):return {key:int(value.strip()) for key,value in (line.split(':',1) for line in text.splitlines())}
try:
 save();started=time.monotonic()
 while True:
  statuses=[read(ROOT/name) for name in ('comparison-status.json','diagnostic-status.json','capacity-status.json')]
  if any(s.get('phase')=='failed' for s in statuses):raise RuntimeError('experiment failed '+json.dumps(statuses))
  if all(s.get('complete') for s in statuses):break
  if time.monotonic()-started>7200:raise TimeoutError('bounded final seal wait')
  time.sleep(2)
 state['phase']='verifying-and-sealing';save()
 census=read(ROOT/'census.json');assert census['complete'] and census['muhash_verified']
 root=bytes(census['muhash_root']).hex()
 rows=[json.loads(line) for line in (ROOT/'requests.jsonl').read_text().splitlines()]
 assert len(rows)==238 and len({r['id'] for r in rows})==238
 oracle={r['height']:r['expected'] for r in read(ROOT/'oracle.json')['expected']}
 server={};ready_count=0
 prefixes=('discovery-','warm-','first-','os-cold-','profile-')
 for directory in ROOT.iterdir():
  if not directory.is_dir() or not directory.name.startswith(prefixes):continue
  ready=read(directory/'ready.json')
  assert ready['read_only'] and ready['head']==census['block_num'] and ready['head_tx']==census['tx_num']
  assert normalized(ready['root'])==root
  assert normalized(ready['head_hash'])==bytes(census['block_hash']).hex()
  ready_count+=1
  for line in (directory/'server-requests.jsonl').read_text().splitlines():
   row=json.loads(line)
   if row['method']=='eth_call' and isinstance(row['id'],int):
    assert row['id'] not in server
    server[row['id']]=row
 assert len(server)==len(rows)
 joined=[];counts=collections.Counter()
 for row in rows:
  assert row['correct'] and row['response'].get('id')==row['id'] and row['response'].get('result')==oracle[row['height']]
  counts[row['mode'],row['cache']]+=1
  s=server[row['id']];a,b=io_values(s['io_before']),io_values(s['io_after'])
  joined.append({**row,'handler_ms':s['elapsed_ns']/1e6,'cpu_ms':(usage(s['usage_after'])-usage(s['usage_before']))*1000,'allocated_bytes':s['alloc_bytes'],'gc_cycles':s['num_gc'],'io_delta':{k:b[k]-a[k] for k in a},'inblocks':s['usage_after']['Inblock']-s['usage_before']['Inblock']})
 assert sum(n for (mode,cache),n in counts.items() if cache in ('warm','fresh-process-os-uncontrolled','private-os-cold-fresh-process'))==140
 (ROOT/'joined-request-metrics.json').write_text(json.dumps(joined,indent=2))
 private=read(ROOT/'private-files.json');readset=set(read(ROOT/'read-files.json'));assert readset<={r['path'] for r in private}
 proof=[]
 for row in private:
  a=(ROOT/'capture-1'/row['path']).stat();b=(ROOT/'cache-input'/row['path']).stat()
  assert (a.st_dev,a.st_ino)!=(b.st_dev,b.st_ino) and a.st_size==b.st_size==row['bytes']
  proof.append({'path':row['path'],'source_inode':a.st_ino,'private_inode':b.st_ino,'bytes':b.st_size})
 (ROOT/'private-inode-proof.json').write_text(json.dumps(proof,indent=2))
 residency=[read(file) for file in ROOT.glob('os-cold-*-residency.json')]
 assert len(residency)==28 and max(r['fraction'] for r in residency)<0.01
 for family in ('chaindata','history','ancient'):
  for receipt in read(ROOT/'capture-1'/(family+'-capture.json')):
   assert h(ROOT/receipt['path'])==receipt['sha256'],receipt['path']
 hashes={}
 for mode in ('baseline','candidate'):
  for file in (SOURCE/mode).rglob('*'):
   if file.is_file() and (file.suffix in ('.go','.rs') or file.name in ('go.mod','go.sum','Cargo.toml','Cargo.lock')) and not file.name.endswith('_test.go') and '/target/' not in str(file) and '/build/' not in str(file):hashes[str(file.relative_to(SOURCE))]=h(file)
 harness={mode:h(SOURCE/mode/'core/offline_fixed_http_test.go') for mode in ('baseline','candidate')}
 assert harness['baseline']==harness['candidate']
 (ROOT/'fixed_http_test.go.txt').write_bytes((SOURCE/'candidate/core/offline_fixed_http_test.go').read_bytes())
 (ROOT/'oracle_test.go.txt').write_bytes((SOURCE/'baseline/core/state/offline_fixed_oracle_test.go').read_bytes())
 source={'source_sha256':hashes,'harness_sha256':harness['candidate'],'http_harness_by_version':harness,'oracle_harness_sha256':h(SOURCE/'baseline/core/state/offline_fixed_oracle_test.go'),'binaries':{file.name:h(file) for file in ROOT.glob('*.test')},'go_version':subprocess.check_output(['/data/go/bin/go','version']).decode().strip(),'sapling_static_library_sha256':h(SOURCE/'candidate/third_party/librustzcash/target/release/librustzcash.a')}
 (ROOT/'source-identity-final.json').write_text(json.dumps(source,indent=2))
 pid=int(subprocess.check_output(['systemctl','show','gtron','-p','MainPID']).decode().split('=')[1])
 identity={'pid':pid,'sha256':h(pathlib.Path(f'/proc/{pid}/exe'))}
 assert identity==read(ROOT/'capture-status.json')['before_identity']
 with urllib.request.urlopen('http://127.0.0.1:8090/wallet/getnodeinfo',timeout=10) as response:node=json.loads(response.read())
 assert not node['syncInfo'].get('paused')
 final={'identity':identity,'node':node,'data_dir_inode':pathlib.Path('/data/gtron/main/datadir').stat().st_ino,'old_database_retained':pathlib.Path('/data/gtron/main/datadir-pre-compact-v3-20261019-659ab078').is_dir(),'fixed_head':census['block_num'],'fixed_root':root,'source_rows':census['source_rows'],'muhash_verified':True,'http_requests_verified':len(rows),'performance_samples':140,'read_only_host_runs':ready_count,'private_file_count':len(private),'private_file_bytes':sum(r['bytes'] for r in private),'os_cold_rounds':len(residency),'maximum_resident_fraction_before_startup':max(r['fraction'] for r in residency),'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'no_production_restart':True,'no_deployment':True}
 (ROOT/'final-proof.json').write_text(json.dumps(final,indent=2))
 files=[]
 for file in ROOT.iterdir():
  if file.is_file() and (file.suffix in ('.py','.json','.jsonl','.log','.txt') and file.name!='census-groups.jsonl'):files.append(file)
 for directory in ROOT.iterdir():
  if directory.is_dir() and directory.name not in ('capture-1','cache-input'):
   for file in directory.iterdir():
    if file.is_file() and file.suffix in ('.json','.jsonl','.log','.gz','.txt','.go'):files.append(file)
 for name in ('chaindata-capture.json','history-capture.json','ancient-capture.json','source-cold-manifest.json'):
  files.append(ROOT/'capture-1'/name)
 files.append(ROOT/'capture-1/state-snapshots/manifest.json')
 manifest={str(file.relative_to(ROOT)):h(file) for file in files if file.name!='seal-status.json'}
 (ROOT/'evidence-sha256.json').write_text(json.dumps(manifest,indent=2));files.append(ROOT/'evidence-sha256.json')
 out=pathlib.Path('/tmp/gtron-eth-call-fixed-final-20261020.zip')
 with zipfile.ZipFile(out,'w',compression=zipfile.ZIP_DEFLATED) as archive:
  for file in files:
   if file.name=='seal-status.json':continue
   archive.write(file,str(file.relative_to(ROOT)))
 os.chmod(out,0o644)
 state.update(complete=True,phase='sealed',zip=str(out),zip_bytes=out.stat().st_size,zip_sha256=h(out));save()
except BaseException as exc:
 state.update(phase='failed',error=repr(exc),traceback=traceback.format_exc());save();raise
