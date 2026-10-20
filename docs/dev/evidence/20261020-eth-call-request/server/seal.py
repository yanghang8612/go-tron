"""Seal the final fixed-copy request comparison; never operates on live DBs."""
import collections, datetime, hashlib, json, os, pathlib, re, shutil, statistics, subprocess, time, traceback, urllib.request, zipfile
ROOT = pathlib.Path('/data/gtron/eth-call-request-20261020-04')
OLD = pathlib.Path('/data/gtron/eth-call-fixed-20261020-01')
INPUT = OLD / 'capture-1'
SOURCE = ROOT / 'source'
state = {'complete': False, 'phase': 'waiting-for-experiments'}
def save():
 state['utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
 (ROOT/'seal-status.json').write_text(json.dumps(state, indent=2))
def read(path):
 try: return json.loads(path.read_text())
 except (FileNotFoundError, json.JSONDecodeError): return {}
def sha(path):
 result = hashlib.sha256()
 with path.open('rb') as file:
  for part in iter(lambda: file.read(4 << 20), b''): result.update(part)
 return result.hexdigest()
def normalize(value):
 if len(value) == 128: value = bytes.fromhex(value).decode('ascii')
 assert re.fullmatch('[0-9a-f]{64}', value)
 return value
def cpu(usage):
 return usage['Utime']['Sec']+usage['Utime']['Usec']/1e6+usage['Stime']['Sec']+usage['Stime']['Usec']/1e6
def io_values(text):
 return {k: int(v) for k,v in (line.split(':', 1) for line in text.splitlines())}
try:
 save(); started = time.monotonic()
 while True:
  statuses = [read(ROOT/name) for name in ('comparison-status.json','diagnostic-status.json')]
  if any(s.get('phase') == 'failed' for s in statuses): raise RuntimeError(json.dumps(statuses))
  if all(s.get('complete') for s in statuses): break
  if time.monotonic()-started > 7200: raise TimeoutError('bounded seal wait')
  time.sleep(2)
 state['phase'] = 'verifying-and-sealing'; save()
 prior = read(ROOT/'census.json'); census = read(ROOT/'census-final.json')
 assert census['complete'] and census['muhash_verified']
 for key in ('block_num','block_hash','tx_num','muhash_root','source_rows','source_sha256'):
  assert census[key] == prior[key], key
 root = bytes(census['muhash_root']).hex()
 oracle = read(ROOT/'oracle-rechecked.json')
 assert oracle['expected'] == read(ROOT/'oracle.json')['expected'] and oracle['unchanged_head']
 expected = {row['height']:row['expected'] for row in oracle['expected']}
 rows = [json.loads(line) for line in (ROOT/'requests.jsonl').read_text().splitlines()]
 assert len(rows) == len({row['id'] for row in rows}) == 238
 server = {}; ready_count = 0
 prefixes = ('discovery-','warm-','first-','os-cold-','profile-')
 for directory in ROOT.iterdir():
  if not directory.is_dir() or not directory.name.startswith(prefixes): continue
  ready = read(directory/'ready.json')
  assert ready['read_only'] and ready['head'] == census['block_num'] and ready['head_tx'] == census['tx_num']
  assert normalize(ready['root']) == root and normalize(ready['head_hash']) == bytes(census['block_hash']).hex()
  assert '\nPASS\n' in (directory/'host.log').read_text()
  ready_count += 1
  for line in (directory/'server-requests.jsonl').read_text().splitlines():
   row = json.loads(line)
   if row['method'] == 'eth_call' and isinstance(row['id'], int):
    assert row['id'] not in server
    server[row['id']] = row
 assert len(server) == len(rows) and ready_count == 78
 joined = []; groups = collections.defaultdict(list)
 for row in rows:
  assert row['correct'] and row['response']['id'] == row['id'] and row['response']['result'] == expected[row['height']]
  s = server[row['id']]; before,after = io_values(s['io_before']),io_values(s['io_after'])
  joined.append({**row,'handler_ms':s['elapsed_ns']/1e6,'cpu_ms':1000*(cpu(s['usage_after'])-cpu(s['usage_before'])),'allocated_bytes':s['alloc_bytes'],'gc_cycles':s['num_gc'],'io_delta':{k:after[k]-before[k] for k in before},'inblocks':s['usage_after']['Inblock']-s['usage_before']['Inblock']})
  if row['cache'] in ('warm','fresh-process-os-uncontrolled','private-os-cold-fresh-process'):
   groups[row['mode'],row['cache'],row['tag']].append(row['milliseconds'])
 assert sum(map(len,groups.values())) == 140 and len(groups) == 42
 counts = {'warm':5,'fresh-process-os-uncontrolled':3,'private-os-cold-fresh-process':2}
 for key,values in groups.items(): assert len(values) == counts[key[1]]
 for summary in read(ROOT/'summary.json'):
  values = groups[summary['mode'],summary['cache'],summary['tag']]
  assert summary['n'] == len(values) and summary['median_ms'] == statistics.median(values)
 (ROOT/'joined-request-metrics.json').write_text(json.dumps(joined,indent=2))
 private = read(ROOT/'private-files.json'); readset = set(read(ROOT/'read-files.json'))
 assert readset <= {r['path'] for r in private}
 proof = []
 for row in private:
  a,b = (INPUT/row['path']).stat(),(ROOT/'cache-input'/row['path']).stat()
  assert (a.st_dev,a.st_ino) != (b.st_dev,b.st_ino) and a.st_size == b.st_size == row['bytes']
  proof.append({'path':row['path'],'source_inode':a.st_ino,'private_inode':b.st_ino,'bytes':b.st_size})
 (ROOT/'private-inode-proof.json').write_text(json.dumps(proof,indent=2))
 residency = [read(file) for file in ROOT.glob('os-cold-*-residency.json')]
 assert len(residency) == 28 and max(r['fraction'] for r in residency) < .01
 metadata = []
 for family in ('chaindata','history','ancient'):
  for receipt in read(INPUT/(family+'-capture.json')):
   path = OLD/receipt['path']; assert sha(path) == receipt['sha256'], receipt['path']
   private_path = ROOT/'cache-input'/path.relative_to(INPUT)
   assert sha(private_path) == receipt['sha256'], str(private_path)
   metadata.append({'path':str(path.relative_to(INPUT)), 'sha256':receipt['sha256'], 'original_and_private_match':True})
 (ROOT/'metadata-integrity.json').write_text(json.dumps(metadata,indent=2))
 diag = read(ROOT/'diagnostic-counts.json'); assert len(diag) == 14
 for row in diag:
  assert row['result']['result'] == expected[row['height']]
  memo = row['stats']['memo']
  assert memo['peak_retention_charge'] <= 4 << 20
 final_identity = read(ROOT/'source-identity.json')
 assert final_identity['candidate'] == 'b6db66fcbde2d17f4b8fca099e5fd642e4435308'
 assert final_identity['baseline'] == '4e6978dd'
 hashes = {}
 for file in SOURCE.rglob('*'):
  if file.is_file() and (file.suffix in ('.go','.rs') or file.name in ('go.mod','go.sum','Cargo.toml','Cargo.lock')) and not file.name.endswith('_test.go') and '/target/' not in str(file) and '/build/' not in str(file):
   hashes[str(file.relative_to(SOURCE))] = sha(file)
 assert hashes == final_identity['source_sha256']
 for name,digest in final_identity['binaries'].items(): assert sha(ROOT/name) == digest
 previous_identity = read(OLD/'source-identity-final.json')
 baseline_hashes = {key[len('candidate/'):]:value for key,value in previous_identity['source_sha256'].items() if key.startswith('candidate/')}
 final_identity['source_sha256_by_version'] = {**{'baseline/'+k:v for k,v in baseline_hashes.items()},**{'candidate/'+k:v for k,v in hashes.items()}}
 assert final_identity['harness_sha256'] == previous_identity['harness_sha256']
 for dest,source in [('fixed_http_test.go.txt',SOURCE/'core/offline_fixed_http_test.go'),('oracle_test.go.txt',pathlib.Path('/data/gtron/cold-query-tail-20261020/baseline/core/state/offline_fixed_oracle_test.go'))]:
  shutil.copy2(source,ROOT/dest)
 final_identity['oracle_harness_sha256'] = sha(ROOT/'oracle_test.go.txt')
 final_identity['diagnostic_binary_sha256'] = sha(ROOT/'diagnostic.test')
 (ROOT/'source-identity-final.json').write_text(json.dumps(final_identity,indent=2))
 pid = int(subprocess.check_output(['systemctl','show','gtron','-p','MainPID']).decode().split('=')[1])
 identity = {'pid':pid,'sha256':sha(pathlib.Path(f'/proc/{pid}/exe'))}
 assert identity == read(ROOT/'prepare-status.json')['before_identity'] == read(ROOT/'comparison-status.json')['after_identity'] == read(ROOT/'diagnostic-status.json')['after_identity']
 with urllib.request.urlopen('http://127.0.0.1:8090/wallet/getnodeinfo',timeout=10) as response: node = json.loads(response.read())
 assert not node['syncInfo'].get('paused')
 final = {'identity':identity,'node':node,'data_dir_inode':pathlib.Path('/data/gtron/main/datadir').stat().st_ino,'old_database_retained':pathlib.Path('/data/gtron/main/datadir-pre-compact-v3-20261019-659ab078').is_dir(),'fixed_head':census['block_num'],'fixed_root':root,'source_rows':census['source_rows'],'muhash_verified':True,'http_requests_verified':len(rows),'performance_samples':140,'diagnostic_requests_verified':len(diag),'read_only_host_runs':ready_count+1,'private_file_count':len(private),'private_file_bytes':sum(r['bytes'] for r in private),'os_cold_rounds':len(residency),'maximum_resident_fraction_before_startup':max(r['fraction'] for r in residency),'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'no_production_restart':True,'no_deployment':True}
 assert final['data_dir_inode'] == 94642617 and final['old_database_retained']
 (ROOT/'final-proof.json').write_text(json.dumps(final,indent=2))
 # Only small evidence artifacts, never DBs, source checkout, binaries or traces.
 files = []
 for file in ROOT.iterdir():
  if file.is_file() and file.suffix in ('.py','.json','.jsonl','.log','.txt'): files.append(file)
 for directory in ROOT.iterdir():
  if directory.is_dir() and directory.name not in ('source','cache-input'):
   for file in directory.iterdir():
    if file.is_file() and file.suffix in ('.json','.jsonl','.log','.gz','.txt','.go'): files.append(file)
 capture_evidence = ROOT/'capture-evidence'; capture_evidence.mkdir()
 for name in ('chaindata-capture.json','history-capture.json','ancient-capture.json','source-cold-manifest.json'):
  shutil.copy2(INPUT/name,capture_evidence/name); files.append(capture_evidence/name)
 shutil.copy2(INPUT/'state-snapshots/manifest.json',capture_evidence/'manifest.json'); files.append(capture_evidence/'manifest.json')
 manifest = {str(file.relative_to(ROOT)):sha(file) for file in files if file.name != 'seal-status.json'}
 (ROOT/'evidence-sha256.json').write_text(json.dumps(manifest,indent=2)); files.append(ROOT/'evidence-sha256.json')
 out = pathlib.Path('/tmp/gtron-eth-call-request-final-20261020.zip')
 with zipfile.ZipFile(out,'w',compression=zipfile.ZIP_DEFLATED) as archive:
  for file in files:
   if file.name != 'seal-status.json': archive.write(file,str(file.relative_to(ROOT)))
 os.chmod(out,0o644)
 state.update(complete=True,phase='sealed',zip=str(out),zip_bytes=out.stat().st_size,zip_sha256=sha(out)); save()
except BaseException as exc:
 state.update(phase='failed',error=repr(exc),traceback=traceback.format_exc()); save(); raise
