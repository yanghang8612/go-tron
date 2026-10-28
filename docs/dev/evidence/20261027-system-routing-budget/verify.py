import sys,pathlib,hashlib,json,tarfile,collections,statistics,math
archive,seal,out=map(pathlib.Path,sys.argv[1:4]);s=json.loads(seal.read_text())
assert archive.stat().st_size==s['bytes'] and hashlib.sha256(archive.read_bytes()).hexdigest()==s['sha256']
out.mkdir(exist_ok=False)
hard_links=0
with tarfile.open(archive,'r:gz') as tar:
 seen=set();regular=set()
 for member in tar.getmembers():
  p=pathlib.PurePosixPath(member.name)
  assert not p.is_absolute() and '..' not in p.parts and member.name not in seen and member.size<=50<<20,member.name
  seen.add(member.name)
  if member.isfile():data=tar.extractfile(member).read();regular.add(member.name)
  elif member.islnk():
   target=pathlib.PurePosixPath(member.linkname)
   assert not target.is_absolute() and '..' not in target.parts and member.linkname in regular,member.name
   data=out.joinpath(*target.parts).read_bytes();hard_links+=1
  else:raise AssertionError('Only regular files or prior internal regular-file hard links allowed: '+member.name)
  dest=out.joinpath(*p.parts);dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(data)
manifest=json.loads((out/'members.json').read_text());assert len(manifest['members'])+1==s['members']
for name,r in manifest['members'].items():
 b=(out/name).read_bytes();assert len(b)==r['bytes'] and hashlib.sha256(b).hexdigest()==r['sha256'],name
load=lambda n:json.loads((out/n).read_text())
status=load('run-status.json');assert status['complete'] and status['production_before']==status['production_after']
pipelines={}
for mode in ('baseline','candidate'):
 d=load(mode+'-pipeline/pipeline.json');cycles=d['cycles'];assert d['complete'] and len(cycles)==64 and sum(c['cold_built'] for c in cycles)==8
 assert all(d[k] for k in ('canonical_refs_unchanged','old_memberships_equal','new_memberships_dictionary_verified','all_cold_content_and_order_equal'))
 seconds=sum(c['build_seconds']+c['route_seconds'] for c in cycles);cpu=sum(c['build_cpu_seconds']+c['route_cpu_seconds'] for c in cycles)
 assert math.isclose(seconds,d['pipeline_seconds'],rel_tol=1e-12) and math.isclose(cpu,d['pipeline_cpu_seconds'],rel_tol=1e-12)
 assert math.isclose(d['rows_per_second'],d['rows_per_cycle']*8/seconds,rel_tol=1e-12)
 assert d['published_routes']==sum(c['built_route'] for c in cycles)
 assert len(d['cold_verification'])==8
 for r in d['cold_verification']:assert r['rows']==d['rows_per_cycle'] and r['sequence_formula_and_uniqueness_verified'] and r['tx_ranges_equal']==256
 assert d['cold_verification'][0]['hot_restore_verified'] and d['cold_verification'][0]['hot_rows_read']==d['rows_per_cycle'] and d['cold_verification'][0]['posting_queries_verified']==16
 pipelines[mode]=d
assert pipelines['baseline']['source_digest']==pipelines['candidate']['source_digest']
assert pipelines['baseline']['published_routes']==0 and pipelines['candidate']['published_routes']>0
assert pipelines['candidate']['cycles'][-1]['policy']['deferred']>0
proof=load('input-and-source-proof.json');assert all(v['canonical_equal'] and v['metadata_frontiers_equal'] for v in proof['manifests'].values())
local=pathlib.Path.cwd()
support=pathlib.Path(__file__).resolve().parent
frozen=json.loads((support/'baseline-product.json').read_text())
for name,digest in frozen.items():assert proof['source_sha256']['baseline'][name]==digest,name
for name,digest in proof['source_sha256']['candidate'].items():
 if '/zz_offline_' not in name:assert hashlib.sha256((local/name).read_bytes()).hexdigest()==digest,name
harness=support/'offline_budget_test.go' if (support/'offline_budget_test.go').exists() else support/'harness/offline_budget_test.go'
assert hashlib.sha256(harness.read_bytes()).hexdigest()==proof['source_sha256']['candidate']['core/state/snapshots/zz_offline_budget_test.go']
http=load('http-comparison/comparison-status.json');assert http['complete'] and http['all_responses_correct'] and http['before_identity']==http['after_identity']
rows=[json.loads(x) for x in (out/'http-comparison/requests.jsonl').read_text().splitlines()]
assert len(rows)==252 and collections.Counter(r['cache'] for r in rows)=={'warmup-excluded':28,'warm':112,'fresh-process-os-uncontrolled':28,'private-os-cold-fresh-process':32,'maintenance-warmup-excluded':4,'warm-with-routing-maintenance':48}
assert len(rows)==http['requests'] and all(r['correct'] and r['response'].get('id')==r['id'] and r['response'].get('jsonrpc')=='2.0' and r['response'].get('result')==r['expected'] and 'error' not in r['response'] for r in rows)
server_rows={}
for log in (out/'http-comparison').glob('*/server-requests.jsonl'):
 for line in log.read_text().splitlines():
  r=json.loads(line);assert r['id'] not in server_rows;server_rows[r['id']]=r
assert set(server_rows)==set(r['id'] for r in rows)
for r in rows:
 a=server_rows[r['id']];assert a['method']=='eth_call' and a['params']==[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},r['tag']]
expected={3144020:6,3144021:7,5000000:63,9000000:207}
for r in rows:
 if r['height'] in expected:assert int(r['expected'],16)==expected[r['height']]
groups=collections.defaultdict(list)
for r in rows:groups[r['mode'],r['cache'],r['tag']].append(r['milliseconds'])
for row in load('http-comparison/summary.json'):
 v=groups[row['mode'],row['cache'],row['tag']];assert len(v)==row['n'] and math.isclose(statistics.median(v),row['median_ms'],rel_tol=1e-12)
residencies=list((out/'http-comparison').glob('*-residency.json'));assert len(residencies)==32
assert all(json.loads(p.read_text())['fraction']<.01 for p in residencies)
unit=load('resource-unit-evidence.json');assert all('CPUQuotaPerSecUSec=2s' in v and 'MemoryLimit=6442450944' in v and 'Nice=15' in v for v in unit['units'].values())
inodes=load('private-inode-audit.json');assert inodes['files']==2255 and inodes['changed_inode_count']==10
private=load('http-comparison/verified-private-files.json');assert private and all(r['sha256']==r['current_private_sha256'] and r['current_private_inode']!=r['current_source_inode'] for r in private)
auto=[]
repeat=load('auto-repeat/status.json');assert repeat['complete'] and repeat['order']==['baseline','candidate','candidate','baseline']
for mode in ('baseline','candidate'):
 a=load(mode+'-auto-build.json');assert a['complete'] and a['format']=='auto' and a['all_content_and_order_equal'] and a['rows_per_cycle']==121637
 auto.append(a)
for process in repeat['processes']:
 a=load('auto-repeat/'+process['result_file']);assert a['complete'] and a['warmup_excluded'] and len(a['cycles'])==8 and a['rows_per_cycle']==121637 and a['all_content_and_order_equal']
 auto.append(a)
for a in auto:
 assert a['content_digest_excluding_derived_seq']=='12986f20b829fd4e90e2aebed89cdaf67f28588d02a36914e70d045ec4d24d32'
 assert math.isclose(sum(c['seconds'] for c in a['cycles']),a['measured_build_seconds'],rel_tol=1e-12)
 for c in a['cycles']:
  assert c['bytes']==2431014 and [r['checksum'] for r in c['refs']]==[r['checksum'] for r in auto[0]['cycles'][0]['refs']]
census=load('muhash-census.json');reference=support/'reference-muhash-census.json' if (support/'reference-muhash-census.json').exists() else local/'build/benchmarks/20261027-small-system-route/final-evidence/muhash-census.json';old=json.loads(reference.read_text())
assert census['complete'] and census['muhash_verified']
for k in ('block_num','tx_num','muhash_root','source_rows','source_sha256'):assert census[k]==old[k],k
restored=load('history-restore.json');assert restored['complete'] and len(restored['windows'])==3
for w in restored['windows']:assert w['full_row_sha256']==w['candidate_row_sha256']==w['restored_row_sha256'] and w['result']['ChangesRestored']==w['rows']
log=(out/'restore.log').read_text();assert '--- PASS: TestSystemRoutingAdaptiveSnapshotAndSignedRestore' in log and log.strip().endswith('PASS')
correctness=load('correctness-status.json');assert correctness['complete']
correct_binary=load('correctness-binary.json')
for name,digest in correct_binary['latest_test_sha256'].items():assert hashlib.sha256((local/'core/state/snapshots'/name).read_bytes()).hexdigest()==digest
for name,digest in correct_binary['product_sha256'].items():assert hashlib.sha256((local/name).read_bytes()).hexdigest()==digest
correct_log=(out/'correctness.log').read_text();assert correct_log.strip().endswith('PASS')
for name in ('TestSystemRoutingAdaptiveSnapshotAndSignedRestore','TestSystemRoutingBackoffSourceReplacementAndDirectorySeparation','TestSmallSystemHistoryRoutingVerifiedSnapshotAndSignedCatalogRestore'):assert '--- PASS: '+name in correct_log
extra=load('extra-nine-million-fresh/comparison-status.json');assert extra['complete'] and extra['all_responses_correct'] and extra['before_identity']==extra['after_identity']==http['before_identity']
for k in ('head','head_tx','head_hash','root'):assert extra['head'][k]==http['head'][k]
extra_rows=[json.loads(x) for x in (out/'extra-nine-million-fresh/requests.jsonl').read_text().splitlines()]
assert len(extra_rows)==16 and collections.Counter(r['mode'] for r in extra_rows)=={'baseline':8,'candidate':8}
assert all(r['correct'] and r['height']==9000000 and r['response']['result']==r['expected'] and int(r['expected'],16)==207 and r['response']['id']==r['id'] and r['response']['jsonrpc']=='2.0' for r in extra_rows)
for summary in load('extra-nine-million-fresh/summary.json'):
 values=[r['milliseconds'] for r in extra_rows if r['mode']==summary['mode']];assert summary['n']==len(values) and statistics.median(values)==summary['median_ms']
assert load('extra-validation-status.json')['complete']
result={'verified':True,'internal_hardlinks_resolved_to_copies':hard_links,'members':s['members'],'http_requests':len(rows),'extra_http_requests':len(extra_rows),'cold_cache_residency_proofs':len(residencies),'private_read_files':len(private),'muhash_source_rows':sum(census['source_rows']),'muhash_root':bytes(census['muhash_root']).hex(),'history_restored_rows':sum(w['rows'] for w in restored['windows']),'baseline_route_seconds':sum(c['route_seconds'] for c in pipelines['baseline']['cycles']),'candidate_route_seconds':sum(c['route_seconds'] for c in pipelines['candidate']['cycles']),'baseline_route_cpu_seconds':sum(c['route_cpu_seconds'] for c in pipelines['baseline']['cycles']),'candidate_route_cpu_seconds':sum(c['route_cpu_seconds'] for c in pipelines['candidate']['cycles'])}
print(json.dumps(result,indent=2));(out.parent/'independent-verification.json').write_text(json.dumps(result,indent=2))
