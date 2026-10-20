from pathlib import Path
import base64,hashlib,json,zlib,re,subprocess
p=Path(__file__).resolve().parent;root=p.parents[3]
sha=lambda b:hashlib.sha256(b).hexdigest()
for name,digest in json.loads((p/'sha256.json').read_text()).items():assert sha((p/name).read_bytes())==digest,name
m=json.loads((p/'transport-meta.json').read_text());wire=(p/'packet.json.zlib').read_bytes();encoded=base64.b64encode(wire).decode();assert len(encoded)==m['base64_chars'];parts=[encoded[i:i+80000] for i in range(0,len(encoded),80000)];assert len(parts)==m['parts'] and [sha(x.encode()) for x in parts]==m['part_sha256']
raw=zlib.decompress(wire);assert len(raw)==m['packet_bytes'] and sha(raw)==m['packet_sha256'];packet=json.loads(raw)
rev='9030ce7a31016491ece014ac76e5656ec149417e';assert packet['source']==rev
for name,row in packet['files'].items():
 b=base64.b64decode(row['data']) if row['encoding']=='base64' else row['data'].encode();assert len(b)==row['bytes'] and sha(b)==row['sha256'] and (p/name).read_bytes()==b,name
for name,digest in json.loads((p/'source-hashes.json').read_text()).items():assert sha(subprocess.check_output(['git','show',rev+':'+name],cwd=root))==digest,name
identity=json.loads((p/'identity.json').read_text());f=json.loads((p/'final-status.json').read_text());assert identity['source']==rev and identity['binary_sha256']==f['binary_sha256']=='5848de48f2f24a128054ff0967cddfd7256c54029f674b317d37f98ac23c63c4';assert identity['pid']==f['pid']==28737 and f['same_process'];assert identity['old_inventory_unchanged'] and identity['datadir_inode']==f['datadir_inode']==94642617
before=json.loads((p/'before-deploy-wallet.json').read_text());assert identity['wallet']['currentBlock']>=before['currentBlock'];assert f['latest_runtime']['wallet']['currentBlock']>identity['wallet']['currentBlock'];assert not f['latest_runtime']['wallet']['syncInfo']['paused']
log=(p/'deploy-complete.log').read_text();assert 'running Sapling-enabled tests' in log and 'successfully deployed root-owned mainnet release '+rev in log;assert not any(x.startswith(('FAIL','--- FAIL')) for x in log.splitlines())
for phase,correct,timeouts in [('before-local',23,1),('after-local',20,4)]:
 rows=[json.loads(x) for x in (p/(phase+'.jsonl')).read_text().splitlines()];assert len(rows)==24 and sum(x['correct'] for x in rows)==correct and sum(x['rc']==28 for x in rows)==timeouts;assert not any('result' in x.get('response',{}) and not x['correct'] for x in rows)
assert f['transaction_location_correct'] and f['counter_receipt_logs_unfiltered_correct'];assert f['counter_address_filter_correct'] is False and f['counter_logs']['result']==[]
assert f['critical_lines']==[] and f['runtime_errors']==[]
assert not any(re.search(r'\b(ERROR|FATAL)\b|panic:|Sync paused|MuHash.*(mismatch|error|fail)|corruption|commitment.*(mismatch|fail)',x,re.I) for x in (p/'journal-after.log').read_text().splitlines())
rows=[json.loads(x) for x in (p/'sealed-runtime-samples.jsonl').read_text().splitlines()];assert len(rows)==15 and any('unindexed_tail_blocks_at_sample' in x for x in rows);assert all(x['identity']['sha256']==identity['binary_sha256'] for x in rows)
for name,versions in json.loads((p/'log-filter-source-identity.json').read_text()).items():assert versions['659ab078']==versions['9030ce7a']
assert '--- FAIL: TestRolloutExistingColdLogAddressFilter' in (p/'address-filter-existing-repro.log').read_text()
print('PASS: transport/files/source identity, native release, retained directories, all returned fixed-probe values and transaction locations; known timeouts and existing filtered-log failure explicitly preserved. No current-height full MuHash recomputation is claimed.')
