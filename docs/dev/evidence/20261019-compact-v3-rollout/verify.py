from pathlib import Path
import hashlib,json,base64,zlib
p=Path(__file__).resolve().parent
for name,expected in json.loads((p/'sha256.json').read_text()).items():
 assert hashlib.sha256((p/name).read_bytes()).hexdigest()==expected,name
for name,expected in json.loads((p/'transport-checks.json').read_text()).items():
 s=(p/name).read_text().strip();assert s.startswith('GTRROL:')
 raw=zlib.decompress(base64.b64decode(s[7:]));assert len(raw)==expected['decoded_bytes']
 assert hashlib.sha256(raw).hexdigest()==expected['decoded_sha256']
c=json.loads((p/'census.json').read_text());assert c['complete'] and c['muhash_verified']
b=json.loads((p/'census-head-binding.json').read_text());assert b['equal'] and b['census_block_hash']==bytes(c['block_hash']).hex()
t=json.loads((p/'transfer-proof.json').read_text());assert all(t['checks'].values())
f=json.loads((p/'cold-format-verify.json').read_text());assert f['complete'] and f['report']['ActiveSegments']==99
idx=[i for i in json.loads((p/'cold-proof.json').read_text())['index_pages'] if i['version']==8]
assert len(idx)==15 and all(i['codec']=='snappy' and i['page_bytes']==4096 and i['header_directory_crc_valid'] for i in idx)
r=json.loads((p/'final-live-status.json').read_text());assert r['old_inventory_unchanged'] and 'ActiveState=active' in r['service'] and 'ActiveState=active' in r['deploy_timer']
a=[json.loads(x) for x in (p/'runtime-samples.jsonl').read_text().splitlines()]
assert all(not r['errors'] and not r.get('new_error_lines') for r in a)
assert a[-1]['first_transaction_lookup_correct'] and all(x['correct'] for x in a[-1]['transfer_history_probes'])
print('PASS: file hashes, transport hashes, full MuHash, head binding, history, cold formats, retained inventory and runtime samples')
