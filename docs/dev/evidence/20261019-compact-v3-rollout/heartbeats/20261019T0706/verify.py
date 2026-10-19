from pathlib import Path
import json,base64,zlib,hashlib
p=Path(__file__).resolve().parent
for name,h in json.loads((p/'sha256.json').read_text()).items():assert hashlib.sha256((p/name).read_bytes()).hexdigest()==h,name
s=json.loads((p/'summary.json').read_text());raw=zlib.decompress(base64.b64decode((p/'packet.txt').read_text().strip()[7:]));assert len(raw)==s['transport_decoded_bytes'];assert hashlib.sha256(raw).hexdigest()==s['transport_decoded_sha256']
d=json.loads(raw);assert json.loads(d['status.json'])==json.loads((p/'status.json').read_text());assert hashlib.sha256(d['runtime-samples.jsonl'].encode()).hexdigest()==s['runtime_samples_sha256']
a=[json.loads(x) for x in d['runtime-samples.jsonl'].splitlines()];assert len(a)==s['sample_count'];assert all(not r['errors'] and not r.get('new_error_lines') for r in a)
assert s['expected_identity'] and s['old_inventory_unchanged'] and not s['paused'];assert s['cold']['errors']==0
assert all(json.loads((p/'transfer-proof.json').read_text())['checks'].values())
print('PASS: heartbeat hashes, unchanged identity/old inventory, runtime samples and historical queries')
