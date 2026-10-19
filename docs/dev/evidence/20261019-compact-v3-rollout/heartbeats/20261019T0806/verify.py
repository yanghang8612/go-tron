from pathlib import Path
import json,hashlib,base64,zlib
p=Path(__file__).resolve().parent
for f,h in json.loads((p/'sha256.json').read_text()).items():assert hashlib.sha256((p/f).read_bytes()).hexdigest()==h,f
for f,v in json.loads((p/'transport-checks.json').read_text()).items():
 raw=zlib.decompress(base64.b64decode((p/f).read_text().strip()[7:]));assert len(raw)==v['bytes'];assert hashlib.sha256(raw).hexdigest()==v['sha256']
 first=json.loads(raw)
 for n,t in first.items():
  if (p/n).is_file():assert (p/n).read_text()==t,n
s=json.loads((p/'status.json').read_text());u=json.loads((p/'monitor-update.json').read_text());t=json.loads((p/'tvm-probe.json').read_text());summary=json.loads((p/'summary.json').read_text())
assert s['expected_identity'] and s['old_inventory_unchanged'] and not s['sample_errors'];assert not u['latest']['errors'];assert summary['pid']==10692 and not summary['paused']
assert all(t['cold_contract']['checks'].values());assert all(v['matches'] for v in t['view_calls']);assert all(t['checks'].values());assert t['code_layout']['wallet_bytecode_ends_with_runtime']
assert all(v['correct'] for v in u['latest']['tvm_counter_history_probes']);assert json.loads((p/'tvm-local-latency.json').read_text())['complete']
assert hashlib.sha256((p/'monitor-tvm.py').read_bytes()).hexdigest()==u['monitor_sha256'];assert all(json.loads((p/'transfer-proof.json').read_text())['checks'].values())
print('PASS: TVM transition evidence, cold counter history/log/receipt, getters, stable identity, old inventory and bounded monitoring')
