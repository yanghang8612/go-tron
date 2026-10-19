from pathlib import Path
import base64, hashlib, json, zlib
p=Path(__file__).resolve().parent
for n,h in json.loads((p/'sha256.json').read_text()).items():
    assert hashlib.sha256((p/n).read_bytes()).hexdigest()==h,n
s=(p/'seal.txt').read_text().split()
b=zlib.decompress(base64.b64decode((p/'packet.txt').read_text().strip()[7:]))
assert len(b)==int(s[3]) and hashlib.sha256(b).hexdigest()==s[1]
for n,t in json.loads(b).items():
    assert (p/n).read_text()==t
s=json.loads((p/'status.json').read_text())
assert s['expected_identity'] and s['pid']==10692 and s['old_inventory_unchanged'] and not s['sample_errors']
r=[json.loads(t) for t in (p/'runtime-samples.jsonl').read_text().splitlines()]
assert len(r)==45
for x in r:
    assert x['identity']['pid']==10692 and x['identity']['sha256']==s['binary_sha256']
    assert not x['errors'] and not x.get('new_error_lines')
    if 'disk_live_sample' in x:
        assert x['genesis_balance_correct'] and x['first_transaction_lookup_correct']
        assert all(y['correct'] for k in ['transfer_history_probes','tvm_counter_history_probes'] for y in x[k])
a=json.loads((p/'prior-summary.json').read_text())
b=json.loads((p/'summary.json').read_text())
assert b['cold_published_block']==b['cold_pruned_block']==6542142
assert b['eligible_backlog_blocks']==262695<a['fallback_observation']['eligible_backlog_blocks']
assert b['pressure_snapshot']['l0_sublevels']==3 and b['pressure_snapshot']['hard']==0
assert b['cold_errors']==0
print('PASS: transport, identity, historical probes and return of cold backlog to approximately26万')
