from pathlib import Path
import base64, hashlib, json, zlib
p=Path(__file__).resolve().parent
for name, digest in json.loads((p/'sha256.json').read_text()).items():
    assert hashlib.sha256((p/name).read_bytes()).hexdigest()==digest, name
seal=(p/'seal.txt').read_text().split()
raw=zlib.decompress(base64.b64decode((p/'packet.txt').read_text().strip()[7:]))
assert len(raw)==int(seal[3]) and hashlib.sha256(raw).hexdigest()==seal[1]
for name, text in json.loads(raw).items():
    assert (p/name).read_text()==text
s=json.loads((p/'status.json').read_text())
assert s['expected_identity'] and s['pid']==10692 and s['old_inventory_unchanged'] and not s['sample_errors']
rows=[json.loads(t) for t in (p/'runtime-samples.jsonl').read_text().splitlines()]
assert len(rows)==45
for row in rows:
    assert row['identity']['pid']==10692 and row['identity']['sha256']==s['binary_sha256']
    assert not row['errors'] and not row.get('new_error_lines')
    if 'disk_live_sample' in row:
        assert row['genesis_balance_correct'] and row['first_transaction_lookup_correct']
        assert all(x['correct'] for k in ['transfer_history_probes','tvm_counter_history_probes'] for x in row[k])
a=json.loads((p/'followup-metrics.json').read_text())['metrics']
b=json.loads((p/'confirm-metrics.json').read_text())['metrics']
prefix='state/snapshot/cold/'
assert a[prefix+'last/published_block']['value']==4803632
assert a[prefix+'history/budget/l0_sublevels']['value']==16
assert a[prefix+'history/budget/reason_bits']['value']==8
assert b[prefix+'last/published_block']['value']==4811231
assert b[prefix+'history/budget/l0_sublevels']['value']==14
assert b[prefix+'history/budget/moderate_l0_ready']['value']==1
assert b[prefix+'history/admission/busy_resource/builds']['value']>a[prefix+'history/admission/busy_resource/builds']['value']
assert a[prefix+'errors']['value']==b[prefix+'errors']['value']==0
print('PASS: transport, identity, history probes, L0 admission pause and subsequent cold-publication recovery')
