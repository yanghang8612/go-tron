#!/usr/bin/env python3
"""Recreate every transferred JSON packet from final ZIP bytes and verify receipts."""
import base64
import hashlib
import io
import json
import pathlib
import zipfile

P = pathlib.Path(__file__).resolve().parent
sha = lambda b: hashlib.sha256(b).hexdigest()
b = (P/'completed-transfer-receipts.zip').read_bytes()
assert len(b) == 33816 and sha(b) == '83d0377e72e41a7c9971518bc486d98e68c995210d5e67477cb82a1b74d78fc7'
with zipfile.ZipFile(io.BytesIO(b)) as z:
    assert z.testzip() is None and len(z.namelist()) == len(set(z.namelist())) == 86
    m = json.loads(z.read('evidence-manifest.json'))
    assert set(z.namelist()) == set(m) | {'evidence-manifest.json'}
    f = {n:z.read(n) for n in m}
    assert all(sha(f[n]) == h for n,h in m.items())
nonces = set()
count = 0
for tag,name,size,digest,chunks in [('native','native-complete.zip',229644,'6a0cd0ad3137575e9264d12a1ca31c76ccd082f1fe39e2af35dbdfe0865f2853',57),('core','publication-core.zip',55048,'dae16510d2c8e77c3483cd41970b2446183912f32ccf78bf68218c9249b64b91',14)]:
    data = (P/name).read_bytes()
    assert len(data) == size and sha(data) == digest
    plan_name = 'server-download-'+tag+'-plan.json'
    plan = json.loads(f[plan_name])
    pt = json.loads(f[plan_name+'.transport.json'])
    assert pt['bytes'] == len(f[plan_name]) and pt['sha256'] == sha(f[plan_name])
    assert plan['archive'] == name and plan['archive_bytes'] == size and plan['archive_sha256'] == digest
    assert plan['chunks'] == chunks and plan['chunk_bytes'] == 4096 and not plan['production_mutations']
    meta = {k:plan[k] for k in ['archive','archive_bytes','archive_sha256','device','inode','mtime_ns']}
    assert all(isinstance(meta[k],str) for k in ['device','inode','mtime_ns'])
    for i,offset in enumerate(range(0,len(data),4096)):
        part = data[offset:offset+4096]
        row = dict(meta,index=i,offset=offset,bytes=len(part),part_sha256=sha(part),data_b64=base64.b64encode(part).decode())
        raw = (json.dumps(row,sort_keys=True,separators=(',',':'))+'\n').encode()
        t = json.loads(f['server-download-%s-%03d.json.transport.json'%(tag,i)])
        assert t['name'] == 'download-%s-%03d.json'%(tag,i)
        assert t['bytes'] == len(raw) and t['sha256'] == sha(raw)
        assert len(t['nonce']) == 36 and t['nonce'] not in nonces
        nonces.add(t['nonce'])
        count += 1
for n in ['server-evidence-seals.json','server-core-evidence-seal.json','server-runtime-03.json']:
    t = json.loads(f[n+'.transport.json'])
    assert t['bytes'] == len(f[n]) and t['sha256'] == sha(f[n])
assert count == len(nonces) == 71
runtime = json.loads(f['server-runtime-03.json'])
assert runtime['unit'] == dict(ActiveState='inactive',ExecMainStatus='0',MainPID='0')
assert runtime['publish']['complete'] and runtime['publish']['samples'] == 60
print(json.dumps(dict(archives_verified=2,unique_fresh_packet_receipts=71,
                      measured_raw_rows_and_native_logs_retrieved=True,
                      unmeasured_full_plan_retained_on_server=True,production_mutations=False),indent=2))
