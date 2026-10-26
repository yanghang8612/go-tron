#!/usr/bin/env python3
"""Verify exactly which binary started final acceptance; not its completion."""
import hashlib
import io
import json
import pathlib
import zipfile

P = pathlib.Path(__file__).resolve().parent
sha = lambda b: hashlib.sha256(b).hexdigest()
b = (P/'acceptance-dispatch.zip').read_bytes()
assert len(b) == 19992 and sha(b) == '5e5617e86ee32296421b73d99a6b738e728129c8cf5eec1486c14aa575f8ebd3'
with zipfile.ZipFile(io.BytesIO(b)) as z:
    assert z.testzip() is None and len(z.namelist()) == len(set(z.namelist())) == 18
    m = json.loads(z.read('evidence-manifest.json'))
    assert set(z.namelist()) == set(m) | {'evidence-manifest.json'}
    f = {n:z.read(n) for n in m}
    assert all(sha(f[n]) == h for n,h in m.items())
for n in f:
    if n.startswith('server-') and not n.endswith('.transport.json'):
        t = json.loads(f[n+'.transport.json'])
        assert t['bytes'] == len(f[n]) and t['sha256'] == sha(f[n])
r = json.loads(f['server-acceptance-start-receipt.json'])
assert r['dispatched_now'] and r['proc_exists'] and r['pid'] == 20092
assert r['production_identity_before'] == r['production_identity_after']
assert r['production_identity_before']['MainPID'] == '0' and not r['production_mutations']
assert r['source_commit'] == '6bb9b95e2a0640dd7642bbd1c9c900ad1e03cc83'
assert r['candidate_http_binary_sha256'] == '1258ec43a1500101193cc79bb40996cd488bac8169c1cb7fa9bd1529063c54f2'
assert r['candidate_snapshots_binary_sha256'] == '541fd9e1be5d88932e744d02397673406c2809183f7033cb2b6bc9c8dc7e5f0d'
assert r['systemctl']['MemoryLimit'] == '4294967296' and r['systemctl']['CPUQuotaPerSecUSec'] == '2s' and r['systemctl']['Nice'] == '15'
assert r['assessment_sha256'] == sha(f['publication-assessment.json'])
a = json.loads(f['publication-assessment.json'])
assert a['complete_raw_verification'] and a['raw_samples'] == 60 and a['timed_samples'] == 48
assert a['core_archive_sha256'] == sha((P/'publication-core.zip').read_bytes())
assert not a['default_policy_changed'] and not a['production_adoption_recommended']
for name,h in r['script_sha256'].items():
    assert sha(f[name]) == h
for n,receipt in [('publication-assessment.json','server-stage-publication-assessment.json'),('start-acceptance.py','server-stage-start-acceptance.json')]:
    d = json.loads(f[receipt])
    assert d['bytes'] == len(f[n]) and d['sha256'] == sha(f[n])
for i in [1,2,3]:
    p = json.loads(f['server-acceptance-pulse-%02d.json'%i])
    assert p['unit']['MainPID'] == '20092' and p['unit']['ActiveState'] == 'active'
    assert not p['complete'] and p['error'] is None
    assert p['source_before']['product_files'] == 2149 and p['source_before']['inherited_go_files'] == 24
last = json.loads(f['server-acceptance-pulse-03.json'])
assert last['phase'] == 'full-muhash' and '21000 20092' in last['processes']
print(json.dumps(dict(private_final_binary_acceptance_started=True,active_parent_at_observation=20092,
                      active_muhash_at_observation=21000,final_acceptance_complete=False,
                      production_mutations=False),indent=2))
