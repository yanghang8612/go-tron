#!/usr/bin/env python3
"""Verify native status checkpoint, without asserting the unfinished raw gates."""
import hashlib
import io
import json
import pathlib
import zipfile

P = pathlib.Path(__file__).resolve().parent
sha = lambda b: hashlib.sha256(b).hexdigest()
b = (P/'native-checkpoint.zip').read_bytes()
assert len(b) == 23150 and sha(b) == 'f4670750c6cb57d31e58f97785c6768f6679e2dfe58ab4efbd608a1d30a00bcf'
with zipfile.ZipFile(io.BytesIO(b)) as z:
    assert z.testzip() is None and len(z.namelist()) == len(set(z.namelist())) == 20
    m = json.loads(z.read('evidence-manifest.json'))
    assert set(z.namelist()) == set(m) | {'evidence-manifest.json'}
    f = {n:z.read(n) for n in m}
    assert all(sha(f[n]) == h for n,h in m.items())
for n in f:
    if n.startswith('server-') and not n.endswith('.transport.json'):
        t = json.loads(f[n+'.transport.json'])
        assert t['bytes'] == len(f[n]) and t['sha256'] == sha(f[n])
s = json.loads(f['server-native-summary-06.json'])
assert s['complete'] and len(s['checks']) == 10 and all(r['returncode'] == 0 for r in s['checks'])
assert s['source_commit'] == '6bb9b95e2a0640dd7642bbd1c9c900ad1e03cc83'
assert s['source_inventory_sha256'] == '3281b8fcd833df0066efba23ac68c6ed329f9a1534ea5be5c75002655b6e9150'
assert not s['full_fixed_copy_acceptance_complete'] and not s['production_mutations']
p = json.loads(f['server-publish-status-01.json'])
assert not p['complete'] and not p['production_mutations'] and p['samples'] == 8
assert p['native_status_sha256'] == s['native_status_sha256']
assert p['old_binary_sha256'] == s['binaries']['old-publish.test']
assert p['new_binary_sha256'] == s['binaries']['new-publish.test']
for script, receipt in [('seal-native-and-publish.py','server-stage-seal-script.json'),('make-transfer.py','server-stage-transfer-script.json'),('run-acceptance.py','server-stage-run-acceptance.json'),('compare-ordered.py','server-stage-compare-ordered.json')]:
    r = json.loads(f[receipt])
    assert r['bytes'] == len(f[script]) and r['sha256'] == sha(f[script])
    if 'executed' in r:
        assert not r['executed']
print(json.dumps(dict(native_checks_passed_on_server=10, archived_publications_so_far=8,
                      full_native_logs_retrieved=False, publication_complete=False,
                      final_http_muhash_recovery_complete=False, production_mutations=False),indent=2))
