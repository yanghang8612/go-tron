#!/usr/bin/env python3
"""Verify native queue provenance, never infer performance or full acceptance."""
import hashlib
import io
import json
import pathlib
import zipfile

P = pathlib.Path(__file__).resolve().parent
sha = lambda b: hashlib.sha256(b).hexdigest()
b = (P/'native-dispatch.zip').read_bytes()
assert len(b) == 265972 and sha(b) == '522f74e11651df3f4e3119a89cdc005d6b12acc224d487046b30ce8791fcd967'
with zipfile.ZipFile(io.BytesIO(b)) as z:
    assert z.testzip() is None and len(z.namelist()) == len(set(z.namelist())) == 40
    m = json.loads(z.read('evidence-manifest.json'))
    assert set(z.namelist()) == set(m) | {'evidence-manifest.json'}
    f = {n: z.read(n) for n in m}
    assert all(sha(f[n]) == h for n,h in m.items())
for n in f:
    if n.startswith('server-') and not n.endswith('.transport.json'):
        t = json.loads(f[n+'.transport.json'])
        assert t['bytes'] == len(f[n]) and t['sha256'] == sha(f[n])
plan = json.loads(f['native-upload-plan.json'])
payload = f['native-source-payload.zip']
assert len(payload) == plan['bytes'] == 244147
assert sha(payload) == plan['sha256'] == '9d13efb13254af1db243d949ee391c9446d939738c2159df622724836ab4a031'
assert len(plan['chunks']) == 13
at = 0
for i, part in enumerate(plan['chunks']):
    chunk = payload[at:at+part['bytes']]
    r = json.loads(f['server-upload-part-%03d.json'%i])
    assert part['index'] == r['index'] == i
    assert len(chunk) == r['bytes'] == part['bytes']
    assert sha(chunk) == r['sha256'] == part['sha256']
    at += len(chunk)
assert at == len(payload)
with zipfile.ZipFile(io.BytesIO(payload)) as z:
    assert z.testzip() is None and len(z.namelist()) == len(set(z.namelist())) == 14
    m = json.loads(z.read('payload-manifest.json'))
    assert set(z.namelist()) == set(m) | {'payload-manifest.json'}
    assert all(sha(z.read(n)) == h for n,h in m.items())
    current = json.loads(z.read('source-inventory.json'))
    base = json.loads(z.read('base-inventory.json'))
    ident = json.loads(z.read('encoder-identity.json'))
    changed = {n for n,h in current.items() if base.get(n) != h}
    assert len(current) == 2149 and len(base) == 2148 and len(changed) == 5
    assert all(sha(z.read('source/'+n)) == current[n] for n in changed)
    assert sha(z.read('source-inventory.json')) == '3281b8fcd833df0066efba23ac68c6ed329f9a1534ea5be5c75002655b6e9150'
    assert ident['byte_identical_encoder'] and ident['bounded_ordered_pages'] and not ident['source_tree_dirty']
    assert ident['source_commit'] == plan['source_commit'] == '6bb9b95e2a0640dd7642bbd1c9c900ad1e03cc83'
seed = json.loads(f['server-seed-receipt.json'])
assert seed['installer_sha256'] == sha(f['install-native.py'])
assert seed['plan_sha256'] == sha(f['native-upload-plan.json'])
assert seed['disk_free_bytes'] > 100*10**9
start = json.loads(f['server-native-start-receipt.json'])
assert start['dispatched_now'] and start['proc_exists'] and start['pid'] == 7477
assert start['source_commit'] == ident['source_commit'] and start['payload_sha256'] == sha(payload)
assert start['production_identity_before'] == start['production_identity_after']
assert start['production_identity_before']['MainPID'] == '0'
unit = start['systemctl']
assert unit['MemoryLimit'] == '4294967296' and unit['CPUQuotaPerSecUSec'] == '2s' and unit['Nice'] == '15'
assert not start['production_mutations']
proof = json.loads(f['server-source-verification.json'])
assert proof == dict(files=2149, inherited_extra_go_files=24, mismatches=[], private_source_inodes=True, tests=1062)
status = json.loads(f['server-native-summary-01.json'])
assert status['phase'] == 'native-focused' and not status['complete']
assert not status['full_fixed_copy_acceptance_complete'] and not status['production_mutations']
print(json.dumps(dict(payload_verified=True, uploaded_parts=13, native_dispatched=True,
                      native_complete_at_archived_observation=False,
                      full_fixed_copy_acceptance_complete=False, production_mutations=False), indent=2))
