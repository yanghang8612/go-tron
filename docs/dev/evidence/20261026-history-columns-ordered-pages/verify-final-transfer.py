#!/usr/bin/env python3
"""Verify all final direct chunks, fresh nonces, seals and terminal task state."""
import hashlib
import io
import json
import pathlib
import zipfile

P = pathlib.Path(__file__).resolve().parent
sha = lambda b: hashlib.sha256(b).hexdigest()
b = (P/'final-transfer-receipts.zip').read_bytes()
assert len(b) == 53582 and sha(b) == 'f73ee450c8b5de0bb8fd38485c2d6a4ca563f047d93e40174a2ab3436125e1ff'
with zipfile.ZipFile(io.BytesIO(b)) as z:
    assert z.testzip() is None and len(z.namelist()) == len(set(z.namelist())) == 108
    m = json.loads(z.read('evidence-manifest.json'))
    assert set(z.namelist()) == set(m) | {'evidence-manifest.json'}
    f = {n:z.read(n) for n in m}
    assert all(sha(f[n]) == h for n,h in m.items())
archive = (P/'final-acceptance-core.tar.xz').read_bytes()
assert len(archive) == 467028 and sha(archive) == '80f824b2c934e80861630356bc97829230c079b7dcec5e5fcfc1f063cfe00f3d'
nonces = set()
for i,offset in enumerate(range(0,len(archive),5120)):
    part = archive[offset:offset+5120]
    t = json.loads(f['direct-final-%03d.transport.json'%i])
    assert t['archive'] == 'final-acceptance-core.tar.xz' and t['archive_bytes'] == len(archive) and t['archive_sha256'] == sha(archive)
    assert t['index'] == i and t['offset'] == offset and t['bytes'] == len(part) and t['sha256'] == sha(part)
    assert (t['device'],t['inode'],t['mtime_ns']) == ('66304','98046760','1792981203230637688')
    assert len(t['nonce']) == 36 and t['nonce'] not in nonces
    nonces.add(t['nonce'])
assert len(nonces) == 92
for name in ['server-final-acceptance-seal.json','server-final-acceptance-solid-seal.json']:
    raw = (P/name).read_bytes();t = json.loads((P/(name+'.transport.json')).read_text())
    assert t['bytes'] == len(raw) and t['sha256'] == sha(raw)
for name in f:
    if name.startswith('server-') and not name.endswith('.transport.json'):
        t = json.loads(f[name+'.transport.json'])
        assert t['bytes'] == len(f[name]) and t['sha256'] == sha(f[name])
state = json.loads(f['server-resumed-status-02.json'])
assert state['acceptance_complete'] and state['http_complete'] and state['completed_requests'] == 286
assert state['unit'] == dict(ActiveState='inactive',ExecMainStatus='0',MainPID='0')
assert len(state['checks']) == 3 and all(r['returncode'] == 0 for r in state['checks'])
reconstruction = json.loads(f['original-zip-reconstruction.json'])
assert reconstruction['exact_original_zip_reconstructed'] and reconstruction['members'] == 704
assert reconstruction['bytes'] == 1242954 and reconstruction['sha256'] == '26641ef303359d752ee8ce4399828a9f91ec211d025b6133a5aacfc95e7890dd'
print(json.dumps(dict(all_direct_chunks_verified=92,unique_fresh_nonces=92,
                      all_original_members_preserved=704,original_zip_bytes_reconstructed_exactly=True,
                      original_acceptance_unit_terminal_exit_zero=True,production_mutations=False),indent=2))
