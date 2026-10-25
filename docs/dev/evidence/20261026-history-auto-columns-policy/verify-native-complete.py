#!/usr/bin/env python3
"""Verify successful native policy checks, separately from fixed-copy results."""
import hashlib
import io
import json
import pathlib
import zipfile

P = pathlib.Path(__file__).resolve().parent


def sha(data):
    return hashlib.sha256(data).hexdigest()


def main():
    data = (P/'native-complete.zip').read_bytes()
    assert len(data) == 104596 and sha(data) == '712a640f494b3e4f5ca5f8067533c2e709215898a98612f4d1cb60e705f3ee3e'
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        assert z.testzip() is None
        names = z.namelist()
        assert len(names) == len(set(names)) == 9
        manifest = json.loads(z.read('evidence-manifest.json'))
        assert set(names) == set(manifest) | {'evidence-manifest.json'}
        files = {name: z.read(name) for name in manifest}
        assert all(sha(files[name]) == digest for name, digest in manifest.items())
    def receipt(name, remote):
        raw = files[name]
        meta = json.loads(files[name+'.transport.json'])
        assert meta['name'] == remote and meta['nonce']
        assert meta['bytes'] == len(raw) and meta['sha256'] == sha(raw)
        return json.loads(raw)
    native = receipt('server-native-summary-final.json', 'native-summary.json')
    source = receipt('server-native-source-verification.json', 'source-verification.json')
    gates = receipt('frozen-v5/server-rollout-gates-02.json', 'rollout-gates-02.json')
    identity = json.loads(files['policy-identity.json'])
    assert native['source_commit'] == identity['source_commit'] == '4d69f0720b27e7ab5c125b535975d042663694ac'
    assert native['source_inventory_sha256'] == sha(files['source-inventory.json']) == '2222de83d69b277003135e120b8e5dba2a9e6c06af07ef670ee204dcddf0bf98'
    assert len(json.loads(files['source-inventory.json'])) == native['source_files'] == source['files'] == 2146
    assert source['tests'] == 1060 and source['mismatches'] == [] and source['private_source_inodes']
    assert native['complete'] and native['phase'] == 'complete' and native['native_checks_only']
    assert not native['full_fixed_copy_acceptance_complete']
    labels = {'native-focused', 'native-packages', 'native-race', 'native-vet',
              'native-sapling-build', 'native-snapshots-build', 'native-http-build'}
    assert len(native['checks']) == 7 and {item['label'] for item in native['checks']} == labels
    assert all(item['returncode'] == 0 and item['seconds'] > 0 for item in native['checks'])
    assert native['go_version'] == 'go version go1.25.5 linux/amd64'
    assert native['binaries'] == {
        'gtron-policy': '7ffec8473efe83ec5a45295263837201fcfbd78137f83eac3b2b72d0e60fe175',
        'policy-http.test': '40fd5c92907e60da060119577e26e8eb578932ac695e702cc2a2980a4bac449b',
        'policy-snapshots.test': '067ca9d546e349f5fa09bdd1b6e9b130074566c33bd18808c1db3f29b2813f99'}
    assert gates['native_policy']['complete'] and gates['native_policy']['checks'] == native['checks']
    assert gates['jobs']['native_policy']['pid'] == 0 and not gates['jobs']['native_policy']['proc_exists']
    assert gates['publication']['complete'] and gates['acceptance']['complete']
    assert all(not item['production_mutations'] for item in (native, gates))
    print(json.dumps(dict(source_commit=native['source_commit'], source_files=2146,
                         native_checks_passed=7, private_source_inodes=True,
                         native_complete_at=native['utc'],
                         full_fixed_copy_acceptance_complete=False, deployment_performed=False), indent=2))


if __name__ == '__main__':
    main()
