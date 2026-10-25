#!/usr/bin/env python3
"""Verify frozen staging/queue evidence only; never claims native completion."""
import hashlib
import io
import json
import pathlib
import zipfile

P = pathlib.Path(__file__).resolve().parent
ARCHIVE_SHA = 'be25281a1afe225fb319fb52c43ec37a1ea08863321d17847c2995b61044fe83'
PAYLOAD_SHA = '9743f128190d97e45c8da5770458167c92aeaf15a8a18e38881dbe8207f5449b'
COMMIT = '4d69f0720b27e7ab5c125b535975d042663694ac'


def sha(data):
    return hashlib.sha256(data).hexdigest()


def checked(data, expected, manifest_name):
    assert sha(data) == expected
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        assert z.testzip() is None
        names = z.namelist()
        assert len(names) == len(set(names))
        manifest = json.loads(z.read(manifest_name))
        assert set(names) == set(manifest) | {manifest_name}
        files = {}
        for name, digest in manifest.items():
            n = pathlib.PurePosixPath(name)
            assert not n.is_absolute() and '..' not in n.parts
            files[name] = z.read(name)
            assert sha(files[name]) == digest
        return files


def receipt(files, name, remote_name):
    data = files[name]
    transport = json.loads(files[name+'.transport.json'])
    assert transport['name'] == remote_name and transport['nonce']
    assert transport['bytes'] == len(data) and transport['sha256'] == sha(data)
    return json.loads(data)


def main():
    files = checked((P/'native-queue.zip').read_bytes(), ARCHIVE_SHA, 'evidence-manifest.json')
    payload = checked(files['native-source-payload.zip'], PAYLOAD_SHA, 'payload-manifest.json')
    plan = json.loads(files['native-upload-plan.json'])
    assert plan['bytes'] == len(files['native-source-payload.zip']) == 267702 and plan['sha256'] == PAYLOAD_SHA
    assert plan['source_commit'] == COMMIT
    offset = 0
    for index, chunk in enumerate(plan['chunks']):
        assert chunk['index'] == index
        data = files['native-source-payload.zip'][offset:offset+chunk['bytes']]
        assert len(data) == chunk['bytes'] and sha(data) == chunk['sha256']
        offset += chunk['bytes']
    assert offset == plan['bytes']
    identity = json.loads(payload['policy-identity.json'])
    source = json.loads(payload['source-inventory.json'])
    baseline = json.loads(payload['base-inventory.json'])
    assert identity['source_commit'] == COMMIT and len(source) == 2146 and len(baseline) == 2145
    assert identity['source_inventory_sha256'] == sha(payload['source-inventory.json'])
    assert identity['base_inventory_sha256'] == sha(payload['base-inventory.json'])
    changed = {n for n, h in source.items() if baseline.get(n) != h}
    assert len(changed) == 14 and changed == set(identity['changed_source_files'])
    assert {n.removeprefix('source/') for n in payload if n.startswith('source/')} == changed
    for n in changed:
        assert sha(payload['source/'+n]) == source[n]
    assert payload['prepare-native.py'] == files['prepare-native.py']
    start = receipt(files, 'server-native-start-receipt.json', 'native-start-receipt.json')
    waiting = receipt(files, 'server-native-summary-01.json', 'native-summary.json')
    observation = receipt(files, 'server-native-observation-01.json', 'native-observation-01.json')
    assert start['unit'] == 'gtron-auto-columns-native-20261026-01.service'
    assert start['payload_sha256'] == PAYLOAD_SHA and start['source_commit'] == COMMIT
    assert start['pid'] == observation['pid'] == 12225 and start['proc_exists'] and observation['proc_exists']
    assert start['systemctl']['ActiveState'] == 'active' and start['systemctl']['SubState'] == 'running'
    assert start['systemctl']['MemoryLimit'] == '4294967296' and start['systemctl']['CPUQuotaPerSecUSec'] == '2s' and start['systemctl']['Nice'] == '15'
    assert start['production_identity_before'] == start['production_identity_after']
    assert start['production_identity_after']['MainPID'] == '0'
    assert not start['production_mutations'] and not observation['production_mutations']
    assert not observation['candidate_source_exists']
    assert '/data/gtron/history-auto-columns-policy-20261026-01/prepare-native.py' in observation['ps']
    assert waiting == observation['native_summary']
    assert waiting['source_commit'] == COMMIT and waiting['phase'] == 'wait-frozen-acceptance-and-publication'
    assert waiting['checks'] == [] and not waiting['complete'] and not waiting['full_fixed_copy_acceptance_complete']
    assert observation['old_progress']['files'] == 4230 and not observation['old_progress']['complete']
    assert observation['old_progress']['before_bytes']-observation['old_progress']['after_bytes'] == 4095415793
    assert observation['old_acceptance'] == 'wait-conversion-and-native' and observation['old_publish'] == 'wait-final-http-and-census'
    old = receipt(files, 'frozen-experiment/server-progress-live-15.json', 'progress-live-15.json')
    assert old['progress']['files'] == 3850 and all(j['proc_exists'] and j['pid'] for j in old['jobs'].values())
    assert '391 28550' in old['jobs']['capture']['ps']
    print(json.dumps(dict(staged_payload_source_files_verified=len(changed), target_source_files_declared=len(source),
                          queue_was_live=True, queue_pid_at_observation=observation['pid'], observed_utc=observation['utc'],
                          native_compilation_started=False, native_complete=False, frozen_capture_files_at_observation=4230,
                          full_fixed_copy_acceptance_complete=False, production_mutations=False), indent=2))


if __name__ == '__main__':
    main()
