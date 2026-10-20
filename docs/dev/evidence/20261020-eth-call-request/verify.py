#!/usr/bin/env python3
"""Verify transport, both source revisions, full HTTP results and cache proofs."""
import collections
import hashlib
import io
import json
import math
from pathlib import Path
import statistics
import subprocess
import tarfile
import zipfile

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
SERVER = HERE / 'server'
ROOT = '8fbf149684146a59b1650b318a768f0edbae0c8c1c3ceb2c5c610129c10f76a5'
PRODUCTION = '5848de48f2f24a128054ff0967cddfd7256c54029f674b317d37f98ac23c63c4'


def read(path):
    return json.loads(path.read_text())


def digest(data):
    return hashlib.sha256(data).hexdigest()


def normalize(value):
    # The unchanged test host used %x on Hash.String(); normalize that ASCII
    # representation rather than silently treating it as the 32-byte root.
    return bytes.fromhex(value).decode('ascii') if len(value) == 128 else value


transport = read(HERE / 'transport.json')
packet = (HERE / 'server-evidence.zip').read_bytes()
assert digest(packet) == transport['sha256']
assert digest((HERE / 'production-and-offline-proof.png').read_bytes()) == transport['proof_image_sha256']
with zipfile.ZipFile(io.BytesIO(packet)) as archive:
    for name in archive.namelist():
        assert not Path(name).is_absolute() and '..' not in Path(name).parts
        assert (SERVER / transport['aliases'].get(name, name)).read_bytes() == archive.read(name), name
for name, expected in read(SERVER / 'evidence-sha256.json').items():
    assert digest((SERVER / transport['aliases'].get(name, name)).read_bytes()) == expected, name

identity = read(SERVER / 'source-identity-final.json')
expected = {}
for mode, ref in [('baseline', '4e6978dd'), ('candidate', 'b6db66fc')]:
    archive_bytes = subprocess.check_output(['git', 'archive', ref], cwd=REPO)
    with tarfile.open(fileobj=io.BytesIO(archive_bytes)) as archive:
        for member in archive:
            if not member.isfile():
                continue
            key = mode + '/' + member.name
            if key in identity['source_sha256_by_version']:
                expected[key] = digest(archive.extractfile(member).read())
assert expected == identity['source_sha256_by_version']
assert len(expected) == 2132
assert identity['candidate'] == 'b6db66fcbde2d17f4b8fca099e5fd642e4435308'
assert identity['baseline'] == '4e6978dd'
assert identity['binaries']['baseline.test'] == '7a5b487b0b016526c8f0391778d36049c895a9183aabc93def9be81c98854363'
assert digest((SERVER / 'fixed_http_test.go.txt').read_bytes()) == identity['harness_sha256']
assert digest((SERVER / 'oracle_test.go.txt').read_bytes()) == identity['oracle_harness_sha256']
assert 'go1.25.5' in identity['go_version']

census = read(SERVER / 'census.json')
assert census['complete'] and census['muhash_verified']
assert census['block_num'] == 11818192 and census['tx_num'] == 601386177
assert census['source_rows'] == [3484117, 481, 141418493]
assert bytes(census['muhash_root']).hex() == ROOT
oracle = read(SERVER / 'oracle.json')
assert oracle['unchanged_head'] and oracle['head'] == census['block_num']
expected_values = {row['height']: row['expected'] for row in oracle['expected']}
assert int(expected_values[3144020], 16) == 6 and int(expected_values[3144021], 16) == 7

requests = [json.loads(line) for line in (SERVER / 'requests.jsonl').read_text().splitlines()]
assert len(requests) == 238 and len({r['id'] for r in requests}) == len(requests)
groups = collections.defaultdict(list)
for row in requests:
    assert row['correct'] and row['response']['id'] == row['id']
    assert row['response']['result'] == expected_values[row['height']]
    assert math.isfinite(row['milliseconds']) and row['milliseconds'] > 0
    if row['cache'] in ('warm', 'fresh-process-os-uncontrolled', 'private-os-cold-fresh-process'):
        groups[(row['mode'], row['cache'], row['tag'])].append(row['milliseconds'])
assert sum(len(values) for values in groups.values()) == 140
expected_n = {'warm': 5, 'fresh-process-os-uncontrolled': 3, 'private-os-cold-fresh-process': 2}
for (mode, cache, tag), values in groups.items():
    assert len(values) == expected_n[cache], (mode, cache, tag)
assert len(groups) == 42
for summary in read(SERVER / 'summary.json'):
    values = groups[summary['mode'], summary['cache'], summary['tag']]
    assert summary['n'] == len(values)
    assert summary['median_ms'] == statistics.median(values)
    assert summary['min_ms'] == min(values) and summary['max_ms'] == max(values)

joined = read(SERVER / 'joined-request-metrics.json')
assert len(joined) == len(requests)
assert {r['id'] for r in joined} == {r['id'] for r in requests}
for directory in SERVER.iterdir():
    ready_file = directory / 'ready.json'
    if directory.is_dir() and ready_file.exists():
        ready = read(ready_file)
        assert ready['read_only'] and ready['head'] == census['block_num']
        assert normalize(ready['root']) == ROOT
        assert normalize(ready['head_hash']) == bytes(census['block_hash']).hex()
        assert '\nPASS\n' in (directory / 'host.log').read_text()

private = {row['path']: row for row in read(SERVER / 'private-files.json')}
assert set(read(SERVER / 'read-files.json')) <= set(private)
inode_proof = read(SERVER / 'private-inode-proof.json')
assert len(inode_proof) == len(private)
assert all(row['source_inode'] != row['private_inode'] and row['bytes'] == private[row['path']]['bytes'] for row in inode_proof)
residency = [read(file) for file in SERVER.glob('os-cold-*-residency.json')]
assert len(residency) == 28 and max(row['fraction'] for row in residency) < .01

diagnostics = read(SERVER / 'diagnostic-counts.json')
assert len(diagnostics) == 14
for row in diagnostics:
    assert row['result']['result'] == expected_values[row['height']]
    headers = row['stats']['header']
    assert headers['hits'] + headers['misses'] >= headers['distinct_keys']
    for stats in row['stats']['tail'].values():
        if stats.get('rows_limit'):
            assert stats['rows_at_rejection'] > 16384
        if stats.get('bytes_limit'):
            assert stats['bytes_at_rejection'] > 8 << 20

final_census = read(SERVER / 'census-final.json')
assert final_census['complete'] and final_census['muhash_verified']
for key in ('block_num', 'block_hash', 'tx_num', 'muhash_root', 'source_rows', 'source_sha256'):
    assert final_census[key] == census[key], key
rechecked = read(SERVER / 'oracle-rechecked.json')
assert rechecked['expected'] == oracle['expected'] and rechecked['unchanged_head']
assert all(row['stats']['memo']['peak_retention_charge'] <= 4 << 20 for row in diagnostics)
assert any(row['stats']['memo']['hits'] > 0 for row in diagnostics)
assert all(row['original_and_private_match'] for row in read(SERVER / 'metadata-integrity.json'))
assert '\nPASS\n' in (SERVER / 'muhash-final.log').read_text()
assert '\nPASS\n' in (SERVER / 'oracle-rechecked.log').read_text()
proof = read(SERVER / 'final-proof.json')
assert proof['identity'] == {'pid': 28737, 'sha256': PRODUCTION}
assert proof['no_deployment'] and proof['no_production_restart'] and proof['old_database_retained']
assert proof['data_dir_inode'] == 94642617
assert proof['muhash_verified'] and proof['fixed_root'] == ROOT
assert proof['node']['currentBlock'] > census['block_num'] and not proof['node']['syncInfo']['paused']
assert proof['http_requests_verified'] == 238 and proof['diagnostic_requests_verified'] == 14
assert proof['read_only_host_runs'] == 79 and proof['performance_samples'] == 140
for name, expected_hash in read(HERE / 'local/sha256.json').items():
    assert digest((HERE / 'local' / name).read_bytes()) == expected_hash, name
for name in ('range-hint-state.log', 'range-hint-race.log', 'snapshots-final-b6.log'):
    assert (HERE / 'local' / name).read_text().startswith('ok '), name
assert not (HERE / 'local/range-hint-vet.log').read_text()
assert digest((SERVER / 'offline_request_muhash_test.go.txt').read_bytes()) == identity['muhash_harness_sha256']
assert 'FAIL' not in (SERVER / 'native-correctness.log').read_text()
print('PASS: exact baseline/candidate sources; identical HTTP host; complete source-fold MuHash; 238 HTTP calls and 140 latency samples; before-image oracle; 28 cold-cache residency proofs and independent inodes; bounded request memo; final candidate source-fold recheck; original production PID/SHA and retained old database')
print(json.dumps({'warm_medians_ms': [row for row in read(SERVER / 'summary.json') if row['cache'] == 'warm'], 'private_cache_files': len(private), 'maximum_resident_fraction_before_startup': max(row['fraction'] for row in residency)}, indent=2))
