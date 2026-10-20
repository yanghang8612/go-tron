#!/usr/bin/env python3
"""Verify sealed offline evidence and the exact final reader sources."""
import base64
import hashlib
import json
import re
import statistics
import zlib
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
EXPECTED_ROOT = '194af866cffd2bec13f1b78269f137a33d736204078431cc466b446a911a4ffa'
PRODUCTION_SHA = '5848de48f2f24a128054ff0967cddfd7256c54029f674b317d37f98ac23c63c4'

def read_json(path):
    return json.loads(path.read_text())

for phase in ('v1', 'final', 'aux'):
    meta = read_json(HERE / (phase + '-transport.json'))
    packet = (HERE / (phase + '-packet.zlib')).read_bytes()
    assert hashlib.sha256(packet).hexdigest() == meta['packet_sha256']
    files = json.loads(zlib.decompress(packet))['files']
    for name, item in files.items():
        raw = base64.b64decode(item['data'])
        assert hashlib.sha256(raw).hexdigest() == item['sha256'], name
        assert (HERE / meta['aliases'][name]).read_bytes() == raw, name

final = HERE / 'server-final'
status = read_json(final / 'status.json')
assert status['complete'] and status['phase'] == 'verified-final'
assert status['early_positive_stop_verified']
assert status['before_identity'] == status['after_identity']
assert status['after_identity']['binary_sha256'] == PRODUCTION_SHA
assert status['production_mutations'] is False
# Linux race build timed out before tests. Never silently count it as passed.
assert status['server_race_verified'] is False
assert '900s' in status['server_race_note']
aux = HERE / 'server-aux'
aux_result = read_json(aux / 'gofuzz-race-status.json')
assert aux_result['returncode'] == 0 and aux_result['tags'] == 'gofuzz'
aux_proof = read_json(aux / 'aux-race-proof.json')
assert aux_proof['production_sha256'] == PRODUCTION_SHA
assert aux_proof['pid'] == status['after_identity']['pid']
assert 'ok ' in (aux / 'final-gofuzz-race.log').read_text()
for name, digest in aux_proof['source_sha256'].items():
    assert hashlib.sha256((ROOT / 'core/rawdb' / name).read_bytes()).hexdigest() == digest

source = read_json(final / 'source-identity-final.json')
local = read_json(HERE / 'local-source-identity.json')
for name, digest in local.items():
    assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == digest, name
    assert source['files_sha256']['candidate/' + name] == digest, name

census = read_json(final / 'final-census.json')
assert census['complete'] and census['muhash_verified']
assert bytes(census['muhash_root']).hex() == EXPECTED_ROOT
assert census['source_rows'] == [4608, 0, 3042]
balance = read_json(final / 'final-balance-result.json')
assert balance['head_block'] == 227222 and balance['head_tx'] == 245565
assert balance['unchanged_head'] and len(balance['results']) == 15
expected = {'genesis-sender': 99000000000000000, 'sender-341': 99000000000000000,
            'sender-342': 65748192475900000, 'receiver-341': 0,
            'receiver-342': 33251807524000000}
assert all(row['sun'] == expected[row['query']] for row in balance['results'])
event = read_json(final / 'event-result.json')
assert event['rows'] == 1 and event['protobuf_address_bytes'] == 20
assert event['original_payload_equal'] and event['block'] == 3144021
assert event['global_index_candidates'] == [3142019]
assert 'filtered 0 vs expected 1' in (final / 'baseline-event.log').read_text()
assert 'PASS' in (final / 'candidate-event.log').read_text()
ref = event['source_ref']
assert hashlib.sha256((final / 'event-copy' / ref['path']).read_bytes()).hexdigest() == ref['checksum'].split(':')[1]

for name in ('final-packages.log', 'final-native-correctness.log'):
    log = (final / name).read_text()
    assert 'ok ' in log or 'ok\t' in log, name
    assert not re.search(r'(^FAIL|--- FAIL:|DATA RACE)', log, re.M), name
for name in ('query-tail-final-race-v3.log', 'query-tail-log-race.log',
             'query-tail-final-native-amended.log'):
    log = (HERE / name).read_text()
    assert 'ok ' in log or 'ok\t' in log, name
    assert not re.search(r'(^FAIL|--- FAIL:|DATA RACE)', log, re.M), name
assert not (HERE / 'query-tail-vet-final.log').read_text().strip()
product = (HERE / 'query-tail-product-native-final.log').read_text().splitlines()
assert sum(row.startswith('ok') for row in product) == 64
assert sum(row.startswith('?') for row in product) == 15
assert not any(row.startswith('FAIL') for row in product)

bench = {}
for mode, ns in re.findall(r'BenchmarkQueryTailDensePointReads/([\w-]+)-\d+\s+1\s+(\d+) ns/op', (final / 'final-dense-bench.log').read_text()):
    bench.setdefault(mode, []).append(int(ns) / 1e6)
assert all(len(rows) == 3 for rows in bench.values()) and len(bench) == 6
medians = {mode: statistics.median(rows) for mode, rows in bench.items()}
assert medians['query-cache'] < medians['owning-oracle']
assert medians['early-query-cache'] < medians['early-owning-oracle']
manifest = read_json(HERE / 'evidence-sha256.json')
for name, digest in manifest.items():
    assert hashlib.sha256((HERE / name).read_bytes()).hexdigest() == digest, name
print('PASS: sealed files, final source identity, actual log regression, 15 balances, full checkpoint MuHash, native/local race and auxiliary Linux gofuzz race; native-CGo server race explicitly unverified')
print(json.dumps({'server_component_median_ms': medians}, indent=2))
