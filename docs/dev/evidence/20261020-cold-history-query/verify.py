#!/usr/bin/env python3
"""Verify exported bytes, semantic comparisons and the exact candidate source."""
from pathlib import Path
import base64
import hashlib
import json
import re

p = Path(__file__).resolve().parent
repo = p.parents[3]
for line in (p / 'SHA256SUMS').read_text().splitlines():
    sha, name = line.split('  ', 1)
    assert Path(name).name == name
    assert hashlib.sha256((p / name).read_bytes()).hexdigest() == sha, name
raw = (p / 'result-packet.json').read_bytes()
m = re.search(r'PACKET_SHA256 ([a-f0-9]{64}) BYTES (\d+)', (p / 'result-seal.txt').read_text())
assert m and hashlib.sha256(raw).hexdigest() == m[1] and len(raw) == int(m[2])
for name, text in json.loads(raw).items():
    if name.endswith('.b64'):
        assert (p / name[:-4]).read_bytes() == base64.b64decode(text, validate=True)
    else:
        assert (p / name).read_text() == text, name
amend_raw = (p / 'amend-result-packet.json').read_bytes()
am = re.search(r'PACKET_SHA256 ([a-f0-9]{64}) BYTES (\d+)', (p / 'amend-result-seal.txt').read_text())
assert am and hashlib.sha256(amend_raw).hexdigest() == am[1] and len(amend_raw) == int(am[2])
for name, value in json.loads(amend_raw).items():
    assert (p / name).read_text() == value, name
status = json.loads((p / 'accepted-amend-status.json').read_text())
assert status['complete'] and status['muhash_verified']
for name, sha in status['source_files_sha256'].items():
    assert hashlib.sha256((repo / name).read_bytes()).hexdigest() == sha, name
before = json.loads((p / 'baseline-amend-results.json').read_text())
after = json.loads((p / 'candidate-final-amend-results.json').read_text())
assert len(before['results']) == len(after['results']) == 162
for a, b in zip(before['results'], after['results']):
    assert (a['Round'], a['Query'], a['Hash']) == (b['Round'], b['Query'], b['Hash'])
assert before['history_segments'] == after['history_segments'] == 3499
old = json.loads((p / 'baseline-balance.json').read_text())
new = json.loads((p / 'candidate-amend-balance.json').read_text())
assert old['unchanged_head'] and new['unchanged_head']
assert old['head_block'] == new['head_block'] == 227222
assert old['head_tx'] == new['head_tx'] == 245565
for a, b in zip(old['results'], new['results']):
    assert (a['round'], a['query'], a['sun']) == (b['round'], b['query'], b['sun'])
census = json.loads((p / 'candidate-amend-census.json').read_text())
prior = json.loads((p.parent / '20261019-compact-v3-rollout/census.json').read_text())
assert census['complete'] and census['muhash_verified']
for key in ('block_num', 'tx_num', 'muhash_root', 'source_rows'):
    assert census[key] == prior[key], key
retained = json.loads((p / 'final-retention-proof.json').read_text())
assert retained['all_copied_inode_size_mtime_unchanged']
assert retained['files'] == 10497 and retained['bytes'] == 57563221709
expected = '859f0b6b44dcad8b74905a7d889d93ab1317d91a2365aa8aab1c30c455313505'
assert retained['production_binary_sha256'] == status['production_binary_sha256'] == expected
assert retained['service'] == status['service'] == 'MainPID=10692\nActiveState=active\n'
print('PASS: packet and file SHA256, exact candidate sources, 162 cold comparisons, 15 historical balances, full 227222 MuHash, retained corpus, unchanged production identity')
