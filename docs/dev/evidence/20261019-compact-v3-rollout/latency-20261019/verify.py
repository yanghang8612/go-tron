from pathlib import Path
import base64
import hashlib
import json
import zlib

p = Path(__file__).resolve().parent
for name, expected in json.loads((p/'sha256.json').read_text()).items():
    assert hashlib.sha256((p/name).read_bytes()).hexdigest() == expected, name
for name, expected in json.loads((p/'transport-checks.json').read_text()).items():
    raw = zlib.decompress(base64.b64decode((p/name).read_text().strip()[7:]))
    assert len(raw) == expected['bytes']
    assert hashlib.sha256(raw).hexdigest() == expected['sha256']
    for filename, contents in json.loads(raw).items():
        assert (p/filename).read_text() == contents, filename
m = json.loads((p/'matrix.json').read_text())
assert m['identity_before'] == m['identity_after']
assert m['identity_before']['MainPID'] == '10692'
assert m['identity_before']['sha256'] == '859f0b6b44dcad8b74905a7d889d93ab1317d91a2365aa8aab1c30c455313505'
assert len(m['rows']) == 16 and all(not row.get('error') and row.get('correct', True) for row in m['rows'])
a = json.loads((p/'cpu-20261019T101936/attribution.json').read_text())
assert a['query_cpu_ns'] == 10370000000
assert sum(a['exclusive_high_level_phases_ns'].values()) == a['query_cpu_ns']
assert a['cumulative_functions']['readStateAccountKVAsOf']['cpu_ns'] == 9200000000
assert a['cumulative_functions']['IterateStateDomainChangesByBlockRangeContext']['cpu_ns'] == 6910000000
assert a['cumulative_functions']['historyIndexPages']['cpu_ns'] == 0
assert all(row['identical'] for row in json.loads((p/'source-comparison.json').read_text())['files'])
c = json.loads((p/'manifest-census.json').read_text())
assert c['overlapping_history_segments_after_target_tx'] == 401
assert all(row['version'] == 8 and row['page_code'] == 2147487744 for row in c['index_header_samples'])
assert 'first-change seek missed unindexed block 3' in (p/'baseline-tail-repro.log').read_text()
assert '--- FAIL: TestDiagnosticColdLatencyFirstSeekUnindexedTail' in (p/'baseline-tail-repro.log').read_text()
candidate = (p/'candidate-tests.log').read_text()
assert '--- PASS: TestDiagnosticColdLatencyFirstSeekUnindexedTail' in candidate
assert '--- FAIL:' not in candidate and candidate.count('\n--- PASS:') == 52
print('PASS: live identity, sealed loopback data, query CPU evidence, format/source comparison, baseline defect and isolated candidate')
