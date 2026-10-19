#!/usr/bin/env python3
"""Verify fixed corpus identity and independently recompute actual byte savings."""
import hashlib
import json
import re
import statistics
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent

def read(name):
    return json.loads((ROOT / name).read_text())

def delta(before, after):
    return dict(before=before, after=after, saved=before-after,
                saved_percent=(before-after)*100/before)

def latency(rows):
    # These are medians of per-file mean observations, not query-level p50s.
    return dict(baseline_file_mean_median_ns=statistics.median(r['BaselineQueryNS'] for r in rows),
                candidate_file_mean_median_ns=statistics.median(r['QueryNS'] for r in rows),
                median_paired_ratio=statistics.median(r['QueryNS']/r['BaselineQueryNS'] for r in rows))

if (ROOT / 'sha256.json').exists():
    for name, expected in read('sha256.json').items():
        assert hashlib.sha256((ROOT/name).read_bytes()).hexdigest() == expected, name

prior = json.loads((ROOT.parent/'20261018-generation-census/census.json').read_text())
export = read('server/corpus/export.json')
hot = read('server/hot-sst/results.json')
assert prior['muhash_verified'] and export['Complete'] and hot['Complete']
assert export['SourceRows'] == prior['source_rows']
assert export['SourceSHA256'] == prior['source_sha256']
assert export['Block'] == prior['block_num'] == 5715086
assert hot['Source'] == export and hot['Rows'] == export['Rows'] == [1349700,12478784]
comparisons = {}
for kind, count in zip(['account','uint64'], export['Rows']):
    for codec in ['snappy','none']:
        before, after = [hot['Experiments'][kind+'-'+variant+'-'+codec] for variant in ['baseline','compact']]
        assert before['rows'] == after['rows'] == count
        assert before['key_bytes'] == after['key_bytes']
        assert before['checksums_verified'] and after['checksums_verified']
        comparisons[kind+'-'+codec] = delta(before['sst_bytes'],after['sst_bytes'])

tx = read('server/tx-index/results.json')
old = json.loads((ROOT.parent/'20261019-other-storage/txindex-width.json').read_text())
assert len(tx) == old['Files'] == 1307
for new, original in zip(tx, old['Runs']):
    assert new['File'] == original['File'] and new['SHA256'] == original['SHA256']
    assert new['Rows'] == original['Rows'] and new['Before'] == original['Bytes']
assert sum(r['Rows'] for r in tx) == old['Rows'] == 491120809
assert sum(r['Before'] for r in tx) == old['Bytes']
comparisons['transaction-index'] = delta(sum(r['Before'] for r in tx),sum(r['After'] for r in tx))

selection = json.loads((ROOT.parent/'20261019-other-storage/cold-selection.json').read_text())
indexes = {r['path']:r for r in selection['files'] if r['path'].endswith('.idx')}
history = {}
for codec, path in [('zstd','server/history-index/results.json'),('snappy','server/history-index-snappy/results.json')]:
    rows = read(path)
    assert len(rows) == 32
    for size in [4096,16384]:
        group = [r for r in rows if r['PageBytes'] == size]
        assert len(group) == len(indexes) == 16
        assert {r['Path'] for r in group} == set(indexes)
        assert all(r['Before'] == indexes[r['Path']]['size'] for r in group)
        assert all(r.get('Codec','zstd') == codec for r in group)
        name = codec+'-'+str(size)
        comparisons['history-index-'+name] = delta(sum(r['Before'] for r in group),sum(r['After'] for r in group))
        history[name] = latency(group)

votes = read('server/votes-final/results.json')
assert len(votes) == 2 and not votes[0]['Paged'] and votes[1]['Paged']
assert all(r['Voters'] == 2048 and r['FullMuHashVerified'] for r in votes)
for key in ['LatestBytes','HistoryBytes','PreviousPayloadBytes']:
    comparisons['votes-'+key] = delta(votes[0][key],votes[1][key])

benches = {}
for file in ['uint64-before.log','uint64-after.log','account-read.log']:
    groups = {}
    for line in (ROOT/'server'/file).read_text().splitlines():
        match = re.match(r'(Benchmark\S+)\s+\d+\s+([\d.]+) ns/op',line)
        if match:
            groups.setdefault(match[1],[]).append(float(match[2]))
    assert len(groups) == 2 and all(len(v) == 3 for v in groups.values()), file
    benches[file] = {k:statistics.median(v) for k,v in groups.items()}
assert read('server/runtime-status.json')['complete']
assert read('server/snappy-status.json')['complete']
assert read('server/final-status.json')['complete']
assert 'ActiveState=active' in read('server/delivery-proof.json')['production_service']
assert not read('server/delivery-proof.json')['production_modified_by_validation']
# Runtime production sources must match the archived baseline plus recorded
# patches. Test-only extensions after a benchmark do not change measured code.
expected = read('local-runtime-final-sha.json')['files']
for patch in ['server/pages-patch.json','server/votes-api-patch.json']:
    for name, change in read(patch).items():
        if name not in expected:
            expected[name] = hashlib.sha256(subprocess.check_output(['git','show','bc939af2:'+name],cwd=ROOT.parents[3])).hexdigest()
        assert expected[name] == change['before'], name
        expected[name] = change['after']
for name, checksum in expected.items():
    if not name.endswith('_test.go'):
        assert hashlib.sha256((ROOT.parents[3]/name).read_bytes()).hexdigest() == checksum, name
analysis = dict(verified=True, source_block=export['Block'], actual_file_comparisons=comparisons,
                transaction_query=latency(tx), history_query=history, read_bench_medians_ns=benches,
                votes_append_ns=[v['AppendNS'] for v in votes], votes_enumerate_ns=[v['EnumerateNS'] for v in votes],
                limits=['SST experiment on complete authenticated 5.715M corpus; not live LSM attribution',
                        'history indexes are 16 deterministic ordinal samples, not all cold indexes',
                        'VotesIndex is synthetic 2048 appends/commits, with exact predecessor append algorithm',
                        'per-file means/medians and warm OS cache, not API-level latency percentiles or cold-device IO',
                        'no production reset/deployment and no full mainnet genesis replay in this task'])
(ROOT/'analysis.json').write_text(json.dumps(analysis,indent=2)+'\n')
print(json.dumps(analysis,indent=2))
