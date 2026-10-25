#!/usr/bin/env python3
"""Recompute the census from the sealed per-file receipts, using only stdlib.

This verifies accounting/provenance; rerunning the Go scanner with the immutable
server files is required to independently re-prove their bytes and decoding.
"""
import collections
import gzip
import hashlib
import json
from pathlib import Path

P = Path(__file__).resolve().parent

def read(name):
    data = (P / name).read_bytes()
    if name.endswith('.gz'):
        data = gzip.decompress(data)
    return json.loads(data)

checks = read('SHA256SUMS.json')
for name, digest in checks.items():
    assert hashlib.sha256((P / name).read_bytes()).hexdigest() == digest, name
summary = read('census-summary.json')
status = read('census-status.json')
plan = read('census-plan.json.gz')
pin = read('census-pin-receipt.json.gz')
native = read('census-native-status.json.gz')
raw = gzip.decompress((P / 'census-rows.jsonl.gz').read_bytes())
rows = [json.loads(line) for line in raw.splitlines()]
assert hashlib.sha256(raw).hexdigest() == summary['source_receipts_sha256']
assert status['complete'] and native['complete'] and not native['production_mutations']
assert status['read_only'] and pin['complete'] and len(rows) == len(plan) == 8651
assert pin['segments'] == 8651 and pin['files'] == 17302
counts = collections.Counter()
buckets = [{}, {}]
physical = [collections.Counter(), collections.Counter()]
split = []
seen = set()
for p, r in zip(plan, rows):
    h, a = p['history_ref'], p['accessor_ref']
    assert h['path'] == r['Path'] and r['Path'] not in seen
    seen.add(r['Path'])
    assert h['checksum'] == r['SHA256'] and a['checksum'] == r['AccessorSHA256']
    assert h['size'] == r['PhysicalBytes'] and a['size'] == r['AccessorBytes']
    assert (p['first_block'], p['last_block']) == (r['FirstBlock'], r['LastBlock'])
    assert r['LogicalBytes'] == p['logical_bytes'] == r['RangeBytes'] + r['RecordBytes']
    assert r['Records'] == p['records']
    counts.update(files=1, records=r['Records'], history_file_bytes=r['PhysicalBytes'],
                  accessor_file_bytes=r['AccessorBytes'], logical_file_bytes=r['LogicalBytes'],
                  range_header_bytes=r['RangeBytes'], record_frame_bytes=r['RecordBytes'],
                  excluded_rows=r['ExcludedRows'])
    if r['FirstBlock'] >= 25000000 and r['LastBlock'] <= 35000000:
        counts['fully_contained_history_file_bytes'] += r['PhysicalBytes']
    bi = 0 if r['LastBlock'] < 30000000 else 1 if r['FirstBlock'] >= 30000000 else -1
    if bi >= 0 and r['FirstBlock'] >= 25000000 and r['LastBlock'] <= 35000000:
        physical[bi].update(files=1, physical_bytes=r['PhysicalBytes'],
                            logical_bytes=r['LogicalBytes'], records=r['Records'])
    else:
        split.append(r['PhysicalBytes'])
    selected = 0
    for bi, domains in enumerate(r['Domains']):
        for d in domains:
            assert d['Rows'] == d['Present'] + d['Absent'] == sum(d['LengthBins'])
            assert d['Empty'] <= d['Present'] and d['LengthBins'][0] == d['Empty'] + d['Absent']
            assert d['FrameBytes'] == 21 * d['Rows']
            selected += d['Rows']
            z = buckets[bi].setdefault(d['Name'], {'Name': d['Name']})
            for k, v in d.items():
                if k == 'Name':
                    continue
                if isinstance(v, list):
                    previous = z.setdefault(k, [0] * len(v))
                    assert len(previous) == len(v)
                    for i, n in enumerate(v):
                        previous[i] += n
                else:
                    z[k] = z.get(k, 0) + v
    assert selected + r['ExcludedRows'] == r['Records']
for previous, following in zip(rows, rows[1:]):
    assert previous['LastBlock'] + 1 == following['FirstBlock']
for previous, following in zip(plan, plan[1:]):
    assert previous['history_ref']['toTxNum'] + 1 == following['history_ref']['fromTxNum']
assert rows[0]['FirstBlock'] == 24998879 and rows[-1]['LastBlock'] == 35000554
assert dict(counts) == summary['counts']
assert [dict(v) for v in physical] == summary['physical_fully_contained_buckets']
assert sum(v['physical_bytes'] for v in physical) + sum(split) == counts['history_file_bytes']
all_domains = {}
for bi, b in enumerate(buckets):
    for d in b.values():
        d['LogicalRecordBytes'] = d['FrameBytes'] + d['PayloadBytes']
        z = all_domains.setdefault(d['Name'], collections.Counter())
        for k in ['Rows', 'Present', 'Absent', 'Empty', 'PayloadBytes', 'FrameBytes']:
            z[k] += d[k]
    assert sorted(b.values(), key=lambda d: d['PayloadBytes'], reverse=True) == summary['buckets'][bi]['domains']
selected = sum(d['Rows'] for d in all_domains.values())
payload = sum(d['PayloadBytes'] for d in all_domains.values())
assert selected == 5610054750 and payload == 214475903513
assert selected + counts['excluded_rows'] == counts['records']
assert counts['record_frame_bytes'] - 21 * counts['records'] == 214509394581
widths = {}
for name, width in [('kv/256', 32), ('kv/515', 8)]:
    histogram = [sum(b[name]['Width' + str(width)][i] for b in buckets) for i in range(width + 1)]
    assert sum(histogram) == all_domains[name]['Present']
    assert all_domains[name]['PayloadBytes'] == width * sum(histogram)
    widths[name] = dict(histogram=histogram, significant_bytes=sum(i * n for i, n in enumerate(histogram)),
                        zero_prefix_bytes=width * sum(histogram) - sum(i * n for i, n in enumerate(histogram)))
result = dict(complete=True, files=len(rows), selected_rows=selected,
              selected_payload_bytes=payload, physical_overlapping_files=counts['history_file_bytes'],
              physical_fully_contained_buckets=[dict(x) for x in physical],
              domains={k: dict(v) for k, v in sorted(all_domains.items(), key=lambda kv: kv[1]['PayloadBytes'], reverse=True)},
              numeric_widths=widths)
print(json.dumps(result, indent=2))
