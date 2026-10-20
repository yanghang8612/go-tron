#!/usr/bin/env python3
"""Recompute medians from complete HTTP requests, excluding all diagnostics."""
import collections
import json
from pathlib import Path
import statistics

HERE = Path(__file__).resolve().parent
rows = json.loads((HERE / 'server/joined-request-metrics.json').read_text())
groups = collections.defaultdict(list)
for row in rows:
    if row['cache'] in ('warm', 'fresh-process-os-uncontrolled', 'private-os-cold-fresh-process'):
        groups[row['mode'], row['cache'], row['height']].append(row)
out = []
for cache in ('warm', 'fresh-process-os-uncontrolled', 'private-os-cold-fresh-process'):
    for height in sorted({r['height'] for r in rows}):
        pair = {}
        for mode in ('baseline', 'candidate'):
            selected = groups[mode, cache, height]
            pair[mode] = {
                'n': len(selected),
                'median_ms': statistics.median(r['milliseconds'] for r in selected),
                'cpu_ms': statistics.median(r['cpu_ms'] for r in selected),
                'allocated_bytes': statistics.median(r['allocated_bytes'] for r in selected),
                'read_calls': statistics.median(r['io_delta']['syscr'] for r in selected),
                'read_returned_bytes': statistics.median(r['io_delta']['rchar'] for r in selected),
                'physical_read_bytes': statistics.median(r['io_delta']['read_bytes'] for r in selected),
            }
        out.append({'cache': cache, 'height': height, **pair,
                    'ratio': pair['baseline']['median_ms'] / pair['candidate']['median_ms']})
print(json.dumps(out, indent=2))
