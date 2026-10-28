"""Independently verify sealed native receipts and optional routing pages.

This reads evidence bytes only; it does not reopen a DB or repeat MuHash.
Arguments: new ZIP, its server seal JSON, prior first-cold ZIP, output folder.
"""
import collections
import hashlib
import json
import pathlib
import struct
import sys
import zipfile

archive, seal_path, parent_archive, output = map(pathlib.Path, sys.argv[1:])
output.mkdir(parents=True, exist_ok=True)
seal = json.loads(seal_path.read_text())
assert archive.stat().st_size == seal['bytes']
assert hashlib.sha256(archive.read_bytes()).hexdigest() == seal['sha256']
assert parent_archive.stat().st_size == 42113144
assert hashlib.sha256(parent_archive.read_bytes()).hexdigest() == 'c57b2b0012dfa5cae5a3f7c77250d19902014f8a93c866a51fcb13003fdbdd98'
z, old = zipfile.ZipFile(archive), zipfile.ZipFile(parent_archive)
assert z.testzip() is None and len(z.namelist()) == seal['members']
read = lambda name: json.loads(z.read(name))
expected = {9000000: 207, 10000000: 213, 11000000: 220, 15000000: 238, 20000000: 249}
statuses = {p: read(p+'/candidate-status.json') for p in ['storage-trace', 'storage-prefix-sample', 'storage-pages', 'storage-observe', 'storage-index-cold']}
for s in statuses.values():
    assert s['complete'] and s['production_before'] == s['production_after']
    assert not s['production_mutations']
assert len({json.dumps(s['production_before'], sort_keys=True) for s in statuses.values()}) == 1

heads, rows = set(), []
def io(value):
    return {k: int(v) for k, v in (line.split(':') for line in value.splitlines())}
def cpu(value):
    return sum(value[k]['Sec']+value[k]['Usec']/1e6 for k in ['Utime', 'Stime'])
for phase, count in [('storage-pages', 60), ('storage-observe', 8), ('storage-trace', 4), ('storage-index-cold', 8)]:
    requests = read(phase+'/requests.json')
    assert len(requests) == count
    for q in requests:
        case = q.get('case', 'trace')
        ready = read(phase+'/'+case+'/ready.json')
        assert ready['read_only']
        heads.add(tuple(ready[k] for k in ['head', 'head_hash', 'head_tx', 'root']))
        log = [json.loads(line) for line in z.read(phase+'/'+case+'/server-requests.jsonl').decode().splitlines()]
        matches = [s for s in log if str(s['id']) == str(q['response']['id'])]
        assert len(matches) == 1
        event = matches[0]
        height = int(event['params'][1], 16)
        assert height == q.get('height', 20000000)
        assert event['method'] == 'eth_call'
        assert event['params'][0] == {'to': '0xa4997124a328a4615eab437cede31da47a31b375', 'data': '0x9fa6a6e3'}
        assert 'error' not in q['response'] and int(q['response']['result'], 16) == expected[height]
        a, b = io(event['io_before']), io(event['io_after'])
        rows.append(dict(q, phase=phase, height=height, io_delta={k: b[k]-a[k] for k in a},
                         cpu_seconds=cpu(event['usage_after'])-cpu(event['usage_before']),
                         server_seconds=event['elapsed_ns']/1e9, alloc_bytes=event['alloc_bytes'],
                         num_gc=event['num_gc'], maxrss_kib=event['usage_after']['Maxrss']))
assert len(heads) == 1
head = next(iter(heads))
assert head[0] == 45836827 and head[2] == 4240407293 and head[3] == '4bb19b7a938ba6c865820ea86f5239235172f84923c350b04c4b0eb75743b627'

receipt = read('storage-pages/routing-receipt.json')
parent_raw = old.read('metadata-candidate/manifest.json')
assert hashlib.sha256(parent_raw).hexdigest() == receipt['source_manifest_sha256']
parent = json.loads(parent_raw)
canonical = [r for r in parent['segments'] if r['kind'] != 'storage-history-routing']
assert hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(',', ':')).encode()).hexdigest() == receipt['canonical_refs_sha256']
assert receipt['canonical_refs_unchanged']
def pair(h, a):
    digest = hashlib.sha256(b'go-tron/system-history-routing/pair/v1\0')
    for r in (h, a):
        digest.update(struct.pack('>QQQQQH', r['fromTxNum'], r['toTxNum'], r.get('aggregationSteps', 0) or 1, r['size'], r.get('rows', 0), r.get('domain', 0)))
        for v in (r.get('dataset', ''), r['kind'], r['path'], r.get('checksum', '')):
            b = v.encode(); digest.update(struct.pack('>Q', len(b))); digest.update(b)
    return digest.digest()
by_path = collections.defaultdict(list)
for ref in canonical:
    by_path[ref['path']].append(ref)
sources = {}
for h in canonical:
    if h['kind'] != 'history' or h.get('dataset') != 'state-domain-change':
        continue
    accessors = [a for a in by_path[str(pathlib.PurePosixPath(h['path']).with_suffix('.kv'))]
                 if a['kind'] == 'accessor' and a.get('dataset') == h['dataset'] and a['fromTxNum'] == h['fromTxNum'] and a['toTxNum'] == h['toTxNum'] and (a.get('aggregationSteps', 0) or 1) == (h.get('aggregationSteps', 0) or 1)]
    assert len(accessors) == 1
    sources[pair(h, accessors[0])] = h
assert len(sources) == 32036
build = read('storage-pages/build.json')
assert build['complete'] and not build['canonical_data_mutations']
wanted = {id_ for id_, h in sources.items() if h['fromTxNum'] >= build['initial_cursor'] and h['toTxNum'] <= build['last_history_tx']}
filters, page_sizes, counts = {}, [], []
refs = receipt['routing_refs']
for ref in refs:
    b = z.read('storage-pages/routes/'+ref['path'])
    assert len(b) == ref['size'] <= 1 << 20
    assert 'sha256:'+hashlib.sha256(b).hexdigest() == ref['checksum']
    assert b[:8] == b'GTSTOR01' and struct.unpack('>IIQQ', b[8:32]) == (1, ref['rows'], ref['fromTxNum'], ref['toTxNum'])
    assert 0 < ref['rows'] <= 256
    pos, prior = 32, None
    for _ in range(ref['rows']):
        id_, n, size = struct.unpack('>32sII', b[pos:pos+40]); pos += 40
        assert prior is None or prior < id_
        assert n <= 65536
        expected_size = 0 if n == 0 else 1 << (max(64, n*2)-1).bit_length()
        assert size == expected_size and pos+size <= len(b)
        assert id_ in sources and id_ not in filters
        h = sources[id_]
        assert ref['fromTxNum'] <= h['fromTxNum'] <= h['toTxNum'] <= ref['toTxNum']
        filters[id_] = (n, b[pos:pos+size]); counts.append(n); pos += size; prior = id_
    assert pos == len(b)
    page_sizes.append(len(b))
assert set(filters) == wanted
assert len(filters) == build['exact_current_pairs'] == 6436
assert len(refs) == build['output_pages'] and sum(page_sizes) == build['output_bytes']

private = read('storage-index-cold/private-proof.json')
private_paths = {p['path'] for p in private}
assert len(private_paths) == len(refs) and sum(p['bytes'] for p in private) == sum(page_sizes)
assert all((p['source_dev'], p['source_ino']) != (p['private_dev'], p['private_ino']) for p in private)
for i in range(4):
    resident = read('storage-index-cold/private-storage-cold-'+str(i)+'/dispatch-residency.json')
    assert {p['path'] for p in resident} == private_paths
    assert all(p['resident_pages'] == 0 for p in resident)
for q in read('storage-index-cold/requests.json'):
    assert not q['overflow']
    assert {p for p in q['paths'] if '/storage-routing-' in p} == private_paths

batches = [json.loads(line) for line in z.read('storage-trace/batches.jsonl').decode().splitlines()]
storage_batches = [b for b in batches if b.get('queries') and b['queries'][0]['domain'] == 256]
assert len(storage_batches) == 4
q = storage_batches[0]['queries'][0]
owner = bytes.fromhex(q['owner']); assert len(owner) == 21
prefix = bytes([q['flat']])+owner[1:]+struct.pack('>QH', q['generation'], q['domain'])
assert len(prefix) == 31
digest = hashlib.sha256(prefix).digest()
first, step = struct.unpack('>QQ', digest[:16]); step |= 1
def may_contain(n, bits):
    if n == 0:
        return False
    return all(bits[((first+i*step) & (len(bits)*8-1))//8] & (1 << (((first+i*step) & (len(bits)*8-1)) % 8)) for i in range(8))
possible = [id_.hex() for id_, f in filters.items() if may_contain(*f)]
sample = read('storage-prefix-sample/sample.json')
assert sample['sample_size'] == 128 and sample['total_unknown_sources'] == 6436
assert all(not r['unsupported'] for r in sample['rows'])
assert sample['target_query'] == q
sample_facts = dict(sources=128, prefix_present=sum(r['target_prefix_present'] for r in sample['rows']),
                    slot_present=sum(r['target_slot_present'] for r in sample['rows']),
                    max_distinct_prefixes=max(r['distinct_prefixes'] for r in sample['rows']),
                    filter_bytes=sum(r['adaptive_filter_bytes'] for r in sample['rows']))

groups = []
for family in ['before', 'control', 'indexed']:
    for height in expected:
        for repeat in [0, 1]:
            selected = [r for r in rows if r['phase'] == 'storage-pages' and r['case'].startswith(family+'-') and r['height'] == height and r['repeat'] == repeat]
            assert len(selected) == 2
            groups.append(dict(family=family, height=height, repeat=repeat,
                               seconds=[r['seconds_loopback_http'] for r in selected],
                               rchar=[r['io_delta']['rchar'] for r in selected],
                               syscr=[r['io_delta']['syscr'] for r in selected],
                               read_bytes=[r['io_delta']['read_bytes'] for r in selected],
                               cpu_seconds=[r['cpu_seconds'] for r in selected]))
observations = []
for q in read('storage-observe/requests.json'):
    assert not q['overflow']
    paths = q['paths']; assert len(paths) == len(set(paths))
    observations.append(dict(case=q['case'], repeat=q['repeat'], distinct_accessed_files=len(paths),
                             by_suffix=dict(collections.Counter(pathlib.PurePosixPath(p).suffix for p in paths))))
verification = dict(archive=seal, correct_primary_http_returns=60, correct_diagnostic_http_returns=20,
                    head=head, production_identity=statuses['storage-pages']['production_after'],
                    canonical_refs_unchanged=True, authenticated_storage_pages=len(refs),
                    authenticated_storage_bytes=sum(page_sizes), exact_current_pairs=len(filters),
                    max_page_bytes=max(page_sizes), max_prefixes_per_source=max(counts),
                    sample=sample_facts, target_prefix_possible_sources=len(possible),
                    build_seconds=build['seconds'], build_passes=len(build['passes']),
                    cache='fresh processes; shared OS caches uncontrolled; no eviction',
                    auxiliary_cold_scope='four processes, only the 421 new storage pages on private inodes zero resident at dispatch; canonical and other routing caches uncontrolled',
                    file_observation_scope='distinct accessed paths, not an open or syscall count')
for name, value in [('verification.json', verification), ('grouped-http.json', groups), ('verified-requests.json', rows), ('file-access-summary.json', observations), ('storage-batches.json', storage_batches)]:
    (output/name).write_text(json.dumps(value, indent=2)+'\n')
for name in z.namelist():
    if name.endswith(('.json', '.jsonl', '.log', '.txt')):
        p = output/'native'/name
        p.parent.mkdir(parents=True, exist_ok=True); p.write_bytes(z.read(name))
print(json.dumps(verification, indent=2))
