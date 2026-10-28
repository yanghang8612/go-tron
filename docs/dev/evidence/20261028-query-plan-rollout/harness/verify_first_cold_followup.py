import collections, hashlib, json, pathlib, statistics, struct, sys, zipfile

archive = pathlib.Path(sys.argv[1])
output = pathlib.Path(sys.argv[2])
output.mkdir(parents=True, exist_ok=True)
assert archive.stat().st_size == 42113144
assert hashlib.sha256(archive.read_bytes()).hexdigest() == 'c57b2b0012dfa5cae5a3f7c77250d19902014f8a93c866a51fcb13003fdbdd98'
z = zipfile.ZipFile(archive)
assert z.testzip() is None
def read(name): return json.loads(z.read(name))
def pair(h, a):
    s = hashlib.sha256(b'go-tron/system-history-routing/pair/v1\0')
    for r in (h, a):
        s.update(struct.pack('>QQQQQH', r['fromTxNum'], r['toTxNum'], r.get('aggregationSteps', 0) or 1, r['size'], r.get('rows', 0), r.get('domain', 0)))
        for v in (r.get('dataset', ''), r['kind'], r['path'], r.get('checksum', '')):
            b = v.encode(); s.update(struct.pack('>Q', len(b))); s.update(b)
    return s.digest()

catalogs = {}
for name in ['fixed-original', 'metadata-base', 'metadata-candidate']:
    m = read(name+'/manifest.json'); by_path = collections.defaultdict(list)
    for r in m['segments']: by_path[r['path']].append(r)
    current = set()
    for h in m['segments']:
        if h['kind'] != 'history' or h.get('dataset') != 'state-domain-change': continue
        a = [r for r in by_path[str(pathlib.PurePosixPath(h['path']).with_suffix('.kv'))]
             if r['kind'] == 'accessor' and r.get('dataset') == h['dataset'] and r['fromTxNum'] == h['fromTxNum'] and r['toTxNum'] == h['toTxNum'] and (r.get('aggregationSteps', 0) or 1) == (h.get('aggregationSteps', 0) or 1)]
        assert len(a) == 1
        current.add(pair(h, a[0]))
    proofs = set(); objects = 0
    for r in m['segments']:
        if r['kind'] != 'metadata-history-routing': continue
        b = z.read(name+'/'+r['path']); objects += 1
        assert len(b) == r['size'] and 'sha256:'+hashlib.sha256(b).hexdigest() == r['checksum']
        assert b[:8] == b'GTMETR01' and int.from_bytes(b[12:16], 'big') == r['rows']
        assert struct.unpack('>QQ', b[16:32]) == (r['fromTxNum'], r['toTxNum'])
        pos = 32
        for _ in range(r['rows']):
            source = b[pos:pos+32]; n = int.from_bytes(b[pos+32:pos+40], 'big')
            assert len(source) == 32
            proofs.add(source); pos += 40+(1024 if n else 0)
        assert pos == len(b)
    catalogs[name] = dict(sources=len(current), exact_current_metadata_coverage=len(current & proofs), metadata_objects=objects)
    canonical = [r for r in m['segments'] if r['kind'] != 'metadata-history-routing']
    if name == 'fixed-original': original_canonical = canonical
    else: assert canonical == original_canonical
assert catalogs['fixed-original']['exact_current_metadata_coverage'] == 32036
assert catalogs['metadata-base']['exact_current_metadata_coverage'] == 31910
assert catalogs['metadata-candidate']['exact_current_metadata_coverage'] == 32036

maintenance = {}
for version in ['base', 'candidate']:
    rows = read('metadata-repair-ablation/'+version+'-maintenance/maintenance.json')
    assert len(rows) == 48 and rows[0]['covered_before'] == 31988
    assert rows[-1]['covered_after'] == catalogs['metadata-'+version]['exact_current_metadata_coverage']
    if version == 'candidate': assert all(r['covered_after'] == r['covered_before']+1 for r in rows)
    maintenance[version] = dict(passes=len(rows), covered_before=rows[0]['covered_before'], covered_after=rows[-1]['covered_after'], total_build_publish_seconds=sum(r['seconds_build_publish'] for r in rows))

all_rows = []; heads = set(); expected = {9000000:207,10000000:213,11000000:220,15000000:238,20000000:249}
for phase in ['account-gap-ablation', 'metadata-repair-ablation']:
    status = read(phase+'/candidate-status.json')
    assert status['complete'] and status['production_before'] == status['production_after']
    rows = read(phase+'/requests.json'); assert len(rows) == 40
    for q in rows:
        assert int(q['response']['result'],16) == expected[q['height']] and 'error' not in q['response']
        ready = read(phase+'/'+q['case']+'/ready.json')
        assert ready['read_only']
        heads.add(tuple(ready[k] for k in ['head', 'head_hash', 'head_tx', 'root']))
        host = [json.loads(line) for line in z.read(phase+'/'+q['case']+'/server-requests.jsonl').decode().splitlines()]
        raw = [s for s in host if str(s['id']) == str(q['response']['id'])]
        assert len(raw) == 1
        def io(s): return {k:int(v) for k,v in (line.split(':') for line in s.splitlines())}
        a, b = io(raw[0]['io_before']), io(raw[0]['io_after'])
        result = dict(q, phase=phase, io_delta={k:b[k]-a[k] for k in a})
        all_rows.append(result)
    for filename in ['candidate-status.json', 'requests.json']:
        p = output/phase/filename; p.parent.mkdir(parents=True, exist_ok=True); p.write_bytes(z.read(phase+'/'+filename))
groups = []
for phase in ['account-gap-ablation', 'metadata-repair-ablation']:
    for height in expected:
        for family in (['gaps','original'] if phase == 'account-gap-ablation' else ['base','candidate']):
            for repeat in [0,1]:
                rows = [r for r in all_rows if r['phase'] == phase and r['height'] == height and r['case'].startswith(family+'-') and r['repeat'] == repeat]
                assert len(rows) == 2
                groups.append(dict(phase=phase, height=height, family=family, repeat=repeat, times=[r['seconds_loopback_http'] for r in rows], rchar=[r['io_delta']['rchar'] for r in rows], read_syscalls=[r['io_delta']['syscr'] for r in rows], read_bytes=[r['io_delta']['read_bytes'] for r in rows]))
assert len(heads) == 1
head = next(iter(heads))
assert head[0] == 45836827 and head[2] == 4240407293 and head[3] == '4bb19b7a938ba6c865820ea86f5239235172f84923c350b04c4b0eb75743b627'
receipt = dict(archive_bytes=archive.stat().st_size, sha256=hashlib.sha256(archive.read_bytes()).hexdigest(), zip_members=len(z.namelist()), correct_http_returns=len(all_rows), head_identity=head, catalogs=catalogs, maintenance=maintenance, canonical_segment_refs_identical=True, cache='shared OS caches uncontrolled; fresh HTTP processes; no eviction')
(output/'verification.json').write_text(json.dumps(receipt, indent=2)+'\n')
(output/'grouped-http.json').write_text(json.dumps(groups, indent=2)+'\n')
(output/'verified-requests.json').write_text(json.dumps(all_rows, indent=2)+'\n')
for n in z.namelist():
    if n.endswith(('/summary.json', '/maintenance.json', '/result.json', '/host.log', '-focused.log')):
        p = output/n; p.parent.mkdir(parents=True, exist_ok=True); p.write_bytes(z.read(n))
print(json.dumps(receipt, indent=2))
