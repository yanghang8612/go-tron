import pathlib, json, hashlib, statistics, collections, sys

P = pathlib.Path(sys.argv[1])
def read(name):
    return json.loads((P / name).read_text())
def lines(name):
    return [json.loads(s) for s in (P / name).read_text().splitlines() if s.strip()]
evidence = read('evidence-sha256.json')
for n, expected in evidence.items():
    assert hashlib.sha256((P / n).read_bytes()).hexdigest() == expected, n
out = {'evidence_members_verified':len(evidence)}
probe = collections.defaultdict(lambda:collections.Counter())
for r in (lines('probe-rows.jsonl') if (P/'probe-rows.jsonl').exists() else []):
    for d in r['Depths']:
        c = probe[d['Variant']]
        c['files'] += 1
        c['source_bytes'] += r['SourceBytes']
        c['candidate_bytes'] += d['CandidateBytes']
        c['numeric_values'] += d['Stats']['Numeric']
        c['selected_pages'] += d['DeltaPages']
out['probe'] = {k:dict(v, saved_bytes=v['source_bytes']-v['candidate_bytes']) for k,v in probe.items()}
m = read('muhash-census.json')
assert m['complete'] and m['muhash_verified'] and m['block_num'] == 11818192 and m['tx_num'] == 601386177
mu = read('muhash-driver-status.json')
assert mu['complete'] and mu['chaindata_before'] == mu['chaindata_after']
assert mu['binary_sha256'] == read('native-status.json')['binaries']['candidate.test']
out['muhash'] = dict(block=m['block_num'], tx=m['tx_num'], root=bytes(m['muhash_root']).hex(),
                     source_rows=m['source_rows'], fixed_chaindata_files=len(mu['chaindata_before']))
if read('layout-status.json')['complete']:
    receipts = lines('complete-input-files.jsonl')
    assert len(receipts) == 4609
    for r in receipts:
        assert len(r['source']) == len(r['candidate']) == 3
        a = r['audit']
        assert a['BeforeBytes'] == r['source'][0]['size'] and a['AfterBytes'] == r['candidate'][0]['size']
    out['layout'] = dict(files=len(receipts), converted=sum(r['audit']['Converted'] for r in receipts),
        before_bytes=sum(r['audit']['BeforeBytes'] for r in receipts),
        after_bytes=sum(r['audit']['AfterBytes'] for r in receipts),
        logical_bytes=sum(r['audit']['LogicalBytes'] for r in receipts),
        account_records=sum(r['audit']['AccountRecords'] for r in receipts))
if (P / 'account-controls-status.json').exists() and read('account-controls-status.json')['complete']:
    controls = lines('account-controls-files.jsonl')
    assert len(controls) == 4609
    source = sum(r['Source']['size'] for r in controls)
    candidate = sum(r['Candidate']['size'] for r in controls)
    plain = sum(r['EffectiveControlBytes'] for r in controls)
    cs = read('account-controls-status.json')
    assert (source,candidate,plain) == (cs['source_bytes'],cs['candidate_bytes'],cs['plain_control_bytes'])
    assert cs['account_transform_increment_bytes'] == plain-candidate
    out['controls'] = dict(cs, account_pages=sum(r['AccountPages'] for r in controls),
        numeric_pages=sum(r['NumericPages'] for r in controls), all_pages=sum(r['Pages'] for r in controls),
        original_to_plain_saved=source-plain, original_to_candidate_saved=source-candidate,
        account_format_extra_saved=plain-candidate)
if (P / 'tail-read-comparison/comparison-status.json').exists():
    status = read('tail-read-comparison/comparison-status.json')
    if status['complete']:
        assert status['all_responses_correct'] and not status['environment_identity_changed']
        def normalized_hash(value):
            decoded=bytes.fromhex(value).decode('ascii')
            if len(decoded)==64 and all(c in '0123456789abcdefABCDEF' for c in decoded):
                return decoded.lower()
            return value.lower()
        assert status['head']['head']==m['block_num'] and status['head']['head_tx']==m['tx_num']
        assert normalized_hash(status['head']['root'])==bytes(m['muhash_root']).hex()
        ready_rows=[json.loads(f.read_text()) for f in (P/'tail-read-comparison').glob('*/ready.json')]
        for r in ready_rows:
            assert r['read_only'] and r['gomaxprocs']==2 and r['history_cache_mib']==r['hot_cache_mib']==512
            assert r['head']==m['block_num'] and r['head_tx']==m['tx_num']
            assert normalized_hash(r['root'])==bytes(m['muhash_root']).hex()
        rows = lines('tail-read-comparison/requests.jsonl')
        assert all(r['correct'] and 'error' not in r['response'] and r['response']['result'] == r['expected'] for r in rows)
        grouped = collections.defaultdict(list)
        expected_n = {'warm':10,'fresh-process-os-uncontrolled':3,'private-os-cold-fresh-process':4}
        for r in rows:
            if r['cache'] in expected_n:
                grouped[(r['mode'],r['cache'],r['tag'])].append(r['milliseconds'])
        assert len(grouped) == 42 and sum(map(len,grouped.values())) == 238
        for key, values in grouped.items():
            assert len(values) == expected_n[key[1]], key
        out['http'] = dict(head=status['head'], response_rows=len(rows), timed_rows=238,
            summaries=[dict(mode=k[0],cache=k[1],tag=k[2],n=len(v),median_ms=statistics.median(v),
                            min_ms=min(v),max_ms=max(v)) for k,v in sorted(grouped.items())])
        proofs = [json.loads(f.read_text()) for f in (P/'tail-read-comparison').glob('*-residency.json')]
        assert len(proofs) == 56 and max(r['fraction'] for r in proofs) < 0.01
        out['http']['cold_residency_proofs'] = len(proofs)
        out['http']['maximum_resident_fraction'] = max(r['fraction'] for r in proofs)
        environments = [json.loads(f.read_text()) for f in (P/'tail-read-comparison').glob('*/environment.json')]
        assert all(e['production_start'] == e['production_end'] == status['before_identity'] for e in environments)
        out['http']['stable_environment_hosts'] = len(environments)
        rss = collections.defaultdict(list)
        for f in (P/'tail-read-comparison').glob('*/process-resources.json'):
            r = json.loads(f.read_text())
            assert not r.get('process_disappeared')
            arm = 'candidate' if 'candidate' in f.parent.name else 'baseline'
            rss[arm].append(int(r['status']['VmHWM:'].split()[0]) if 'VmHWM:' in r['status'] else int(r['status']['VmHWM'].split()[0]))
        out['http']['max_host_hwm_kib'] = {k:max(v) for k,v in rss.items()}
if (P/'history-restore.json').exists():
    restore = read('history-restore.json')
    assert restore['complete']
    for w in restore['windows']:
        assert w['full_row_sha256'] == w['candidate_row_sha256'] == w['restored_row_sha256']
    out['restore'] = dict(windows=len(restore['windows']), blocks=sum(w['range_rows_verified'] for w in restore['windows']),
                          rows=sum(w['rows'] for w in restore['windows']))
if (P/'final-space-and-physical-closure.json').exists():
    closure=read('final-space-and-physical-closure.json')
    assert closure['complete'] and closure['all_referenced_physical_sha_verified']
    out['space'] = {k:v for k,v in closure.items() if k != 'per_file'}
print(json.dumps(out,indent=2))
