import collections,hashlib,json,pathlib,struct,sys,zipfile

archive,study_archive,storage_archive=map(pathlib.Path,sys.argv[1:4]);out=pathlib.Path(sys.argv[4]);out.mkdir(parents=True,exist_ok=True)
assert hashlib.sha256(study_archive.read_bytes()).hexdigest()=='803e1ed63c4974e1cfba75d54e121e0dffe05e5502069113718f07352d8705cd'
assert hashlib.sha256(storage_archive.read_bytes()).hexdigest()=='b799a1df631281be649a768ad047ee33017a6f1e5e8406e61ff37d3130265ec2'
z=zipfile.ZipFile(archive);assert z.testzip() is None
read=lambda name:json.loads(z.read(name))
s=read('candidate-status.json');assert s['complete'] and s['production_before']==s['production_after'] and not s['production_mutations'] and not s['canonical_data_mutations']
with zipfile.ZipFile(study_archive) as p:
    parent=json.loads(p.read('input-manifest.json'));study=json.loads(p.read('distribution.json'))
    base_meta=p.read('account-base-metadata.bin')
with zipfile.ZipFile(storage_archive) as p:storage=json.loads(p.read('storage-pages/routing-receipt.json'))['routing_refs']
refs=[r for r in parent['segments'] if r['kind']!='storage-history-routing']+storage
digest=hashlib.sha256(json.dumps(sorted(refs,key=lambda r:r['path']),sort_keys=True,separators=(',',':')).encode()).hexdigest()
base,=[r for r in refs if r['kind']=='account-history-routing']
missing={r['pair_id'] for r in study['rows']};assert len(missing)==1521 and all(not r['target_present'][1] for r in study['rows'])
receipt=read('routing-receipt.json');assert receipt['canonical_refs_and_progress_unchanged']
tables={};base_root=base['lookupRoot']
for tag,v in receipt['variants'].items():
    assert v['canonical_and_other_auxiliary_refs_sha256']==digest and v['base_ref']==base and v['progress']==parent.get('progress') and v['visible']==[parent['visibleTxStart'],parent['visibleTxEnd']]
    union=set();meta_bytes=0;file_bytes=0;allocated=0;rows=0
    for object_ in v['objects']:
        r=object_['ref'];b=z.read(object_['metadata']);assert r['kind']=='account-history-routing-delta' and b[:8]==b'GTACRT01' and b[56:64]==bytes(8)
        assert 'sha256:'+hashlib.sha256(b).hexdigest()==r['lookupRoot']
        version,n=struct.unpack('>II',b[8:16]);count,pages,dirs,lo,hi=struct.unpack('>QQQQQ',b[16:56]);assert version==1
        assert count==r.get('rows',0)<=8_000_000 and (lo,hi)==(r['fromTxNum'],r['toTxNum'])
        assert pages==(count+140)//141 and dirs==(pages+76)//77
        assert len(b)==64+32*n+53*dirs and r['size']==len(b)+(dirs+pages)*4096<=256<<20
        ids={b[64+32*i:96+32*i].hex() for i in range(n)};assert len(ids)==n and ids<=missing and not union&ids
        union|=ids;meta_bytes+=len(b)+64;file_bytes+=r['size'];allocated+=object_['allocated_bytes'];rows+=count
    assert union==(set() if tag=='gaps' else missing)
    tables[tag]=dict(objects=len(v['objects']),rows=rows,file_bytes=file_bytes,allocated_bytes=allocated,all_metadata_and_headers_bytes=meta_bytes,exact_new_proofs=len(union),base_lookup_root=base_root)
build=read('build-result.json');assert build['complete'] and build['source_reads']==build['new_exact_proofs']==1521 and build['canonical_refs_unchanged'] and build['frontiers_unchanged']
assert sum(p['source_reads'] for p in build['passes'])==1521 and sum(p['sources'] for p in build['passes'])==1521 and not any(p['unsupported'] for p in build['passes'])
assert len(build['refs'])==tables['delta']['objects']
compact=read('consolidation-result.json');assert compact['complete']
assert tables['delta']['objects']-len(compact['passes'])==tables['compact']['objects']
def io(text):return {k:int(v) for k,v in (line.split(':') for line in text.splitlines())}
def delta(a,b):a,b=io(a),io(b);return {k:b[k]-a[k] for k in a}
all_rows=[];heads=set();expected={9000000:207,10000000:213,11000000:220,15000000:238,20000000:249}
requests=read('requests.json');assert len(requests)==60
for q in requests:
    assert int(q['response']['result'],16)==expected[q['height']] and 'error' not in q['response']
    ready=read(q['case']+'/ready.json');assert ready['read_only'];heads.add(tuple(ready[k] for k in ['head','head_hash','head_tx','root']))
    server=[json.loads(x) for x in z.read(q['case']+'/server-requests.jsonl').decode().splitlines()]
    r,=[r for r in server if str(r['id'])==str(q['response']['id'])]
    assert r['method']=='eth_call' and r['params']==[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},hex(q['height'])]
    all_rows.append(dict(q,io_delta=delta(r['io_before'],r['io_after'])))
assert len(heads)==1;head=next(iter(heads));assert head[0]==45836827 and head[2]==4240407293 and head[3]=='4bb19b7a938ba6c865820ea86f5239235172f84923c350b04c4b0eb75743b627'
grouped=[]
for tag in ['gaps','delta','compact']:
    for height in expected:
        for repeat in [0,1]:
            qs=[q for q in all_rows if q['case'].startswith(tag+'-') and q['height']==height and q['repeat']==repeat];assert len(qs)==2
            grouped.append(dict(family=tag,height=height,repeat=repeat,seconds=[q['seconds_loopback_http'] for q in qs],rchar=[q['io_delta']['rchar'] for q in qs],syscr=[q['io_delta']['syscr'] for q in qs],read_bytes=[q['io_delta']['read_bytes'] for q in qs]))
cold_status=read('private-index-cold/candidate-status.json');assert cold_status['complete'] and not cold_status['production_mutations'] and cold_status['binary_source']==s['source']
assert cold_status['production_before']==cold_status['production_after']==s['production_after']
proof=read('private-index-cold/private-proof.json');private_paths=set()
compact_objects={r['ref']['path']:r['ref'] for r in receipt['variants']['compact']['objects']}
for p in proof:
    assert (p['source_dev'],p['source_ino'])!=(p['private_dev'],p['private_ino'])
    assert p['path'].startswith('state-snapshots/') and p['path'][len('state-snapshots/'):] in compact_objects
    assert p['bytes']==compact_objects[p['path'][len('state-snapshots/'):]]['size']
    private_paths.add(p['path'])
assert len(private_paths)==len(compact_objects)
cold_rows=[]
for q in read('private-index-cold/requests.json'):
    assert int(q['response']['result'],16)==expected[q['height']]
    root='private-index-cold/'+q['case'];ready=read(root+'/ready.json');assert ready['read_only']
    assert tuple(ready[k] for k in ['head','head_hash','head_tx','root'])==head
    resident=read(root+'/dispatch-residency.json');assert {x['path'] for x in resident}==private_paths and all(x['resident_pages']==0 for x in resident)
    server=[json.loads(x) for x in z.read(root+'/server-requests.jsonl').decode().splitlines()]
    r,=[r for r in server if str(r['id'])==str(q['response']['id'])]
    assert r['method']=='eth_call' and r['params'][1]==hex(q['height'])
    cold_rows.append(dict(q,io_delta=delta(r['io_before'],r['io_after'])))
assert len(cold_rows)==16
for height in [9000000,10000000,15000000,20000000]:
    for repeat in [0,1]:
        qs=[q for q in cold_rows if q['height']==height and q['repeat']==repeat];assert len(qs)==2
        grouped.append(dict(family='private-account-index-cold',height=height,repeat=repeat,seconds=[q['seconds_loopback_http'] for q in qs],rchar=[q['io_delta']['rchar'] for q in qs],syscr=[q['io_delta']['syscr'] for q in qs],read_bytes=[q['io_delta']['read_bytes'] for q in qs]))
ab_status=read('code-ab/candidate-status.json');assert ab_status['complete'] and not ab_status['production_mutations'] and ab_status['candidate_source']==s['source'] and ab_status['base_source']=='69d74ae09d953a40a65b98b0a43eb22d115e7f70'
assert ab_status['production_before']==ab_status['production_after']==s['production_after']
ab_rows=[]
for q in read('code-ab/requests.json'):
    assert int(q['response']['result'],16)==expected[q['height']]
    root='code-ab/'+q['case'];ready=read(root+'/ready.json');assert ready['read_only'] and tuple(ready[k] for k in ['head','head_hash','head_tx','root'])==head
    server=[json.loads(x) for x in z.read(root+'/server-requests.jsonl').decode().splitlines()]
    r,=[r for r in server if str(r['id'])==str(q['response']['id'])]
    assert r['method']=='eth_call' and r['params'][1]==hex(q['height'])
    ab_rows.append(dict(q,io_delta=delta(r['io_before'],r['io_after'])))
assert len(ab_rows)==40
for tag in ['before','candidate']:
    for height in expected:
        for repeat in [0,1]:
            qs=[q for q in ab_rows if q['case'].startswith(tag+'-') and q['height']==height and q['repeat']==repeat];assert len(qs)==2
            grouped.append(dict(family='code-'+tag,height=height,repeat=repeat,seconds=[q['seconds_loopback_http'] for q in qs],rchar=[q['io_delta']['rchar'] for q in qs],syscr=[q['io_delta']['syscr'] for q in qs],read_bytes=[q['io_delta']['read_bytes'] for q in qs]))
build_io=[delta(p['io_before'],p['io_after']) for p in build['passes']]
summary=dict(complete=True,source=s['source'],production_identity=s['production_after'],http_returns=len(all_rows),head=head,tables=tables,build_seconds=build['seconds'],source_reads=1521,build_io={k:sum(x[k] for x in build_io) for k in build_io[0]},consolidation_seconds=compact['seconds'],consolidation_passes=len(compact['passes']),canonical_refs_and_frontiers_unchanged=True,archive=dict(bytes=archive.stat().st_size,sha256=hashlib.sha256(archive.read_bytes()).hexdigest()),scope='actual builder/publication and full HTTP; authenticated metadata roots/exact IDs; no repeated full state/table scan; shared OS cache uncontrolled; Runner admission incomplete')
summary['private_account_index_cold_returns']=len(cold_rows)
summary['compiled_version_comparison_returns']=len(ab_rows)
summary['base_source']=ab_status['base_source'];summary['binary_sha256']=ab_status['binary_sha256']
summary['cold_scope']='only copied account delta inodes: zero resident pages at dispatch; canonical and other auxiliary caches uncontrolled'
for name,value in [('verification.json',summary),('grouped-http.json',grouped),('verified-requests.json',all_rows)]:
    (out/name).write_text(json.dumps(value,indent=2)+'\n')
(out/'verified-private-cold-requests.json').write_text(json.dumps(cold_rows,indent=2)+'\n')
(out/'verified-code-ab-requests.json').write_text(json.dumps(ab_rows,indent=2)+'\n')
for name in z.namelist():
    if name.endswith(('.json','.jsonl','.log')) and not name.startswith('metadata/'):
        p=out/'native'/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(z.read(name))
print(json.dumps(summary,indent=2))
