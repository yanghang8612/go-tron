"""Recompute the complete page study; never present it as file/HTTP acceptance."""
import hashlib
import io
import json
import pathlib
import statistics
import tarfile
import zipfile

HERE=pathlib.Path(__file__).resolve().parent
archive=HERE/'study-core.tar.xz'
assert archive.stat().st_size==438076 and hashlib.sha256(archive.read_bytes()).hexdigest()=='1f1148ee9885c9efcc793ac3058826b5029fe77f7b9941b83c5f456864bd03c5'
with tarfile.open(archive,'r:xz') as t:
    names=t.getnames()
    assert len(names)==len(set(names)) and all(pathlib.PurePosixPath(n).name==n for n in names)
    members={n:t.extractfile(n).read() for n in names}
manifest=json.loads(members['evidence-manifest.json'])
assert set(members)==set(manifest)|{'evidence-manifest.json'}
for n,v in manifest.items():
    assert len(members[n])==v['bytes'] and hashlib.sha256(members[n]).hexdigest()==v['sha256'],n
load=lambda n:json.loads(members[n])
state=load('study-status.json')
assert state['complete'] and state['phase']=='complete' and not state['production_mutations']
assert [r['label'] for r in state['checks']]==['native-bounds','native-race','native-build','real-pages']
for row in state['checks']:
    log=members[row['label']+'.log']
    assert row['returncode']==0 and hashlib.sha256(log).hexdigest()==row['log_sha256']
    if row['label']!='native-build':assert b'PASS' in log
assert state['production_before']==state['production_after']
assert members['source-before.json']==members['source-after.json']
assert members['inputs-before.json']==members['inputs-after.json']
assert hashlib.sha256(members['source-before.json']).hexdigest()==state['source_before_sha256']
frozen=load('frozen-source-inventory.json');actual=load('source-before.json')
assert hashlib.sha256(members['frozen-source-inventory.json']).hexdigest()=='3281b8fcd833df0066efba23ac68c6ed329f9a1534ea5be5c75002655b6e9150'
assert len(frozen)==2149 and len(actual)==2173 and all(actual.get(n)==d for n,d in frozen.items())
assert hashlib.sha256(members['offline_shared_payload_test.go']).hexdigest()==state['helper_sha256']
assert state['sapling_sha256']=='b6c99f8fabac36d5a836c644bb79059449c89740bf33670d66088d094447e7ea'
terminal=load('terminal-unit.json')['unit']
assert terminal['MainPID']=='0' and terminal['ActiveState']!='active' and terminal['ExecMainStatus']=='0'
inputs=load('selected-inputs.json')
assert inputs==load('inputs-before.json')
assert [r['pair'] for r in inputs]==[458,2309,4147]
pinned_inputs={458:(15476527,'c4958d095f09201151cab8cc4b3ddfd5eae9cdbd4a81997f003480bb1402e950'),2309:(17437607,'39b4aa987f0581eb5d52e126665f46e664a27960518d587e94174e5955621f0a'),4147:(17153457,'899baa2a4e3352576012484a2ce98948df0256f4b3aec31fcdcd92747cbb5707')}
assert all((r['size'],r['sha256'])==pinned_inputs[r['pair']] for r in inputs)
assert hashlib.sha256(members['pages.jsonl']).hexdigest()==state['pages_sha256']
assert hashlib.sha256(members['summary.json']).hexdigest()==state['summary_sha256']
rows=[json.loads(line) for line in members['pages.jsonl'].splitlines()]
assert len(rows)==1817 and sum(r['AnnotationsRecovered'] for r in rows)==1806
summary=[]
for item in inputs:
    part=[r for r in rows if r['Pair']==item['pair']]
    assert [r['Page'] for r in part]==list(range(len(part)))
    assert sum(r['OriginalMode']<0 for r in part)==item['plain_pages']
    assert [sum(r['OriginalMode']==m for r in part) for m in range(3)]==item['column_key_modes']
    for r in part:
        assert 0<r['RawBytes']<=128*1024
        assert all(len(r[k])==64 and int(r[k],16)>=0 for k in ['RawSHA','OriginalSHA','PrototypeSHA'])
        c=r['Costs'];best=c['ordinary'];mode=-1
        assert 0<r['OriginalBytes']<=2*r['RawBytes']+1024
        if r['AnnotationsRecovered']:
            assert 0<=r['OriginalMode']<=2 and 0<r['Records']<=r['RawBytes']//21
            assert c['tail']>0 and all(k>0 for k in c['keys'])
            assert c['candidates']==[49+c['tail']+k for k in c['keys']]
            for m,cost in enumerate(c['candidates']):
                if cost<best:best,mode=cost,m
        else:
            assert r['Records']==0 and r['OriginalMode']==-1 and c['candidates']==[0,0,0]
            assert r['OriginalBytes']==c['ordinary'] and r['OriginalSHA']==r['PrototypeSHA']
        assert mode==c['selected'] and best==r['PrototypeBytes']
        for k in ['OldEncodeNS','PrototypeEncodeNS','OldDecodeNS','PrototypeDecodeNS']:
            assert len(r[k])==3 and all(isinstance(v,int) and v>0 for v in r[k])
    original=sum(r['OriginalBytes'] for r in part);proposed=sum(r['PrototypeBytes'] for r in part)
    assert item['size']-original==28*len(part)+96
    timings={}
    for key in ['OldEncodeNS','PrototypeEncodeNS','OldDecodeNS','PrototypeDecodeNS']:
        totals=[sum(r[key][i] for r in part) for i in range(3)]
        timings[key]=dict(round_total_ns=totals,median_total_ns=statistics.median(totals))
    summary.append(dict(pair=item['pair'],pages=len(part),original_page_bytes=original,prototype_page_bytes=proposed,
                        saved_page_bytes=original-proposed,file_control_bytes=item['size']-original,
                        plain_unannotated_pages=item['plain_pages'],prototype_modes=[sum(r['Costs']['selected']==m for r in part) for m in [-1,0,1,2]],
                        smaller=sum(r['PrototypeBytes']<r['OriginalBytes'] for r in part),
                        equal=sum(r['PrototypeBytes']==r['OriginalBytes'] for r in part),
                        larger=sum(r['PrototypeBytes']>r['OriginalBytes'] for r in part),timings=timings))
assert summary==load('summary.json')
with zipfile.ZipFile(io.BytesIO(members['study-payload.zip'])) as z:
    payload_manifest=json.loads(z.read('payload-manifest.json'))
    assert set(z.namelist())==set(payload_manifest)|{'payload-manifest.json'}
    for n,v in payload_manifest.items():
        b=z.read(n)
        assert len(b)==v['bytes'] and hashlib.sha256(b).hexdigest()==v['sha256']
        assert members[n]==b
start=load('start-receipt.json')
assert start['payload_sha256']==hashlib.sha256(members['study-payload.zip']).hexdigest()
assert start['unit']=='gtron-columns-shared-payload-study-20261026-03.service'
assert start['production_before']==start['production_after']
assert 'go1.25.5' in state['go_version']
assert state['binary_sha256']=='484acbb5c0cb2e1f887cb9c01889578c93df1402c57389ffc4a40fd4430f348b'
assert state['helper_sha256']=='86378ea65aa3685f1752be89847a3ac39affb42f31e36b956873463ccef7773c'
assert start['observed_unit']['MemoryLimit']=='4294967296' and start['observed_unit']['CPUQuotaPerSecUSec']=='2s' and start['observed_unit']['Nice']=='15'
go_env=load('effective-go-env.json')
assert go_env['GOMAXPROCS']=='2' and go_env['GOMEMLIMIT']=='3GiB' and go_env['GOMODCACHE']=='/root/go/pkg/mod'
assert b'Files beneath GOMODCACHE' in members['prior02-native-bounds.log']
assert not load('prior01-study-status.json')['complete'] and not load('prior02-study-status.json')['complete']
assert 'not physical file, publication, HTTP' in state['scope']
effects=[]
for r in summary:
    timing=r['timings'];old=timing['OldEncodeNS']['median_total_ns'];new=timing['PrototypeEncodeNS']['median_total_ns']
    od=timing['OldDecodeNS']['median_total_ns'];nd=timing['PrototypeDecodeNS']['median_total_ns']
    effects.append(dict(pair=r['pair'],pages=r['pages'],saved_page_bytes=r['saved_page_bytes'],
                        page_size_effect_percent=(r['prototype_page_bytes']/r['original_page_bytes']-1)*100,
                        resident_encode_effect_percent=(new/old-1)*100,resident_decode_effect_percent=(nd/od-1)*100,
                        larger_pages_retained=r['larger']))
print(json.dumps(dict(all_pages=1817,all_canonical_checks=True,product_source_unchanged=True,effects=effects,
                      unannotated_plain_pages=11,scope=state['scope'],production_mutations=False),indent=2))
