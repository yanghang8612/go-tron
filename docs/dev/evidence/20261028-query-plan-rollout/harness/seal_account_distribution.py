import hashlib,json,pathlib,struct,zipfile

r=pathlib.Path('/data/gtron/maintenance/first-cold-followup-20261028-01/account-distribution')
assert json.loads((r/'candidate-status.json').read_text())['complete']
d=json.loads((r/'distribution.json').read_text())
assert d['complete'] and len(d['rows'])==1521
p=r.parent/'account-gap-ablation/input-1521-gaps/state-snapshots'
raw=(p/'manifest.json').read_bytes()
assert hashlib.sha256(raw).hexdigest()==d['manifest_sha256']
m=json.loads(raw);refs=[x for x in m['segments'] if x['kind']=='account-history-routing'];assert len(refs)==1
ref=refs[0]
with (p/ref['path']).open('rb') as f:
    h=f.read(64);assert h[:8]==b'GTACRT01'
    sources=struct.unpack('>I',h[12:16])[0];dirs=struct.unpack('>Q',h[32:40])[0]
    f.seek(0);meta=f.read(64+32*sources+53*dirs)
assert 'sha256:'+hashlib.sha256(meta).hexdigest()==ref['lookupRoot']
headers=[]
for row in d['rows']:
    a=row['accessor'];path=p/a['path'];assert path.stat().st_size==a['size']
    with path.open('rb') as f:header=f.read(120)
    assert len(header)==120
    headers.append(dict(pair_id=row['pair_id'],raw_header=header.hex(),version=struct.unpack('>I',header[8:12])[0],dictionary_keys=struct.unpack('>Q',header[40:48])[0]))
target=pathlib.Path('/tmp/AAA-gtron-account-distribution-20261029-01.zip')
with zipfile.ZipFile(str(target),'w',zipfile.ZIP_DEFLATED) as z:
    for name in ['candidate-status.json','distribution.json','study.log']:
        z.write(str(r/name),name)
    z.writestr('input-manifest.json',raw)
    z.writestr('account-base-metadata.bin',meta)
    z.writestr('accessor-headers.json',json.dumps(headers))
seal=dict(path=str(target),bytes=target.stat().st_size,sha256=hashlib.sha256(target.read_bytes()).hexdigest(),members=6)
(r/'evidence-seal.json').write_text(json.dumps(seal,indent=2))
print(json.dumps(seal,indent=2))
