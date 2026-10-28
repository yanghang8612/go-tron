import hashlib,json,pathlib,struct,zipfile,os

r=pathlib.Path('/data/gtron/maintenance/first-cold-followup-20261028-01/account-delta')
assert json.loads((r/'candidate-status.json').read_text())['complete']
def identity(refs):
    rows=sorted(refs,key=lambda x:x['path'])
    return hashlib.sha256(json.dumps(rows,sort_keys=True,separators=(',',':')).encode()).hexdigest()
files={};blobs={};variants={};stable=None
for tag in ['gaps','delta','compact']:
    root=r/('input-'+tag)/'state-snapshots';m=json.loads((root/'manifest.json').read_text())
    canonical=[x for x in m['segments'] if x['kind']!='account-history-routing-delta']
    if stable is None:stable=canonical;progress=m.get('progress');bounds=[m['visibleTxStart'],m['visibleTxEnd']]
    assert canonical==stable and m.get('progress')==progress and [m['visibleTxStart'],m['visibleTxEnd']]==bounds
    refs=[x for x in m['segments'] if x['kind']=='account-history-routing-delta'];metas=[]
    for ref in refs:
        p=root/ref['path'];assert p.stat().st_size==ref['size']
        with p.open('rb') as f:
            h=f.read(64);assert h[:8]==b'GTACRT01'
            n=struct.unpack('>I',h[12:16])[0];dirs=struct.unpack('>Q',h[32:40])[0]
            f.seek(0);raw=f.read(64+n*32+53*dirs)
        digest=hashlib.sha256(raw).hexdigest();assert 'sha256:'+digest==ref['lookupRoot']
        name='metadata/'+digest+'.bin';blobs[name]=raw
        metas.append(dict(ref=ref,metadata=name,allocated_bytes=p.stat().st_blocks*512))
    base,=[x for x in m['segments'] if x['kind']=='account-history-routing']
    variants[tag]=dict(canonical_and_other_auxiliary_refs_sha256=identity(canonical),base_ref=base,objects=metas,progress=m.get('progress'),visible=bounds)
receipt=dict(canonical_refs_and_progress_unchanged=True,variants=variants,scope='LookupRoot metadata proof and native HTTP returns; no repeated full table/state scan')
p=r/'routing-receipt.json';p.write_text(json.dumps(receipt,indent=2))
for directory,dirs,names in os.walk(str(r)):
    dirs[:]=[d for d in dirs if not d.startswith(('source-','input-'))]
    for name in names:
        if name.endswith('progress.json') or name=='evidence-seal.json':continue
        p=pathlib.Path(directory)/name
        if p.is_file() and not p.is_symlink() and p.suffix in ['.json','.jsonl','.log','.txt']:files[str(p.relative_to(r))]=p
cold=r.parent/'account-delta-cold'
assert json.loads((cold/'candidate-status.json').read_text())['complete']
for directory,dirs,names in os.walk(str(cold)):
    dirs[:]=[d for d in dirs if not d.startswith(('source-','input-'))]
    for name in names:
        p=pathlib.Path(directory)/name
        if p.is_file() and not p.is_symlink() and p.suffix in ['.json','.jsonl','.log','.txt']:
            files['private-index-cold/'+str(p.relative_to(cold))]=p
target=pathlib.Path('/tmp/AAA-gtron-account-delta-20261029-01.zip')
with zipfile.ZipFile(str(target),'w',zipfile.ZIP_DEFLATED) as z:
    for name,p in sorted(files.items()):z.write(str(p),name)
    for name,data in sorted(blobs.items()):z.writestr(name,data)
seal=dict(path=str(target),bytes=target.stat().st_size,sha256=hashlib.sha256(target.read_bytes()).hexdigest(),members=len(files)+len(blobs))
(r/'evidence-seal.json').write_text(json.dumps(seal,indent=2));print(json.dumps(seal,indent=2))
