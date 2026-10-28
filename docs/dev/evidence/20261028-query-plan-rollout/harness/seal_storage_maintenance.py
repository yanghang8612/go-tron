import pathlib,json,hashlib,zipfile,os
root=pathlib.Path('/data/gtron/maintenance/first-cold-followup-20261028-01');r=root/'storage-maintenance-r2'
assert json.loads((r/'candidate-status.json').read_text())['complete']
files={}
for directory,dirs,names in os.walk(str(r)):
 dirs[:]=[d for d in dirs if not d.startswith(('source-','input-'))]
 for name in names:
  p=pathlib.Path(directory)/name
  if p.is_file() and not p.is_symlink() and p.suffix in ['.json','.jsonl','.log','.txt']:files[str(p.relative_to(r))]=p
parent=root/'storage-pages/input-storage/state-snapshots';current=r/'input-runtime/state-snapshots';before=json.loads((parent/'manifest.json').read_text());after=json.loads((current/'manifest.json').read_text())
canonical=lambda m:[x for x in m['segments'] if x['kind']!='storage-history-routing']
assert canonical(before)==canonical(after)
refs=[x for x in after['segments'] if x['kind']=='storage-history-routing'];prior={x['path']:x for x in before['segments'] if x['kind']=='storage-history-routing'}
added=[x for x in refs if prior.get(x['path'])!=x]
receipt={'parent_manifest_sha256':hashlib.sha256((parent/'manifest.json').read_bytes()).hexdigest(),'canonical_refs_sha256':hashlib.sha256(json.dumps(canonical(after),sort_keys=True,separators=(',',':')).encode()).hexdigest(),'canonical_refs_unchanged':True,'routing_refs':refs,'new_refs':added}
p=r/'routing-receipt.json';p.write_text(json.dumps(receipt,indent=2));files[p.name]=p
for ref in added:files['routes/'+ref['path']]=current/ref['path']
target=pathlib.Path('/tmp/AAA-gtron-storage-maintenance-20261029-02.zip')
with zipfile.ZipFile(str(target),'w',zipfile.ZIP_DEFLATED) as z:
 for name,p in sorted(files.items()):z.write(str(p),name)
seal={'path':str(target),'bytes':target.stat().st_size,'sha256':hashlib.sha256(target.read_bytes()).hexdigest(),'members':len(files)}
(r/'evidence-seal.json').write_text(json.dumps(seal,indent=2));print(json.dumps(seal,indent=2))
