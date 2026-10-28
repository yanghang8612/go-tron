import pathlib,json,hashlib,zipfile,os
root=pathlib.Path('/data/gtron/maintenance/first-cold-followup-20261028-01')
assert json.loads((root/'storage-pages/candidate-status.json').read_text())['complete']
assert json.loads((root/'storage-observe/candidate-status.json').read_text())['complete']
assert json.loads((root/'storage-index-cold/candidate-status.json').read_text())['complete']
files={}
for family in ['storage-trace','storage-prefix-sample','storage-pages','storage-observe','storage-index-cold']:
 for directory,dirs,names in os.walk(str(root/family)):
  dirs[:]=[d for d in dirs if not d.startswith(('source-','input-'))]
  for name in names:
   p=pathlib.Path(directory)/name
   if p.is_file() and not p.is_symlink() and p.suffix in ['.json','.jsonl','.log','.txt','.pprof']:files[str(p.relative_to(root))]=p
parent=root/'metadata-repair-ablation/input-candidate/state-snapshots';current=root/'storage-pages/input-storage/state-snapshots';before=json.loads((parent/'manifest.json').read_text());after=json.loads((current/'manifest.json').read_text())
canonical=lambda m:[r for r in m['segments'] if r['kind']!='storage-history-routing']
assert canonical(before)==canonical(after)
refs=[r for r in after['segments'] if r['kind']=='storage-history-routing']
receipt={'source_manifest_sha256':hashlib.sha256((parent/'manifest.json').read_bytes()).hexdigest(),'indexed_manifest_sha256':hashlib.sha256((current/'manifest.json').read_bytes()).hexdigest(),'canonical_refs_sha256':hashlib.sha256(json.dumps(canonical(after),sort_keys=True,separators=(',',':')).encode()).hexdigest(),'canonical_refs_unchanged':True,'routing_refs':refs}
page_receipt=root/'storage-pages/routing-receipt.json';page_receipt.write_text(json.dumps(receipt,indent=2));files['storage-pages/routing-receipt.json']=page_receipt
for r in refs:files['storage-pages/routes/'+r['path']]=current/r['path']
target=pathlib.Path('/tmp/AAA-gtron-storage-pages-20261028-01.zip')
with zipfile.ZipFile(str(target),'w',zipfile.ZIP_DEFLATED) as z:
 for name,p in sorted(files.items()):z.write(str(p),name)
receipt={'path':str(target),'bytes':target.stat().st_size,'sha256':hashlib.sha256(target.read_bytes()).hexdigest(),'members':len(files)}
(root/'storage-pages/evidence-seal.json').write_text(json.dumps(receipt,indent=2));print(json.dumps(receipt,indent=2))
