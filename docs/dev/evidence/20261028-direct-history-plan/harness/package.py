import pathlib,json,hashlib,zipfile,os,subprocess,datetime
P=pathlib.Path('/data/gtron/maintenance/direct-history-plan-20261028-01')
status=json.loads((P/'block-cache-final/candidate-status.json').read_text());assert status['complete'],status
assert status['production_before']==status['production_after']
excluded={'history','chaindata','ancient','source','source-candidate','source-current','indexed-input','private-indexed-input'}
allowed={'.json','.jsonl','.log','.txt','.py','.pprof'}
paths=[]
def visit(root):
 for p in root.iterdir():
  if p.is_symlink():continue
  if p.is_dir():
   if p.name not in excluded and not p.name.startswith('source') and p.name!='__pycache__':visit(p)
  elif p.suffix in allowed and p.name not in ['member-seal.json','archive-receipt.json']:
   assert p.stat().st_size<25<<20,str(p)
   paths.append(p)
visit(P)
seal={str(p.relative_to(P)):{'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(paths)}
raw=json.dumps(seal,indent=2).encode();(P/'member-seal.json').write_bytes(raw)
out=pathlib.Path('/tmp/AAA-gtron-direct-history-plan-final-20261028-01.zip')
with zipfile.ZipFile(str(out),'w',zipfile.ZIP_DEFLATED,6) as z:
 for p in sorted(paths):z.write(str(p),str(p.relative_to(P)))
 z.writestr('member-seal.json',raw)
os.chmod(str(out),0o644)
receipt={'utc':datetime.datetime.utcnow().isoformat()+'Z','path':str(out),'bytes':out.stat().st_size,'sha256':hashlib.sha256(out.read_bytes()).hexdigest(),'members':len(paths),'source':status['source'],'production_unchanged':True}
(P/'archive-receipt.json').write_text(json.dumps(receipt,indent=2));print(json.dumps(receipt))
