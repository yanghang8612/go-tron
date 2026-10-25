import pathlib,json,hashlib,zipfile,os
P=pathlib.Path('/data/gtron/history-account-delta-20261025-01')
assert json.loads((P/'acceptance-status.json').read_text())['complete']
files={}
for n in ('acceptance-status.json','native-status.json','final-source-inventory.json','muhash-driver-status.json',
          'muhash-census.json','history-restore.json','history-restore.log','complete-input-files.jsonl',
          'complete-input-status.json','complete-input-routing.json','layout-status.json','retained-source-relocations.json'):
 files[n]=P/n
for directory,dirs,names in os.walk(P/'tail-read-comparison'):
 dirs[:]=[d for d in dirs if not d.endswith('input')]
 for n in names:
  f=pathlib.Path(directory)/n;files[str(f.relative_to(P))]=f
def sha(f):
 h=hashlib.sha256()
 with f.open('rb')as r:
  for b in iter(lambda:r.read(4<<20),b''):h.update(b)
 return h.hexdigest()
checks={n:sha(f)for n,f in files.items()};out=pathlib.Path('/tmp/gtron-account-delta-http-20261025-01.zip');assert not out.exists()
with zipfile.ZipFile(out,'x',zipfile.ZIP_DEFLATED)as z:
 for n,f in sorted(files.items()):
  z.write(f,n);assert sha(f)==checks[n],n
 z.writestr('evidence-sha256.json',json.dumps(checks,indent=2))
r=dict(bytes=out.stat().st_size,sha256=sha(out),members=len(files)+1)
pathlib.Path('/tmp/gtron-account-delta-http-20261025-01-receipt.json').write_text(json.dumps(r,indent=2));print(json.dumps(r,indent=2),flush=True)
