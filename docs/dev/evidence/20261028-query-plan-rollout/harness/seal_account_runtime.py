"""Seal only after the original offline unit is terminal; no production changes."""
import pathlib,sys,json,os,subprocess,hashlib,struct,zipfile,time,urllib.request
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'cold.py').read_text().split('\ntry:\n until=',1)[0],'fixed-host-definitions','exec'))
R=pathlib.Path('/data/gtron/maintenance/first-cold-followup-20261028-01/account-runtime')
s=json.loads((R/'candidate-status.json').read_text());assert s['complete'] and s['source']=='0f93254f81b8c49738cc7c0cae79b84f63b32f26'
unit=subprocess.check_output(['systemctl','show','gtron-account-runtime-20261029-01','-p','ActiveState']).decode();assert 'ActiveState=active' not in unit,unit
assert identity()==s['production_after']==s['production_before']
requests=json.loads((R/'requests.json').read_text());assert len(requests)==56
with host('runtime-built-input',R/'http.test',R/'input-runtime') as ready:
 for height,want in [(9000000,207),(10000000,213),(15000000,238),(20000000,249)]:
  for repeat in range(2):
   q={'jsonrpc':'2.0','id':len(requests)+1,'method':'eth_call','params':[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},hex(height)]}
   began=time.perf_counter();req=urllib.request.Request(ready['url'],data=json.dumps(q).encode(),headers={'Content-Type':'application/json'})
   with urllib.request.urlopen(req,timeout=150) as f:body=json.loads(f.read())
   requests.append({'case':'runtime-built-input','height':height,'repeat':repeat,'seconds_loopback_http':time.perf_counter()-began,'request':q,'response':body});assert int(body['result'],16)==want
   (R/'requests.json').write_text(json.dumps(requests,indent=2))
before=json.loads((R.parent/'account-delta/input-gaps/state-snapshots/manifest.json').read_text());snap=R/'input-runtime/state-snapshots';after=json.loads((snap/'manifest.json').read_text())
stable=lambda m:sorted([r for r in m['segments'] if r['kind']!='account-history-routing-delta'],key=lambda r:r['path'])
assert stable(before)==stable(after) and before.get('progress')==after.get('progress')
assert [before['visibleTxStart'],before['visibleTxEnd']]==[after['visibleTxStart'],after['visibleTxEnd']]
receipt={'canonical_and_other_auxiliary_refs_sha256':hashlib.sha256(json.dumps(stable(before),sort_keys=True,separators=(',',':')).encode()).hexdigest(),'progress':after.get('progress'),'visible':[after['visibleTxStart'],after['visibleTxEnd']],'objects':[]}
(R/'metadata').mkdir()
for ref in after['segments']:
 if ref['kind']!='account-history-routing-delta':continue
 path=snap/ref['path']
 with path.open('rb') as f:
  head=f.read(64);n=struct.unpack('>I',head[12:16])[0];directories=struct.unpack('>Q',head[32:40])[0];f.seek(0);data=f.read(64+32*n+53*directories)
 assert 'sha256:'+hashlib.sha256(data).hexdigest()==ref['lookupRoot'] and path.stat().st_size==ref['size']
 name='metadata/'+ref['lookupRoot'].split(':')[1]+'.bin';(R/name).write_bytes(data);receipt['objects'].append({'ref':ref,'metadata':name,'allocated_bytes':path.stat().st_blocks*512})
(R/'routing-receipt.json').write_text(json.dumps(receipt,indent=2))
s.update(production_after=identity(),requests=len(requests),stage='runtime policy, built-input and same-input HTTP sealed');assert s['production_before']==s['production_after'];save()
target=pathlib.Path('/tmp/AAA-gtron-account-runtime-20261029-final.zip');assert not target.exists()
with zipfile.ZipFile(str(target),'x',zipfile.ZIP_DEFLATED) as z:
 for p in sorted(R.rglob('*')):
  if p.is_file() and not p.is_symlink() and p.suffix in ['.json','.jsonl','.log','.bin'] and 'source-candidate' not in p.parts and 'input-runtime' not in p.parts:z.write(str(p),str(p.relative_to(R)))
sealed={'bytes':target.stat().st_size,'sha256':hashlib.sha256(target.read_bytes()).hexdigest(),'path':str(target)}
(R/'final-archive-receipt.json').write_text(json.dumps(sealed,indent=2));print(json.dumps(sealed),flush=True)
