import pathlib,json,zipfile,hashlib,os,datetime,subprocess,urllib.request,time,traceback
p=pathlib.Path('/data/gtron/cold-query-layout-20261020-01')
begin=time.monotonic()
while True:
 f=p/'lazy-final-comparison/comparison-status.json';s=json.loads(f.read_text()) if f.exists() else {}
 if s.get('phase')=='failed':raise RuntimeError(s)
 if s.get('complete') and subprocess.check_output(['systemctl','show','gtron-cold-lazy-final-compare-20261020.service','-p','ActiveState']).decode().strip()=='ActiveState=inactive':break
 if time.monotonic()-begin>1200:raise TimeoutError('bounded final comparison wait')
 time.sleep(2)
pid=int(subprocess.check_output(['systemctl','show','gtron','-p','MainPID']).decode().split('=')[1]);binary=hashlib.sha256(pathlib.Path(f'/proc/{pid}/exe').read_bytes()).hexdigest();assert pid==28737 and binary=='5848de48f2f24a128054ff0967cddfd7256c54029f674b317d37f98ac23c63c4'
proof={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'production_mutations':False,'pid':pid,'binary_sha256':binary,'service':subprocess.check_output(['systemctl','show','gtron','-p','MainPID','-p','ActiveState','-p','SubState','-p','ExecMainStartTimestamp']).decode(),'datadir':{},'old_database':{},'process_status':pathlib.Path(f'/proc/{pid}/status').read_text(),'meminfo':pathlib.Path('/proc/meminfo').read_text(),'own_units':{}}
for key,path in [('datadir','/data/gtron/main/datadir'),('old_database','/data/gtron/main/datadir-pre-compact-v3-20261019-659ab078')]:
 f=pathlib.Path(path);st=f.stat();proof[key]={'path':str(f),'exists':True,'inode':st.st_ino,'device':st.st_dev,'realpath':str(f.resolve())}
for unit in ['gtron-cold-lazy-final-prepare-20261020.service','gtron-cold-lazy-final-compare-20261020.service','gtron-cold-account-gc-final-20261020.service','gtron-cold-lazy-caller-check-20261020.service']:
 proof['own_units'][unit]=subprocess.check_output(['systemctl','show',unit,'-p','ActiveState','-p','SubState','-p','CPUQuotaPerSecUSec','-p','MemoryLimit']).decode()
request=urllib.request.Request('http://127.0.0.1:8090/wallet/getnowblock',b'{}',{'Content-Type':'application/json'})
with urllib.request.urlopen(request,timeout=20) as r:head=json.loads(r.read())
proof['live_head']=head['block_header']['raw_data']['number'];proof['live_block_id']=head['blockID'];assert proof['live_head']>11818192
manifest=pathlib.Path('/data/gtron/main/datadir/gtron/state-snapshots/manifest.json');m=json.loads(manifest.read_text());proof['live_manifest']={k:v for k,v in m.items() if k not in ('segments','retired','pending')};proof['live_manifest_active_refs']=len(m.get('segments',[]));proof['live_manifest_retired_refs']=len(m.get('retired',[]))
(p/'lazy-final-production-proof.json').write_text(json.dumps(proof,indent=2))
files=[]
for f in p.iterdir():
 if f.is_file() and f.name.startswith(('lazy-final-','lazy-caller-','prepare-lazy-final','compare-lazy-final','check-lazy-caller','seal-lazy-final','lazy-history-request-cache')) and f.suffix in ('.json','.jsonl','.log','.py','.go'):files.append(f)
for d in ['lazy-final-comparison','lazy-caller-preflight']:
 for f in (p/d).rglob('*'):
  if f.is_file() and 'cache-input' not in str(f.relative_to(p)):files.append(f)
files=sorted(set(files));inventory={str(f.relative_to(p)):{'bytes':f.stat().st_size,'sha256':hashlib.sha256(f.read_bytes()).hexdigest()} for f in files}
z=pathlib.Path('/tmp/gtron-cold-lazy-final-20261020-01.zip');assert not z.exists()
with zipfile.ZipFile(z,'x',zipfile.ZIP_DEFLATED,6) as out:
 for f in files:out.write(f,str(f.relative_to(p)))
 out.writestr('evidence-inventory.json',json.dumps(inventory,indent=2))
os.chmod(z,0o644);receipt={'complete':True,'files':len(files),'bytes':z.stat().st_size,'sha256':hashlib.sha256(z.read_bytes()).hexdigest(),'server':str(z),'live_head':proof['live_head']};(p/'lazy-final-seal-receipt.json').write_text(json.dumps(receipt,indent=2));print(json.dumps(receipt))
