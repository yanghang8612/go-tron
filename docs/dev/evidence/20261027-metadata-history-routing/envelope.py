import pathlib,zipfile,json,hashlib,time,subprocess,os,pwd,traceback,datetime
A=pathlib.Path('/data/gtron/maintenance/9m-query-20261027-01');E=A/'metadata-envelope';E.mkdir(mode=0o700,exist_ok=True);s={'complete':False,'stage':'wait page64 supplement','production_mutations':False}
def save():s['utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();(E/'status.json').write_text(json.dumps(s,indent=2))
def sha(b):return hashlib.sha256(b).hexdigest()
try:
 save()
 for i in range(180):
  st=json.loads((A/'metadata-page64/status.json').read_text())
  if st.get('error'):raise RuntimeError(st)
  if st.get('complete'):break
  time.sleep(10)
 else:raise RuntimeError('page64 timeout')
 s.update(stage='merge sealed measurements and full manifest evidence');save();files={};seals=[]
 for name in ['metadata-evidence-seal.json','metadata-page64-seal.json']:
  seal=json.loads((A/name).read_text());path=pathlib.Path(seal['file']);data=path.read_bytes();assert len(data)==seal['bytes'] and sha(data)==seal['sha256'];seals.append(seal)
  with zipfile.ZipFile(str(path)) as z:
   expected=json.loads(z.read('files-sha256.json'));assert set(z.namelist())==set(expected)|{'files-sha256.json'}
   for member,info in expected.items():
    b=z.read(member);assert len(b)==info['bytes'] and sha(b)==info['sha256'];assert member not in files or files[member]==b;files[member]=b
 for p in sorted((A/'metadata-final').glob('*-manifest.json.gz')):
  name=str(p.relative_to(A));assert name not in files;files[name]=p.read_bytes()
 assert all('metadata-final/'+n+'-manifest.json.gz' in files for n in ['before','previous','after'])
 for name in ['metadata-final/revalidated-runtime-limits.json','metadata-final/status.json','metadata-page64/status.json']:
  files['completed-state/'+name]=(A/name).read_bytes()
 files['envelope-provenance.json']=json.dumps({'inputs':seals,'measurements_unchanged':True,'added_evidence':'three original immutable full manifests omitted by initial glob; completed receipts'},indent=2).encode();files['run-metadata-envelope.py']=pathlib.Path(__file__).read_bytes()
 dest=pathlib.Path('/tmp/AAA-gtron-metadata-routing-acceptance-20261027.zip');members={}
 with zipfile.ZipFile(str(dest),'w',allowZip64=True) as z:
  for name,b in sorted(files.items()):members[name]={'bytes':len(b),'sha256':sha(b)};z.writestr(name,b,compress_type=zipfile.ZIP_STORED if name.endswith('.gz') else zipfile.ZIP_DEFLATED)
  z.writestr('files-sha256.json',json.dumps(members,indent=2),compress_type=zipfile.ZIP_DEFLATED)
 u=pwd.getpwnam('java-tron');os.chown(str(dest),u.pw_uid,u.pw_gid);os.chmod(str(dest),0o644);seal={'file':str(dest),'bytes':dest.stat().st_size,'sha256':sha(dest.read_bytes()),'members':len(members)};(A/'metadata-acceptance-seal.json').write_text(json.dumps(seal,indent=2));s.update(complete=True,stage='done',seal=seal);save();print(json.dumps(s))
except Exception:s['error']=traceback.format_exc();save();raise
