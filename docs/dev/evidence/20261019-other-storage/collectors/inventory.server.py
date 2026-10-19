import collections,datetime,hashlib,json,os,pathlib,subprocess,struct
out=pathlib.Path('/data/gtron/other-storage-distribution-20261019-0245')
base=pathlib.Path('/data/gtron/main/datadir/gtron');p=base/'state-snapshots';raw=(p/'manifest.json').read_bytes();m=json.loads(raw)
def sumrows(rows):
 d={}
 for r in rows:
  k=r.get('dataset','')+'/'+r.get('kind','')
  if k not in d:d[k]={'files':0,'bytes':0,'rows':0,'from':r['fromTxNum'],'to':0}
  v=d[k];v['files']+=1;v['bytes']+=r['size'];v['rows']+=r.get('rows',0);v['from']=min(v['from'],r['fromTxNum']);v['to']=max(v['to'],r['toTxNum'])
 return d
refs=set(r['path'] for r in m['segments']);retired=set(r['path'] for r in m.get('retired',[]));physical={};unref=collections.defaultdict(lambda:{'files':0,'bytes':0});missing=[];changed=[];header=collections.Counter()
for f in p.rglob('*'):
 if not f.is_file():continue
 try:st=f.stat()
 except FileNotFoundError:continue
 rel=str(f.relative_to(p));key=rel.split('/')[0]+'/'+f.suffix
 v=physical.setdefault(key,{'files':0,'bytes':0,'allocated':0});v['files']+=1;v['bytes']+=st.st_size;v['allocated']+=st.st_blocks*512
 if rel not in refs:
  k='retired' if rel in retired else 'outside-current-manifest';v=unref[k];v['files']+=1;v['bytes']+=st.st_size
for r in m['segments']:
 f=p/r['path']
 try:
  st=f.stat()
  if st.st_size!=r['size']:changed.append(r['path'])
  with f.open('rb') as h:b=h.read(12)
  header[r.get('dataset','')+'/'+r['kind']+'/'+b[:8].hex()+'/'+str(struct.unpack('>I',b[8:12])[0] if len(b)>=12 else -1)]+=1
 except FileNotFoundError:missing.append(r['path'])
a=base/'ancient';ag={};versions=collections.Counter();v2=[]
for f in a.rglob('*'):
 if not f.is_file():continue
 try:st=f.stat()
 except FileNotFoundError:continue
 rel=str(f.relative_to(a));parts=rel.split('/');key=parts[0]+'/'+f.suffix if len(parts)>1 else f.name.split('.')[0]+'/'+f.suffix
 v=ag.setdefault(key,{'files':0,'bytes':0,'allocated':0});v['files']+=1;v['bytes']+=st.st_size;v['allocated']+=st.st_blocks*512
 if f.suffix=='.json' and f.parent.name=='v2':
  try:v2.append(json.loads(f.read_text()))
  except ValueError:pass
 if st.st_size>=64 and (f.suffix in ['.seg','.gtan','.dat'] or 'v2' in parts):
  with f.open('rb') as h:b=h.read(64)
  if b[:8]==b'gtanv201':versions[key+'/codec-'+str(struct.unpack('>I',b[52:56])[0])]+=1
pid=subprocess.check_output(['systemctl','show','gtron','-p','MainPID']).decode().strip().split('=')[1]
r={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'service_pid':pid,'service_exe':os.readlink('/proc/'+pid+'/exe'),'manifest_sha256':hashlib.sha256(raw).hexdigest(),'manifest_header':{k:v for k,v in m.items() if k not in ['segments','retired']},'active':sumrows(m['segments']),'retired':sumrows(m.get('retired',[])),'state_physical':physical,'state_outside_manifest':dict(unref),'missing':missing,'size_mismatches':changed,'state_file_versions':dict(header),'ancient_physical':ag,'ancient_codecs':dict(versions),'ancient_v2_manifests':len(v2)}
(out/'inventory.json').write_text(json.dumps(r,indent=2));(out/'pinned-state-manifest.json').write_bytes(raw)
print(json.dumps({'utc':r['utc'],'active':r['active'],'ancient':ag,'missing':len(missing),'size_mismatches':len(changed)},indent=2))
