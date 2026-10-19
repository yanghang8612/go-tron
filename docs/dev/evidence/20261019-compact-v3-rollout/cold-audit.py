from pathlib import Path
import os,json,hashlib,struct,zlib,datetime,subprocess,time,collections,urllib.request
p=Path('/data/gtron/compact-v3-rollout-20261019-01');src=Path('/data/gtron/main/datadir/gtron/state-snapshots');out=p/'cold-checkpoint-1';out.mkdir(mode=0o700)
m=json.loads((src/'manifest.json').read_text());assert m['version']==4 and m['elementVersion']==3 and m['commitmentScheme']=='state-flat-latest-muhash3072-compact-v3'
result={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'manifest_fields':{k:v for k,v in m.items() if k not in ('segments','retired')},'active_files':[],'index_pages':[],'bytes_by_kind':{}}
for r in m['segments']:
 rel=Path(r['path']);assert not rel.is_absolute() and '..' not in rel.parts
 f=src/rel;dest=out/rel;dest.parent.mkdir(parents=True,exist_ok=True);assert not f.is_symlink();os.link(str(f),str(dest));s=dest.stat();assert s.st_size==r['size'];h=hashlib.sha256(dest.read_bytes()).hexdigest();assert r['checksum']=='sha256:'+h
 result['active_files'].append({'path':str(rel),'size':s.st_size,'sha256':h,'kind':r['kind'],'dataset':r.get('dataset')});key=str(r.get('dataset',''))+'/'+r['kind'];result['bytes_by_kind'][key]=result['bytes_by_kind'].get(key,0)+s.st_size
 if str(rel).endswith('.idx'):
  with dest.open('rb') as stream:header=stream.read(96)
  version=struct.unpack_from('>I',header,8)[0];item={'path':str(rel),'version':version,'size':s.st_size}
  if version==8:
   code=struct.unpack_from('>I',header,36)[0];n=struct.unpack_from('>Q',header,48)[0];directory_len=struct.unpack_from('>Q',header,56)[0]
   assert zlib.crc32(header[:76])&0xffffffff==struct.unpack_from('>I',header,76)[0]
   with dest.open('rb') as stream:stream.seek(96);directory=stream.read(directory_len)
   assert zlib.crc32(directory)&0xffffffff==struct.unpack_from('>I',header,72)[0]
   assert len(directory)==n*24 and code&0x7fffffff==4096 and code>>31==1
   item.update({'codec':'snappy','page_bytes':code&0x7fffffff,'pages':n,'logical_bytes':struct.unpack_from('>Q',header,40)[0],'header_directory_crc_valid':True})
  result['index_pages'].append(item)
(out/'manifest.json').write_text(json.dumps(m));result['active_files_count']=len(result['active_files']);(p/'cold-proof.json').write_text(json.dumps(result,indent=2));print('COLD_PINNED',len(m['segments']),len(result['index_pages']),flush=True)
# Release only our temporary rollout locks and restore the previously active timer.
a=json.loads((p/'audit-status.json').read_text());assert a['complete'] and a['muhash_verified']
with urllib.request.urlopen('http://127.0.0.1:8090/wallet/getnodeinfo',timeout=8) as f:w=json.load(f)
assert w['currentBlock']>a['block_num'] and w['syncInfo']['active'] and not w['syncInfo']['paused']
(p/'release-locks').touch()
for i in range(20):
 r=subprocess.run(['systemctl','is-active','--quiet','gtron-compact-v3-rollout-locks-20261019.service'])
 if r.returncode:break
 time.sleep(1)
assert r.returncode!=0
subprocess.run(['systemctl','start','gtron-deploy.timer'],check=True)
r=subprocess.run(['systemctl','show','gtron-deploy.timer','-p','ActiveState','-p','SubState','-p','NextElapseUSecRealtime'],stdout=subprocess.PIPE,check=True)
(p/'timer-restored.json').write_text(json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'timer':r.stdout.decode(),'rollout_locks_released':True,'wallet':w},indent=2));print(r.stdout.decode(),flush=True)
