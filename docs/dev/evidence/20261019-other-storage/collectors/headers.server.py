import collections,datetime,json,pathlib,struct,hashlib,zlib
root=pathlib.Path('/data/gtron/other-storage-distribution-20261019-0245');base=pathlib.Path('/data/gtron/main/datadir/gtron');snap=base/'state-snapshots';m=json.loads((root/'pinned-state-manifest.json').read_text())
def u64(b,n):return struct.unpack_from('>Q',b,n)[0]
def u32(b,n):return struct.unpack_from('>I',b,n)[0]
ev=collections.Counter();ac=collections.Counter();tx=collections.Counter();anc={};codec=collections.Counter();txprefix=collections.Counter();txraw=(base/'ancient/tx-index/manifest.json').read_bytes();txm=json.loads(txraw)
for r in m['segments']:
 if r['dataset']=='event-log' and r['kind']=='event-log':
  with (snap/r['path']).open('rb') as f:b=f.read(176)
  assert b[:8]==b'gtevlg4\n';v=struct.unpack_from('>21Q',b,8);lengths=[v[i] for i in [6,8,10,12,14,16,18,20]];assert 176+sum(lengths)==r['size']
  ev['files']+=1;ev['rows']+=v[2];ev['header']+=176
  for name,n in zip(['block_dictionary','tx_dictionary','row_directory','row_data','payload_directory','payload_data','address_lookup','topic_lookup'],lengths):ev[name]+=n
  ev['total']+=r['size']
 if r['dataset']=='state-domain-change' and r['kind']=='accessor':
  with (snap/r['path']).open('rb') as f:b=f.read(120)
  assert b[:8]==b'gtsdckv1' and u32(b,8)==7 and u32(b,112)==zlib.crc32(b[:112]);assert 120+u64(b,56)+u64(b,64)+u64(b,104)==r['size']
  ac['files']+=1;ac['records']+=u64(b,28);ac['keys']+=u64(b,40);ac['block_directory']+=u64(b,56);ac['key_data']+=u64(b,64);ac['posting_data']+=u64(b,104);ac['header']+=120;ac['total']+=r['size']
for r in txm['runs']:
 f=base/'ancient/tx-index/runs'/r['file']
 with f.open('rb') as h:b=h.read(128)
 assert b[:8]==b'gtxi0001' and u32(b,8)==1;st=f.stat();assert u64(b,72)==st.st_size
 tx['files']+=1;tx['rows']+=u64(b,40);tx['directory']+=u64(b,56)*8;tx['fingerprints']+=u64(b,40)*8;tx['locations']+=u64(b,40)*8;tx['header']+=128;tx['total']+=st.st_size;txprefix[str(u32(b,16))]+=1
for f in (base/'ancient/v2').rglob('*.json'):
 v=json.loads(f.read_text())
 if 'tables' not in v:continue
 for name,path in v['tables'].items():
  q=base/'ancient/v2'/name/path
  with q.open('rb') as h:b=h.read(64);table=h.read(u64(b,32)*48)
  assert b[:8]==b'gtanv201';x=anc.setdefault(name,collections.Counter());x['files']+=1;x['blocks']+=u64(b,24);x['total']+=q.stat().st_size;x['frame_table']+=len(table);x['dictionary']+=u64(b,40)-64-len(table);x['header']+=64;codec[name+'/'+str(u32(b,52))]+=1
  for i in range(0,len(table),48):x['compressed']+=u64(table,i+24);x['uncompressed']+=u64(table,i+32)
  assert q.stat().st_size==u64(b,40)+sum(u64(table,i+24) for i in range(0,len(table),48))
tx['bucket_headers_and_trailers']=tx['total']-tx['header']-tx['directory']-tx['fingerprints']-tx['locations']
r={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'state_manifest_sha256':hashlib.sha256((root/'pinned-state-manifest.json').read_bytes()).hexdigest(),'event_log_v4_sections':dict(ev),'history_accessor_v7_sections':dict(ac),'transaction_index_manifest_sha256':hashlib.sha256(txraw).hexdigest(),'transaction_index_sections':dict(tx),'transaction_index_prefix_bits':dict(txprefix),'ancient_v2_tables':anc,'ancient_v2_codecs':dict(codec),'limitation':'header and section bounds verified; payload checksums not scanned in this header inventory'}
(root/'headers.json').write_text(json.dumps(r,indent=2));print(json.dumps(r,indent=2))
