import pathlib,json,hashlib,collections,statistics,subprocess,datetime,zipfile,os
P=pathlib.Path('/data/gtron/history-tx-runs-20261025-01');OLD=pathlib.Path('/data/gtron/history-key-owner-20261025-01');R=P/'tail-read-comparison'
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(4<<20),b''):h.update(b)
 return h.hexdigest()
def read(p):return json.loads(p.read_text())
def write(n,x):(P/n).write_text(json.dumps(x,indent=2))
assert read(P/'pipeline-status.json')['complete'] and read(P/'corpus-status.json')['complete'] and read(P/'native-status.json')['complete']
assert read(P/'census-native-status.json')['complete']
subprocess.run(['/usr/bin/python3',str(P/'census-summary.py')],check=True,timeout=180)
original={x['candidate']['path']:x for x in map(json.loads,(OLD/'corpus-files.jsonl').open())}
a=collections.Counter();allocated0=allocated1=0;wins=losses=equal=0;largest=[];seen=set();endchecks=[];versions=collections.Counter();saved_bins=collections.Counter()
for line in (P/'corpus-files.jsonl').open():
 x=json.loads(line);src=x['source'];dst=x['candidate'];name=src['path'];assert name not in seen;seen.add(name);o=original[name];v=x['audit'];assert src==o['candidate'];assert src['size']==v['SourceBytes'];assert v['Keys']==o['audit']['Keys'] and v['Records']==o['audit']['Records'];assert v['DictionaryDigest']==o['audit']['DictionaryDigest'];assert v['SourcePostingBytes']==o['audit']['PostingBytes'];assert v['OffsetBytes']==o['audit']['OffsetBytes']
 for k,value in v.items():
  if isinstance(value,int) and k!='Version':a[k]+=value
 versions[v['Version']]+=1

 out=P/'corpus-v11'/name;source=OLD/'corpus-v10'/name
 with out.open('rb') as durable:os.fsync(durable.fileno())
 digest=sha(out);source_digest=sha(source);assert 'sha256:'+digest==dst['checksum'];assert 'sha256:'+source_digest==src['checksum'];st=out.stat();ss=source.stat();assert st.st_size==dst['size']==v['CandidateBytes'];assert st.st_ino==x['candidate_inode'];assert ss.st_ino==x['source_inode'];assert (st.st_dev,st.st_ino)!=(ss.st_dev,ss.st_ino)
 endchecks.append({'path':name,'source_sha256':source_digest,'sha256':digest,'size':st.st_size,'allocated_bytes':st.st_blocks*512});allocated1+=st.st_blocks*512;allocated0+=ss.st_blocks*512
 gain=src['size']-dst['size'];wins+=gain>0;losses+=gain<0;equal+=gain==0;largest.append({'path':name,'source_bytes':src['size'],'candidate_bytes':dst['size'],'saved_bytes':gain})
 pct=gain/src['size']*100;saved_bins['negative' if pct<0 else 'zero' if pct==0 else '<0.1%' if pct<.1 else '<1%' if pct<1 else '<5%' if pct<5 else '>=5%']+=1
write('end-output-checksums.json',endchecks)
assert len(seen)==19949==len(original);assert a['SourceBytes']==119501234404;assert a['Keys']==2443423104 and a['Records']==14302423656
header=19949*120;directory=a['DirectoryBytes']
source_counts=collections.Counter()
for x in original.values():
 for k in ['OwnerDataBytes','OwnerDirectoryBytes','OwnerFooterBytes']:source_counts[k]+=x['audit'][k]
components=[{'part':'header','source':header,'candidate':header},{'part':'directory','source':directory,'candidate':directory},{'part':'key_pages','source':a['SourceKeyBytes'],'candidate':a['CandidateKeyBytes']},{'part':'multi_postings','source':a['SourcePostingBytes'],'candidate':a['PostingBytes']},{'part':'shared_offset_locator','source':a['OffsetBytes'],'candidate':a['OffsetBytes']}]
for k,n in [('OwnerDataBytes','owner_data'),('OwnerDirectoryBytes','owner_directory'),('OwnerFooterBytes','owner_footer')]:components.append(dict(part=n,source=source_counts[k],candidate=a[k]))
for k,n in [('TxDataBytes','shared_transaction_data'),('TxDirectoryBytes','shared_transaction_directory'),('TxFooterBytes','shared_transaction_footer')]:components.append(dict(part=n,source=0,candidate=a[k]))
assert sum(c['source'] for c in components)==a['SourceBytes'];assert sum(c['candidate'] for c in components)==a['CandidateBytes'];largest.sort(key=lambda x:x['saved_bytes'],reverse=True)
write('space-final.json',{'complete':True,'files':len(seen),'counts':dict(a),'formats':dict(versions),'saving_bins':dict(saved_bins),'saved_bytes':a['SourceBytes']-a['CandidateBytes'],'saved_percent':100*(1-a['CandidateBytes']/a['SourceBytes']),'source_allocated':allocated0,'candidate_allocated':allocated1,'allocated_saved_bytes':allocated0-allocated1,'smaller_files':wins,'larger_files':losses,'equal_files':equal,'components':components,'largest_savings':largest[:20],'smallest_savings':list(reversed(largest[-20:])),'allocation_note':'Final counts after fsync and full source/candidate SHA reread; initial writeback allocation not used. Both larger and smaller files included; shared table directories and checksums fully counted. Offline candidates remain additional disk occupancy until a separately authorized replacement.'})
rows=list(map(json.loads,(R/'requests.jsonl').open()));assert all(x['correct'] for x in rows);assert read(R/'comparison-status.json')['complete'];resources={}
for f in R.glob('*/server-requests.jsonl'):
 mode='candidate' if 'candidate' in f.parent.name else 'baseline'
 for line in f.open():
  x=json.loads(line);key=(mode,x['id'])
  if x['id']==999999:continue
  assert key not in resources;resources[key]=x
measured=[]
def tv(x):return x['Sec']*1000+x['Usec']/1000
def io(x):return {k:int(v) for k,v in (l.split(':',1) for l in x.splitlines())}
for r in rows:
 x=resources[(r['mode'],r['id'])];assert x['params'][1]==r['tag'];z=dict(r);z['server_ms']=x['elapsed_ns']/1e6;z['alloc_bytes']=x['alloc_bytes'];z['cpu_ms']=sum(tv(x['usage_after'][k])-tv(x['usage_before'][k]) for k in ['Utime','Stime']);before=io(x['io_before']);after=io(x['io_after']);z.update({k:after[k]-before[k] for k in ['rchar','syscr','read_bytes']});measured.append(z)
groups=collections.defaultdict(list)
for r in measured:
 if r['cache'] in ['warm','fresh-process-os-uncontrolled','private-os-cold-fresh-process']:groups[(r['mode'],r['cache'],r['tag'])].append(r)
metrics=['milliseconds','server_ms','cpu_ms','alloc_bytes','rchar','syscr','read_bytes'];summary=[]
for (mode,cache,tag),v in sorted(groups.items()):
 summary.append(dict(mode=mode,cache=cache,tag=tag,height=v[0]['height'],n=len(v),**{k:{'median':statistics.median(x[k] for x in v),'min':min(x[k] for x in v),'max':max(x[k] for x in v)} for k in metrics}))
write('http-resource-summary.json',summary)
proofs=[read(q) for q in R.glob('os-cold-*-residency.json')];assert len(proofs)==56 and all(x['fraction']<0.01 for x in proofs)
private_manifests={mode:read(R/(mode+'-private-files.json')) for mode in ['baseline','candidate']}
assert all(row.get('sha256') and row['private_inode']!=row['source_inode'] for rows_ in private_manifests.values() for row in rows_)
ready=read(R/'comparison-status.json')['head']
def normalize_hash(value):
 try:
  decoded=bytes.fromhex(value).decode('ascii')
  if len(decoded)==64 and all(c in '0123456789abcdefABCDEF' for c in decoded):return decoded.lower()
 except (ValueError,UnicodeError):pass
 return value.lower()
normalized=dict(ready)
for name in ['head_hash','root']:normalized[name]=normalize_hash(ready[name])
assert normalized['root']=='8fbf149684146a59b1650b318a768f0edbae0c8c1c3ceb2c5c610129c10f76a5'
write('http-final.json',{'complete':True,'all_responses_correct':True,'requests':len(rows),'measured_requests':sum(len(v) for v in groups.values()),'cold_residency_proofs':len(proofs),'max_pre_start_residency':max(x['fraction'] for x in proofs),'head':normalized,'raw_host_head':ready,'host_hash_display_note':'Unchanged host fmt %x on Stringer; normalize one ASCII-hex layer. Keep original ready receipts.','private_inputs_sha_verified_files':{mode:len(rows_) for mode,rows_ in private_manifests.items()},'baseline_sha256':sha(P/'baseline.test'),'candidate_sha256':sha(P/'candidate.test'),'oracle_specs_sha256':sha(R/'specs.json')})
for name,digest in read(P/'native-status.json')['production_source_inventory'].items():assert sha(P/'candidate-source'/name)==digest,name
harnesses={}
for rel in ['core/offline_fixed_http_test.go','core/offline_request_muhash_test.go','core/state/snapshots/offline_routing_test.go','core/state/snapshots/offline_v9_corpus_test.go','core/state/snapshots/offline_growth_spans_test.go','core/state/snapshots/history_v9_test.go','core/state/snapshots/history_v8_test.go','core/state/snapshots/history_v9_transcode_test.go','core/state/snapshots/history_v10_transcode_test.go','core/state/snapshots/history_v10_test.go','core/state/snapshots/offline_v10_corpus_test.go','core/state/snapshots/offline_v11_corpus_test.go','core/state/snapshots/history_v11_test.go','core/state/snapshots/history_v11_transcode_test.go']:
 q=P/'candidate-source'/rel;dest=P/(q.name+'.txt');dest.write_bytes(q.read_bytes());harnesses[rel]=sha(q)
write('harness-sha256.json',harnesses)

write('toolchain-final.json',{'go_version':subprocess.check_output(['/data/go/bin/go','version']).decode().strip(),'sapling_sha256':sha(P/'candidate-source/third_party/librustzcash/target/release/librustzcash.a'),'production_source_hashes_unchanged':True})
subprocess.run(['/usr/bin/python3',str(P/'complete-allocation.py')],check=True,timeout=1800)
subprocess.run(['/usr/bin/python3',str(P/'canonical-final.py')],check=True,timeout=1800)
subprocess.run(['/usr/bin/python3',str(P/'trace-summary.py')],stdout=(P/'trace-summary.log').open('wb'),check=True,timeout=60)
x=dict(l.split('=',1) for l in subprocess.check_output(['systemctl','show','gtron','-p','MainPID','-p','ActiveState','-p','ExecMainStartTimestamp']).decode().splitlines());pid=int(x['MainPID']);x.update(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),exe=os.readlink('/proc/%d/exe'%pid) if pid else None,sha256=sha(pathlib.Path('/proc/%d/exe'%pid)) if pid else None,cpu_count=os.cpu_count(),load=os.getloadavg(),meminfo=pathlib.Path('/proc/meminfo').read_text(),df=subprocess.check_output(['df','-B1','/data','/']).decode());write('final-context.json',x)
files=[q for q in P.iterdir() if q.is_file() and q.suffix in ['.json','.jsonl','.log','.py','.txt','.gz','.go'] and q.name not in ['evidence-sha256.json','archive-receipt.json','finalizer-status.json','finalize.log']]
for q in R.rglob('*'):
 if q.is_file() and not any(part.endswith('-input') for part in q.relative_to(R).parts):files.append(q)
checks={str(q.relative_to(P)):sha(q) for q in files};write('evidence-sha256.json',checks);files.append(P/'evidence-sha256.json')
zpath=pathlib.Path('/tmp/gtron-history-tx-runs-final-20261025-01.zip');assert not zpath.exists()
with zipfile.ZipFile(str(zpath),'w',zipfile.ZIP_DEFLATED) as z:
 for q in files:z.write(str(q),str(q.relative_to(P)))
os.chmod(str(zpath),0o644);write('archive-receipt.json',{'path':str(zpath),'bytes':zpath.stat().st_size,'sha256':sha(zpath),'members':len(files),'complete':True});print((P/'archive-receipt.json').read_text());print((P/'space-final.json').read_text()[:2300]);print((P/'http-final.json').read_text())
