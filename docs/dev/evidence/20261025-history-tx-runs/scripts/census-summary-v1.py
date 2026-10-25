import pathlib,json,collections,hashlib,datetime,sys
P=pathlib.Path(sys.argv[1]) if len(sys.argv)>1 else pathlib.Path('/data/gtron/history-tx-runs-20261025-01')
status=json.loads((P/'census-status.json').read_text());assert status['complete']
plan=json.loads((P/'census-plan.json').read_text());rows=[json.loads(l) for l in (P/'census-rows.jsonl').open()];assert len(rows)==len(plan)==status['files']==8651
bucket=[{},{}];boundaries=[];counts=collections.Counter();seen=set()
for p,r in zip(plan,rows):
 ref=p['history_ref'];assert r['Path']==ref['path'] and r['Path'] not in seen;seen.add(r['Path']);assert r['SHA256']==ref['checksum'] and r['AccessorSHA256']==p['accessor_ref']['checksum'];assert r['Records']==p['records'] and r['LogicalBytes']==p['logical_bytes'];assert r['RangeBytes']+r['RecordBytes']==r['LogicalBytes']
 counts.update(files=1,records=r['Records'],history_file_bytes=r['PhysicalBytes'],accessor_file_bytes=r['AccessorBytes'],logical_file_bytes=r['LogicalBytes'],range_header_bytes=r['RangeBytes'],record_frame_bytes=r['RecordBytes'],excluded_rows=r['ExcludedRows'])
 if r['FirstBlock']<25000000 or r['LastBlock']>35000000:boundaries.append({k:r[k] for k in ['Path','FirstBlock','LastBlock','Records','ExcludedRows','PhysicalBytes','LogicalBytes']})
 else:counts['fully_contained_history_file_bytes']+=r['PhysicalBytes']
 selected=0
 for bi,domains in enumerate(r['Domains']):
  for d in domains:
   assert d['Rows']==d['Present']+d['Absent']==sum(d['LengthBins']) and d['Empty']<=d['Present'] and d['FrameBytes']==d['Rows']*21
   z=bucket[bi].setdefault(d['Name'],{'Name':d['Name']});selected+=d['Rows']
   for k,v in d.items():
    if k=='Name':continue
    if isinstance(v,list):
     prev=z.setdefault(k,[0]*len(v));assert len(prev)==len(v)
     for i,n in enumerate(v):prev[i]+=n
    else:z[k]=z.get(k,0)+v
 assert selected+r['ExcludedRows']==r['Records']
for b in bucket:
 for d in b.values():d['LogicalRecordBytes']=d['PayloadBytes']+d['FrameBytes']
result={'complete':True,'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'coverage_inclusive':[25000000,35000000],'buckets':[{'from':25000000,'to_exclusive':30000000,'domains':sorted(bucket[0].values(),key=lambda d:d['PayloadBytes'],reverse=True)},{'from':30000000,'to_inclusive':35000000,'domains':sorted(bucket[1].values(),key=lambda d:d['PayloadBytes'],reverse=True)}],'counts':dict(counts),'boundary_segments':boundaries,'source_receipts_sha256':hashlib.sha256((P/'census-rows.jsonl').read_bytes()).hexdigest(),'note':'Domain bytes are exact decoded stored before-image payloads and 21-byte record headers. Physical compressed bytes are measured per file, never apportioned by domain. Boundary files are reported separately; every record was block-filtered.'}
(P/'census-summary.json').write_text(json.dumps(result,indent=2));print(json.dumps({'counts':result['counts'],'top_payloads':[[[d['Name'],d['Rows'],d['PayloadBytes']] for d in b['domains'][:8]] for b in result['buckets']]},indent=2))
