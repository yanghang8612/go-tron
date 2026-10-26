"""Recompute every resident-decoder result from archived raw pages."""
import collections,hashlib,json,pathlib,statistics,sys,tarfile,zipfile,io

ARMS=('v5-fresh','reference-fresh','scratch-fresh','v5-reused','reference-reused','scratch-reused')
METRICS=('Nanoseconds','Allocated','Mallocs','GC')
def digest(b):return hashlib.sha256(b).hexdigest()

def verify(archive,prior):
 assert pathlib.Path(archive).stat().st_size==712128 and digest(pathlib.Path(archive).read_bytes())=='cc18b1830e0016afc364c56e74e8aff2fd81f1e2408c3147461863ac0e08ed27'
 with tarfile.open(archive) as t:
  assert len(t.getnames())==len(set(t.getnames()))
  blobs={m.name:t.extractfile(m).read() for m in t.getmembers() if m.isfile()}
 declared=json.loads(blobs['archive-members.json'])
 assert set(blobs)==set(declared)|{'archive-members.json'}
 assert all(digest(blobs[n])==h for n,h in declared.items())
 status=json.loads(blobs['study-status.json']);assert status['complete'] and status['phase']=='complete' and not status['production_mutations']
 assert len(status['checks'])==4 and [x['label'] for x in status['checks']]==['native-bounds','native-race','native-build','real-pages']
 for check in status['checks']:
  assert check['returncode']==0 and digest(blobs[check['label']+'.log'])==check['log_sha256']
 assert b'PASS' in blobs['real-pages.log'] and b'SKIP' not in blobs['real-pages.log']
 assert status['production_before']==status['production_after']
 assert blobs['source-before.json']==blobs['source-after.json'] and blobs['inputs-before.json']==blobs['inputs-after.json']
 assert len(json.loads(blobs['source-before.json']))==2173
 assert status['go_version']=='go version go1.25.5 linux/amd64'
 assert status['sapling_sha256']=='b6c99f8fabac36d5a836c644bb79059449c89740bf33670d66088d094447e7ea'
 names=('DecoderArmParity','PayloadBounds','ScratchBoundsAndReuse','ScratchConcurrentOwnership')
 for label in ('native-bounds','native-race'):
  log=blobs[label+'.log']
  for name in names:assert ('--- PASS: TestOfflineShared'+name+' ').encode() in log
  assert b'FAIL' not in log and b'SKIP' not in log
 assert b'all 1817 pages, 11 ordinary pages, 6 arms, 5 rounds, 8 calls per cell' in blobs['real-pages.log']
 start=json.loads(blobs['start-receipt.json']);unit=start['observed_unit']
 assert unit['MemoryLimit']=='4294967296' and unit['CPUQuotaPerSecUSec']=='2s' and unit['Nice']=='15'
 assert start['production_before']==start['production_after']
 dispatch=json.loads(blobs['dispatch.json'])
 assert digest(blobs['study-payload.zip'])==dispatch['payload_sha256'] and len(blobs['study-payload.zip'])==dispatch['payload_bytes']
 assert dispatch['unit']=='gtron-history-shared-decoder-20261026-02.service' and not dispatch['production_mutations']
 for name,m in dispatch['manifest'].items():assert len(blobs[name])==m['bytes'] and digest(blobs[name])==m['sha256']
 with zipfile.ZipFile(io.BytesIO(blobs['study-payload.zip'])) as z:
  assert set(z.namelist())==set(dispatch['manifest']) and len(z.namelist())==len(dispatch['manifest'])
  for name in z.namelist():assert z.read(name)==blobs[name]
 local=json.loads(blobs['local-status.json']);assert local['go_version']=='go version go1.25.5 darwin/arm64'
 for name,m in local['manifest'].items():assert len(blobs[name])==m['bytes'] and digest(blobs[name])==m['sha256']
 for label in local['checks']:
  assert b'PASS' in blobs[label] and b'FAIL' not in blobs[label]
 env=json.loads(blobs['effective-go-env.json'])
 assert env['GOCACHE']=='/data/gtron/history-shared-decoder-20261026-01/go-build' and env['GOMAXPROCS']=='2' and env['GOMEMLIMIT']=='3GiB'
 previous=json.loads(blobs['previous-attempt-status.json'])
 assert previous['phase']=='failed' and not previous['complete'] and 'TimeoutExpired' in previous['error']
 assert digest(blobs['previous-attempt-status.json'])==status['retry_context']['previous_status_sha256']
 assert digest(blobs['offline_shared_payload_test.go'])=='86378ea65aa3685f1752be89847a3ac39affb42f31e36b956873463ccef7773c'
 assert digest(blobs['offline_shared_scratch_test.go'])=='bf3f425e9575676c5bfca3f75e45de3f2465fc5774f74baa257151f5c54545d6'
 assert digest(blobs['offline_shared_decoder_test.go'])=='e2c013b1169415403704e0c2f8804552b429a08a7172f543d87b0b11f3b73a6a'
 assert digest(pathlib.Path(prior).read_bytes())=='1f1148ee9885c9efcc793ac3058826b5029fe77f7b9941b83c5f456864bd03c5'
 with tarfile.open(prior) as t:
  old=t.extractfile('pages.jsonl').read();frozen=json.loads(t.extractfile('frozen-source-inventory.json').read())
 actual=json.loads(blobs['source-before.json']);assert len(frozen)==2149 and all(actual.get(n)==v for n,v in frozen.items())
 assert blobs['reference-pages.jsonl']==old
 assert digest(old)==status['prior_fingerprints']['pages.jsonl']
 assert digest(pathlib.Path(prior).read_bytes())==status['prior_fingerprints']['study-core.tar.xz']
 for name,h in status['helpers'].items():assert digest(blobs[name])==h
 rows=[json.loads(x) for x in blobs['decoder-pages.jsonl'].splitlines()]
 reference={(x['Pair'],x['Page']):x for x in map(json.loads,old.splitlines())}
 assert len(rows)==1817 and len({(x['Pair'],x['Page']) for x in rows})==len(rows)
 assert sum(not x['AnnotationsRecovered'] for x in rows)==11
 inputs=json.loads(blobs['selected-inputs.json'])
 assert [x['pair'] for x in inputs]==[458,2309,4147]
 for item in inputs:
  part=[x for x in rows if x['Pair']==item['pair']]
  assert [x['Page'] for x in part]==list(range(len(part)))
  assert sum(x['OriginalBytes'] for x in part)+28*len(part)+96==item['size']
  assert sum(x['OriginalMode']==-1 for x in part)==item['plain_pages']
  assert [sum(x['OriginalMode']==m for x in part) for m in range(3)]==item['column_key_modes']
 for row in rows:
  before=reference[(row['Pair'],row['Page'])]
  for k in ('RawBytes','OriginalBytes','PrototypeBytes','OriginalMode','RawSHA','OriginalSHA','PrototypeSHA','AnnotationsRecovered'):assert row[k]==before[k]
  assert row['Batch']==8 and row['Rounds']==5 and set(row['Arms'])==set(ARMS)
  for arm,value in row['Arms'].items():
   assert 0<=value['RetainedScratchMax']<=131108
   if arm.endswith('fresh') or arm=='reference-reused':assert value['RetainedScratchMax']==0
   for metric in METRICS:
    a=value[metric];assert len(a)==5 and all(isinstance(x,int) and x>=0 for x in a)
   assert all(x>0 for x in value['Nanoseconds'])
 generated=[]
 for pair in (458,2309,4147,'all'):
  part=[r for r in rows if pair=='all' or r['Pair']==pair]
  entry={'pair':pair,'pages':len(part),'original_bytes':sum(r['OriginalBytes'] for r in part),'prototype_bytes':sum(r['PrototypeBytes'] for r in part),'arms':{}}
  for arm in ARMS:
   metric={}
   for name in METRICS:
    totals=list(map(sum,zip(*(r['Arms'][arm][name] for r in part))))
    middle=sorted(totals)[2]
    metric[name]={'round_totals':totals,'median_per_pass':middle/8}
   metric['retained_scratch_max']=max(r['Arms'][arm]['RetainedScratchMax'] for r in part)
   entry['arms'][arm]=metric
  generated.append(entry)
 assert generated==json.loads(blobs['summary.json'])
 assert digest(blobs['decoder-pages.jsonl'])==status['pages_sha256'] and digest(blobs['summary.json'])==status['summary_sha256']
 result=[]
 for entry in generated:
  arms=entry['arms'];d={'pair':entry['pair'],'pages':entry['pages']}
  for cache in ('fresh','reused'):
   old,reference,new=(arms[n+'-'+cache] for n in ('v5','reference','scratch'))
   ot,rt,nt=(a['Nanoseconds']['median_per_pass'] for a in (old,reference,new))
   d[cache]={'v5_pass_seconds':ot/1e9,'reference_pass_seconds':rt/1e9,'scratch_pass_seconds':nt/1e9,'scratch_vs_reference_percent':(nt/rt-1)*100,'scratch_vs_v5_percent':(nt/ot-1)*100,'allocated_bytes_per_pass':{n:arms[n+'-'+cache]['Allocated']['median_per_pass'] for n in ('v5','reference','scratch')},'mallocs_per_pass':{n:arms[n+'-'+cache]['Mallocs']['median_per_pass'] for n in ('v5','reference','scratch')}}
  result.append(d)
 return {'verified_pages':len(rows),'ordinary_pages':11,'scope':status['scope'],'production_identity':status['production_before'],'effects':result}

if __name__=='__main__':
 if len(sys.argv)==1:
  here=pathlib.Path(__file__).resolve().parent;args=[here/'decoder-study-core.tar.xz',here/'study-core.tar.xz']
 elif len(sys.argv)==3:args=sys.argv[1:]
 else:raise SystemExit('usage: verify-decoder-study.py [decoder-study-core.tar.xz prior-study-core.tar.xz]')
 print(json.dumps(verify(*args),indent=2,sort_keys=True))
