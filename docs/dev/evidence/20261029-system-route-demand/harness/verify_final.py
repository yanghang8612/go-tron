"""Independently verify downloaded evidence, including resumed strace reads."""
import pathlib,json,zipfile,hashlib,collections,re,subprocess,sys
root=pathlib.Path(__file__).resolve().parents[1];repo=root.parents[3]
archive=pathlib.Path('/Users/asuka/Downloads/AAA-gtron-system-route-demand-20261029-02-final.zip')
sha='45389b15cfb1ff1d38d54cb4805705e048d9be229c7f9bfc6d4cb555cfcdda13'
out=repo/'build/benchmarks/20261029-system-route-demand/final'
# Reuse independent RPC/ready/root, no mmap, full-file read-closure and per-file
# zero-residency verifier. No database scan and no timing rerun.
verifier=repo/'docs/dev/evidence/20261028-query-plan-rollout/harness/verify_http_path_cold.py'
result=subprocess.check_output([sys.executable,str(verifier),str(archive),'4975677',sha,str(out)])
summary=json.loads(result);z=zipfile.ZipFile(archive)
def read(name):return json.loads(z.read(name))
s=read('whole-query-status.json');assert s['source']=='1d93528771f35d7e564c11372a32079a6f71e442' and not s['experimental_query_filters_enabled']
assert s['production_before']==s['production_after'] and s['binary_sha256']['candidate']=='5750c13701c7c24316b2efc55499f1df06642a8419666e464755dc730e3ffc9a'
before,after=read('coverage-before.json'),read('coverage-after.json');n=32036
assert before['coverage']==dict(sources=n,small=n,metadata=n,storage=6436)
assert after['coverage']==dict(sources=n,small=n,metadata=n,storage=n)
bootstrap=read('bootstrap-result.json');assert bootstrap['complete'] and bootstrap['canonical_refs_unchanged'] and bootstrap['frontiers_unchanged']
assert len(bootstrap['passes'])==101 and sum(x['built'] for x in bootstrap['passes'])==100
assert sum(x['canonical_source_reads'] for x in bootstrap['passes'])==0
assert sum(x['input_bytes'] for x in bootstrap['passes'])==0
summary['coverage_before']=before['coverage'];summary['coverage_after']=after['coverage'];summary['storage_route_added_bytes']=after['bytes_by_kind']['storage-history-routing']-before['bytes_by_kind']['storage-history-routing'];summary['bootstrap_seconds']=bootstrap['seconds']
summary['broad_route_read_bytes']={};summary['server_io_9m']=[]
for version in ['baseline','candidate']:
 pending={};total=0;calls=0
 for line in z.read('traced-discovery-'+version+'/syscalls.log').decode().splitlines():
  m=re.match(r'(\d+)\s+(.*)',line)
  if not m:continue
  pid,call=m.groups()
  if '<unfinished ...>' in call:pending[pid]=call.split('<unfinished ...>')[0];continue
  if call.startswith('<... ') and ' resumed>' in call:
   prior=pending.pop(pid,None)
   if prior is None:continue
   call=prior+call.split(' resumed>',1)[1]
  if '/history/system-routing-' in call and re.match(r'(read|pread64|readv|preadv|preadv2)\(',call):
   v=re.search(r' = (\d+)\s*$',call)
   if v:total+=int(v.group(1));calls+=1
 summary['broad_route_read_bytes'][version]=total
 if version=='baseline':assert total==4*66948766 and calls==4
 else:assert total==0 and calls==0
 for row in read('accepted.json'):
  if row['version']!=version or row['height']!=9000000:continue
  for repeat,r in enumerate([json.loads(x) for x in z.read(row['case']+'/server-requests.jsonl').decode().splitlines()]):
   def io(name):return {k:int(v) for k,v in re.findall(r'(\w+): (\d+)',r[name])}
   a,b=io('io_before'),io('io_after')
   summary['server_io_9m'].append(dict(version=version,round=row['round'],repeat=repeat,alloc_bytes=r['alloc_bytes'],read_bytes=b['read_bytes']-a['read_bytes'],logical_read_bytes=b['rchar']-a['rchar'],read_syscalls=b['syscr']-a['syscr']))
summary['production_identity_stable_during_matrix']=True
(root/'verification.json').write_text(json.dumps(summary,indent=2)+'\n')
for name in ['accepted.json','requests.json','coverage-before.json','coverage-after.json','bootstrap-result.json','whole-query-status.json','native-focused.log']:(root/name).write_bytes(z.read(name))
print(json.dumps(summary,indent=2))
