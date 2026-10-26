"""One reviewed counts proof and one separately approved formal window."""
import argparse,sys
from pathlib import Path
sys.dont_write_bytecode=True
from support import *
p=argparse.ArgumentParser();p.add_argument('--output',required=True);p.add_argument('--start-lock-fd',type=int,required=True);p.add_argument('--phase',choices=['counts','timing'],required=True);a=p.parse_args()
out=Path(a.output);results=[]
try:
 assert not (out/'FAILED.json').exists(),'failed one-attempt evaluation cannot resume'
 require_linux(a.start_lock_fd)
 f=read_json(out/'freeze-manifest.json');node=f['node'];fh=sha(out/'freeze-manifest.json')
 marker=out/(a.phase.upper()+'_ATTEMPTED.json');assert not marker.exists(),'phase already attempted, no retry'
 def check_identity():
  for arm,files in f['source_sha256'].items():
   assert source_index(out/arm)==files,arm+' source changed'
   assert sha(out/arm/'third_party/librustzcash/target/release/librustzcash.a')==f['native_sha256'],'native changed'
  for q,h in f['binary_sha256'].items():assert sha(q)==h,q+' binary changed'
  for q,h in f['packet_file_sha256'].items():assert sha(PACKET/q)==h,q+' packet changed'
  assert sha(PACKET/'packet-manifest.json')==f['packet_manifest_sha256']
  assert node_identity()==node,'node identity changed'
 ready=read_json(out/'READY_FOR_COUNTS.json')
 assert ready['freeze_sha256']==fh and ready['preflight_sha256']==sha(out/'preflight.json') and ready['packet_manifest_sha256']==f['packet_manifest_sha256']
 check_identity()
 if a.phase=='counts':
  assert not (out/'counts-proof.json').exists(),'no second counts proof'
  write_json(marker,{'phase':'counts','freeze_sha256':fh,'ready_counts_sha256':sha(out/'READY_FOR_COUNTS.json'),'start_unix':time.time()})
  for arm in ['parent-counts','candidate-counts']:
   cmd=[str(out/(arm+'-state.test')),'-test.run=^$','-test.bench=^BenchmarkCanonicalStorage(Cursor|Scatter)$','-test.benchtime=64x','-test.count=1','-test.timeout=600s']
   r=run_job(out,node,cmd,out/arm/'core/state',arm+'-counts-proof',{'arm':arm,'counts_only':True,'purpose':'counts only, instrumented ns/op excluded from every gate'},timeout=900)
   results.append(r);write_json(out/'counts-proof.json',results)
  check_identity();assert sha('/proc/%d/exe'%node['pid'])==f['node_binary_sha256'],'live binary changed'
  write_json(out/'post-counts-identity.json',{'all_unchanged':True,'freeze_sha256':fh,'ready_counts_sha256':sha(out/'READY_FOR_COUNTS.json'),'attempt_sha256':sha(marker),'proof_sha256':sha(out/'counts-proof.json'),'packet_manifest_sha256':f['packet_manifest_sha256'],'node':node})
  write_json(out/'READY_FOR_TIMING.json',{'freeze_sha256':fh,'postcounts_sha256':sha(out/'post-counts-identity.json'),'counts_proof_sha256':sha(out/'counts-proof.json')})
 else:
  rt=read_json(out/'READY_FOR_TIMING.json');pc=read_json(out/'post-counts-identity.json')
  assert rt['freeze_sha256']==fh and rt['postcounts_sha256']==sha(out/'post-counts-identity.json') and rt['counts_proof_sha256']==sha(out/'counts-proof.json')
  assert pc['all_unchanged'] is True and pc['freeze_sha256']==fh and pc['proof_sha256']==sha(out/'counts-proof.json') and pc['attempt_sha256']==sha(out/'COUNTS_ATTEMPTED.json') and pc['ready_counts_sha256']==sha(out/'READY_FOR_COUNTS.json')
  assert pc['packet_manifest_sha256']==f['packet_manifest_sha256']
  assert not (out/'timing-results.json').exists(),'no second formal window'
  proof=read_json(out/'counts-proof.json');assert len(proof)==2 and all(r['exit']==0 and not r['aborted'] for r in proof)
  for r in proof:assert sha(r['log'])==r['sha256']
  write_json(marker,{'phase':'timing','freeze_sha256':fh,'ready_timing_sha256':sha(out/'READY_FOR_TIMING.json'),'postcounts_sha256':sha(out/'post-counts-identity.json'),'start_unix':time.time()})
  jobs=[('component','state','^BenchmarkCanonicalStorage(Cursor|Scatter)$',64,'core/state'),('canonical','core','^BenchmarkCanonicalStorageBlock$',64,'core'),('golden','vm','^BenchmarkNileSerialCallLifecycle$/^GoldenTron$',1024,'vm'),('tusd','vm','^BenchmarkNileSerialCallLifecycle$/^TUSD$',8192,'vm')]
  for ordinal,letter in enumerate('ABBAABBAAB',1):
   arm='parent' if letter=='A' else 'candidate'
   for label,pkg,selector,ops,cwd in jobs:
    cmd=[str(out/(arm+'-'+pkg+'.test')),'-test.run=^$','-test.bench='+selector,'-test.benchtime='+str(ops)+'x','-test.count=1','-test.timeout=600s']
    r=run_job(out,node,cmd,out/arm/cwd,('%02d-%s-%s'%(ordinal,arm,label)),{'arm':arm,'label':label,'ordinal':ordinal,'expected_iterations':ops,'counts_only':False},timeout=900)
    results.append(r);write_json(out/'timing-results.json',results)
  check_identity();assert sha('/proc/%d/exe'%node['pid'])==f['node_binary_sha256'],'live binary changed'
  write_json(out/'post-timing-identity.json',{'all_unchanged':True,'freeze_sha256':fh,'timing_results_sha256':sha(out/'timing-results.json'),'attempt_sha256':sha(marker),'ready_timing_sha256':sha(out/'READY_FOR_TIMING.json'),'postcounts_sha256':sha(out/'post-counts-identity.json'),'packet_manifest_sha256':f['packet_manifest_sha256'],'node':node})
except BaseException as e:
 if not (out/'FAILED.json').exists():write_json(out/'FAILED.json',{'phase':a.phase,'error':type(e).__name__+': '+str(e),'one_attempt_only':True})
 raise
