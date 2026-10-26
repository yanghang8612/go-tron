"""Local lightweight dummy-process proof; never executes any benchmark or server action."""
import argparse,importlib.util,os,subprocess,sys,json,hashlib
from pathlib import Path
sys.dont_write_bytecode=True
packet=Path(__file__).resolve().parent
p=argparse.ArgumentParser();p.add_argument('--output',required=True);a=p.parse_args();out=Path(a.output).resolve();assert not out.exists();out.mkdir(parents=True)
spec=importlib.util.spec_from_file_location('linux_packet_support_dryrun',str(packet/'support.py'));support=importlib.util.module_from_spec(spec);spec.loader.exec_module(support)
real_popen=subprocess.Popen;real_resources=support.resources;real_pressure=support.pressure;results=[]
for kind in ['resource_exception','json_exception','interruption']:
 root=out/kind;root.mkdir();(root/'tmp').mkdir();children=[];calls=[0]
 def launch(cmd,**kwargs):
  child=real_popen([sys.executable,'-c','import time; time.sleep(60)'],**kwargs);children.append(child);return child
 def resource(node):
  calls[0]+=1
  if calls[0]==1:return {}
  if kind=='resource_exception':raise RuntimeError('injected observer error')
  if kind=='interruption':raise KeyboardInterrupt('injected interruption')
  return {'non_json':object()}
 support.subprocess.Popen=launch;support.resources=resource;support.pressure=lambda *args,**kwargs:False
 caught=None
 try:support.run_job(root,{},['DUMMY_ONLY'],root,'dummy')
 except BaseException as e:caught=type(e).__name__
 finally:
  support.subprocess.Popen=real_popen;support.resources=real_resources;support.pressure=real_pressure
 assert caught is not None and len(children)==1 and children[0].poll() is not None
 try:os.kill(children[0].pid,0)
 except ProcessLookupError:alive=False
 else:alive=True
 assert not alive,'orphan dummy process'
 row=json.loads((root/'commands.jsonl').read_text().strip());assert row['aborted'] and row['exit']!=0
 results.append({'case':kind,'caught':caught,'child_exit':children[0].returncode,'child_reaped':True,'no_orphan':True})
root=out/'failed_marker';root.mkdir();failed=b'{"phase":"injected","reason":"must preserve"}\n';(root/'FAILED.json').write_bytes(failed)
for script,extra in [('analyze.py',[]),('run.py',['--phase','timing','--start-lock-fd','9999'])]:
 cmd=[sys.executable,str(packet/script)]+([str(root)] if script=='analyze.py' else ['--output',str(root)]+extra)
 r=subprocess.run(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT);(root/(script+'.log')).write_bytes(r.stdout)
 assert r.returncode!=0 and not (root/'performance-summary.json').exists() and (root/'FAILED.json').read_bytes()==failed
 results.append({'case':'FAILED blocks '+script,'exit_nonzero':True,'no_PASS_output':True,'original_failure_preserved':True})
root=out/'post_identity_failure';root.mkdir()
def write(name,value):(root/name).write_text(json.dumps(value,sort_keys=True)+'\n')
def digest(name):return hashlib.sha256((root/name).read_bytes()).hexdigest()
pm=hashlib.sha256((packet/'packet-manifest.json').read_bytes()).hexdigest();node={'pid':1,'exe':'DUMMY','start_ticks':'1'}
write('preflight.json',{});write('counts-proof.json',[{},{}]);write('timing-results.json',[{}]*40)
write('freeze-manifest.json',{'baseline_commit':'572dcc377584be867542d0a56e0faf164cb1aa41','output_root':'/data/gtron/go-tron/build/linux-a2.DUMMY','packet_manifest_sha256':pm,'node':node});fh=digest('freeze-manifest.json')
write('READY_FOR_COUNTS.json',{'freeze_sha256':fh,'preflight_sha256':digest('preflight.json'),'packet_manifest_sha256':pm})
write('COUNTS_ATTEMPTED.json',{'phase':'counts','freeze_sha256':fh,'ready_counts_sha256':digest('READY_FOR_COUNTS.json')})
write('post-counts-identity.json',{'all_unchanged':True,'freeze_sha256':fh,'proof_sha256':digest('counts-proof.json'),'attempt_sha256':digest('COUNTS_ATTEMPTED.json'),'ready_counts_sha256':digest('READY_FOR_COUNTS.json'),'packet_manifest_sha256':pm})
write('READY_FOR_TIMING.json',{'freeze_sha256':fh,'postcounts_sha256':digest('post-counts-identity.json'),'counts_proof_sha256':digest('counts-proof.json')})
write('TIMING_ATTEMPTED.json',{'phase':'timing','freeze_sha256':fh,'ready_timing_sha256':digest('READY_FOR_TIMING.json'),'postcounts_sha256':digest('post-counts-identity.json')})
write('post-timing-identity.json',{'all_unchanged':False,'freeze_sha256':fh,'timing_results_sha256':digest('timing-results.json'),'attempt_sha256':digest('TIMING_ATTEMPTED.json'),'ready_timing_sha256':digest('READY_FOR_TIMING.json'),'postcounts_sha256':digest('post-counts-identity.json'),'packet_manifest_sha256':pm,'node':node})
r=subprocess.run([sys.executable,str(packet/'analyze.py'),str(root)],stdout=subprocess.PIPE,stderr=subprocess.STDOUT);(root/'analyze.log').write_bytes(r.stdout)
assert r.returncode!=0 and not (root/'performance-summary.json').exists()
results.append({'case':'40 metadata rows but failed post-identity','exit_nonzero':True,'no_PASS_output':True})
write_path=out/'dryrun-proof.json';write_path.write_text(json.dumps({'kind':'dummy processes and synthetic identity JSON only; no timings/benchmarks/server actions','cases':results,'all_pass':True},indent=2)+'\n');print(write_path)
