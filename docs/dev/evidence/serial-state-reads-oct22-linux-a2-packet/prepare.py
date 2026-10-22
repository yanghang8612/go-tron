"""Authorized once-only preparation; no service/helper/production DB action."""
import argparse,os,re,shutil,subprocess,json,time,sys
from pathlib import Path
sys.dont_write_bytecode=True
from support import *
p=argparse.ArgumentParser();p.add_argument('--repo',required=True);p.add_argument('--output',required=True);p.add_argument('--start-lock-fd',type=int,required=True);a=p.parse_args()
out=Path(a.output)
assert re.match(r'^/data/gtron/go-tron/build/linux-a2\.[A-Za-z0-9_-]+$',str(out))
assert not out.exists(),'unique single evaluation output required'
require_linux(a.start_lock_fd)
identity=read_json(PACKET/'source-identity.json');inputs=read_json(PACKET/'input-manifest.json');repo=Path(a.repo)
assert repo.resolve()==Path('/data/gtron/go-tron')
base=identity['tested_B_commit'];assert plain(['git','rev-parse',base+'^{tree}'],repo)==identity['tested_B_tree']
# Compare old entries and explicit production changes before accepting B.
def git_entries(commit):
 raw=subprocess.check_output(['git','ls-tree','-r','-z',commit],cwd=str(repo));entries={}
 for row in raw.split(b'\0'):
  if not row:continue
  meta,name=row.split(b'\t');mode,kind,oid=meta.split();entries[name.decode()]={'mode':mode.decode(),'kind':kind.decode(),'oid':oid.decode()}
 return entries
entries=git_entries(base)
for rel,entry in identity['baseline_original_git_entries'].items():
 if rel in identity['B_production_sha256']:continue
 assert entries.get(rel)==entry,'B changed an unapproved old input: '+rel
for rel,entry in entries.items():
 if rel not in identity['baseline_original_git_entries'] and rel.endswith('.go'):
  assert rel in ['core/blockbuffer/predecessor_candidates_test.go','core/blockbuffer/predecessor_candidates_bench_test.go','core/blockbuffer/predecessor_candidates_v2_test.go'],'unexpected active Go source in B'
for rel,h in identity['B_production_sha256'].items():
 data=subprocess.check_output(['git','show',base+':'+rel],cwd=str(repo));assert __import__('hashlib').sha256(data).hexdigest()==h,rel
assert sha(PACKET/'a2-production.patch')==inputs['sealed_A2_patch_sha256']
node=node_identity();before=resources(node);assert not pressure(before,node,True),'initial resource admission failed'
out.mkdir();(out/'tmp').mkdir()
def checked(cmd,cwd=None):
 started=time.time();r=subprocess.run(cmd,cwd=str(cwd) if cwd else None,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
 with (out/'prepare-actions.jsonl').open('a') as f:f.write(json.dumps({'command':cmd,'cwd':str(cwd) if cwd else None,'start_unix':started,'elapsed_sec':time.time()-started,'exit':r.returncode,'stdout_stderr':r.stdout.decode('utf-8','replace')},sort_keys=True)+'\n')
 if r.returncode:raise RuntimeError('preparation command failed: '+str(cmd))
def diagnostic(cmd):
 try:return {'command':cmd,'exit':0,'stdout':plain(cmd)}
 except subprocess.CalledProcessError as e:return {'command':cmd,'exit':e.returncode,'stdout':e.output.decode('utf-8','replace')}
try:
 write_json(out/'preflight.json',{'node':node,'node_binary_sha256':sha('/proc/%d/exe'%node['pid']),'resources':before,'affinity_before_observer_pin':'see readonly-preflight.json; allowed0..15','observer_affinity':sorted(os.sched_getaffinity(0)),'environment':{k:ENV[k] for k in ['GOMAXPROCS','GOGC','GOMEMLIMIT','CGO_ENABLED','GOOS','GOARCH','GOFLAGS','GOWORK','GOPROXY','GOTOOLCHAIN','GOCACHE','GOMODCACHE']},'diagnostics':[diagnostic(c) for c in [[GO,'version'],['lscpu','-p=CPU,CORE,SOCKET,NODE'],['taskset','-c','15','python3','-c','import os; print(sorted(os.sched_getaffinity(0)))'],['lsblk','-o','NAME,MAJ:MIN,TYPE,MOUNTPOINT'],['systemctl','is-active','gtron-deploy.timer'],['systemctl','is-active','gtron-deploy.service'],['systemctl','is-active','gtron-cold-query-observe.service'],['gcc','--version']]],'selected_cpu':15,'SMT_sibling':7,'no_service_change':True})
 # Timer must already be quiesced by the audited outer release protocol.
 assert diagnostic(['systemctl','is-active','gtron-deploy.timer'])['stdout']=='inactive','auto timer must already be paused'
 assert diagnostic(['systemctl','is-active','gtron-deploy.service'])['stdout']=='inactive','auto deploy must already be drained'
 assert diagnostic(['taskset','-c','15','python3','-c','import os; print(sorted(os.sched_getaffinity(0)))'])['stdout']=='[15]'
except Exception as e:
 write_json(out/'FAILED.json',{'phase':'preflight','error':str(e)});raise
try:
 pins=read_json(PACKET/'toolchain-pins.json')
 for arm in ['parent','candidate']:
  dest=out/arm
  checked(['git','clone','--shared','--no-checkout',str(repo),str(dest)])
  checked(['git','checkout','--detach',base],cwd=dest)
  assert plain(['git','rev-parse','HEAD'],dest)==base
  if arm=='candidate':
   checked(['git','apply','--check',str(PACKET/'a2-production.patch')],cwd=dest)
   checked(['git','apply',str(PACKET/'a2-production.patch')],cwd=dest)
   for rel,h in identity['B_plus_A2_production_sha256'].items():assert sha(dest/rel)==h,rel
   for rel in ['core/blockbuffer/predecessor.go','core/blockbuffer/layer_view.go']:assert sha(dest/rel)==identity['B_production_sha256'][rel],rel
   for rel,item in inputs['correctness'].items():
    target=dest/rel;target.parent.mkdir(parents=True,exist_ok=True);assert sha(PACKET/item['packet_path'])==item['sha256'];shutil.copyfile(str(PACKET/item['packet_path']),str(target))
  for rel,item in inputs['common'].items():
   target=dest/rel;target.parent.mkdir(parents=True,exist_ok=True);assert sha(PACKET/item['packet_path'])==item['sha256'];shutil.copyfile(str(PACKET/item['packet_path']),str(target))
  target=dest/'third_party/librustzcash/target/release/librustzcash.a';target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(pins['sapling']['path'],str(target));assert sha(target)==pins['sapling']['sha256']
 for arm in ['parent','candidate']:
  dest=out/(arm+'-counts');shutil.copytree(str(out/arm),str(dest))
  checked(['git','apply',str(PACKET/'counts-hook.patch')],cwd=dest)
  shutil.copyfile(str(PACKET/'counts-hook.go.txt'),str(dest/'core/state/storage_cursor_counts_test.go'))
  for rel,h in inputs['counts_hook'].items():assert sha(dest/rel)==h
 prevalidation_sources={arm:source_index(out/arm) for arm in ['parent','candidate','parent-counts','candidate-counts']}
 write_json(out/'prevalidation-sources.json',prevalidation_sources)
 # No performance result exists yet. Functional/race failures stop this attempt.
 selector='TestStorageReadScope|TestSnapshotCacheFill|TestSnapshotFillPublish|TestStorageCursor|TestCanonicalStorageScope|TestProcessBlockCanonicalStorageScope|TestPhysicalReadCallerClassify'
 for label,race in [('functional',False),('race',True)]:
  cmd=[GO,'test','-p','1','-tags=sapling']+(['-race'] if race else [])+['./core/blockbuffer','./core/rawdb/pebbledb','./core/state','./core','-run',selector,'-count=1','-timeout=300s']
  run_job(out,node,cmd,out/'candidate',label,timeout=1800)
 for arm in ['parent','candidate','parent-counts','candidate-counts']:
  for pkg in (['state','core','vm'] if '-counts' not in arm else ['state']):
   package='./core/state' if pkg=='state' else './'+pkg
   run_job(out,node,[GO,'test','-p','1','-tags=sapling',package,'-c','-o',str(out/(arm+'-'+pkg+'.test'))],out/arm,'build-'+arm+'-'+pkg,timeout=1800)
 sources={arm:source_index(out/arm) for arm in ['parent','candidate','parent-counts','candidate-counts']}
 assert sources==prevalidation_sources,'validation/build changed source inputs'
 for arm in sources:assert sha(out/arm/'third_party/librustzcash/target/release/librustzcash.a')==pins['sapling']['sha256'],'native input changed'
 for rel in inputs['common']:
  assert len(set(sources[arm][rel] for arm in sources))==1,rel
 # Freeze all binaries, text sources and fixed execution inputs before counts.
 freeze={'output_root':str(out),'prevalidation_sources_sha256':sha(out/'prevalidation-sources.json'),'baseline_commit':base,'baseline_tree':identity['tested_B_tree'],'source_sha256':sources,'binary_sha256':{str(q):sha(q) for q in out.glob('*.test')},'packet_manifest_sha256':sha(PACKET/'packet-manifest.json'),'packet_file_sha256':{str(q.relative_to(PACKET)):sha(q) for q in PACKET.rglob('*') if q.is_file() and '__pycache__' not in q.parts},'node':node,'node_binary_sha256':sha('/proc/%d/exe'%node['pid']),'environment':{k:ENV[k] for k in ENV if k in ['GOMAXPROCS','GOGC','GOMEMLIMIT','CGO_ENABLED','GOOS','GOARCH','GOFLAGS','GOWORK','GOPROXY','GOTOOLCHAIN','GOCACHE','GOMODCACHE']},'affinity_cpu':15,'observer_cpu':0,'native_sha256':pins['sapling']['sha256'],'go_sha256':pins['go']['sha256'],'timing_started':False,'Darwin_NO_GO_unchanged':True}
 assert len(freeze['binary_sha256'])==8
 write_json(out/'freeze-manifest.json',freeze);write_json(out/'READY_FOR_COUNTS.json',{'freeze_sha256':sha(out/'freeze-manifest.json'),'preflight_sha256':sha(out/'preflight.json'),'packet_manifest_sha256':sha(PACKET/'packet-manifest.json')})
 print('Prepared once, correctness passed; counts and formal timing require explicit review.',flush=True)
except Exception as e:
 write_json(out/'FAILED.json',{'phase':'prepare','error':str(e),'one_attempt_only':True});raise
