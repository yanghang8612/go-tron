"""Explicit audited recovery: retain successful functional, finish race and builds."""
import argparse,os,sys,signal,time,traceback
from pathlib import Path
sys.dont_write_bytecode=True
from support import *
p=argparse.ArgumentParser();p.add_argument('--output',required=True);p.add_argument('--start-lock-fd',type=int,required=True);a=p.parse_args()
out=Path(a.output).resolve()
assert str(out)=='/data/gtron/go-tron/build/linux-a2.20261022-b62686-cpu15'
assert not (out/'PTY_RECOVERY_ATTEMPTED.json').exists(),'one recovery attempt only'
write_json(out/'PTY_RECOVERY_ATTEMPTED.json',{'pid':os.getpid(),'pgid':os.getpgrp(),'sid':os.getsid(0),'started_unix':time.time()})
code=1
try:
 assert os.getsid(0)==os.getpid() and signal.getsignal(signal.SIGHUP)==signal.SIG_IGN,'launch through nohup setsid'
 require_linux(a.start_lock_fd)
 for name in ['FAILED.json','freeze-manifest.json','READY_FOR_COUNTS.json','COUNTS_ATTEMPTED.json','TIMING_ATTEMPTED.json']:
  assert not (out/name).exists(),name
 cleanup=read_json(out/'pty-interruption/cleanup.json')
 assert cleanup['no_live_owned_group'] and not cleanup['group_after']
 reference=read_json(PACKET/'pty-recovery-reference.json')
 for rel,h in reference['original_raw_sha256'].items():assert sha(out/rel)==h,'interrupted raw changed: '+rel
 manifest_sha=sha(PACKET/'packet-manifest.json');manifest=read_json(PACKET/'packet-manifest.json')
 assert sorted(q['path'] for q in manifest['files'])==sorted(str(q.relative_to(PACKET)) for q in PACKET.rglob('*') if q.is_file() and q.name!='packet-manifest.json' and '__pycache__' not in q.parts)
 for q in manifest['files']:assert (PACKET/q['path']).stat().st_size==q['bytes'] and sha(PACKET/q['path'])==q['sha256'],q['path']
 # PID reuse is not an orphan. Match saved start ticks, or an actual old-group
 # member still working in this evaluation's candidate directory.
 old={q['pid']:q['stat'].rsplit(') ',1)[1].split()[19] for q in cleanup['owned_group_before']}
 for proc in Path('/proc').iterdir():
  if not proc.name.isdigit():continue
  try:
   fields=(proc/'stat').read_text().rsplit(') ',1)[1].split()
   assert not (int(proc.name) in old and fields[19]==old[int(proc.name)]),'old owned identity still present'
   if fields[2:4]==['842','842']:
    assert os.readlink(str(proc/'cwd'))!=str(out/'candidate'),'old owned group still present'
  except (FileNotFoundError,ProcessLookupError,PermissionError):pass
 expected=read_json(out/'prevalidation-sources.json')
 arms=['parent','candidate','parent-counts','candidate-counts']
 assert {arm:source_index(out/arm) for arm in arms}==expected,'source changed after interruption'
 pins=read_json(PACKET/'toolchain-pins.json');inputs=read_json(PACKET/'input-manifest.json');identity=read_json(PACKET/'source-identity.json')
 for arm in arms:
  assert plain(['git','rev-parse','HEAD'],out/arm)==identity['tested_B_commit']
  assert sha(out/arm/'third_party/librustzcash/target/release/librustzcash.a')==pins['sapling']['sha256']
 for rel,h in identity['B_plus_A2_production_sha256'].items():assert sha(out/'candidate'/rel)==h,rel
 completed=[json.loads(l) for l in (out/'commands.jsonl').read_text().splitlines()]
 assert len(completed)==1 and completed[0]['label']=='functional' and completed[0]['exit']==0 and not completed[0]['aborted']
 assert sha(out/'functional.log')==completed[0]['sha256']==sha(out/'pty-interruption/functional.log.before-cleanup')
 assert not list(out.glob('*.test')),'no build may already have started'
 preflight=read_json(out/'preflight.json')
 assert {k:ENV[k] for k in preflight['environment']}==preflight['environment'],'frozen environment changed'
 oldnode=preflight['node'];node=node_identity();assert node==oldnode
 assert sha('/proc/%d/exe'%node['pid'])==preflight['node_binary_sha256'],'node binary changed'
 assert not pressure(resources(node),node,True)
 for unit in ['gtron-deploy.timer','gtron-deploy.service']:
  q=subprocess.run(['systemctl','is-active',unit],stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
  assert q.stdout.decode('utf-8','replace').strip()=='inactive'
 write_json(out/'PTY_RECOVERY_IDENTITY.json',{'pid':os.getpid(),'pgid':os.getpgrp(),'sid':os.getsid(0),'SIGHUP_ignored':True,'fd9_exclusive':True,'source_prevalidation_sha256':sha(out/'prevalidation-sources.json'),'functional_preserved_sha256':completed[0]['sha256'],'packet_manifest_sha256':sha(PACKET/'packet-manifest.json'),'node':node,'original_raw_sha256':reference['original_raw_sha256'],'environment':preflight['environment']})
 selector='TestStorageReadScope|TestSnapshotCacheFill|TestSnapshotFillPublish|TestStorageCursor|TestCanonicalStorageScope|TestProcessBlockCanonicalStorageScope|TestPhysicalReadCallerClassify'
 cmd=[GO,'test','-p','1','-tags=sapling','-race','./core/blockbuffer','./core/rawdb/pebbledb','./core/state','./core','-run',selector,'-count=1','-timeout=300s']
 run_job(out,node,cmd,out/'candidate','race-controller-recovery',timeout=1800)
 for arm in arms:
  for pkg in (['state','core','vm'] if '-counts' not in arm else ['state']):
   package='./core/state' if pkg=='state' else './'+pkg
   run_job(out,node,[GO,'test','-p','1','-tags=sapling',package,'-c','-o',str(out/(arm+'-'+pkg+'.test'))],out/arm,'build-'+arm+'-'+pkg,timeout=1800)
 assert sha(PACKET/'packet-manifest.json')==manifest_sha
 for q in manifest['files']:assert (PACKET/q['path']).stat().st_size==q['bytes'] and sha(PACKET/q['path'])==q['sha256'],q['path']
 assert node_identity()==oldnode and sha('/proc/%d/exe'%node['pid'])==preflight['node_binary_sha256']
 for name in ['go','sapling','header']:assert sha(pins[name]['path'])==pins[name]['sha256'],name
 assert {k:ENV[k] for k in preflight['environment']}==preflight['environment']
 sources={arm:source_index(out/arm) for arm in arms};assert sources==expected
 for arm in arms:assert sha(out/arm/'third_party/librustzcash/target/release/librustzcash.a')==pins['sapling']['sha256']
 for rel in inputs['common']:assert len(set(sources[arm][rel] for arm in arms))==1,rel
 freeze={'output_root':str(out),'prevalidation_sources_sha256':sha(out/'prevalidation-sources.json'),'baseline_commit':identity['tested_B_commit'],'baseline_tree':identity['tested_B_tree'],'source_sha256':sources,'binary_sha256':{str(q):sha(q) for q in out.glob('*.test')},'packet_manifest_sha256':sha(PACKET/'packet-manifest.json'),'packet_file_sha256':{str(q.relative_to(PACKET)):sha(q) for q in PACKET.rglob('*') if q.is_file() and '__pycache__' not in q.parts},'node':node,'node_binary_sha256':sha('/proc/%d/exe'%node['pid']),'environment':{k:ENV[k] for k in ENV if k in ['GOMAXPROCS','GOGC','GOMEMLIMIT','CGO_ENABLED','GOOS','GOARCH','GOFLAGS','GOWORK','GOPROXY','GOTOOLCHAIN','GOCACHE','GOMODCACHE']},'affinity_cpu':15,'observer_cpu':0,'native_sha256':pins['sapling']['sha256'],'go_sha256':pins['go']['sha256'],'timing_started':False,'Darwin_NO_GO_unchanged':True,'controller_recovery_identity_sha256':sha(out/'PTY_RECOVERY_IDENTITY.json')}
 assert len(freeze['binary_sha256'])==8
 write_json(out/'freeze-manifest.json',freeze);write_json(out/'READY_FOR_COUNTS.json',{'freeze_sha256':sha(out/'freeze-manifest.json'),'preflight_sha256':sha(out/'preflight.json'),'packet_manifest_sha256':sha(PACKET/'packet-manifest.json')})
 code=0
except BaseException as e:
 if not (out/'FAILED.json').exists():write_json(out/'FAILED.json',{'phase':'PTY-controller-recovery','error':str(e),'no_retry':True})
 traceback.print_exc()
finally:
 if not (out/'PTY_RECOVERY_EXIT.json').exists():write_json(out/'PTY_RECOVERY_EXIT.json',{'pid':os.getpid(),'exit':code,'finished_unix':time.time(),'counts_started':False,'timing_started':False})
sys.exit(code)
