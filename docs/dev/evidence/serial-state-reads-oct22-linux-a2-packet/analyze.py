from pathlib import Path
import json,re,statistics,hashlib,sys
out=Path(sys.argv[1]).resolve()
assert not (out/'FAILED.json').exists(),'failed evaluation cannot produce PASS'
def read(name):return json.loads((out/name).read_text())
def digest(name):return hashlib.sha256((out/name).read_bytes()).hexdigest()
freeze=read('freeze-manifest.json');remote=Path(freeze['output_root']);fh=digest('freeze-manifest.json')
assert freeze['baseline_commit']=='572dcc377584be867542d0a56e0faf164cb1aa41'
packet=Path(__file__).resolve().parent
assert hashlib.sha256((packet/'packet-manifest.json').read_bytes()).hexdigest()==freeze['packet_manifest_sha256']
rc=read('READY_FOR_COUNTS.json');ca=read('COUNTS_ATTEMPTED.json');pc=read('post-counts-identity.json');rt=read('READY_FOR_TIMING.json');ta=read('TIMING_ATTEMPTED.json');pt=read('post-timing-identity.json')
assert rc['freeze_sha256']==fh and rc['preflight_sha256']==digest('preflight.json') and rc['packet_manifest_sha256']==freeze['packet_manifest_sha256']
assert ca['phase']=='counts' and ca['freeze_sha256']==fh and ca['ready_counts_sha256']==digest('READY_FOR_COUNTS.json')
assert pc['all_unchanged'] is True and pc['freeze_sha256']==fh and pc['proof_sha256']==digest('counts-proof.json') and pc['attempt_sha256']==digest('COUNTS_ATTEMPTED.json') and pc['ready_counts_sha256']==digest('READY_FOR_COUNTS.json') and pc['packet_manifest_sha256']==freeze['packet_manifest_sha256']
assert rt['freeze_sha256']==fh and rt['postcounts_sha256']==digest('post-counts-identity.json') and rt['counts_proof_sha256']==digest('counts-proof.json')
assert ta['phase']=='timing' and ta['freeze_sha256']==fh and ta['ready_timing_sha256']==digest('READY_FOR_TIMING.json') and ta['postcounts_sha256']==digest('post-counts-identity.json')
assert pt['all_unchanged'] is True and pt['freeze_sha256']==fh and pt['timing_results_sha256']==digest('timing-results.json') and pt['attempt_sha256']==digest('TIMING_ATTEMPTED.json') and pt['ready_timing_sha256']==digest('READY_FOR_TIMING.json') and pt['postcounts_sha256']==digest('post-counts-identity.json') and pt['packet_manifest_sha256']==freeze['packet_manifest_sha256'] and pt['node']==freeze['node']
runs=json.loads((out/'timing-results.json').read_text())
assert len(runs)==40 and all(x['exit']==0 and not x['aborted'] for x in runs),'complete frozen 10 arms first'
shapes={
 'component':set('BenchmarkCanonicalStorageCursor/'+x for x in ['cold4096','steady4096','hot4096','no-cache4096','overlay4096','mixed128','single','empty'])|{'BenchmarkCanonicalStorageScatter/scatter4096'},
 'canonical':{'BenchmarkCanonicalStorageBlock/storage128','BenchmarkCanonicalStorageBlock/empty'},
 'golden':{'BenchmarkNileSerialCallLifecycle/GoldenTron'},
 'tusd':{'BenchmarkNileSerialCallLifecycle/TUSD'},
}
expected_shapes=set().union(*shapes.values());assert len(expected_shapes)==13
jobs=[('component','state','^BenchmarkCanonicalStorage(Cursor|Scatter)$',64),('canonical','core','^BenchmarkCanonicalStorageBlock$',64),('golden','vm','^BenchmarkNileSerialCallLifecycle$/^GoldenTron$',1024),('tusd','vm','^BenchmarkNileSerialCallLifecycle$/^TUSD$',8192)]
for x,(ordinal,letter,(label,pkg,selector,ops)) in zip(runs,[(i,l,j) for i,l in enumerate('ABBAABBAAB',1) for j in jobs]):
 arm='parent' if letter=='A' else 'candidate'
 expected_command=['taskset','-c','15',str(remote/(arm+'-'+pkg+'.test')),'-test.run=^$','-test.bench='+selector,'-test.benchtime='+str(ops)+'x','-test.count=1','-test.timeout=600s']
 assert (x['arm'],x['ordinal'],x['label'],x['expected_iterations'])==(arm,ordinal,label,ops)
 assert x['command']==expected_command,(x['command'],expected_command)
 assert x['log']==str(remote/(f'{ordinal:02}-{arm}-{label}.log'))
 assert not x['counts_only']
def parse(path):
 rows={}
 for l in Path(path).read_text().splitlines():
  if not l.startswith('Benchmark'):continue
  f=l.split()
  assert len(f)>=4,(path,l)
  assert f[0] in expected_shapes,(path,f[0])
  assert f[0] not in rows,(path,'duplicate',f[0])
  expected=8192 if '/TUSD' in f[0] else (1024 if '/GoldenTron' in f[0] else 64)
  assert int(f[1])==expected,(path,f[0],f[1],expected)
  metrics={}
  for i in range(2,len(f)-1,2):
   try:metrics[f[i+1]]=float(f[i])
   except ValueError:pass
  if 'ns/op' in metrics:rows[f[0]]=metrics
 return rows
samples={}
for x in runs:
 assert hashlib.sha256((out/Path(x['log']).name).read_bytes()).hexdigest()==x['sha256']
 rows=parse(out/Path(x['log']).name)
 assert set(rows)==shapes[x['label']],(x['log'],set(rows),shapes[x['label']])
 for name,metrics in rows.items():samples.setdefault(name,{}).setdefault(x['arm'],[]).append(metrics)
assert set(samples)==expected_shapes
proof=json.loads((out/'counts-proof.json').read_text())
assert len(proof)==2 and [x['arm'] for x in proof]==['parent-counts','candidate-counts']
counts={}
for x in proof:
 assert x['exit']==0 and not x['aborted']
 assert hashlib.sha256((out/Path(x['log']).name).read_bytes()).hexdigest()==x['sha256']
 arm=x['arm']
 assert x['command']==['taskset','-c','15',str(remote/(arm+'-state.test')),'-test.run=^$','-test.bench=^BenchmarkCanonicalStorage(Cursor|Scatter)$','-test.benchtime=64x','-test.count=1','-test.timeout=600s']
 counts[arm]=parse(out/Path(x['log']).name);assert set(counts[arm])==shapes['component']
results={}
all_gates=[]
primaries={'BenchmarkCanonicalStorageCursor/cold4096','BenchmarkCanonicalStorageCursor/steady4096'}
absolute={'BenchmarkCanonicalStorageCursor/single','BenchmarkCanonicalStorageCursor/empty','BenchmarkCanonicalStorageBlock/empty'}
relative={'BenchmarkCanonicalStorageCursor/hot4096','BenchmarkCanonicalStorageCursor/overlay4096','BenchmarkCanonicalStorageCursor/mixed128','BenchmarkCanonicalStorageScatter/scatter4096','BenchmarkCanonicalStorageBlock/storage128','BenchmarkNileSerialCallLifecycle/GoldenTron','BenchmarkNileSerialCallLifecycle/TUSD'}
for name,arms in samples.items():
 assert set(arms)=={'parent','candidate'} and all(len(v)==5 for v in arms.values())
 p=[x['ns/op'] for x in arms['parent']];c=[x['ns/op'] for x in arms['candidate']]
 pm,cm=statistics.median(p),statistics.median(c)
 item={'parent_ns':p,'candidate_ns':c,'parent_median_ns':pm,'candidate_median_ns':cm,'time_improvement_pct':100*(pm-cm)/pm,'incremental_usec':(cm-pm)/1000,'parent_spread':(max(p)-min(p))/pm,'candidate_spread':(max(c)-min(c))/cm,'gates':{}}
 if name in primaries:
  pc,cc=counts['parent-counts'][name],counts['candidate-counts'][name]
  item['count_metrics']={'parent':pc,'candidate':cc}
  denom=pc['SSTiters/op'];reduction=(denom-cc['SSTiters/op'])/denom if denom else None
  item['SST_construction_reduction_pct']=None if reduction is None else reduction*100
  item['gates']={'time_at_least_10pct':cm<=pm*.90,'SST_construction_at_least_50pct':reduction is not None and reduction>=.50,'basecalls_max_5pct_increase':cc['basecalls/op']<=pc['basecalls/op']*1.05,'parent_spread_below_0_30':item['parent_spread']<.30,'candidate_spread_below_0_30':item['candidate_spread']<.30}
 elif name in absolute:item['gates']={'absolute_increment_at_most_25_usec':cm-pm<=25000}
 elif name in relative:item['gates']={'median_regression_at_most_5pct':cm<=pm*1.05}
 else:item['classification']='diagnostic; no additional post-result gate invented'
 item['passed']=all(item['gates'].values()) if item['gates'] else None
 all_gates.extend(item['gates'].values());results[name]=item
summary={'performance_gate':'PASS' if all(all_gates) else 'FAIL','Darwin_A2_still_NO_GO':True,'baseline_B_commit':'572dcc377584be867542d0a56e0faf164cb1aa41','frozen_manifest_sha256':hashlib.sha256((out/'freeze-manifest.json').read_bytes()).hexdigest(),'counts_only_timing_excluded':True,'samples':5,'iterations':{'component':64,'canonical':64,'golden':1024,'tusd':8192},'shape_count':len(results),'results':results,'limitations':['One Linux/amd64 Go1.25.5 CGO1/Sapling evaluation of B-only versus B+A2 on a live shared host; not an online speedup claim.','Counter metrics are benchmark-reported rounded values from actual counters.','Live SST counts from humanized Stat are quantized; raw level text and filesystem counts are retained separately.','No-cache component is diagnostic under the originally frozen explicit thresholds.']}
(out/'performance-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps({'gate':summary['performance_gate'],'failed':{n:r['gates'] for n,r in results.items() if r['passed'] is False}},indent=2))
