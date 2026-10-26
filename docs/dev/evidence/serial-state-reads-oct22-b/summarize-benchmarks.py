import json,pathlib,re,statistics
p=pathlib.Path(__file__).resolve().parent
all={}
for revision in ['parent','candidate']:
    data={}
    for n in range(1,6):
        for line in (p/f'{revision}-bench-{n}.log').read_text().splitlines():
            if not line.startswith('BenchmarkPredecessorCandidates/'):continue
            parts=line.split();name=parts[0].removeprefix('BenchmarkPredecessorCandidates/').removesuffix('-1')
            metrics={parts[i+1]:float(parts[i]) for i in range(2,len(parts)-1,2)}
            data.setdefault(name,[]).append(metrics)
    for name,rows in data.items():assert len(rows)==5,(revision,name,len(rows))
    all[revision]=data
summary={};passed=True
for name in all['parent']:
    entry={}
    for revision in ['parent','candidate']:
        rows=all[revision][name];ns=[r['ns/op'] for r in rows];med=statistics.median(ns)
        entry[revision]={'median_ns':med,'spread':(max(ns)-min(ns))/med,'ns_samples':ns,'metrics_median':{k:statistics.median([r[k] for r in rows]) for k in rows[0]}}
    a,b=entry['parent'],entry['candidate'];entry['speedup_fraction']=1-b['median_ns']/a['median_ns']
    primary=name.endswith('/cold') or name.endswith('/churn')
    if primary:
        af=a['metrics_median']['fullsorts/op'];bf=b['metrics_median']['fullsorts/op'];entry['sort_reduction_fraction']=1-bf/af if af else None
        entry['passed']=entry['speedup_fraction']>=0.20 and af>0 and bf<=af/2 and a['spread']<0.30 and b['spread']<0.30
    else:entry['passed']=b['median_ns']<=a['median_ns']*1.10
    passed=passed and entry['passed'];summary[name]=entry
result={'passed':passed,'workloads':summary}
(p/'benchmark-summary.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
