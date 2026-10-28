import pathlib,json,statistics,sys
root=pathlib.Path(sys.argv[1]);samples=json.loads((root/'recomputed-samples.json').read_text());groups={}
def group(name,rows):
 if not rows:return
 fields=['seconds','cpu_seconds','allocated_bytes','logical_bytes','physical_read_bytes','read_syscalls','num_gc']
 groups[name]={'n':len(rows),'seconds_min':min(x['seconds'] for x in rows),'seconds_max':max(x['seconds'] for x in rows),'samples':[(x['path'],x['iteration']) for x in rows],'medians':{k:statistics.median(x[k] for x in rows) for k in fields}}
for mode in ['baseline','previous','candidate']:
 rows=[x for x in samples if x['path'].startswith('metadata-v3/repeat-') and x['path'].endswith(mode)]
 group(mode+'-process-first-os-uncontrolled',[x for x in rows if x['iteration']==0]);group(mode+'-subsequent',[x for x in rows if x['iteration']>0]);group(mode+'-subsequent-zero-physical',[x for x in rows if x['iteration']>0 and x['physical_read_bytes']==0])
 group(mode+'-metadata',[x for x in samples if x['path']=='metadata-v3/metadata-'+mode and x['iteration']>0])
 group(mode+'-os-cold',[x for x in samples if x['path'].startswith('metadata-cold-v3/revalidated-os-cold-') and x['path'].endswith(mode)])
(root/'performance-summary.json').write_text(json.dumps(groups,indent=2))
for name,v in groups.items():print(name,json.dumps({k:w for k,w in v.items() if k!='samples'}))
