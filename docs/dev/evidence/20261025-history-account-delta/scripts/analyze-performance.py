import pathlib, json, re, collections, statistics, sys
P = pathlib.Path(sys.argv[1])
R = P / 'tail-read-comparison'
remote = pathlib.Path('/data/gtron/history-account-delta-20261025-01')
resources = {}
for f in R.glob('*/server-requests.jsonl'):
    mode = 'candidate' if 'candidate' in f.parent.name else 'baseline'
    for line in f.read_text().splitlines():
        x = json.loads(line)
        if x['id'] == 999999:
            continue
        key = (mode,x['id'])
        assert key not in resources, key
        resources[key] = x
def tv(x):
    return x['Sec']*1000+x['Usec']/1000
def io(x):
    return {k:int(v) for k,v in (line.split(':',1) for line in x.splitlines())}
measured = []
for r in map(json.loads,(R/'requests.jsonl').read_text().splitlines()):
    x=resources[(r['mode'],r['id'])]
    assert x['params'][1] == r['tag']
    z=dict(r,server_ms=x['elapsed_ns']/1e6,alloc_bytes=x['alloc_bytes'],
        cpu_ms=sum(tv(x['usage_after'][k])-tv(x['usage_before'][k]) for k in ('Utime','Stime')))
    a,b=io(x['io_before']),io(x['io_after'])
    z.update({k:b[k]-a[k] for k in ('rchar','syscr','read_bytes')})
    measured.append(z)
groups=collections.defaultdict(list)
for r in measured:
    if r['cache'] in ('warm','fresh-process-os-uncontrolled','private-os-cold-fresh-process'):
        groups[(r['mode'],r['cache'],r['tag'])].append(r)
metrics=('milliseconds','server_ms','cpu_ms','alloc_bytes','rchar','syscr','read_bytes')
out=[]
for (mode,cache,tag),v in sorted(groups.items()):
    out.append(dict(mode=mode,cache=cache,tag=tag,height=v[0]['height'],n=len(v),
        **{k:dict(median=statistics.median(x[k] for x in v),min=min(x[k] for x in v),max=max(x[k] for x in v)) for k in metrics}))
(P/'http-resource-summary.json').write_text(json.dumps(out,indent=2))
traces=[]
for name in ('nine-million','oldest'):
    for mode in ('baseline','candidate'):
        f=R/(name+'-trace-'+mode)/'read.trace'
        markers=0;pending={};counts=collections.defaultdict(collections.Counter);files=collections.defaultdict(collections.Counter)
        for line in f.open(errors='replace'):
            if re.search(r'read\(\d+</proc/\d+/io>, "rchar:',line):
                markers+=1
                continue
            if markers!=1:
                continue
            m=re.match(r'\s*(\d+)\s+(.*)',line)
            if not m:
                continue
            tid,body=m.groups()
            if '<unfinished ...>' in body:
                pending[tid]=body.split('<unfinished ...>')[0]
                continue
            if body.startswith('<... ') and ' resumed>' in body:
                prefix=pending.pop(tid,None)
                if prefix is None:
                    continue
                body=prefix+body.split(' resumed>',1)[1]
            m=re.match(r'(read|pread64)\(\d+<([^>]+)>',body)
            n=re.search(r'= (\d+)\s*$',body)
            if not m or not n or int(n.group(1))==0:
                continue
            file=pathlib.Path(m.group(2));size=int(n.group(1))
            try:
                relative=file.relative_to(remote/(mode+'-input'))
            except ValueError:
                continue
            counts[file.suffix or '(no suffix)'].update(calls=1,bytes=size,small_reads=int(size<256))
            if file.suffix in ('.seg','.kv','.idx','.route'):
                files[str(relative)].update(calls=1,bytes=size,small_reads=int(size<256))
        assert markers==2,(name,mode,markers)
        traces.append(dict(name=name,mode=mode,by_suffix=dict(counts),snapshot_files=dict(files),
            database_read_calls=sum(c['calls'] for c in counts.values()),
            database_read_bytes=sum(c['bytes'] for c in counts.values()),
            small_read_calls=sum(c['small_reads'] for c in counts.values())))
(P/'query-trace-summary.json').write_text(json.dumps(traces,indent=2))
print(json.dumps({'resource_groups':len(out),'trace_queries':len(traces)},indent=2))
