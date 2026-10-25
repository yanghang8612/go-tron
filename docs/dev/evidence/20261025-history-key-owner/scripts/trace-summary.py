import pathlib,re,json,collections
P=pathlib.Path('/data/gtron/history-key-owner-20261025-01');R=P/'tail-read-comparison';out=[]
for name in ['nine-million','oldest']:
 for mode in ['baseline','candidate']:
  path=R/(name+'-trace-'+mode)/'read.trace';markers=0;pending={};counts=collections.defaultdict(collections.Counter);samples=[]
  for line in path.open(errors='replace'):
   if re.search(r'read\(\d+</proc/\d+/io>, "rchar:',line):markers+=1;continue
   if markers!=1:continue
   m=re.match(r'\s*(\d+)\s+(.*)',line)
   if not m:continue
   tid,body=m.groups()
   if '<unfinished ...>' in body:
    pending[tid]=body.split('<unfinished ...>')[0];continue
   if body.startswith('<... ') and ' resumed>' in body:
    prefix=pending.pop(tid,None)
    if prefix is None:continue
    body=prefix+body.split(' resumed>',1)[1]
   m=re.match(r'(read|pread64)\(\d+<([^>]+)>',body);n=re.search(r'= (\d+)\s*$',body)
   if not m or not n or int(n.group(1))==0:continue
   file=pathlib.Path(m.group(2));size=int(n.group(1))
   try:relative=file.relative_to(P/(mode+'-input'))
   except ValueError:continue
   kind=file.suffix or '(no suffix)';counts[kind].update(calls=1,bytes=size,small_reads=int(size<256));samples.append({'path':str(relative),'bytes':size})
  assert markers==2,(name,mode,markers)
  row={'name':name,'mode':mode,'query_io_markers':markers,'by_suffix':dict(counts),'database_read_calls':len(samples),'database_read_bytes':sum(x['bytes'] for x in samples),'small_read_calls':sum(x['bytes']<256 for x in samples)};out.append(row)
(P/'query-trace-summary.json').write_text(json.dumps(out,indent=2));print(json.dumps(out,indent=2))
