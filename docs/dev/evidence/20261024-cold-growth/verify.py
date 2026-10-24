"""Recompute reported bucket sizes and domain counts from frozen receipts."""
from pathlib import Path
import json, gzip, hashlib, collections
P=Path(__file__).resolve().parent
load=lambda n:json.loads((P/n).read_text())
for n,h in load('SHA256SUMS.json').items():assert hashlib.sha256((P/n).read_bytes()).hexdigest()==h,n
m=json.loads(gzip.decompress((P/'growth-live-manifest.json.gz').read_bytes()))
spans={x['ref']['path']:x for x in map(json.loads,gzip.decompress((P/'growth-spans.jsonl.gz').read_bytes()).decode().splitlines())}
s=load('growth-summary.json');out=collections.defaultdict(collections.Counter);members=collections.defaultdict(list);assignment={};ranges={}
for x in spans.values():
 if 'error' not in x:
  pair=(x['ref']['fromTxNum'],x['ref']['toTxNum']);assert pair not in ranges;ranges[pair]=x
for r in m['segments']:
 if r['dataset']=='state-domain-change' and r['kind']=='history':
  x=spans[r['path']];assert x['ref']==r
  if 'error' in x:continue
  assert r['fromTxNum']==x['first']['BeginTxNum'] and r['toTxNum']==x['last']['EndTxNum']
  a=(x['first']['BlockNum']-1)//5000000;b=(x['last']['BlockNum']-1)//5000000
  if a!=b:continue
  assignment[(r['fromTxNum'],r['toTxNum'])]=a;members[a].append(x)
  out[a].update(records=x['records'],tx_numbers=r['toTxNum']-r['fromTxNum']+1,blocks=x['tx_range_count'])
 elif r['dataset']=='event-log':
  a=(r['fromTxNum']-1)//5000000;b=(r['toTxNum']-1)//5000000
  if a==b:out[a][r['kind']+'_bytes']+=r['size']
for r in m['segments']:
 if r['dataset']=='state-domain-change':
  a=assignment.get((r['fromTxNum'],r['toTxNum']))
  if a is not None:out[a][r['kind']+'_bytes']+=r['size']
for a in [4,5,6]:
 rows=sorted(members[a],key=lambda x:x['first']['BlockNum'])
 for u,v in zip(rows,rows[1:]):assert u['last']['BlockNum']+1==v['first']['BlockNum'] and u['ref']['toTxNum']+1==v['ref']['fromTxNum']
 for x in rows:assert x['tx_range_count']==x['last']['BlockNum']-x['first']['BlockNum']+1
 c=out[a];c['real_transactions']=c['tx_numbers']-c['blocks'];c['state_history_bytes']=sum(c[k+'_bytes'] for k in ['history','accessor','inverted']);c['total_bytes']=c['state_history_bytes']+sum(c[k+'_bytes'] for k in ['event-log','event-log-index'])
 g=s['current']['groups'][f'{a*5}M-{(a+1)*5}M']['counts']
 assert all(c[k]==g[k] for k in ['records','tx_numbers','history_bytes','accessor_bytes','inverted_bytes'])
 print(a*5,(a+1)*5,'published bytes',c['total_bytes'],'state bytes/real tx',c['state_history_bytes']/c['real_transactions'])
keys=records=count=0;domains=collections.defaultdict(lambda:collections.defaultdict(collections.Counter));frozen=collections.defaultdict(collections.Counter)
with gzip.open(P/'frozen-kv-domains.jsonl.gz','rt') as f:
 for line in f:
  r=json.loads(line);x=ranges[(r['fromTxNum'],r['toTxNum'])];assert r['records']==x['records'];keys+=r['keys'];records+=r['records'];count+=1
  assert sum(d['keys'] for d in r['domains'].values())==r['keys'] and sum(d['records'] for d in r['domains'].values())==r['records']
  a=(x['first']['BlockNum']-1)//5000000;b=(x['last']['BlockNum']-1)//5000000
  if a!=b:continue
  frozen[a].update(keys=r['keys'],records=r['records'])
  for d,v in r['domains'].items():domains[a][d].update(v)
assert count==19949 and keys==2443423104 and records==14302423656
for a in [4,5,6]:
 g=s['frozen']['groups'][f'{a*5}M-{(a+1)*5}M'];assert frozen[a]['keys']==g['counts']['keys'] and frozen[a]['records']==g['counts']['records'];assert dict(domains[a])==g['domains']
print('Verified all bucket totals, 19,949 domain receipts, 14,302,423,656 index records.')
