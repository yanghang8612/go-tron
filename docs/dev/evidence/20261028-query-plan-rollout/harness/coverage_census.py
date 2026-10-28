# Exact current history/accessor identity census. Does not publish any index.
import pathlib,sys,json,hashlib,struct,datetime,collections
root=pathlib.Path(sys.argv[1]);out=pathlib.Path(sys.argv[2]);out.mkdir(parents=True,exist_ok=True)
raw=(root/'manifest.json').read_bytes();m=json.loads(raw);refs=m['segments'];paths=collections.defaultdict(list)
for r in refs:paths[r['path']].append(r)
def pair(h,a):
 s=hashlib.sha256(b'go-tron/system-history-routing/pair/v1\0')
 for r in [h,a]:
  s.update(struct.pack('>QQQQQH',r['fromTxNum'],r['toTxNum'],r.get('aggregationSteps',0) or 1,r['size'],r.get('rows',0),r.get('domain',0)))
  for v in [r.get('dataset',''),r['kind'],r['path'],r.get('checksum','')]:b=v.encode();s.update(struct.pack('>Q',len(b)));s.update(b)
 return s.digest()
sources=[]
for h in refs:
 if h['kind']!='history' or h.get('dataset')!='state-domain-change':continue
 p=str(pathlib.PurePosixPath(h['path']).with_suffix('.kv'));a=[x for x in paths[p] if x['kind']=='accessor' and x.get('dataset')=='state-domain-change' and x['fromTxNum']==h['fromTxNum'] and x['toTxNum']==h['toTxNum'] and (x.get('aggregationSteps',0) or 1)==(h.get('aggregationSteps',0) or 1)]
 assert len(a)==1,(h,a)
 sources.append((h,a[0],pair(h,a[0])))
routes={};summary=[]
for r in refs:
 if r['kind'] not in ['small-system-history-routing','system-history-routing','account-history-routing']:continue
 with (root/r['path']).open('rb') as f:
  if r['kind']=='account-history-routing':
   header=f.read(64);assert header[:8]==b'GTACRT01';n=int.from_bytes(header[12:16],'big');dirs=int.from_bytes(header[32:40],'big');length=64+n*32+dirs*53;f.seek(0);b=f.read(length);assert len(b)==length and 'sha256:'+hashlib.sha256(b).hexdigest()==r['lookupRoot'];start=64
  else:
   b=f.read();assert len(b)==r['size'] and 'sha256:'+hashlib.sha256(b).hexdigest()==r['checksum'];n=int.from_bytes(b[12:16],'big');start=40
 ids=set(b[i:i+32] for i in range(start,start+32*n,32));assert len(ids)==n;routes[r['kind']]=ids;hits=[h for h,a,id_ in sources if id_ in ids];missing=[{'history':h['path'],'accessor':a['path'],'from':h['fromTxNum'],'to':h['toTxNum'],'pairID':id_.hex()} for h,a,id_ in sources if id_ not in ids];(out/(r['kind']+'-uncovered.json')).write_text(json.dumps(missing,indent=2));summary.append({'kind':r['kind'],'path':r['path'],'advertised_sources':n,'exact_current_pairs':len(hits),'uncovered_current_pairs':len(missing),'authenticated_source_table':True,'covered_tx_min':min((h['fromTxNum'] for h in hits),default=None),'covered_tx_max':max((h['toTxNum'] for h in hits),default=None)})
result={'utc':datetime.datetime.utcnow().isoformat()+'Z','root':str(root),'manifest_sha256':hashlib.sha256(raw).hexdigest(),'manifest_generation':m.get('generation'),'history_sources':len(sources),'routes':summary,'scan_is_read_only':True}
(out/'summary.json').write_text(json.dumps(result,indent=2));print(json.dumps(result,indent=2))
