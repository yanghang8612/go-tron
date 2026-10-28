# Exact census of the single broad auxiliary object; no database scan or rewrite.
import pathlib,json,hashlib,struct,collections,os,datetime,zipfile
P=pathlib.Path('/data/gtron/maintenance/direct-history-plan-20261028-01');R=P/'query-plan-scope-final'
s=json.loads((R/'candidate-status.json').read_text());assert s['complete'] and s['production_before']==s['production_after'];SOURCE=s['source']
root=P/'final/indexed-input/state-snapshots';m=json.loads((root/'manifest.json').read_text());refs=m['segments'];histories=[r for r in refs if r['kind']=='history' and r.get('dataset')=='state-domain-change']
ref=next(r for r in refs if r['kind']=='system-history-routing');raw=(root/ref['path']).read_bytes();assert len(raw)==ref['size'] and ref['checksum']=='sha256:'+hashlib.sha256(raw).hexdigest()
n,keys=struct.unpack_from('>II',raw,12);off=40+32*n;families={}
for _ in range(keys):
 begin=off;length=struct.unpack_from('>H',raw,off)[0];off+=2;key=raw[off:off+length];off+=length;mode=raw[off];payload=struct.unpack_from('>I',raw,off+1)[0];off+=5+payload;assert off<=len(raw)
 domain=int.from_bytes(key[29:31],'big') if len(key)>=31 else None
 family={256:'ContractStorage-prefix',257:'ContractMetadata'}.get(domain,'system-and-other')
 r=families.setdefault(family,{'rows':0,'bytes':0,'payload_bytes':0,'dense_rows':0,'sparse_rows':0});r['rows']+=1;r['bytes']+=off-begin;r['payload_bytes']+=payload;r['dense_rows' if mode==0 else 'sparse_rows']+=1
assert off==len(raw) and sum(r['rows'] for r in families.values())==ref['rows']
routes=[dict(r,sources=int.from_bytes((root/r['path']).open('rb').read(16)[12:16],'big')) for r in refs if r['kind'] in ['system-history-routing','small-system-history-routing']]
summary={'cold_sources':len(histories),'cold_max_tx':max(r['toTxNum'] for r in histories),'routes':routes,'broad_bytes':len(raw),'broad_budget_bytes':64<<20,'broad_header_and_source_bytes':40+32*n,'families':families,'input_bytes_authenticated':True,'database_scan':False}
(R/'routing-layout.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary))
# Re-seal completed samples plus this auxiliary census, keeping identities intact.
paths=[p for p in R.rglob('*') if p.is_file() and not p.is_symlink() and 'source' not in p.relative_to(R).parts and p.suffix in {'.json','.jsonl','.log','.py','.pprof','.txt'} and p.name not in {'archive-receipt.json','member-seal.json'}]
seal={str(p.relative_to(R)):{'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(paths)}
b=json.dumps(seal,indent=2).encode();(R/'member-seal.json').write_bytes(b);out=pathlib.Path('/tmp/AAA-gtron-query-plan-scope-final-20261028-01.zip')
with zipfile.ZipFile(str(out),'w',zipfile.ZIP_DEFLATED,6) as z:
 for p in sorted(paths):z.write(str(p),str(p.relative_to(R)))
 z.writestr('member-seal.json',b)
os.chmod(str(out),0o644);receipt={'utc':datetime.datetime.utcnow().isoformat()+'Z','path':str(out),'bytes':out.stat().st_size,'sha256':hashlib.sha256(out.read_bytes()).hexdigest(),'members':len(paths),'source':SOURCE};(R/'archive-receipt.json').write_text(json.dumps(receipt,indent=2));print(json.dumps(receipt))
