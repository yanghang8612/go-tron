"""Preserve every raw evaluation log; no source/binary/DB or authentication data."""
from pathlib import Path
import sys,tarfile,hashlib,json
out=Path(sys.argv[1]).resolve();target=out/'linux-a2-evidence.tar.gz'
assert not target.exists(),'one final evidence archive, never replace an earlier one'
files=[p for p in sorted(out.iterdir()) if p.is_file() and p.suffix in ['.log','.json','.jsonl']]
items=[]
for p in files:
 h=hashlib.sha256(p.read_bytes()).hexdigest();items.append({'path':p.name,'bytes':p.stat().st_size,'sha256':h})
manifest=out/'archive-manifest.json';assert not manifest.exists();manifest.write_text(json.dumps({'files':items,'raw_failures_preserved':True,'source_binary_bytes_excluded':True},indent=2)+'\n')
with tarfile.open(str(target),'w:gz') as tar:
 for p in files+[manifest]:tar.add(str(p),arcname=p.name)
print(str(target));print(hashlib.sha256(target.read_bytes()).hexdigest())
