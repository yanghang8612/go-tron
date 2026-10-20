from pathlib import Path
import subprocess,json,hashlib,base64,zlib,datetime
p=Path('/data/gtron/cold-query-rollout-20261020-9030ce7a')
r=subprocess.run(['/usr/bin/python3',str(p/'seal-rollout.py')],stdout=subprocess.PIPE,stderr=subprocess.STDOUT);(p/'seal-summary.log').write_bytes(r.stdout);assert r.returncode==0,r.stdout.decode()[-2000:]
files={}
for f in sorted(p.iterdir()):
 if not f.is_file() or f.name in ['runtime-samples.jsonl','latest-observation.json','deploy-current.log','packet.json','packet-meta.json'] or f.name.startswith('packet-part-'):continue
 if f.suffix not in ['.json','.jsonl','.log','.py','.patch','.gz','.txt']:continue
 b=f.read_bytes();binary=f.suffix=='.gz';files[f.name]={'encoding':'base64' if binary else 'utf8','sha256':hashlib.sha256(b).hexdigest(),'bytes':len(b),'data':base64.b64encode(b).decode() if binary else b.decode()}
packet=json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source':'9030ce7a31016491ece014ac76e5656ec149417e','files':files},separators=(',',':')).encode();(p/'packet.json').write_bytes(packet);encoded=base64.b64encode(zlib.compress(packet,9)).decode();parts=[encoded[i:i+80000] for i in range(0,len(encoded),80000)]
for i,part in enumerate(parts):(p/('packet-part-'+str(i)+'.txt')).write_text('GTRROL:'+part+'\n')
meta={'packet_sha256':hashlib.sha256(packet).hexdigest(),'packet_bytes':len(packet),'base64_chars':len(encoded),'parts':len(parts),'part_sha256':[hashlib.sha256(x.encode()).hexdigest() for x in parts],'files':len(files)};(p/'packet-meta.json').write_text(json.dumps(meta,indent=2));print(json.dumps(meta))
