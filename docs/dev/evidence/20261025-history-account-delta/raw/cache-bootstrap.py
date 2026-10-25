import pathlib,json,hashlib,subprocess,shutil,os
P=pathlib.Path('/data/gtron/history-account-delta-20261025-01');S=P/'candidate-source'
assert subprocess.run(['systemctl','stop','gtron-account-delta-final-native-20261025-01']).returncode in (0,5)
shutil.copyfile(str(P/'native-status.json'),str(P/'native-status.cache16.json'));shutil.copyfile(str(P/'final-source-inventory.json'),str(P/'source-inventory.pre-cache-bounds.json'))
inv=json.loads((P/'final-source-inventory.json').read_text())
for n,d in inv.items():assert hashlib.sha256((S/n).read_bytes()).hexdigest()==d,n
updates=json.loads((P/'cache-updates.json').read_text())
for row in updates['updates']:
 b=(P/row['file']).read_bytes();assert hashlib.sha256(b).hexdigest()==row['sha256'];f=S/row['path'];tmp=f.with_suffix('.bounded-next');tmp.write_bytes(b);tmp.replace(f);inv[row['path']]=row['sha256']
b=json.dumps(inv,indent=2).encode();assert hashlib.sha256(b).hexdigest()==updates['inventory_sha256'];(P/'final-source-inventory.json').write_bytes(b)
subprocess.run(['systemd-run','--unit=gtron-account-delta-bounded-native-20261025-01','-p','MemoryLimit=4G','-p','CPUQuota=200%','-p','Nice=15','/usr/bin/ionice','-c3','/usr/bin/python3',str(P/'bounded-native.py')],check=True)
