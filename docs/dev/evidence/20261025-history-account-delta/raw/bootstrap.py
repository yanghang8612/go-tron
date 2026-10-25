import pathlib,json,hashlib,os,subprocess
P=pathlib.Path('/data/gtron/history-account-delta-20261025-01');S=P/'candidate-source';OLD=pathlib.Path('/data/gtron/history-tx-runs-20261025-01')
assert json.loads((P/'probe-v3-driver-status.json').read_text())['complete']
inv=json.loads((P/'source-inventory.json').read_text())
subprocess.run(['/usr/bin/patch','-p1','--batch','--forward','-i',str(P/'code.patch')],cwd=str(S),check=True)
for n,d in inv.items():
 if not (P/'upload'/n).exists():assert hashlib.sha256((S/n).read_bytes()).hexdigest()==d,n;continue
 b=(P/'upload'/n).read_bytes();assert hashlib.sha256(b).hexdigest()==d,n;p=S/n;p.parent.mkdir(parents=True,exist_ok=True);tmp=p.with_suffix('.upload-next');tmp.write_bytes(b);tmp.replace(p)
(S/'core/state/snapshots/offline_account_delta_capture_test.go').write_bytes((P/'offline_account_delta_capture_test.go').read_bytes())
os.link(str(OLD/'candidate.test'),str(P/'baseline.test'))
for name,script in [('native','native.py'),('layout','layout.py')]:
 subprocess.run(['systemd-run','--unit=gtron-account-delta-'+name+'-20261025-01','-p','MemoryLimit=4G','-p','CPUQuota=200%','-p','Nice=15','/usr/bin/ionice','-c3','/usr/bin/python3',str(P/script)],check=True)
