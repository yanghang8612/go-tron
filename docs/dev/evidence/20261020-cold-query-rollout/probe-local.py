from pathlib import Path
import datetime,json,subprocess,time,sys
p=Path('/data/gtron/cold-query-rollout-20261020-9030ce7a')
phase=sys.argv[1]; rounds=int(sys.argv[2]) if len(sys.argv)>2 else 3
out=p/(phase+'.jsonl')
assert not out.exists(),out
addr='0xa4997124a328a4615eab437cede31da47a31b375'
sender='0x71b0af54e0a1182a5e0947d6a64f3b22740ef318'
receiver='0xb87f2be4dede9fc25387f8df7e0944b5cb7900e1'
probes=[('storage6','eth_getStorageAt',[addr,'0x0',hex(3144020)],'0x'+format(6,'064x')),('storage7','eth_getStorageAt',[addr,'0x0',hex(3144021)],'0x'+format(7,'064x')),('genesis','eth_getBalance',[sender,'0x0'],hex(99000000000000000*10**12)),('balance341','eth_getBalance',[receiver,hex(341)],'0x0'),('balance342','eth_getBalance',[receiver,hex(342)],hex(33251807524000000*10**12)),('latest','eth_getStorageAt',[addr,'0x0','latest'],None),('call6','eth_call',[{'to':addr,'data':'0x9fa6a6e3'},hex(3144020)],'0x'+format(6,'064x')),('call7','eth_call',[{'to':addr,'data':'0x9fa6a6e3'},hex(3144021)],'0x'+format(7,'064x'))]
def req(route,payload=None,limit=15):
 cmd=['curl','--connect-timeout','5','--max-time',str(limit),'-sS','--fail',('http://127.0.0.1:8545/' if route=='gmr/' else 'http://127.0.0.1:8090/wallet/getnodeinfo' if route.startswith('gm/') else 'http://127.0.0.1:6062/debug/metrics')]
 if payload is not None:cmd+=['-H','Content-Type: application/json','--data-binary',json.dumps(payload)]
 begin=time.monotonic();r=subprocess.run(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
 ans={'rc':r.returncode,'milliseconds':(time.monotonic()-begin)*1000}
 try:ans['response']=json.loads(r.stdout)
 except Exception:ans['raw']=r.stdout.decode(errors='replace')[-2000:]
 if r.stderr:ans['stderr']=r.stderr.decode(errors='replace')
 return ans
for i in range(rounds):
 for name,method,params,expected in probes:
  row={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'phase':phase,'round':i,'name':name,'method':method,'params':params}
  row.update(req('gmr/',{'jsonrpc':'2.0','id':1,'method':method,'params':params}))
  row['correct']=bool(row['rc']==0 and 'error' not in row.get('response',{}) and row.get('response',{}).get('result') is not None and (expected is None or row['response']['result']==expected))
  with out.open('a') as f:f.write(json.dumps(row)+'\n')
  print(name,round(row['milliseconds'],2),'ok' if row['correct'] else 'FAIL',flush=True)
 for route,key in [('gm/wallet/getnodeinfo','wallet'),('debug/metrics','metrics')]:
  (p/(phase+'-'+str(i)+'-'+key+'.json')).write_text(json.dumps(req(route),indent=2))
print('COMPLETE',phase,flush=True)
