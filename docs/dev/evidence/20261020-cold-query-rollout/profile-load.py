from pathlib import Path
import json,time,threading,urllib.request,subprocess,datetime,hashlib,sys
p=Path('/data/gtron/cold-query-rollout-20261020-9030ce7a');phase=sys.argv[1]
assert not (p/(phase+'-load.json')).exists()
def req(url,payload=None,timeout=15):
 data=None if payload is None else json.dumps(payload).encode();r=urllib.request.Request(url,data=data,headers={'Content-Type':'application/json'})
 with urllib.request.urlopen(r,timeout=timeout) as f:return f.read()
def identity():
 pid=int(subprocess.check_output(['systemctl','show','gtron','-p','MainPID']).decode().split('=')[1]);return {'pid':pid,'exe_sha256':hashlib.sha256(Path('/proc/'+str(pid)+'/exe').read_bytes()).hexdigest()}
v={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'identity_before':identity(),'queries':[],'wallet_before':json.loads(req('http://127.0.0.1:8090/wallet/getnodeinfo')),'metrics_before':json.loads(req('http://127.0.0.1:6062/debug/metrics'))}
def profile():
 try:
  b=req('http://127.0.0.1:6062/debug/pprof/profile?seconds=15',timeout=25);(p/(phase+'-load.cpu.pb.gz')).write_bytes(b);v['profile_bytes']=len(b);v['profile_sha256']=hashlib.sha256(b).hexdigest()
 except Exception as e:v['profile_error']=repr(e)
t=threading.Thread(target=profile);t.start();a='0xa4997124a328a4615eab437cede31da47a31b375'
for i in range(3):
 for method,params,expected in [('eth_getStorageAt',[a,'0x0',hex(3144020)],6),('eth_getStorageAt',[a,'0x0',hex(3144021)],7),('eth_call',[{'to':a,'data':'0x9fa6a6e3'},hex(3144020)],6),('eth_call',[{'to':a,'data':'0x9fa6a6e3'},hex(3144021)],7)]:
  begin=time.monotonic();r={'round':i,'method':method,'params':params,'expected':expected}
  try:
   r['response']=json.loads(req('http://127.0.0.1:8545',{'jsonrpc':'2.0','id':1,'method':method,'params':params}));r['correct']=r['response'].get('result')=='0x'+format(expected,'064x')
  except Exception as e:r['error']=repr(e)
  r['milliseconds']=(time.monotonic()-begin)*1000;v['queries'].append(r)
t.join(30);assert not t.is_alive();v['identity_after']=identity();v['wallet_after']=json.loads(req('http://127.0.0.1:8090/wallet/getnodeinfo'));v['metrics_after']=json.loads(req('http://127.0.0.1:6062/debug/metrics'));v['same_process']=v['identity_before']==v['identity_after'];(p/(phase+'-load.json')).write_text(json.dumps(v,indent=2));print('PROFILE_LOAD_COMPLETE',phase,'same_process',v['same_process'],'correct',sum(x.get('correct',False) for x in v['queries']),flush=True)
