# Read-only production HTTP measurements; never evict caches or control services.
import datetime,json,pathlib,subprocess,time,traceback,sys
base='http://3.12.206.71:6060'
stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
out=pathlib.Path('build/benchmarks/20261028-production-multiheight')/stamp
out.mkdir(parents=True,exist_ok=False)
state={'complete':False,'gateway':base+'/gmr/','proxy':'socks5h://127.0.0.1:1088','cache':'uncontrolled production caches; this process has previous 9M/35M queries; no eviction','requests':[]}
def save(): (out/'results.json').write_text(json.dumps(state,indent=2))
def curl(path,payload=None):
 cmd=['curl','--proxy',state['proxy'],'--silent','--show-error','--max-time','90','--write-out','\n%{http_code} %{time_connect} %{time_pretransfer} %{time_starttransfer} %{time_total} %{size_download}']
 if payload is not None:cmd+=['-H','Content-Type: application/json','--data-binary',json.dumps(payload)]
 p=subprocess.run(cmd+[base+path],stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=100)
 if p.returncode:raise RuntimeError('curl: '+p.stderr.decode(errors='replace'))
 body,tail=p.stdout.rsplit(b'\n',1);code,connect,pretransfer,ttfb,total,size=tail.decode().split()
 timing={'http_status':int(code),'connect_seconds':float(connect),'pretransfer_seconds':float(pretransfer),'ttfb_seconds':float(ttfb),'total_seconds':float(total),'response_bytes':int(size)}
 assert int(code)==200,(path,timing,body[:300])
 return body,timing
def identity():
 body,timing=curl('/debug/pprof/cmdline');exe=body.split(b'\0')[0].decode();assert '/auto-52aea62ef567-' in exe,exe
 return {'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'exe':exe}
def rpc(method,params):
 body,timing=curl('/gmr/',{'jsonrpc':'2.0','id':len(state['requests'])+1,'method':method,'params':params});body=json.loads(body);assert 'result' in body and 'error' not in body,body
 return body,timing
try:
 state['identity_before']=identity();state['head_before']=rpc('eth_blockNumber',[])[0];save()
 for height in [10000000,15000000,20000000]:
  result=None
  for repeat in range(3):
   start=datetime.datetime.now(datetime.timezone.utc).isoformat()
   body,timing=rpc('eth_call',[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},hex(height)])
   assert isinstance(body['result'],str) and len(body['result'])==66 and body['result'].startswith('0x'),body
   if result is None:result=body['result']
   assert body['result']==result,(height,body,result)
   row={'height':height,'repeat':repeat,'utc':start,'request_cache':'first sample of this height in this run' if repeat==0 else 'immediate repeat','response':body,'uint256':int(body['result'],16),**timing}
   state['requests'].append(row);save();print(json.dumps(row),flush=True)
 state['head_after']=rpc('eth_blockNumber',[])[0];state['identity_after']=identity();assert state['identity_after']['exe']==state['identity_before']['exe'];state['complete']=True;save();print('COMPLETE '+str(out/'results.json'),flush=True)
except BaseException:
 state['error']=traceback.format_exc();save();raise
