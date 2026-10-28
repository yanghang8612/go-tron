import pathlib,subprocess,json,hashlib,os,time,datetime,urllib.request,re,zipfile
A=pathlib.Path('/data/gtron/maintenance/9m-query-20261027-01')
cleanup=json.loads((A/'metadata-cold-v3/cleanup-receipt.json').read_text())
assert all(not pathlib.Path(p).exists() for p in cleanup['removed_roots'])
state=subprocess.check_output(['systemctl','show','gtron','--property=MainPID','--property=ActiveState']).decode()
pid=re.search(r'^MainPID=(\d+)$',state,re.M).group(1);assert 'ActiveState=active' in state
proc=pathlib.Path('/proc')/pid;stat=(proc/'stat').read_text().rsplit(')',1)[1].split();ticks=stat[19]
exe=os.readlink(str(proc/'exe'));sha=hashlib.sha256((proc/'exe').read_bytes()).hexdigest()
prior=json.loads((A/'metadata-final/production-identity.json').read_text());unchanged=all([pid==prior['pid'],ticks==prior['start_ticks'],exe==prior['exe'],sha==prior['sha256']]);assert unchanged
calls=[]
def rpc(method,params):
 t=time.monotonic();r=json.load(urllib.request.urlopen(urllib.request.Request('http://127.0.0.1:8545',data=json.dumps({'jsonrpc':'2.0','id':1,'method':method,'params':params}).encode(),headers={'Content-Type':'application/json'}),timeout=25));assert 'error' not in r,r
 calls.append({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'method':method,'params':params,'seconds_local_http':time.monotonic()-t,'response':r});return r['result']
h1=int(rpc('eth_blockNumber',[]),16)
a='0xb87f2be4dede9fc25387f8df7e0944b5cb7900e1';assert int(rpc('eth_getBalance',[a,'0x155']),16)==0
assert int(rpc('eth_getBalance',[a,'0x156']),16)==33251807524000000*10**12
assert int(rpc('eth_getBalance',['0x71b0af54e0a1182a5e0947d6a64f3b22740ef318','0x0']),16)==99000000000000000*10**12
th='0x5281eb99b3cab6b3cb4777e6b070df936f2cb068163fb8f3959a1cd88733a4c2';tx=rpc('eth_getTransactionByHash',[th]);assert tx['hash'].lower()==th and int(tx['blockNumber'],16)==342
h2=int(rpc('eth_blockNumber',[]),16)
if h2<=h1:
 time.sleep(3);h2=int(rpc('eth_blockNumber',[]),16)
assert h2>h1,(h1,h2)
tail=subprocess.check_output(['tail','-n','2000','/data/gtron/main/gtron.log']).decode(errors='replace').splitlines()
muerr=[s for s in tail if re.search(r'muhash',s,re.I) and re.search(r'error|mismatch|fail|corrupt',s,re.I)]
result={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'production_mutations':False,'identity_unchanged':unchanged,'service':state,'pid':pid,'start_ticks':ticks,'exe':exe,'sha256':sha,'head_start':h1,'head_end':h2,'historical_balance_checks':True,'transaction_342_check':True,'muhash_error_lines_in_last_2000_log_lines':muerr,'log_tail_lines':len(tail),'full_current_state_muhash_recomputed':False,'calls':calls,'memory_status':[(s) for s in (proc/'status').read_text().splitlines() if s.startswith(('VmRSS:','VmSwap:','Threads:'))]}
(A/'metadata-final/closure-final.json').write_text(json.dumps(result,indent=2))
files={'cleanup-receipt.json':(A/'metadata-cold-v3/cleanup-receipt.json').read_bytes(),'closure-final.json':json.dumps(result,indent=2).encode(),'cleanup-private.py':(A/'cleanup-metadata-private.py').read_bytes(),'local-evidence-verified.json':(A/'metadata-local-evidence-verified.json').read_bytes()}
files['SHA256SUMS.json']=json.dumps({k:hashlib.sha256(v).hexdigest() for k,v in files.items()},indent=2).encode()
p=pathlib.Path('/tmp/AAA-gtron-metadata-cleanup-closure-20261027.zip')
with zipfile.ZipFile(str(p),'w',zipfile.ZIP_DEFLATED) as z:
 for k,v in sorted(files.items()):z.writestr(k,v)
os.chmod(str(p),0o644)
seal={'file':str(p),'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()};(A/'metadata-cleanup-closure-seal.json').write_text(json.dumps(seal,indent=2))
print(json.dumps({'seal':seal,'closure':{k:v for k,v in result.items() if k!='calls'},'cleanup':cleanup},indent=2))
