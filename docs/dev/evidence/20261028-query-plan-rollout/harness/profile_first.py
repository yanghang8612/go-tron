# One production historical query with bounded read-only CPU/stack observation.
import pathlib,json,datetime,subprocess,concurrent.futures,time,traceback
stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
out=pathlib.Path('build/benchmarks/20261028-first-query-profile')/stamp;out.mkdir(parents=True)
base='http://3.12.206.71:6060'
def fetch(path,payload=None,timeout=100):
 cmd=['curl','--proxy','socks5h://127.0.0.1:1088','--silent','--show-error','--fail','--max-time',str(timeout)]
 if payload is not None:cmd+=['-H','Content-Type: application/json','--data-binary',json.dumps(payload)]
 return subprocess.check_output(cmd+[base+path],timeout=timeout+5)
def call(repeat):
 start=time.perf_counter();q={'jsonrpc':'2.0','id':repeat+1,'method':'eth_call','params':[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},hex(11000000)]};body=json.loads(fetch('/gmr/',q));row={'repeat':repeat,'seconds_gateway_http':time.perf_counter()-start,'response':body};(out/('request-'+str(repeat)+'.json')).write_text(json.dumps(row,indent=2));assert 'error' not in body,body;print(json.dumps(row),flush=True);return row
def profile():
 try:(out/'cpu.pprof').write_bytes(fetch('/debug/pprof/profile?seconds=30',timeout=45))
 except Exception:(out/'profile-error.txt').write_text(traceback.format_exc())
identity=fetch('/debug/pprof/cmdline').split(b'\0')[0].decode();assert '/auto-52aea62ef567-' in identity,identity
(out/'identity.json').write_text(json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'exe':identity,'height':11000000,'cache':'production caches uncontrolled; novel height in this sampling sequence'},indent=2))
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
 p=pool.submit(profile);time.sleep(1);f=pool.submit(call,0)
 for i in range(6):
  time.sleep(2)
  if f.done():break
  (out/('goroutine-'+str(i)+'.txt')).write_bytes(fetch('/debug/pprof/goroutine?debug=2',timeout=15))
 first=f.result();second=call(1);assert first['response']['result']==second['response']['result'];p.result()
assert fetch('/debug/pprof/cmdline').split(b'\0')[0].decode()==identity
print('COMPLETE '+str(out),flush=True)
