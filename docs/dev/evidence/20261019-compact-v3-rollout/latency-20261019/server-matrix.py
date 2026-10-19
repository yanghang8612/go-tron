from pathlib import Path
import base64, datetime, hashlib, json, os, subprocess, time, urllib.request, zlib

p = Path('/data/gtron/compact-v3-rollout-20261019-01/cold-history-latency-20261019-01')
p.mkdir(exist_ok=False)
def http(path, obj=None, port=8545):
    request = urllib.request.Request('http://127.0.0.1:%d%s' % (port,path),
              data=json.dumps(obj).encode() if obj is not None else None,
              headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(request,timeout=10) as stream:
        return json.load(stream)
def identity():
    fields = dict(line.split('=',1) for line in subprocess.check_output(
        ['systemctl','show','gtron','-p','MainPID','-p','ActiveState']).decode().splitlines())
    pid = fields['MainPID']
    fields.update(exe=os.path.realpath('/proc/'+pid+'/exe'),
                  sha256=hashlib.sha256(Path('/proc/'+pid+'/exe').read_bytes()).hexdigest())
    return fields
before = http('/wallet/getnodeinfo',port=8090)
head = before['currentBlock']
addr = '0xa4997124a328a4615eab437cede31da47a31b375'
cases = [
    ('cold_storage_before','eth_getStorageAt',[addr,'0x0',hex(3144020)],6),
    ('cold_balance','eth_getBalance',[addr,hex(3144020)],None),
    ('cold_code','eth_getCode',[addr,hex(3144020)],None),
    ('cold_storage_after','eth_getStorageAt',[addr,'0x0',hex(3144021)],7),
    ('genesis_storage','eth_getStorageAt',[addr,'0x0','0x0'],0),
    ('recent_storage','eth_getStorageAt',[addr,'0x0',hex(head-128)],None),
    ('cold_missing_slot','eth_getStorageAt',[addr,'0xdeadbeef',hex(3144020)],0),
    ('latest_storage','eth_getStorageAt',[addr,'0x0','latest'],None),
]
result = {'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
          'identity_before':identity(),'wallet_before':before,'cases':cases,'rows':[],
          'note':'Two interleaved sequential rounds on server loopback during live sync; 60 s loop budget plus one request timeout. No restart or configuration change.'}
metrics_before = http('/debug/metrics',port=6062)
start = time.monotonic()
for repeat in range(2):
    for name,method,params,expect in cases:
        if time.monotonic()-start>60:break
        at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        t = time.monotonic()
        try:
            response = http('/',{'jsonrpc':'2.0','id':len(result['rows'])+1,'method':method,'params':params})
            elapsed = (time.monotonic()-t)*1000
            assert 'result' in response,response
            value = response['result']
            row = {'round':repeat+1,'case':name,'utc':at,'milliseconds':elapsed,
                   'response_bytes':len(value),'response_sha256':hashlib.sha256(value.encode()).hexdigest()}
            if method!='eth_getCode':row['result']=value
            if expect is not None:row['correct']=int(value,16)==expect
        except Exception as error:
            row={'round':repeat+1,'case':name,'utc':at,'milliseconds':(time.monotonic()-t)*1000,'error':str(error)}
        result['rows'].append(row)
result['identity_after']=identity()
result['wallet_after']=http('/wallet/getnodeinfo',port=8090)
result['memory']=subprocess.check_output(['free','-b']).decode()
(p/'metrics-before.json').write_text(json.dumps(metrics_before))
(p/'metrics-after.json').write_text(json.dumps(http('/debug/metrics',port=6062)))
s=json.dumps(result,indent=2)
(p/'matrix.json').write_text(s)
b=json.dumps({'matrix.json':s},separators=(',',':')).encode()
print(chr(27)+'[2J'+chr(27)+'[H',end='')
print('GTRROL:'+base64.b64encode(zlib.compress(b,9)).decode())
print('PACKET_SHA256',hashlib.sha256(b).hexdigest(),'BYTES',len(b))
