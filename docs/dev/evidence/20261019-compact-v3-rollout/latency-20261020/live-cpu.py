from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import datetime
import json
import subprocess
import time

p = Path(__file__).resolve().parent
out = p / datetime.datetime.now(datetime.timezone.utc).strftime('cpu-%Y%m%dT%H%M%S')
out.mkdir()
def fetch(path, payload=None, timeout=25):
    args = ['curl', '--silent', '--show-error', '--max-time', str(timeout),
            '--proxy', 'socks5h://127.0.0.1:1088']
    if payload is not None:
        args += ['-H', 'Content-Type: application/json', '--data-binary', json.dumps(payload)]
    t = time.monotonic()
    r = subprocess.run(args+['http://3.12.206.71:6060'+path], capture_output=True)
    return r.returncode, r.stdout, r.stderr.decode(), (time.monotonic()-t)*1000

results = []
with ThreadPoolExecutor(max_workers=1) as pool:
    profile = pool.submit(fetch, '/debug/pprof/profile?seconds=15')
    time.sleep(0.5)
    start = time.monotonic()
    for i in range(6):
        if time.monotonic()-start > 12:
            break
        h = 3144020+(i%2)
        req = {'jsonrpc':'2.0', 'id':200+i, 'method':'eth_getStorageAt',
               'params':['0xa4997124a328a4615eab437cede31da47a31b375','0x0',hex(h)]}
        rc, data, err, ms = fetch('/gmr/', req, 12)
        results.append({'height':h,'rc':rc,'milliseconds':ms,'stderr':err,
                        'response':json.loads(data) if rc==0 else None})
    rc, data, err, ms = profile.result()
    assert rc == 0, err
    assert data[:2] == b'\x1f\x8b', data[:100]
    (out/'cpu.pprof').write_bytes(data)
    (out/'requests.json').write_text(json.dumps(results,indent=2)+'\n')
print(json.dumps({'directory':str(out),'profile_bytes':len(data),'profile_ms':ms,
                  'requests':results}),flush=True)
