from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import datetime
import json
import subprocess
import time

p = Path(__file__).resolve().parent
out = p / datetime.datetime.now(datetime.timezone.utc).strftime('live-%Y%m%dT%H%M%S')
out.mkdir()
base = 'http://3.12.206.71:6060'
proxy = 'socks5h://127.0.0.1:1088'

def fetch(path, payload=None):
    args = ['curl', '--silent', '--show-error', '--max-time', '15', '--proxy', proxy]
    if payload is not None:
        args += ['-H', 'Content-Type: application/json', '--data-binary', json.dumps(payload)]
    start = time.monotonic()
    r = subprocess.run(args + [base + path], capture_output=True)
    return {'milliseconds': (time.monotonic()-start)*1000, 'rc': r.returncode,
            'stdout': r.stdout, 'stderr': r.stderr.decode()}

for name, path in [('wallet-before.json', '/gm/wallet/getnodeinfo'),
                   ('metrics-before.json', '/debug/metrics')]:
    r = fetch(path)
    assert r['rc'] == 0, r['stderr']
    (out / name).write_bytes(r['stdout'])

results = []
with ThreadPoolExecutor(max_workers=1) as pool:
    for i, height in enumerate([3144020, 3144021, 3144020, 3144021]):
        payload = {'jsonrpc': '2.0', 'id': 100+i, 'method': 'eth_getStorageAt',
                   'params': ['0xa4997124a328a4615eab437cede31da47a31b375', '0x0', hex(height)]}
        start = time.monotonic()
        future = pool.submit(fetch, '/gmr/', payload)
        snapshots = []
        for j in range(6):
            time.sleep(0.12)
            if future.done():
                break
            r = fetch('/debug/pprof/goroutine?debug=2')
            name = 'request-%d-stack-%d.txt' % (i+1, j+1)
            (out / name).write_bytes(r['stdout'])
            snapshots.append({'file': name, 'at_ms': (time.monotonic()-start)*1000,
                              'fetch_ms': r['milliseconds'], 'rc': r['rc']})
        r = future.result()
        result = {'height': height, 'milliseconds': r['milliseconds'], 'rc': r['rc'],
                  'stderr': r['stderr'], 'snapshots': snapshots}
        if r['rc'] == 0:
            result['response'] = json.loads(r['stdout'])
        results.append(result)
        print(json.dumps(result), flush=True)

for name, path in [('wallet-after.json', '/gm/wallet/getnodeinfo'),
                   ('metrics-after.json', '/debug/metrics')]:
    r = fetch(path)
    assert r['rc'] == 0, r['stderr']
    (out / name).write_bytes(r['stdout'])
(out / 'results.json').write_text(json.dumps(results, indent=2)+'\n')
print('EVIDENCE', out, flush=True)
