#!/usr/bin/env python3
"""Check raw final profiles and optionally reproduce the native top reports."""
import argparse
import hashlib
import io
import json
import os
import pathlib
import re
import subprocess
import tarfile
import tempfile
import zipfile

P = pathlib.Path(__file__).resolve().parent
sha = lambda b: hashlib.sha256(b).hexdigest()
args = argparse.ArgumentParser()
args.add_argument('--pprof',action='store_true')
opts = args.parse_args()
b = (P/'final-profile-reproduction.zip').read_bytes()
assert len(b) == 19207 and sha(b) == 'a5f741fe3ee9e82b022b2884d0db293685361bad58ecd6412fa6bbd89fcbcb0c'
with zipfile.ZipFile(io.BytesIO(b)) as z:
    assert z.testzip() is None and len(z.namelist()) == len(set(z.namelist())) == 8
    manifest = json.loads(z.read('evidence-manifest.json'))
    assert set(z.namelist()) == set(manifest) | {'evidence-manifest.json'}
    generated = {n:z.read(n) for n in manifest}
    assert all(sha(generated[n]) == h for n,h in manifest.items())
blob = (P/'final-acceptance-core.tar.xz').read_bytes()
assert sha(blob) == '80f824b2c934e80861630356bc97829230c079b7dcec5e5fcfc1f063cfe00f3d'
with tarfile.open(fileobj=io.BytesIO(blob),mode='r:xz') as t:
    core = {m.name:t.extractfile(m).read() for m in t.getmembers()}
selected = {}
for mode in ['baseline','candidate']:
    base = 'tail-read-comparison/nine-million-profile-'+mode+'/'
    assert generated['local-'+mode+'-top.txt'] == core[base+'cpu-top.txt']
    workload = json.loads(core[base+'workload.json'])
    assert workload['responses_correct'] and workload['requests'] == {'baseline':95,'candidate':93}[mode]
    text = generated['local-'+mode+'-rpc-all.txt'].decode()
    rows = {}
    for line in text.splitlines():
        m = re.match(r'^\s*(\S+)\s+(\S+)%\s+(\S+)%\s+(\S+)\s+(\S+)%\s+(.+)$',line)
        if m:
            rows[m[6]] = dict(flat=m[1],flat_percent=float(m[2]),cumulative=m[4],cumulative_percent=float(m[5]))
    selected[mode] = {n:v for n,v in rows.items() if n in [
        'github.com/tronprotocol/go-tron/core/rawdb.iterateHistoryOwnerSpans',
        'github.com/golang/snappy.Decode',
        'github.com/tronprotocol/go-tron/core/state/snapshots.decodeRecordColumnStoredPage',
        'github.com/tronprotocol/go-tron/core/state/snapshots.decodeHistoryRecordColumns']}
    if opts.pprof:
        with tempfile.TemporaryDirectory(prefix='gtron-final-profile-') as tmp:
            profile = pathlib.Path(tmp)/'cpu.pprof';profile.write_bytes(core[base+'cpu.pprof'])
            env = dict(os.environ,TZ='UTC')
            commands = [
                ('local-'+mode+'-top.txt',['-top','-nodecount=35']),
                ('local-'+mode+'-rpc-cum.txt',['-top','-cum','-nodecount=25','-tagfocus=rpc=eth_call']),
                ('local-'+mode+'-rpc-all.txt',['-top','-cum','-nodefraction=0','-edgefraction=0','-nodecount=0','-tagfocus=rpc=eth_call'])]
            for output,flags in commands:
                result = subprocess.check_output(['go','tool','pprof']+flags+[str(profile)],env=env,stderr=subprocess.STDOUT)
                assert result == generated[output], output+' cannot be exactly reproduced'
assert selected['candidate']['github.com/tronprotocol/go-tron/core/rawdb.iterateHistoryOwnerSpans']['cumulative_percent'] == 43.52
assert selected['candidate']['github.com/golang/snappy.Decode']['cumulative_percent'] == 19.98
assert selected['candidate']['github.com/tronprotocol/go-tron/core/state/snapshots.decodeHistoryRecordColumns']['cumulative_percent'] == 1.29
print(json.dumps(dict(two_native_top_reports_byte_exact=True,all_six_pprof_reports_recreated=opts.pprof,
                      scope='Warm nine-million eth_call CPU samples; nested cumulative percentages must not be added.',
                      selected=selected),indent=2))
