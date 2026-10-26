#!/usr/bin/env python3
"""Verify all six raw profiles; --pprof independently recreates both reports."""
import base64
import hashlib
import io
import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import zipfile
import zlib

P = pathlib.Path(__file__).resolve().parent
sha = lambda data: hashlib.sha256(data).hexdigest()


def unpack(data, count, manifest_name='evidence-manifest.json'):
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        assert z.testzip() is None
        names = z.namelist()
        assert len(names) == len(set(names)) == count
        manifest = json.loads(z.read(manifest_name))
        assert set(names) == set(manifest) | {manifest_name}
        files = {name: z.read(name) for name in manifest}
        assert all(sha(files[name]) == digest for name, digest in manifest.items())
    return files


data = (P/'cpu-profiles.zip').read_bytes()
assert len(data) == 342821 and sha(data) == '8faae4c2d065d638369b974f9a9035e5199c7d7115ede2485988f8f879aae8e2'
files = unpack(data, 91)
nonces = set()


def receipt(name, remote):
    data = files[name]
    meta = json.loads(files[name+'.transport.json'])
    assert meta['name'] == remote and meta['nonce'] not in nonces
    nonces.add(meta['nonce'])
    assert meta['bytes'] == len(data) and meta['sha256'] == sha(data)
    return json.loads(data)


parts, identity = [], None
for i in range(36):
    row = receipt('server-cpu-zip-part-%03d.json' % i, 'cpu-zip-part-%03d.json' % i)
    value = base64.b64decode(row['base64'])
    assert row['name'] == 'gtron-columns-encoder-reuse-cpu-20261026-01.zip'
    assert row['index'] == i and row['offset'] == sum(map(len, parts))
    assert row['file_bytes'] == 146190 and row['bytes'] == len(value) and row['sha256'] == sha(value)
    assert row['file_sha256'] == '3a5d8aa349b6087c19b9521a84994db53a5e37ab79b08189dfffef8c837550e6'
    stat = tuple(row[k] for k in ('device', 'inode', 'mtime_ns'))
    assert all(isinstance(v, str) and v.isdigit() for v in stat)
    identity = identity or stat
    assert stat == identity
    parts.append(value)
inner = b''.join(parts)
assert inner == files['server-cpu-profiles.zip'] and len(inner) == 146190
assert sha(inner) == '3a5d8aa349b6087c19b9521a84994db53a5e37ab79b08189dfffef8c837550e6'
raw = unpack(inner, 20)
state = json.loads(raw['profile-status.json'])
assert state == receipt('server-profile-status-01.json', 'profile-status.json')
assert state['complete'] and state['source_unchanged'] and not state['production_mutations']
assert state['source_inventory_sha256'] == 'dc5ec54833fe50341583394f732b9d587d59c5275c18fc2c11c11e5e9fbd7150'
assert state['binary_sha256'] == '11124195069f575e0bf4d2523d8ce2a1c049e8bdef34238199573013b1a32a49'
assert len(state['checks']) == 4 and all(r['returncode'] == 0 for r in state['checks'])
for row in state['checks']:
    assert sha(raw[row['label']+'.log']) == row['log_sha256']
assert len(state['profiles']) == 6
for row in state['profiles']:
    assert re.fullmatch(r'cpu-profile-comparison/pair-(458|2309|4147)-profile-format-[25]\.pprof', row['path'])
    assert len(raw[row['path']]) == row['bytes'] and sha(raw[row['path']]) == row['sha256']
dispatch = receipt('server-profile-dispatch-receipt.json', 'profile-dispatch-receipt.json')
assert dispatch['systemctl']['CPUQuotaPerSecUSec'] == '2s'
assert int(dispatch['systemctl']['MemoryLimit']) == 4 << 30 and not dispatch['production_mutations']
payload = raw['profile-payload.zip']
assert len(payload) == 7511 and sha(payload) == dispatch['payload_sha256'] == 'e8bb6d9eab332fcf271fc026dc01b93c1902fe72f4d8c227567e5a37254411ec'
source = unpack(payload, 5, 'profile-payload-manifest.json')
for name in ('run-profile.py', 'profile_publish_overlay_test.go', 'offline_column_reuse_profile_test.go'):
    assert source[name] == raw[name]
for fmt in ('2', '5'):
    compressed = receipt('server-cpu-top-format-'+fmt+'.json', 'cpu-top-format-'+fmt+'.json')
    top = zlib.decompress(base64.b64decode(compressed['zlib_base64']))
    assert top == raw['cpu-format-'+fmt+'-top.log'] and sha(top) == compressed['sha256']
    assert len(top) == compressed['bytes']
    assert files['local-cpu-format-'+fmt+'-cum.log'] == raw['cpu-format-'+fmt+'-cum.log']


def cumulative(fmt, function):
    lines = raw['cpu-format-'+fmt+'-cum.log'].decode().splitlines()
    matches = [line for line in lines if line.endswith(function)]
    assert len(matches) == 1
    return float(matches[0].split()[-3].removesuffix('s'))


encode = 'github.com/klauspost/compress/zstd.(*Encoder).EncodeAll'
etl = 'github.com/tronprotocol/go-tron/core/rawdb/etl.(*Collector).loadRows'
assert cumulative('2', encode) == 1.91 and cumulative('5', encode) == 6.21
assert cumulative('2', etl) == cumulative('5', etl) == 3.52
if '--pprof' in sys.argv:
    with tempfile.TemporaryDirectory(prefix='gtron-cpu-verify-') as tmp:
        root = pathlib.Path(tmp)
        for row in state['profiles']:
            out = root/pathlib.PurePosixPath(row['path']).name
            out.write_bytes(raw[row['path']])
        for fmt in ('2', '5'):
            paths = sorted(root.glob('*-format-'+fmt+'.pprof'))
            got = subprocess.check_output(['go', 'tool', 'pprof', '-top', '-cum', '-nodecount=40']+[str(p) for p in paths],
                                          env=dict(os.environ, TZ='UTC'), stderr=subprocess.STDOUT)
            assert got == raw['cpu-format-'+fmt+'-cum.log']
print(json.dumps(dict(raw_profiles=6, exact_local_pprof_reports=True,
                      recreated_now='--pprof' in sys.argv, encode_cpu_seconds=[1.91, 6.21],
                      etl_load_cpu_seconds=[3.52, 3.52], product_source_unchanged=True,
                      full_final_acceptance_complete=False, production_mutations=False), indent=2))
