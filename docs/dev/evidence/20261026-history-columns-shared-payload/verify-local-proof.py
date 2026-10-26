import hashlib
import json
import pathlib
import statistics
import tarfile
import zipfile

HERE = pathlib.Path(__file__).resolve().parent


def read_zip(name, size, digest, manifest_name, count):
    path = HERE/name
    assert path.stat().st_size == size
    assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
    with zipfile.ZipFile(path) as z:
        assert z.testzip() is None and len(z.namelist()) == count
        assert len(z.namelist()) == len(set(z.namelist()))
        members = {n:z.read(n) for n in z.namelist()}
    manifest = json.loads(members[manifest_name])
    assert set(members) == set(manifest) | {manifest_name}
    for name, expected in manifest.items():
        value = members[name]
        assert len(value) == expected['bytes']
        assert hashlib.sha256(value).hexdigest() == expected['sha256']
    return members


proof = read_zip('completed-local-proof.zip', 6087,
                 '1560fb95575fbb63644b93a47c0195dfa4169584429a782b05bcc210f17ad68c',
                 'manifest.json', 9)
checkpoint = read_zip('local-and-server-summary-checkpoint.zip', 178091,
                      '0c5ea91c27b58ba5a1c4251e2e2acb470dbef722dc851924a12c3034545945a8',
                      'checkpoint-manifest.json', 40)
assert not json.loads(checkpoint['checkpoint-status.json'])['raw_archive_retrieved']
assert json.loads(checkpoint['checkpoint-status.json'])['product_source_unchanged']
assert proof['offline_shared_scratch_test.go'] == checkpoint['offline_shared_scratch_test.go']
assert proof['local-scratch-race.log'] == checkpoint['local-scratch-race.log']
assert b'PASS: TestOfflineSharedScratchBoundsAndReuse' in proof['local-scratch-race.log']
assert b'PASS: TestOfflineSharedScratchConcurrentOwnership' in proof['local-scratch-race.log']
seal = json.loads(proof['server-study-seal.json'])
transport = json.loads(proof['server-study-seal.json.transport.json'])
assert transport['bytes'] == len(proof['server-study-seal.json'])
assert transport['sha256'] == hashlib.sha256(proof['server-study-seal.json']).hexdigest()
assert len(transport['nonce']) == 36
archive = HERE/'study-core.tar.xz'
assert archive.stat().st_size == seal['bytes'] == 438076
assert hashlib.sha256(archive.read_bytes()).hexdigest() == seal['sha256'] == '1f1148ee9885c9efcc793ac3058826b5029fe77f7b9941b83c5f456864bd03c5'
download = json.loads(proof['download-receipt.json'])
assert download['bytes'] == seal['bytes'] and download['sha256'] == seal['sha256']
assert download['server_seal_matches'] and not download['production_mutations']
assert seal['members'] == 37 and seal['unit']['MainPID'] == '0' and seal['unit']['ExecMainStatus'] == '0'
with tarfile.open(archive, 'r:xz') as t:
    rows = [json.loads(line) for line in t.extractfile('pages.jsonl').read().splitlines()]
    inputs = json.load(t.extractfile('selected-inputs.json'))
    inventory = t.extractfile('frozen-source-inventory.json').read()
assert len(rows) == 1817
assert checkpoint['frozen-source-inventory.json'] == inventory
assert json.loads(proof['live-source-proof.json'])['all_match']
assert json.loads(proof['live-source-proof.json'])['expected_inventory_sha256'] == hashlib.sha256(inventory).hexdigest()
result = json.loads(proof['complete-arithmetic.json'])
assert result['raw_pages'] == len(rows) and result['column_pages'] == sum(r['AnnotationsRecovered'] for r in rows) == 1806
assert result['original_page_bytes'] == sum(r['OriginalBytes'] for r in rows) == 50016427
assert result['prototype_page_bytes'] == sum(r['PrototypeBytes'] for r in rows) == 49461177
saved = sum(r['OriginalBytes']-r['PrototypeBytes'] for r in rows)
assert result['saved_page_bytes'] == saved == 555250
physical = sum(r['size'] for r in inputs)
assert result['original_three_file_bytes'] == physical == 50067591
assert result['estimated_three_file_bytes'] == physical-saved == 49512341
assert result['estimated_file_effect_percent'] == -saved/physical*100
totals = {k:[sum(r[k][i] for r in rows) for i in range(3)]
          for k in ['OldEncodeNS','PrototypeEncodeNS','OldDecodeNS','PrototypeDecodeNS']}
assert totals == result['round_totals_ns']
median = {k:statistics.median(v) for k,v in totals.items()}
assert result['resident_encode_effect_percent'] == (median['PrototypeEncodeNS']/median['OldEncodeNS']-1)*100
assert result['resident_decode_effect_percent'] == (median['PrototypeDecodeNS']/median['OldDecodeNS']-1)*100
assert not result['production_mutations']
print(json.dumps(dict(all_37_raw_members_downloaded=True,all_1817_raw_rows_recomputed=True,
                      saved_page_bytes=saved,estimated_file_effect_percent=result['estimated_file_effect_percent'],
                      resident_encode_effect_percent=result['resident_encode_effect_percent'],
                      resident_decode_effect_percent=result['resident_decode_effect_percent'],
                      subsequent_scratch_scope='Local correctness and race only; no native real-page speed claim.',
                      production_mutations=False),indent=2))
