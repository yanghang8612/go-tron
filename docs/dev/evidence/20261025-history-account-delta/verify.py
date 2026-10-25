#!/usr/bin/env python3
"""Verify archived bytes, recompute space/HTTP/correctness receipts, check sources."""
import argparse,base64,gzip,hashlib,json,pathlib,subprocess,sys,tempfile
E=pathlib.Path(__file__).resolve().parent
ap=argparse.ArgumentParser();ap.add_argument('--source-root',type=pathlib.Path,default=E.parents[3]);args=ap.parse_args()
def read(n):return json.loads((E/n).read_text())
def sha(b):return hashlib.sha256(b).hexdigest()
checks=read('SHA256SUMS.json')
actual={str(p.relative_to(E))for p in E.rglob('*')if p.is_file()and p.name!='SHA256SUMS.json'and'__pycache__'not in p.parts}
assert actual==set(checks),'evidence membership changed'
for n,h in checks.items():assert sha((E/n).read_bytes())==h,n
original=read('archive-member-sha256.json');mapping=read('original-file-map.json');data={}
for n,destination in mapping['files'].items():
 b=(E/destination).read_bytes();data[n]=gzip.decompress(b)if destination.endswith('.gz')else b
for line in gzip.decompress((E/mapping['host_bundle']).read_bytes()).splitlines():
 r=json.loads(line);assert r['path']not in data;data[r['path']]=base64.b64decode(r['base64'],validate=True)
assert len(data)==len(mapping['files'])+mapping['host_members']==870
for n,b in data.items():
 assert not pathlib.PurePosixPath(n).is_absolute()and'..'not in pathlib.PurePosixPath(n).parts,n
 assert sha(b)==original[n],n
receipt=read('local/final-download-verification.json')
assert receipt['members']==906 and receipt['hashed_members']==len(original)==905
assert receipt['sha256']=='bceae43bca71bae1f75a402d11e3855869c7d8ab751e323ea624fe20ba9d94f3'and receipt['bytes']==14539613
assert receipt['crc_verified']and receipt['all_member_sha256_verified']and not receipt['production_mutations']
with tempfile.TemporaryDirectory(prefix='gtron-account-delta-evidence-')as name:
 p=pathlib.Path(name)
 for n,b in data.items():
  target=p/n;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(b)
 (p/'evidence-sha256.json').write_text(json.dumps({n:original[n]for n in data}))
 def run(script,*params):
  return subprocess.check_output([sys.executable,str(E/'scripts'/script),str(p),*map(str,params)],text=True)
 a=json.loads(run('analyze-evidence.py'));expected=read('derived/final-analysis.json')
 assert a.pop('evidence_members_verified')==870;assert expected.pop('evidence_members_verified')==905
 assert a==expected,'recomputed accounting or correctness differs'
 associations=json.loads(run('verify-final-associations.py',args.source_root.resolve()))
 assert associations==read('derived/final-associations.json'),'source or manifest associations differ'
 run('analyze-performance.py')
 for n in ['http-resource-summary.json','query-trace-summary.json']:
  assert json.loads((p/n).read_text())==read('derived/'+n),n
print(json.dumps(dict(local_evidence_files=len(checks),selected_original_members_verified=len(data),original_archive_hashed_members=len(original),source_files_verified=associations['source_inventory_files'],converted_segments=a['layout']['converted'],ordinary_controls=a['controls']['files'],net_seg_bytes=a['controls']['original_to_candidate_saved'],account_format_extra_bytes=a['controls']['account_format_extra_saved'],full_http_correct_responses=a['http']['response_rows'],timed_http_requests=a['http']['timed_rows'],cold_residency_proofs=a['http']['cold_residency_proofs'],muhash_verified=a['muhash']['root'],actual_history_restore_blocks=a['restore']['blocks'],decoder_samples=associations['decoder']['samples'],production_mutations=False),indent=2))
