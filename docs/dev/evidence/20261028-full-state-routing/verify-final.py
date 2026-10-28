"""Independently verify downloaded native receipts and recompute HTTP statistics.

Usage: python3 verify-final.py ARCHIVE EXPECTED_BYTES EXPECTED_SHA256 FRESH_ROOT
The billion-row folds are native receipts, not a second local database scan.
"""
import pathlib,json,hashlib,zipfile,sys,subprocess,gzip

archive=pathlib.Path(sys.argv[1]);expected_bytes=int(sys.argv[2]);expected_sha=sys.argv[3];R=pathlib.Path(sys.argv[4])
assert archive.stat().st_size==expected_bytes
h=hashlib.sha256()
with archive.open('rb') as f:
 for b in iter(lambda:f.read(4<<20),b''):h.update(b)
assert h.hexdigest()==expected_sha
R.mkdir(parents=True,exist_ok=False)
with zipfile.ZipFile(archive) as z:
 names=z.namelist();assert len(names)==len(set(names))
 files=json.loads(z.read('files-sha256.json'));assert set(names)==set(files)|{'files-sha256.json'}
 for n,q in files.items():
  p=pathlib.PurePosixPath(n);assert not p.is_absolute() and '..' not in p.parts
  b=z.read(n);assert len(b)==q['bytes'] and hashlib.sha256(b).hexdigest()==q['sha256'],n
  target=R/n;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(b)
def read(n):return json.loads((R/n).read_text())
final=read('final-acceptance.json');assert final['complete'] and final['all_http_results_correct'] and final['production_http_identity_unchanged']
assert not final['historical_restore_is_full'] and not final['runnable_cli_bootstrap_verified'] and not final['production_mutations_after_capture']
for n in ['setup-status.json','index-pipeline-status.json','source-muhash/status.json','snapshot-final-status.json','snapshot-v5/status.json','snapshot-v6/status.json','http-final/status.json','http-profile/status.json']:
 assert read(n)['complete'],n
source=read('source-muhash/census.json');restored=read('snapshot-v6/reopened-census.json');head=read('source-muhash/head.json')
for q in [source,restored]:
 assert q['complete'] and q['muhash_verified']
 assert (q['block_num'],q['tx_num'],q['block_hash'],q['muhash_root'])==(head['BlockNum'],head['TxNum'],head['BlockHash'],head['Root'])
for k in ['source_rows','source_logical_bytes','source_sha256','class_sha256','total_kv','classes','domains']:
 assert source[k]==restored[k],k
assert source['source_rows']==[119819707,389068,1036811084]
state=bytes(head['State']);assert len(state)==384 and 0<int.from_bytes(state,'little')<(1<<3072)-1103717
assert hashlib.sha256(state).digest()==bytes(head['Root'])
before=read('snapshot-v4/source-anchor.json');after=read('snapshot-v6/restored-anchor.json');assert before==after and before['Head']==head
codes=[read(n) for n in ['snapshot-v4/source-code.json','snapshot-v4/snapshot-code.json','snapshot-v6/restored-code.json']];assert codes[0]==codes[1]==codes[2]
snapshot=read('snapshot-v6/status.json');assert snapshot['all_accounts_code_checked']==source['source_rows'][0] and snapshot['normalized_state_and_digest_verified'] and snapshot['close_reopen_verified']
assert not snapshot['historical_rows_imported'] and not snapshot['runnable_node_cli_restore']
restore=read('snapshot-v5/restore-result.json');assert restore['RestoredTxNum']==head['TxNum'] and restore['ChangesRestored']==0 and restore['TxRangesRestored']==0
manifest=read('snapshot-v4/latest-manifest.json');assert manifest['latestStateTxNum']==head['TxNum'] and manifest['sourceRows']==source['source_rows']
proof=read('snapshot-v4/chain-proof.json');assert proof['canonical_genesis_verified'] and proof['canonical_cut_verified'] and proof['internal_cut_root_verified']
assert proof['cut']==head['BlockNum'] and proof['cut_block_hash']==bytes(head['BlockHash']).hex()
assert manifest['chain']==proof['expected_identity']
assert before==read('snapshot-v5/source-anchor.json')==read('snapshot-v5/restored-anchor.json')
assert read('source-anchor-preflight/status.json')['complete'] and read('harness-anchor-fix/status.json')['complete']
assert read('source-anchor-preflight/anchor.json')==before
assert read('source-anchor-preflight/status.json')['history_has_current_tx_range']
assert read('full-source-comparison.json')['complete']
assert proof==read('snapshot-v5/chain-proof.json')
assert codes[0]==read('snapshot-v5/source-code.json')==read('snapshot-v5/snapshot-code.json')
assert read('snapshot-v5/latest-manifest.json')==manifest
combo=read('snapshot-final-status.json')
for k in ['complete','phase1_signed_export_verified','phase2_official_restore_verified','phase3_independent_full_reopen_verified']:assert combo[k],k
assert not combo['runnable_node_cli_restore'] and combo['historical_rows_imported']==0
handoff=read('snapshot-phase1-handoff.json');assert handoff['complete'] and not handoff['old_auditor_pids_after_stop']
assert handoff['official_publish_return_evidence']['stage']=='strict signed latest-state restore to NEW persistent target'
for n,q in handoff['signed_export_files'].items():assert hashlib.sha256((R/n).read_bytes()).hexdigest()==q['sha256'],n
ro=read('snapshot-export-readonly.json');assert ro['complete'] and ro['private_mount_namespace']!=ro['host_mount_namespace'] and ro['no_shared_inode_permission_changes']
assert 'ro' in ro['mountinfo'].split()[5].split(',')
for n in ['http-final-pre-private-budget/exclusion.json','http-profile-pre-private-budget/exclusion.json','http-final-pre-separated-gates/exclusion.json','http-profile-pre-separated-gates/exclusion.json']:
 assert read(n)['complete'] and read(n)['measured_requests']==0
runtime=read('http-final/runtime-limits.json')
assert final['native_binaries']['baseline-http-final.test']['sha256']==runtime['baseline_sha256']
assert final['native_binaries']['candidate-http-final.test']['sha256']==runtime['candidate_sha256']
for binary,source_path in [('snapshot-restore-phase','source-candidate/cmd/offline-full-snapshot-restore/main.go'),('snapshot-reopen-phase','source-candidate/cmd/offline-full-snapshot-reopen/main.go')]:
 q=combo['native_binaries'][binary];assert q['sha256']==final['native_binaries'][binary]['sha256'] and hashlib.sha256((R/source_path).read_bytes()).hexdigest()==q['harness_sha256']
# Rebind the final HTTP inputs to the previously verified canonical catalog.
for mode in ['baseline','candidate']:
 data=gzip.decompress((R/'manifests'/(mode+'.json.gz')).read_bytes())
 prior=pathlib.Path('build/benchmarks/20261028-full-state-acceptance/downloaded-prelude/manifests')/(mode+'.json.gz')
 assert data==gzip.decompress(prior.read_bytes()),('canonical manifest changed',mode)
 m=json.loads(data)
 if mode=='candidate':
  for q in m['segments']:
   if q['kind'] in ['small-system-history-routing','metadata-history-routing']:
    b=(R/'routes'/q['path']).read_bytes();assert len(b)==q['size'] and 'sha256:'+hashlib.sha256(b).hexdigest()==q['checksum']

ident=read('source-identity.json')
for mode in ['baseline','candidate']:
 for n,digest in ident[mode+'_sources'].items():
  p=R/('source-'+mode)/n
  assert (not p.exists()) if digest is None else hashlib.sha256(p.read_bytes()).hexdigest()==digest,(mode,n)
for n,digest in ident['candidate_sources'].items():assert hashlib.sha256(pathlib.Path(n).read_bytes()).hexdigest()==digest,('local product changed',n)
D=pathlib.Path(__file__).resolve().parent
catalog=subprocess.check_output(['go','run',str(D/'verify-catalog.go'),str(R/'snapshot-v4/export'),str(R/'snapshot-v4/trusted-key.json')],text=True)
catalog=json.loads(catalog);assert catalog['complete'] and catalog['ed25519_signature_verified']
subprocess.check_call([sys.executable,str(D/'analyze-http.py'),str(R)],stdout=(R/'locally-recomputed-http-output.json').open('w'))
receipt={'complete':True,'archive_sha256':expected_sha,'archive_bytes':expected_bytes,'members':len(files)+1,
 'scope':'archive integrity, signed catalog, native full-fold/restore receipts and independently recomputed HTTP rows/resources; no second local billion-row fold',
 'head':head['BlockNum'],'tx':head['TxNum'],'root':bytes(head['Root']).hex(),
 'source_rows':source['source_rows'],'total_source_rows':sum(source['source_rows']),
 'bytecode':codes[0],'all_accounts_code_checked':snapshot['all_accounts_code_checked'],
 'http_requests':read('http-final/status.json')['requests'],'local_product_sources_unchanged':True,'signed_catalog':catalog}
(R/'independent-final-verification.json').write_text(json.dumps(receipt,indent=2));print(json.dumps(receipt,indent=2))
