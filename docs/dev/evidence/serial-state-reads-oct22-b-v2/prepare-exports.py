from pathlib import Path
import hashlib,io,json,subprocess,tarfile,shutil
root=Path(__file__).resolve().parents[4]
evidence=Path(__file__).resolve().parent
out=root/'build/benchmarks/serial-state-reads-oct22-b-v2'
out.mkdir(parents=True,exist_ok=True)
common=['core/blockbuffer/predecessor_candidates_test.go','core/blockbuffer/predecessor_candidates_bench_test.go']
prod=['core/blockbuffer/buffer.go','core/blockbuffer/layer_view.go','core/blockbuffer/predecessor.go']
manifest={'parent':'9030ce7a31016491ece014ac76e5656ec149417e','exports':str(out),'common_tests':common,'production':prod,'candidate_only_tests':['core/blockbuffer/predecessor_candidates_v2_test.go'],'source_hashes':{}}
for name in ['parent','candidate']:
    p=out/name
    if p.exists():raise SystemExit(f'Export already exists: {p}')
    p.mkdir()
    archive=subprocess.check_output(['git','archive',manifest['parent']],cwd=root)
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:tar.extractall(p,filter='data')
    for src in common:shutil.copy2(root/src,p/src)
    if name=='candidate':
        for src in ['core/blockbuffer/layer_view.go','core/blockbuffer/predecessor.go']+manifest['candidate_only_tests']:shutil.copy2(root/src,p/src)
        # Copy only the frozen index implementation region. A owns concurrent
        # edits elsewhere in buffer.go, which must not enter the B export.
        src='core/blockbuffer/buffer.go';current=(root/src).read_text();old=(p/src).read_text()
        start='type layerPrefixBucketIndex struct {';end='// bufferReadView is an immutable snapshot'
        region=current[current.index(start):current.index(end)]
        old=old[:old.index(start)]+region+old[old.index(end):]
        (p/src).write_text(old)
    manifest['source_hashes'][name]={src:hashlib.sha256((p/src).read_bytes()).hexdigest() for src in prod+common+(manifest['candidate_only_tests'] if name=='candidate' else [])}
for src in common:assert manifest['source_hashes']['parent'][src]==manifest['source_hashes']['candidate'][src]
manifest['spec_hash']=hashlib.sha256((root/'docs/superpowers/specs/2026-10-22-prefix-predecessor-candidates-v2.md').read_bytes()).hexdigest()
(evidence/'export-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
patch=[]
for src in prod:
    import difflib
    patch.extend(difflib.unified_diff((out/'parent'/src).read_text().splitlines(keepends=True),(out/'candidate'/src).read_text().splitlines(keepends=True),fromfile='a/'+src,tofile='b/'+src))
(evidence/'production.patch').write_text(''.join(patch))
print(json.dumps(manifest,indent=2))
