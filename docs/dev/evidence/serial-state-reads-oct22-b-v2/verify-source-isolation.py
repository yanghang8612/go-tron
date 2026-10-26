import hashlib,json,pathlib,shutil,subprocess
root=pathlib.Path(__file__).resolve().parents[4]
evidence=pathlib.Path(__file__).resolve().parent
exports=root/'build/benchmarks/serial-state-reads-oct22-b-v2'
manifest=json.loads((evidence/'export-manifest.json').read_text())
def index(folder):return {str(p.relative_to(folder)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(folder.rglob('*')) if p.is_file()}
a=index(exports/'parent');b=index(exports/'candidate')
changed=[p for p in sorted(a.keys()|b.keys()) if a.get(p)!=b.get(p)]
expected=sorted(manifest['production']+manifest['candidate_only_tests'])
assert changed==expected,(changed,expected)
# Reconstruct a fresh candidate from the actual parent with an explicit patch,
# then require whole-tree byte identity, including every unmodified file.
r=exports/'reconstructed-from-explicit-patch'
if r.exists():raise SystemExit(f'Already exists: {r}')
shutil.copytree(exports/'parent',r)
p=subprocess.run(['patch','-p1','--input',str(evidence/'production.patch')],cwd=r,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
assert p.returncode==0,p.stdout
for f in manifest['candidate_only_tests']:shutil.copy2(exports/'candidate'/f,r/f)
c=index(r);assert c==b,[f for f in sorted(c.keys()|b.keys()) if c.get(f)!=b.get(f)]
# The exact patch cannot include A's separate snapshot/cache-admission work.
patch=(evidence/'production.patch').read_text()
assert 'cacheVersion' not in patch and 'captureReadSnapshot' not in patch and 'base_read_cache.go' not in patch
result={'parent':manifest['parent'],'whole_tree_changed':changed,'whole_tree_counts':{'parent':len(a),'candidate':len(b)},'parent_source_index':a,'candidate_source_index':b,'explicit_patch_reconstruction_matches_candidate':True,'patch_sha256':hashlib.sha256(patch.encode()).hexdigest(),'patch_output':p.stdout,'excluded_A_hunks_verified':True}
(evidence/'source-isolation.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({k:v for k,v in result.items() if not k.endswith('_index')},indent=2))
