import fcntl,hashlib,json,os,pathlib,platform,subprocess,time
root=pathlib.Path(__file__).resolve().parents[4]
evidence=pathlib.Path(__file__).resolve().parent
exports=root/'build/benchmarks/serial-state-reads-oct22-b-v2'
lock=pathlib.Path('/Users/asuka/Projects/asuka/go/go-tron/build/benchmarks/20261018-tvm-event-replay/local-benchmark.lock')
inputs=json.loads((evidence/'export-manifest.json').read_text())
assert hashlib.sha256((root/'docs/superpowers/specs/2026-10-22-prefix-predecessor-candidates-v2.md').read_bytes()).hexdigest()==inputs['spec_hash']
for p in inputs['common_tests']+inputs['candidate_only_tests']+['core/blockbuffer/predecessor.go','core/blockbuffer/layer_view.go']:
    assert hashlib.sha256((root/p).read_bytes()).hexdigest()==inputs['source_hashes']['candidate'][p],('workspace changed',p)
p='core/blockbuffer/buffer.go';start='type layerPrefixBucketIndex struct {';end='// bufferReadView is an immutable snapshot'
a=(root/p).read_text();b=(exports/'candidate'/p).read_text()
assert a[a.index(start):a.index(end)]==b[b.index(start):b.index(end)],'workspace B index hunks changed'
assert json.loads((evidence/'source-isolation.json').read_text())['explicit_patch_reconstruction_matches_candidate']
manifest={'parent':inputs['parent'],'order':'ABBAABBAAB','samples':5,'iterations':{'cold':32,'churn':256,'hotstable':32768,'smallprefix':32768,'tombstones':128,'generic':32768},'cpu':1,'command_timeout':'30m','lock':str(lock),'platform':platform.platform(),'go':subprocess.check_output(['go','version'],text=True).strip(),'source_hashes':inputs['source_hashes'],'binaries':{},'harness_scope':'local overlay and 512 actual unrelated Put mutations; excludes full Pebble and SaveCurrentAsBest stack'}
for revision,files in inputs['source_hashes'].items():
    for p,want in files.items():assert hashlib.sha256((exports/revision/p).read_bytes()).hexdigest()==want,(revision,p)
    manifest['binaries'][revision]=hashlib.sha256((exports/(revision+'.test')).read_bytes()).hexdigest()
(evidence/'benchmark-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
with lock.open('a+') as f:
    fcntl.flock(f,fcntl.LOCK_EX)
    seen={'parent':0,'candidate':0}
    for letter in manifest['order']:
        revision='parent' if letter=='A' else 'candidate';seen[revision]+=1
        log=evidence/f'{revision}-bench-{seen[revision]}.log'
        if log.exists():raise SystemExit(f'Refusing to overwrite sample {log}')
        log.write_text('')
        print(revision,seen[revision],'start',time.strftime('%H:%M:%S'),flush=True)
        for mib in [8,128]:
            for mode,n in manifest['iterations'].items():
                args=[str(exports/(revision+'.test')),'-test.run=^$',f'-test.bench=^BenchmarkPredecessorCandidates$/^{mib}MiB$/^{mode}$',f'-test.benchtime={n}x','-test.cpu=1','-test.count=1','-test.timeout=30m']
                started=time.time();p=subprocess.run(args,cwd=exports/revision,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,env={**os.environ,'GOMAXPROCS':'1'})
                with log.open('a') as out:out.write('command: '+repr(args)+'\n'+p.stdout+f'\nexit={p.returncode} elapsed={time.time()-started:.3f}s\n')
                measured=[line for line in p.stdout.splitlines() if line.startswith('BenchmarkPredecessorCandidates/')]
                print(revision,seen[revision],f'{mib}MiB/{mode}',*measured,flush=True)
                if p.returncode or len(measured)!=1:raise SystemExit(p.returncode or 'expected exactly one workload')
