import fcntl, hashlib, json, os, pathlib, platform, subprocess, time
root=pathlib.Path(__file__).resolve().parents[4]
evidence=pathlib.Path(__file__).resolve().parent
exports=root/'build/benchmarks/serial-state-reads-oct22-b'
lock=pathlib.Path('/Users/asuka/Projects/asuka/go/go-tron/build/benchmarks/20261018-tvm-event-replay/local-benchmark.lock')
relative=['core/blockbuffer/predecessor.go','core/blockbuffer/predecessor_candidates_bench_test.go','core/blockbuffer/predecessor_candidates_test.go']
manifest={'parent':'9030ce7a31016491ece014ac76e5656ec149417e','order':'ABBAABBAAB','samples':5,'benchtime':'16x','cpu':1,'lock':str(lock),'platform':platform.platform(),'go':subprocess.check_output(['go','version'],text=True).strip(),'source_hashes':{name:{p:hashlib.sha256((exports/name/p).read_bytes()).hexdigest() for p in relative} for name in ['parent','candidate']}}
for p in relative[1:]:assert manifest['source_hashes']['parent'][p]==manifest['source_hashes']['candidate'][p]
(evidence/'benchmark-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
with lock.open('a+') as f:
    fcntl.flock(f,fcntl.LOCK_EX)
    seen={'parent':0,'candidate':0}
    for letter in manifest['order']:
        revision='parent' if letter=='A' else 'candidate';seen[revision]+=1
        args=[str(exports/(revision+'.test')),'-test.run=^$','-test.bench=^BenchmarkPredecessorCandidates$','-test.benchtime=16x','-test.cpu=1','-test.count=1','-test.timeout=30m']
        print(revision,seen[revision],'start',time.strftime('%H:%M:%S'),flush=True)
        started=time.time();p=subprocess.run(args,cwd=exports/revision,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,env={**os.environ,'GOMAXPROCS':'1'})
        (evidence/f'{revision}-bench-{seen[revision]}.log').write_text('command: '+repr(args)+'\n'+p.stdout+f'\nexit={p.returncode} elapsed={time.time()-started:.3f}s\n')
        print(p.stdout,flush=True)
        if p.returncode:raise SystemExit(p.returncode)
