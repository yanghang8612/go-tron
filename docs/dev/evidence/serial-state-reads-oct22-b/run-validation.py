import fcntl, pathlib, subprocess, time
root=pathlib.Path(__file__).resolve().parents[4]
exports=root/'build/benchmarks/serial-state-reads-oct22-b'
evidence=pathlib.Path(__file__).resolve().parent
lock=pathlib.Path('/Users/asuka/Projects/asuka/go/go-tron/build/benchmarks/20261018-tvm-event-replay/local-benchmark.lock')
with lock.open('a+') as f:
    fcntl.flock(f,fcntl.LOCK_EX)
    for revision in ['parent','candidate']:
        cwd=exports/revision
        for name,args in [
            ('oracle',['go','test','./core/blockbuffer','-run','TestPredecessor','-count=1']),
            ('race',['go','test','-race','./core/blockbuffer','-run','TestPredecessorCandidates','-count=1']),
            ('blockbuffer',['go','test','./core/blockbuffer','-count=1','-timeout=300s']),
            ('build',['go','test','-c','./core/blockbuffer','-o',str(exports/(revision+'.test'))]),
        ]:
            print(revision,name,' '.join(args),flush=True)
            started=time.time();p=subprocess.run(args,cwd=cwd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
            (evidence/f'{revision}-{name}.log').write_text(p.stdout+f'\nexit={p.returncode} elapsed={time.time()-started:.3f}s\n')
            print(p.stdout[-2000:],flush=True)
            if p.returncode:raise SystemExit(p.returncode)
