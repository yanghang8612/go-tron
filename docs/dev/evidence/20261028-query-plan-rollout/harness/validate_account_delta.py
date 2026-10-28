import pathlib,sys,json,os,shutil,time,urllib.request,traceback
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'cold.py').read_text().split('\ntry:\n until=',1)[0],'fixed-host-definitions','exec'))
R=pathlib.Path('/data/gtron/maintenance/first-cold-followup-20261028-01/account-delta');R.mkdir()
P=R.parent
s={'complete':False,'stage':'compile account delta prototype','source':SOURCE,'production_mutations':False,'canonical_data_mutations':False,'runner_admission_tested':False,'cache':'fresh HTTP processes, shared canonical/OS caches uncontrolled; no eviction'};requests=[]
def clone_input(original,name):
    dest=R/name;dest.mkdir()
    for p in original.iterdir():
        if p.name in ['state-snapshots','ancient']:shutil.copytree(str(p),str(dest/p.name),copy_function=os.link,symlinks=True)
        else:os.symlink(str(p),str(dest/p.name),target_is_directory=p.is_dir())
    return dest
try:
    s['production_before']=identity();save();src=archive_source('source-candidate',SOURCE)
    fixture=git('show',SOURCE+':docs/dev/evidence/20261028-query-plan-rollout/harness/offline_account_delta_build_test.go.txt');(src/'core/state/snapshots/offline_account_delta_build_test.go').write_bytes(fixture)
    command(['/data/go/bin/go','test','-tags=sapling','./core/state/snapshots','-run','^TestAccountRoutingDelta|^TestAccountHistoryRouting','-count=1','-timeout=180s'],src,R/'focused.log',240)
    command(['/data/go/bin/go','test','-tags=sapling','-c','./core/state/snapshots','-o',str(R/'snapshots.test')],src,R/'build-snapshots.log',300)
    command(['/data/go/bin/go','test','-tags=sapling','-c','./core','-o',str(R/'http.test')],src,R/'build-http.log',300)
    control=clone_input(P/'storage-pages/input-storage','input-gaps')
    snap=control/'state-snapshots';gap=P/'account-gap-ablation/input-1521-gaps/state-snapshots'
    m=json.loads((snap/'manifest.json').read_text());g=json.loads((gap/'manifest.json').read_text())
    ref,=[r for r in g['segments'] if r['kind']=='account-history-routing']
    target=snap/ref['path'];target.parent.mkdir(parents=True,exist_ok=True)
    assert not target.exists();os.link(str(gap/ref['path']),str(target))
    m['segments']=[ref if r['kind']=='account-history-routing' else r for r in m['segments']]
    # Break the copied manifest hard link before changing this private view.
    tmp=snap/'account-gap-manifest.tmp';tmp.write_text(json.dumps(m));os.replace(str(tmp),str(snap/'manifest.json'))
    candidate=clone_input(control,'input-delta')
    s['stage']='bounded account delta build and private publication';save()
    env=dict(E,GTRON_ACCOUNT_DELTA_INPUT=str(candidate/'state-snapshots'),GTRON_ACCOUNT_DELTA_OUTPUT=str(R))
    with (R/'build-delta.log').open('wb') as log:subprocess.check_call([str(R/'snapshots.test'),'-test.run=^TestOfflineAccountDeltaBuild$','-test.v','-test.timeout=30m'],env=env,cwd=str(R),stdout=log,stderr=subprocess.STDOUT,timeout=1860)
    compact=clone_input(candidate,'input-compact')
    s['stage']='bounded delta-only consolidation';save()
    env=dict(E,GTRON_ACCOUNT_DELTA_INPUT=str(compact/'state-snapshots'),GTRON_ACCOUNT_DELTA_OUTPUT=str(R))
    with (R/'consolidation.log').open('wb') as log:subprocess.check_call([str(R/'snapshots.test'),'-test.run=^TestOfflineAccountDeltaConsolidate$','-test.v','-test.timeout=15m'],env=env,cwd=str(R),stdout=log,stderr=subprocess.STDOUT,timeout=960)
    matrix=[('gaps-1',control),('delta-1',candidate),('compact-1',compact),('compact-2',compact),('delta-2',candidate),('gaps-2',control)]
    for name,input_ in matrix:
        s['stage']=name+' full HTTP';save()
        with host(name,R/'http.test',input_) as ready:
            for height,want in [(9000000,207),(10000000,213),(11000000,220),(15000000,238),(20000000,249)]:
                for repeat in range(2):
                    q={'jsonrpc':'2.0','id':len(requests)+1,'method':'eth_call','params':[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},hex(height)]};began=time.perf_counter()
                    req=urllib.request.Request(ready['url'],data=json.dumps(q).encode(),headers={'Content-Type':'application/json'})
                    with urllib.request.urlopen(req,timeout=150) as f:body=json.loads(f.read())
                    row={'case':name,'height':height,'repeat':repeat,'seconds_loopback_http':time.perf_counter()-began,'response':body};requests.append(row);(R/'requests.json').write_text(json.dumps(requests,indent=2));assert int(body['result'],16)==want;save()
    s['production_after']=identity();assert s['production_before']==s['production_after'];s.update(complete=True,stage='account delta prototype build and HTTP complete',requests=len(requests),all_returns_match=True);save()
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
