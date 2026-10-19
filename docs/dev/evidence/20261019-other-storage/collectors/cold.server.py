import hashlib,json,os,pathlib,shutil,subprocess,datetime
root=pathlib.Path('/data/gtron/other-storage-distribution-20261019-0245');base=pathlib.Path('/data/gtron/main/datadir/gtron/state-snapshots');pin=root/'cold-pinned';pin.mkdir(mode=0o700)
raw=(root/'pinned-state-manifest.json').read_bytes();m=json.loads(raw);refs=sorted([s for s in m['segments'] if s.get('dataset')=='state-domain-change' and s['kind']=='history'],key=lambda x:x['fromTxNum'])
indices=sorted(set(i*(len(refs)-1)//15 for i in range(16)));selected=[refs[i] for i in indices];ranges=set((r['fromTxNum'],r['toTxNum']) for r in selected)
allrefs=[r for r in m['segments'] if r.get('dataset')=='state-domain-change' and (r['fromTxNum'],r['toTxNum']) in ranges]
proof=[]
for r in allrefs:
 f=base/r['path'];dest=pin/r['path'];dest.parent.mkdir(parents=True,exist_ok=True);os.link(str(f),str(dest));st=dest.stat();assert st.st_size==r['size'];proof.append({'path':r['path'],'size':st.st_size,'dev':st.st_dev,'inode':st.st_ino,'manifest_checksum':r['checksum']})
m['segments']=allrefs;m['retired']=[];(pin/'manifest.json').write_text(json.dumps(m));(root/'cold-selection.json').write_text(json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'population_segments':len(refs),'selection':'16 evenly spaced ordinal positions, entire selected segment, no size filter','selected_indices':indices,'original_manifest_sha256':hashlib.sha256(raw).hexdigest(),'files':proof},indent=2))
source=root/'source';shutil.copytree(str(root/'cmd'/'other-cold-distribution'),str(source/'cmd'/'other-cold-distribution'))
env=dict(os.environ,PATH='/data/go/bin:'+os.environ['PATH'],GOMAXPROCS='1',GOTOOLCHAIN='local',CGO_ENABLED='0',GOPATH='/root/go',GOCACHE='/root/.cache/go-build')
subprocess.run(['go','build','-o',str(root/'cold-profiler'),'./cmd/other-cold-distribution'],cwd=str(source),env=env,check=True)
subprocess.run([str(root/'cold-profiler'),'--dir',str(pin),'--out',str(root/'cold-distribution.json')],env=env,check=True)
