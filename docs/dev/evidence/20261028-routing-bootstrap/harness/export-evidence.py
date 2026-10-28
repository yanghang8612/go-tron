import pathlib,json,subprocess,os,pwd,hashlib,datetime
R=pathlib.Path('/data/gtron/maintenance/routing-bootstrap-20261028-01');REPO='/data/gtron/go-tron';BRANCH='codex/routing-bootstrap-20261028'
s=json.loads((R/'candidate-status.json').read_text());assert s['complete'] and s['all_returns_match']
# Isolated index; never touch the deployment checkout, its index or HEAD.
index_dir=R/'evidence-git-index';index_dir.mkdir(exist_ok=False);u=pwd.getpwnam('java-tron');os.chown(str(index_dir),u.pw_uid,u.pw_gid)
env=['GIT_TERMINAL_PROMPT=0','GIT_INDEX_FILE='+str(index_dir/'index'),'GIT_AUTHOR_NAME=Codex','GIT_AUTHOR_EMAIL=codex@users.noreply.github.com','GIT_COMMITTER_NAME=Codex','GIT_COMMITTER_EMAIL=codex@users.noreply.github.com']
def git(*args,**kw):return subprocess.check_output(['sudo','-u','java-tron','-H','env']+env+['git']+list(args),cwd=REPO,**kw)
def main_git(*args):return subprocess.check_output(['sudo','-u','java-tron','-H','git']+list(args),cwd=REPO)
head=main_git('rev-parse','HEAD').strip();status=main_git('status','--porcelain')
git('fetch','origin','refs/heads/'+BRANCH+':refs/remotes/origin/'+BRANCH);parent=git('rev-parse','refs/remotes/origin/'+BRANCH).decode().strip()
assert git('merge-base',s['source'],parent).decode().strip()==s['source']
assert all(p.startswith('docs/') for p in git('diff','--name-only',s['source'],parent).decode().splitlines())
git('read-tree',parent);files={}
paths=[R/n for n in ['candidate-status.json','candidate-requests.json','native-focused.log','candidate-build-http.log','candidate-status-link-failure.json'] if (R/n).is_file()]
for folder in ['current-maintenance','candidate-maintenance','baseline','current-partial','candidate-partial','candidate-bootstrap','candidate-complete','current-partial-reverse']:
 p=R/folder
 if p.is_dir():paths.extend(q for q in p.iterdir() if q.is_file() and q.suffix in ['.json','.jsonl','.log','.pprof','.txt'])
for p in sorted(set(paths)):
 raw=p.read_bytes();assert len(raw)<2*1024*1024,str(p)
 name='docs/dev/evidence/20261028-routing-bootstrap/native/'+str(p.relative_to(R));blob=git('hash-object','-w','--stdin',input=raw).decode().strip();git('update-index','--add','--cacheinfo','100644',blob,name);files[name]={'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
seal=json.dumps({'utc':datetime.datetime.utcnow().isoformat()+'Z','tested_source':s['source'],'files':files},indent=2).encode();blob=git('hash-object','-w','--stdin',input=seal).decode().strip();git('update-index','--add','--cacheinfo','100644',blob,'docs/dev/evidence/20261028-routing-bootstrap/native/member-seal.json')
tree=git('write-tree').decode().strip();commit=git('commit-tree',tree,'-p',parent,input=b'test(history): record fixed-copy routing bootstrap measurements\n').decode().strip();git('push','origin',commit+':refs/heads/'+BRANCH)
assert main_git('rev-parse','HEAD').strip()==head and main_git('status','--porcelain')==status
receipt={'commit':commit,'parent':parent,'files':len(files)+1,'deployment_head_unchanged':True};(R/'evidence-export-receipt.json').write_text(json.dumps(receipt,indent=2));print(json.dumps(receipt))
