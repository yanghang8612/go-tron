import pathlib,subprocess,json,os,shutil,datetime
A=pathlib.Path('/data/gtron/maintenance/9m-query-20261027-01');Q=A/'metadata-cold-v3';F=A/'metadata-final'
assert json.loads((Q/'status.json').read_text())['complete'] and json.loads((F/'status.json').read_text())['complete']
assert (A/'metadata-local-evidence-verified.json').is_file()
R=A/'metadata-page64';assert json.loads((R/'status.json').read_text())['complete']
for unit in ['gtron-offline-metadata-20261027-03','gtron-offline-metadata-cold-20261027-03','gtron-offline-metadata-final-20261027-05','gtron-offline-metadata-page64-20261027-02','gtron-offline-metadata-envelope-20261027-01']:
 state=subprocess.check_output(['systemctl','show',unit,'--property=ActiveState']).decode().strip();assert state=='ActiveState=inactive',state
roots=[Q/'baseline',Q/'candidate',R/'catalog'];expected={str(x) for x in roots}
for p in roots:assert not p.is_symlink() and str(p.resolve()) in expected
for proc in pathlib.Path('/proc').iterdir():
 if not proc.name.isdigit():continue
 try:
  for fd in (proc/'fd').iterdir():
   try:target=os.readlink(str(fd))
   except OSError:continue
   assert not any(target==str(p) or target.startswith(str(p)+'/') for p in roots),(proc,fd,target)
 except (FileNotFoundError,PermissionError):pass
proof=json.loads((Q/'private-files-proof.json').read_text())+json.loads((Q/'private-extra-proof.json').read_text())+json.loads((R/'new-private-files.json').read_text());inodes={}
for p in roots:
 for f in p.rglob('*'):
  assert not f.is_symlink(),f
  if f.is_file():
   st=f.stat();key=(st.st_dev,st.st_ino);inodes.setdefault(key,{'links_in_roots':0,'nlink':st.st_nlink,'bytes':st.st_size,'allocated_bytes':st.st_blocks*512})['links_in_roots']+=1
private={(r['private_dev'],r['private_ino']) for r in proof}
assert private<=inodes.keys()
for key in private:assert inodes[key]['nlink']==inodes[key]['links_in_roots'],(key,inodes[key])
receipt={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'removed_roots':[str(x) for x in roots],'private_inode_count':len(private),'private_logical_bytes':sum(inodes[k]['bytes'] for k in private),'private_allocated_bytes':sum(inodes[k]['allocated_bytes'] for k in private),'production_mutations':False,'retained_fixed_catalog':str(A/'baseline-catalog'),'retained_candidate_catalog':str(A/'metadata-v3/catalog'),'archive_seal':json.loads((A/'metadata-acceptance-seal.json').read_text()),'layout64_allocated_bytes':sum((R/'catalog'/r['path']).stat().st_blocks*512 for r in json.loads((R/'layout-proof.json').read_text())['refs'])}
for p in roots:shutil.rmtree(str(p));assert not p.exists()
(Q/'cleanup-receipt.json').write_text(json.dumps(receipt,indent=2));dest=pathlib.Path('/tmp/AAA-gtron-metadata-cleanup-20261027.json');dest.write_text(json.dumps(receipt,indent=2));os.chmod(str(dest),0o644);print(json.dumps(receipt,indent=2))
