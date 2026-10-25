import pathlib,json,os
P=pathlib.Path('/data/gtron/history-account-delta-20261025-01');assert json.loads((P/'layout-status.json').read_text())['complete']
active={r['path'] for r in json.loads((P/'candidate-input/state-snapshots/manifest.json').read_text())['segments']};pins=P/'original-source-pins';pins.mkdir();moves=[]
for line in (P/'complete-input-files.jsonl').read_text().splitlines():
 row=json.loads(line)
 for ref in row['source']:
  if ref['path'] in active:continue
  a=P/'candidate-input/state-snapshots'/ref['path'];b=pins/ref['path'];assert a.is_file() and not b.exists();b.parent.mkdir(parents=True,exist_ok=True);x=a.stat();a.rename(b);y=b.stat();assert (x.st_dev,x.st_ino,x.st_size)==(y.st_dev,y.st_ino,y.st_size);moves.append(dict(path=ref['path'],bytes=x.st_size,inode=x.st_ino,retained=True))
for d in [P/'candidate-input/state-snapshots/history',pins/'history',pins]:
 if d.exists():
  fd=os.open(str(d),os.O_RDONLY);os.fsync(fd);os.close(fd)
(P/'retained-source-relocations.json').write_text(json.dumps(dict(complete=True,production_mutations=False,retained_root=str(pins),files=moves)))
