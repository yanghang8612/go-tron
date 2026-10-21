import pathlib,json,subprocess,hashlib,datetime
r=pathlib.Path('/data/gtron/cold-query-layout-20261020-01/hot-owner-spans-20261021-03')
def j(n):return json.loads((r/n).read_text())
print(chr(27)+'[2J'+chr(27)+'[H',end='')
print('go-tron fixed-copy final acceptance | '+datetime.datetime.now(datetime.timezone.utc).isoformat())
print('Candidate 43c3d47b | fixed HEAD 11818192 | NO production start/restart/deploy')
c=j('tail-read-comparison/comparison-status.json'); print('HTTP requests:',c['requests'],'all responses correct:',c['all_responses_correct'])
ss=j('tail-read-comparison/summary.json')
for cache in ['warm','fresh-process-os-uncontrolled','private-os-cold-fresh-process']:
 q=[x for x in ss if x['tag']=='0x895440' and x['cache']==cache]; print('9M '+cache+': '+', '.join(x['mode']+'='+str(round(x['median_ms'],3))+' ms' for x in q))
m=j('muhash-census.json'); print('MuHash verified:',m['muhash_verified'],'source rows:',sum(m['source_rows']))
h=j('history-restore.json');print('Restore rows:',sum(x['rows'] for x in h['windows']),'range rows:',sum(x['range_rows_verified'] for x in h['windows']))
print(subprocess.check_output(['systemctl','show','gtron','-p','MainPID','-p','ActiveState','-p','SubState'],universal_newlines=True).strip())
print('Old DB retained:',pathlib.Path('/data/gtron/main/datadir-pre-compact-v3-20261019-659ab078').is_dir())
for phase in ['prepare','layout','compare','audit','diagnostic','resources','seal']:
 unit='gtron-owner-blake-r2-'+phase+'-20261021.service';print(phase+': '+subprocess.check_output(['systemctl','show',unit,'-p','ActiveState'],universal_newlines=True).strip())
p=pathlib.Path('/tmp/gtron-hot-owner-spans-20261021-03.zip');print('Evidence ZIP:',p.stat().st_size,'B SHA256',hashlib.sha256(p.read_bytes()).hexdigest())
