# Reuse the complete fixed-cut HTTP host, without touching production or data.
import pathlib, sys, json, os, time, traceback, threading, urllib.request, subprocess
W = pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
code = (W/'cold.py').read_text().split('\ntry:\n until=', 1)[0]
exec(compile(code, 'existing-fixed-cut-host-definitions', 'exec'))
R = pathlib.Path('/data/gtron/maintenance/direct-history-plan-20261028-01')
R.mkdir(exist_ok=True)
s = {'complete': False, 'stage': 'full-route discovery', 'source': SOURCE,
     'production_mutations': False, 'full_validation_repeated': False}
requests = []
try:
 s['production_before'] = identity(); save()
 full = A/'http-performance-priority/candidate-input'
 exe = W/'candidate-http.test'
 with host('full-discovery', exe, full) as ready:
  obs = Observer(full)
  try: call(ready, 'full-discovery', 'discovery-excluded')
  finally: seen = obs.close()
  dump('full-readset.json', sorted(seen))
  for _ in range(2): call(ready, 'full-discovery', 'process-warm')
 # CPU sampling is outside all admitted timing rows.
 s['stage'] = 'full-route CPU profile'; save()
 with host('full-profile', exe, full) as ready:
  def capture():
   with urllib.request.urlopen(ready['url']+'debug/pprof/profile?seconds=15', timeout=30) as f:
    (R/'full-profile/cpu.pprof').write_bytes(f.read())
  thread = threading.Thread(target=capture); thread.start()
  end = time.monotonic()+16
  while time.monotonic() < end: call(ready, 'full-profile', 'profile-excluded')
  thread.join()
 with (R/'full-profile/cpu-top.txt').open('wb') as f:
  subprocess.check_call(['/data/go/bin/go','tool','pprof','-top','-cum','-nodecount=45','-tagfocus=rpc=eth_call',str(exe),str(R/'full-profile/cpu.pprof')],stdout=f,stderr=subprocess.STDOUT,env=E)
 s['production_after'] = identity()
 assert s['production_before'] == s['production_after']
 s.update(complete=True, stage='baseline profile complete', requests=len(requests))
 save()
except BaseException:
 s.update(stage='failed', error=traceback.format_exc());save();raise
