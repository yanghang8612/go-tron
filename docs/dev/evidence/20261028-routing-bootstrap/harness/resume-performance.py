import pathlib,sys,json,traceback,os,shutil
R=pathlib.Path('/data/gtron/maintenance/routing-bootstrap-20261028-01')
source=(R/'native-validation.py').read_text();prefix=source.split('\ntry:\n save();assert len(SOURCE)',1)[0];assert len(prefix)<len(source)
exec(compile(prefix,'frozen-http-runner','exec'))
s=json.loads((R/'candidate-status.json').read_text());requests=json.loads((R/'candidate-requests.json').read_text());assert len(requests)==14
expected={x['height']:int(x['response']['result'],16) for x in requests};assert expected[9000000]==207
s.update(complete=False,stage='resume remaining HTTP cases');s.pop('error',None);save()
try:
 # Freezer correctly refuses a symlink datadir. Materialize just the ancient
 # directory with hardlinks; preserve the prior symlink and all canonical files.
 p=R/'candidate-maintenance-input/ancient'
 if p.is_symlink():
  origin=p.resolve();p.rename(p.with_name('ancient.saved-link'));shutil.copytree(str(origin),str(p),copy_function=os.link,symlinks=True)
 http_case('candidate-bootstrap-fixed',R/'candidate-http.test',R/'candidate-maintenance-input')
 http_case('candidate-complete',R/'candidate-http.test',A/'http-performance-priority/candidate-input')
 # Add an order-reversed current-reader 9M case without repeating other heights.
 scope={};exec(compile(prefix.replace('for height in [9000000,35000000,45836826]:','for height in [9000000]:'),'reverse-http-runner','exec'),scope)
 scope.update(s=s,requests=requests,expected=expected)
 scope['http_case']('current-partial-reverse',pathlib.Path('/data/gtron/maintenance/history-routing-rollout-20261028-01/bootstrap-offline/bootstrap-http.test'),base_input)
 s['production_after']=identity();assert s['production_before']==s['production_after']
 s.update(complete=True,stage='fixed-cut HTTP, bounded maintenance and reverse 9M comparison complete',requests=len(requests),all_returns_match=True);save()
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
