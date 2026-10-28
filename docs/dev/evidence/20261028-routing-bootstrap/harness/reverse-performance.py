import pathlib,sys,json,traceback
R=pathlib.Path('/data/gtron/maintenance/routing-bootstrap-20261028-01')
source=(R/'native-validation.py').read_text();prefix=source.split('\ntry:\n save();assert len(SOURCE)',1)[0]
assert len(prefix)<len(source)
# Reuse the identical HTTP host runner; reverse only the 9M cases after the
# candidate has run. Preserve all earlier measurements and canonical identity.
prefix=prefix.replace('for height in [9000000,35000000,45836826]:','for height in [9000000]:')
exec(compile(prefix,'frozen-http-runner','exec'))
s=json.loads((R/'candidate-status.json').read_text());assert s['complete'];requests=json.loads((R/'candidate-requests.json').read_text());expected={9000000:207};s.update(complete=False,stage='reverse current-reader 9M comparison');save()
try:
 previous=pathlib.Path('/data/gtron/maintenance/history-routing-rollout-20261028-01/bootstrap-offline/bootstrap-http.test')
 http_case('current-partial-reverse',previous,base_input)
 s['production_after']=identity();assert s['production_before']==s['production_after']
 s.update(complete=True,stage='fixed-cut HTTP, bounded maintenance and reverse 9M comparison complete',requests=len(requests),all_returns_match=True);save()
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
