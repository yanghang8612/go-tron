# Diagnostic only: patched source archive, no production or input changes.
import pathlib,sys,json,os,time,urllib.request,traceback
W=pathlib.Path('/data/gtron/maintenance/tail-read-ahead-20261028-01')
exec(compile((W/'cold.py').read_text().split('\ntry:\n until=',1)[0],'fixed-host-definitions','exec'))
R=pathlib.Path('/data/gtron/maintenance/first-cold-followup-20261028-01/storage-trace');R.mkdir()
s={'complete':False,'stage':'build diagnostic source','source':SOURCE,'production_mutations':False,'performance_samples':False,'cache':'OS cache uncontrolled; diagnostic instrumentation'};requests=[]
try:
 s['production_before']=identity();save();src=archive_source('source-trace',SOURCE)
 helper=git('show',SOURCE+':docs/dev/evidence/20261028-query-plan-rollout/harness/diagnostic_history_batch.go.txt');(src/'core/state/snapshots/diagnostic_history_batch.go').write_bytes(helper)
 p=src/'core/state/snapshots/history_segment.go';b=p.read_text();old='\tout := make([]*rawdb.StateDomainChange, len(queries))';assert b.count(old)==1;b=b.replace(old,old+'\n trace := beginDiagnosticHistoryBatch(fromTxNum, toTxNum, queries)\n defer trace.finish(out)')
 old='for source, more := candidatePlan.next(active); more; source, more = candidatePlan.next(active) {';assert b.count(old)==1;b=b.replace(old,old+'\n trace.candidates++')
 old='\t\t\treads.submit(&historyReadAheadTask{keys: lookupKeys,';assert b.count(old)==1;b=b.replace(old,'\t\t\ttrace.submissions++; trace.keys += len(lookupKeys)\n'+old);p.write_text(b)
 command(['/data/go/bin/go','test','-tags=sapling','-c','./core','-o',str(R/'trace-http.test')],src,R/'build.log',300)
 E['GTRON_DIAGNOSTIC_HISTORY_BATCH']=str(R/'batches.jsonl')
 s['stage']='trace complete HTTP at 20M/9M/15M';save()
 input_=R.parent/'metadata-repair-ablation/input-candidate'
 with host('trace',R/'trace-http.test',input_) as ready:
  s['ready']=ready;save()
  for height,want in [(20000000,249),(20000000,249),(9000000,207),(15000000,238)]:
   q={'jsonrpc':'2.0','id':len(requests)+1,'method':'eth_call','params':[{'to':'0xa4997124a328a4615eab437cede31da47a31b375','data':'0x9fa6a6e3'},hex(height)]};start=time.perf_counter();req=urllib.request.Request(ready['url'],data=json.dumps(q).encode(),headers={'Content-Type':'application/json'})
   with urllib.request.urlopen(req,timeout=150) as f:body=json.loads(f.read())
   row={'height':height,'seconds_diagnostic_http':time.perf_counter()-start,'response':body};requests.append(row);(R/'requests.json').write_text(json.dumps(requests,indent=2));assert int(body['result'],16)==want;save()
 s['production_after']=identity();assert s['production_before']==s['production_after'];s.update(complete=True,stage='diagnostic complete',all_returns_match=True);save()
except BaseException:s.update(stage='failed',error=traceback.format_exc());save();raise
