import pathlib,json,collections,statistics,sys
p=pathlib.Path(sys.argv[1])
out={'scope':'same frozen V9 cohort and complete 11,818,192 capture; no live deployment'}
for label in ['corpus','complete-input']:
 f=p/(label+'-files.jsonl')
 if not f.exists():continue
 rows=[json.loads(l) for l in f.open()];s=collections.Counter();versions=collections.Counter();wins=equal=loss=0
 for r in rows:
  a=r['audit'];versions[a['Version']]+=1
  for k,v in a.items():
   if isinstance(v,int) and k!='Version':s[k]+=v
  diff=a['SourceBytes']-a['CandidateBytes'];wins+=diff>0;equal+=diff==0;loss+=diff<0
 assert loss==0
 out[label]={'files':len(rows),'smaller':wins,'unchanged':equal,'larger':loss,'formats':dict(versions),'bytes':dict(s),'net_saved_bytes':s['SourceBytes']-s['CandidateBytes'],'saved_percent':100*(s['SourceBytes']-s['CandidateBytes'])/s['SourceBytes']}
 assert s['SourceBytes']-s['CandidateBytes']==s['SourceKeyBytes']-s['CandidateKeyBytes']-s['OwnerDataBytes']-s['OwnerDirectoryBytes']-s['OwnerFooterBytes']
requests=p/'tail-read-comparison/requests.jsonl'
if requests.exists():
 rows=[json.loads(l) for l in requests.open()];assert all(r['correct'] for r in rows)
 groups=collections.defaultdict(list)
 for r in rows:
  if r['cache'] in ('warm','fresh-process-os-uncontrolled','private-os-cold-fresh-process'):groups[(r['cache'],r['tag'],r['mode'])].append(r['milliseconds'])
 table=[]
 for cache,tag in sorted({(a,b) for a,b,c in groups}):
  a=groups[cache,tag,'baseline'];b=groups[cache,tag,'candidate']
  if not a or not b:continue
  aa=statistics.median(a);bb=statistics.median(b)
  table.append({'cache':cache,'tag':tag,'baseline_n':len(a),'candidate_n':len(b),'baseline_median_ms':aa,'candidate_median_ms':bb,'percent_change':100*(bb-aa)/aa,'baseline_samples_ms':a,'candidate_samples_ms':b})
 out['http']={'all_responses_correct':True,'normal_responses':len(rows),'timings':table}
print(json.dumps(out,indent=2))
