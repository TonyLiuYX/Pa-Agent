"""Read-only cross-file audit; does not mark records reviewed or validate profit.
Optional --output writes a report; rebuild afterward to refresh the manifest.
"""
import argparse,collections,csv,hashlib,json,pathlib,re,sys,xml.etree.ElementTree as ET
from html.parser import HTMLParser
from urllib.parse import urlsplit,unquote
from build import ROOT,records,rows,visual_status,VERSION,DATE
from release_support import delivery_files,safe_relative
class HTML(HTMLParser):
 def __init__(self):super().__init__(convert_charrefs=True);self.links=[];self.images=[];self.ids=[];self.scripts=[];self.text=[]
 def handle_starttag(self,tag,attrs):
  a=dict(attrs)
  if 'id' in a:self.ids.append(a['id'])
  if tag=='a' and a.get('href'):self.links.append(a['href'])
  if tag=='img':self.images.append(a)
  if tag=='script' and a.get('src'):self.scripts.append(a['src'])
 def handle_data(self,data):self.text.append(data)
def main():
 ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('--require-jsonschema',action='store_true');ap.add_argument('--no-manifest',action='store_true');ap.add_argument('--output',type=pathlib.Path);args=ap.parse_args()
 errors=[];warnings=[];checks=[]
 sys.path.insert(0,str(ROOT))
 from recognition.validation import validate_specs
 recognition=validate_specs(require_jsonschema=args.require_jsonschema)
 errors.extend(recognition['errors'])
 if recognition['schema_checks']=='not_executed':warnings.append('Recognition JSON Schema not executed: jsonschema unavailable')
 checks.append('Recognition: 50 source-linked base graphs, 34 variant graphs, feature providers and generated catalog')
 def check(ok,message):
  if not ok:errors.append(message)
 def done(name):checks.append(name)
 try:
  xs=records();ss=rows('sources.jsonl');es=rows('evidence.jsonl');vs=rows('visuals.jsonl');rs=rows('relations.jsonl');candidates=rows('../research/candidates.jsonl');decisions=rows('../research/merge-decisions.jsonl');reviews=rows('../research/semantic-review.jsonl')
 except Exception as e:print('Load failure:',str(e));return 1
 groups={'setup':xs,'source':ss,'evidence':es,'visual':vs,'relation':rs};tables={k:{a['id']:a for a in v} for k,v in groups.items()}
 for k,a in groups.items():check(len(tables[k])==len(a),k+' duplicate ID')
 xd,sd,ed,vd=tables['setup'],tables['source'],tables['evidence'],tables['visual'];done('JSONL/front matter UTF-8 parsing and unique stable IDs')
 schema_checks='not_executed'
 try:
  from jsonschema import Draft202012Validator,FormatChecker
  for name,arr in groups.items():
   schema=json.loads((ROOT/'schema'/f'{name}.schema.json').read_text());Draft202012Validator.check_schema(schema);validator=Draft202012Validator(schema,format_checker=FormatChecker())
   for a in arr:
    for e in validator.iter_errors(a):errors.append(name+' '+a['id']+' / '+'.'.join(map(str,e.absolute_path))+': '+e.message)
  schema_checks='passed' if not errors else 'failed';done('Five Draft 2020-12 JSON Schemas with format checks')
 except ImportError:
  warnings.append('Full JSON Schema not executed: jsonschema unavailable; structural and reference checks still executed')
  if args.require_jsonschema:errors.append('jsonschema is required but unavailable')
 except Exception as e:errors.append('Schema exception: '+str(e))
 tax=json.loads((ROOT/'data/taxonomy.yaml').read_text());allowed={k:{v['id'] for v in d['values']} for k,d in tax['dimensions'].items()}
 fingerprints={};reviewed=[x for x in xs if x['status']=='reviewed']
 for x in xs:
  i=x['id'];secs=x['sections'];check(len(secs)==15,i+' must have 15 sections');check(all(v.strip() for v in secs.values()),i+' empty section');check([int(h.split('.')[0]) for h in secs]==list(range(1,16)),i+' section numbering/order')
  fp=x['logic_fingerprint'];check(len(fp)==7 and all(isinstance(v,str) and v.strip() for v in fp),i+' seven substantive fingerprint axes')
  key=tuple(fp);check(key not in fingerprints,i+' exact duplicate fingerprint with '+fingerprints.get(key,''));fingerprints[key]=i
  for k,values in allowed.items():
   check(k in x,i+' missing taxonomy '+k)
   actual=x.get(k,[]);actual=actual if isinstance(actual,list) else [actual];check(set(actual)<=values,i+' unknown taxonomy '+k)
  check(bool(x['source_ids']) and bool(x['evidence_ids']),i+' no sources/evidence')
  for sid in x['source_ids']:
   check(sid in sd,i+' dangling source '+sid);check(sd.get(sid,{}).get('actual_read_scope') not in [None,'not_read'],i+' cites unread source '+sid);check(not sd.get(sid,{}).get('requires_reread'),i+' source version changed and requires reread '+sid)
  for eid in x['evidence_ids']:check(eid in ed and ed[eid]['setup_id']==i,i+' evidence ownership '+eid)
  for vid in x['visual_ids']:check(vid in vd and i in vd[vid]['setup_ids'],i+' visual ownership '+vid)
  near=re.findall(r'\[(SET-\d{4})\]\(',secs.get('11. 与相近 setups 的区别',''));check(1<=len(set(near))<=3,i+' nearest comparison must have 1–3 targets')
  for q in near:check(q in xd and q!=i,i+' invalid nearest '+q)
  cited=re.findall(r'\bEVD-\d{6}\b',x['body_markdown']);check(set(x['evidence_ids'])==set(cited),i+' metadata/body evidence mismatch')
  citedv=re.findall(r'\bVIS-\d{6}\b',secs.get('12. 图表与案例解读',''));check(set(x['visual_ids'])==set(citedv),i+' metadata/body visual mismatch')
  check(not re.search(r'相近记录将在去重审查时补充|TODO|待填写|lorem ipsum',x['body_markdown'],re.I),i+' placeholder in reviewed body')
  if x.get('trigger_timeframe') is None:check(bool(x['unknown_fields'].get('trigger_timeframe')),i+' unknown trigger timeframe has no reason')
  supports=[e['source_id'] for e in es if e['setup_id']==i and e['relationship']=='supports'];check(bool(supports),i+' has no core supporting evidence');check(set(supports)==set(x['source_coverage']['core_support_source_ids']),i+' core source coverage mismatch')
  if x['status']=='reviewed':check(bool(x['reviewed_at']) and any(r['setup_id']==i and r['decision']=='retain_reviewed' for r in reviews),i+' missing semantic review trail')
 check(len(reviewed)>=50,'Fewer than 50 reviewed independent IDs');done('15 sections, seven axes, nearest comparisons, taxonomy, review trail and body references')
 for e in es:
  check(e['setup_id'] in xd and e['source_id'] in sd,e['id']+' dangling evidence reference')
  if e['setup_id'] in xd:check(e['id'] in xd[e['setup_id']]['evidence_ids'] and e['source_id'] in xd[e['setup_id']]['source_ids'],e['id']+' not cited by its setup')
  if e['claim_type']=='editor_operationalization':check(e['verification']=='editor_proposal_not_tested' and e['relationship']=='operationalizes',e['id']+' mislabels editor rule')
  else:check(e['verification']=='text_checked',e['id']+' unchecked source claim')
 for s in ss:
  for k in ['url','canonical_url']:check(urlsplit(s[k]).scheme in ['http','https'] and bool(urlsplit(s[k]).netloc),s['id']+' malformed URL')
  if s['actual_read_scope']!='not_read':check(bool(s['read_locator']) and bool(s['summary']),s['id']+' missing actual read record')
 for v in vs:
  for sid in v['setup_ids']:check(sid in xd and v['id'] in xd[sid]['visual_ids'],v['id']+' dangling reverse visual link')
  if v['source_id'] is not None:check(v['source_id'] in sd,v['id']+' missing visual source')
  check(bool(v['alt']) and bool(v['rights']) and bool(v['locator']),v['id']+' missing visual context')
  if v['material_type']=='real_source_chart':check(v['verification'].startswith('chart_viewed') and v['url'] is not None and v['local_path'] is None,v['id']+' source chart verification/delivery mismatch')
  if v['material_type']=='synthetic_structure_diagram':check(v['verification']=='generated_not_market_evidence' and v['source_id'] is None and v['case_scope']=='illustration_only',v['id']+' illustration misrepresented')
  if any(v[k] is None for k in ['symbol','market_date','timeframe','timezone']):check(bool(v['missing']),v['id']+' missing values have no explanation')
  if v['local_path']:
   p=(ROOT/v['local_path']).resolve();check(p.is_relative_to(ROOT) and p.exists(),v['id']+' invalid local asset')
   if p.exists():check(hashlib.sha256(p.read_bytes()).hexdigest()==v['sha256'],v['id']+' asset hash mismatch');ET.parse(p)
 done('Evidence/source ownership, read scopes, editor-rule attribution, visual rights/types and SVG assets')
 pairs=set()
 for r in rs:
  check(r['from'] in xd and r['to'] in xd and r['from']!=r['to'],r['id']+' dangling/self relation');key=(tuple(sorted([r['from'],r['to']])),r['type']);check(key not in pairs,r['id']+' duplicate relation');pairs.add(key)
 for c in candidates:check(c.get('target') is None or c['target'] in xd,c['id']+' dangling candidate target')
 for d in decisions:check(any(c['id']==d['candidate'] for c in candidates),d['id']+' missing candidate');check(d.get('target') is None or d['target'] in xd,d['id']+' dangling merge target');check(bool(d['reason']) and bool(d['source_ids']),d['id']+' missing dedup evidence')
 done('Relations and candidate/merge targets')
 exported=json.loads((ROOT/'exports/setups.json').read_text());check(exported==xs,'Full JSON is stale or differs from Markdown')
 with (ROOT/'exports/setups.csv').open(encoding='utf-8-sig',newline='') as f:csvrows=list(csv.DictReader(f))
 check(len(csvrows)==len(xs),'CSV row count mismatch');check({a['id'] for a in csvrows}==set(xd),'CSV ID mismatch')
 for a in csvrows:
  check(a['name_zh']==xd[a['id']]['name_zh'],'CSV Chinese name mismatch '+a['id'])
  for j,v in enumerate(xd[a['id']]['logic_fingerprint'],1):check(a['fingerprint_'+str(j)]==v,'CSV fingerprint mismatch '+a['id'])
 done('Generated full JSON and UTF-8 CSV consistency')
 pages={}
 for p in sorted((ROOT/'site').rglob('*.html')):
  parser=HTML();parser.feed(p.read_text());pages[p.resolve()]=parser;check(len(parser.ids)==len(set(parser.ids)),str(p.relative_to(ROOT))+' duplicate HTML anchor');check(not parser.scripts,str(p.relative_to(ROOT))+' external scripts')
 for p,parser in pages.items():
  for u in parser.links:
   url=urlsplit(u)
   if url.scheme:check(url.scheme in ['http','https'],str(p.relative_to(ROOT))+' unsafe link scheme');continue
   target=(p.parent/unquote(url.path)).resolve() if url.path else p;check(target.is_relative_to(ROOT) and target.exists(),str(p.relative_to(ROOT))+' broken link '+u)
   if url.fragment and target in pages:check(url.fragment in pages[target].ids,str(p.relative_to(ROOT))+' missing fragment '+u)
  for im in parser.images:
   u=im.get('src','');check(bool(im.get('alt')),str(p.relative_to(ROOT))+' empty image alt');check(not urlsplit(u).scheme and (p.parent/unquote(u)).resolve().exists(),str(p.relative_to(ROOT))+' broken/nonlocal image '+u)
 for x in xs:
  p=(ROOT/'site/setups'/f'{x["id"]}.html').resolve();check(p in pages,x['id']+' missing HTML page')
  if p in pages:check(all('sec'+str(i) in pages[p].ids for i in range(15)),x['id']+' missing rendered chapter anchors')
 home=(ROOT/'site/index.html').read_text();m=re.search(r'<script id="dataset" type="application/json">(.*?)</script>',home,re.S);embedded=json.loads(m[1]) if m else [];check(len(embedded)==len(xs),'Homepage data missing')
 for x in embedded:
  base={k:v for k,v in x.items() if k!='visual_status'};check(base==xd[x['id']],'Homepage data stale '+x['id'])
 done('All local HTML links, anchors, chapter pages, image paths and embedded offline data')
 if not args.no_manifest:
  manifest=json.loads((ROOT/'manifest.json').read_text());listed=set()
  for f in manifest['files']:
   check(safe_relative(f['path']),f['path']+' unsafe manifest path')
   p=(ROOT/f['path']).resolve();listed.add(f['path']);check(p.is_relative_to(ROOT) and p.is_file(),f['path']+' missing/unsafe manifest file')
   if p.is_relative_to(ROOT) and p.is_file():check(hashlib.sha256(p.read_bytes()).hexdigest()==f['sha256'] and p.stat().st_size==f['bytes'],f['path']+' manifest hash/size mismatch')
  actual={p.relative_to(ROOT).as_posix() for p in delivery_files(ROOT)};check(listed==actual,'Manifest allowlisted file set mismatch')
  check(len(listed)==len(manifest['files']),'Manifest contains duplicate file entries')
  check(manifest['version']==VERSION and manifest['date']==DATE,'Manifest release metadata stale')
  check(manifest['counts']==dict(collections.Counter(x['status'] for x in xs)),'Manifest status counts stale');check(manifest['evidence_count']==len(es) and manifest['visual_count']==len(vs),'Manifest database counts stale');check(manifest['real_chart_record_coverage']==sum(visual_status(x,vd) for x in xs),'Manifest visual coverage stale');done('Manifest exact file set, SHA-256, sizes and count consistency')
 result={'date':DATE,'version':VERSION,'status':'passed' if not errors else 'failed','reviewed_count':len(reviewed),'schema_checks':schema_checks,'recognition':recognition,'checks':checks,'errors':errors,'warnings':warnings,'counts':{k:len(v) for k,v in groups.items()},'scope_note':'Structural checks do not replace semantic review or demonstrate profitability.'}
 if args.output:args.output.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
 print(json.dumps(result,ensure_ascii=False,indent=2));return 1 if errors else 0
if __name__=='__main__':sys.exit(main())
