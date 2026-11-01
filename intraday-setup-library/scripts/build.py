"""Deterministic offline build; Markdown is the sole setup body source.
Run python3 scripts/build.py from any cwd. Python standard library only.
"""
import argparse,collections,csv,hashlib,html,json,pathlib,posixpath,re,shutil,subprocess,sys,xml.etree.ElementTree as ET
from urllib.parse import urlsplit
ROOT=pathlib.Path(__file__).resolve().parents[1]
from release_support import delivery_files,release_metadata,GENERATED_FILES
RELEASE=release_metadata(ROOT)
VERSION=RELEASE['version']
SCHEMA_VERSION=RELEASE['schema_version']
DATE=RELEASE['content_date']
DOCS={'indexes/learning-paths.md':'learning.html','indexes/glossary.md':'glossary.html','indexes/taxonomy.md':'taxonomy.html','indexes/comparisons.md':'comparisons.html','indexes/aliases.md':'aliases.html','indexes/catalog.md':'index.html','reports/quality-audit.md':'quality.html','reports/research-report.md':'research.html','reports/verification.md':'verification.html','reports/verification-v1.2.0.md':'verification-v1.2.0.html','reports/recognition-fixes.md':'recognition-fixes.html','README.md':'readme.html','recognition/README.md':'recognition.html','recognition/catalog.md':'recognition-catalog.html','evaluation/README.md':'evaluation.html','runtime/README.md':'runtime.html','workbench/README.md':'workbench.html','connectors/README.md':'connectors.html','paper/README.md':'paper.html','reports/release-notes.md':'release-notes.html','runtime/SOAK.md':'soak.html','reports/baseline.md':'baseline.html','reports/implementation-acceptance.md':'implementation-acceptance.html','schema/runtime-contracts.md':'runtime-contracts.html','schema/data-dictionary.md':'data-dictionary.html','research/handoff.md':'handoff.html','research/handoff-v1.2.0.md':'handoff-v1.2.0.html'}
def rows(name):
 p=ROOT/'data'/name
 return [json.loads(x) for x in p.read_text(encoding='utf-8').splitlines() if x.strip()] if p.exists() else []
def records():
 out=[]
 for p in sorted((ROOT/'setups').glob('SET-*.md')):
  raw=p.read_text(encoding='utf-8');_,front,body=raw.split('---',2);meta=json.loads(front)
  sections={m[1]:m[2].strip() for m in re.finditer(r'^## ([^\n]+)\n(.*?)(?=^## |\Z)',body,re.M|re.S)}
  out.append(dict(meta,body_markdown=body.strip(),sections=sections,file_path=p.relative_to(ROOT).as_posix(),source_sha256=hashlib.sha256(raw.encode()).hexdigest()))
 return out
def local_url(url,doc,output):
 parsed=urlsplit(url)
 if parsed.scheme:return url if parsed.scheme in ['http','https'] else None
 if url.startswith('//') or parsed.netloc:return None
 if url.startswith('#'):return url
 path=posixpath.normpath(posixpath.join(posixpath.dirname(doc),parsed.path))
 if path=='..' or path.startswith('../') or path.startswith('/'):return None
 if path.startswith('setups/') and path.endswith('.md'):dest='site/setups/'+pathlib.PurePosixPath(path).stem+'.html'
 elif path in DOCS:dest='site/'+DOCS[path]
 else:dest=path
 result=posixpath.relpath(dest,posixpath.dirname(output))
 if parsed.fragment:result+='#'+parsed.fragment
 return result
def inline(s,doc='README.md',output='site/readme.html'):
 s=html.escape(s);saved=[]
 def token(value):saved.append(value);return '\x00'+str(len(saved)-1)+'\x00'
 def image(m):
  alt,url=m[1],html.unescape(m[2]);u=local_url(url,doc,output)
  if not u or urlsplit(u).scheme:return token('<span>'+alt+'（外部图仅见来源链接）</span>')
  size='';asset=(ROOT/output).parent/u
  if asset.suffix=='.svg' and asset.exists():
   attrs=ET.parse(asset).getroot().attrib
   if all(re.fullmatch(r'[0-9]+',attrs.get(k,'')) for k in ['width','height']):size=' width="'+attrs['width']+'" height="'+attrs['height']+'"'
  return token('<img loading="lazy"'+size+' src="'+html.escape(u,quote=True)+'" alt="'+alt+'">')
 def link(m):
  label,url=m[1],html.unescape(m[2]);u=local_url(url,doc,output)
  if not u:return token(label)
  return token('<a href="'+html.escape(u,quote=True)+'"'+(' rel="noopener noreferrer"' if u.startswith('http') else '')+'>'+label+'</a>')
 s=re.sub(r'!\[([^\]]*)\]\(([^)]+)\)',image,s)
 s=re.sub(r'\[([^\]]+)\]\(([^)]+)\)',link,s)
 s=re.sub(r'\*\*(.+?)\*\*',r'<strong>\1</strong>',s)
 s=re.sub(r'`([^`]+)`',r'<code>\1</code>',s)
 return re.sub(r'\x00(\d+)\x00',lambda m:saved[int(m[1])],s)
def render(md,doc='README.md',output='site/readme.html'):
 lines=md.splitlines();parts=[];para=[];lis=[];i=0
 def fmt(s):return inline(s,doc,output)
 def flush():
  if para:parts.append('<p>'+fmt('\n'.join(para)).replace('\n','<br>')+'</p>');para.clear()
  if lis:parts.append('<ul>'+''.join('<li>'+fmt(x)+'</li>' for x in lis)+'</ul>');lis.clear()
 while i<len(lines):
  line=lines[i]
  if line.startswith('```'):
   flush();code=[];i+=1
   while i<len(lines) and not lines[i].startswith('```'):code.append(lines[i]);i+=1
   parts.append('<pre><code>'+html.escape('\n'.join(code))+'</code></pre>')
  elif line.startswith('|') and i+1<len(lines) and re.fullmatch(r'[\s|:\-]+',lines[i+1]):
   flush();headers=[x.strip() for x in line.strip().strip('|').split('|')];i+=2;body=[]
   while i<len(lines) and lines[i].startswith('|'):
    cells=[x.strip() for x in lines[i].strip().strip('|').split('|')];body.append('<tr>'+''.join('<td>'+fmt(x)+'</td>' for x in cells)+'</tr>');i+=1
   parts.append('<div class="table-scroll"><table><thead><tr>'+''.join('<th>'+fmt(x)+'</th>' for x in headers)+'</tr></thead><tbody>'+''.join(body)+'</tbody></table></div>');continue
  elif re.match(r'^#{1,6} ',line):
   flush();m=re.match(r'(#{1,6}) (.*)',line);parts.append(f'<h{len(m[1])}>'+fmt(m[2])+f'</h{len(m[1])}>')
  elif line.startswith('- '):
   if para:flush()
   lis.append(line[2:].replace('[ ]','☐').replace('[x]','☑'))
  elif not line.strip():flush()
  elif line.startswith('> '):flush();parts.append('<blockquote>'+fmt(line[2:])+'</blockquote>')
  else:
   if lis:flush()
   para.append(line)
  i+=1
 flush();return '\n'.join(parts)
CSS='''*{box-sizing:border-box}body{margin:0;background:#f5f3ec;color:#24332f;font:16px/1.75 system-ui,-apple-system,"PingFang SC","Microsoft YaHei",sans-serif}header{background:#132e2a;color:#f7f3e8;padding:22px 5vw;display:flex;flex-wrap:wrap;gap:8px 20px}header a{color:#d6e8d6}main{max-width:1160px;margin:32px auto;padding:0 28px}h1{font-size:32px;line-height:1.35;font-weight:650}h2{margin-top:2em;font-size:22px;border-top:1px solid #d1d8cf;padding-top:1em}h3{font-size:18px}a{color:#19605c;overflow-wrap:anywhere}p,li,td,th{overflow-wrap:anywhere}.lead{max-width:860px}.muted{color:#62736a;font-size:14px}.tag{display:inline-block;background:#e4e9dc;border:1px solid #ced9c8;border-radius:4px;font-size:12px;padding:1px 7px;margin:2px}input,select,button{font:inherit;border:1px solid #aabfb0;border-radius:5px;background:#fff;padding:9px 12px}button{cursor:pointer}button:hover{background:#e4eddf}input[type=search]{width:100%}.filters{display:flex;flex-wrap:wrap;gap:8px;margin:15px 0}select{max-width:220px;font-size:14px}.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(min(300px,100%),1fr));gap:15px}.card{background:#fffef9;border:1px solid #d4ddd0;padding:20px;border-radius:8px}.card h3{margin:4px 0}.card p{font-size:14px}.notice{background:#e7ede1;border-left:4px solid #6e9278;padding:12px 16px}.toc{display:flex;flex-wrap:wrap;gap:6px 16px;font-size:13px}.toc a{max-width:290px}.meta{display:grid;grid-template-columns:repeat(2,1fr);gap:8px;font-size:14px;background:#e9ede2;padding:16px}.compare,.table-scroll{overflow-x:auto;margin:20px 0}table{width:100%;border-collapse:collapse}td,th{vertical-align:top;text-align:left;border-bottom:1px solid #ccd8ce;padding:10px;font-size:14px}.compare td{min-width:230px}.compare th:first-child{min-width:95px}.compare ol{padding-left:20px;margin:0}img{max-width:100%;height:auto;display:block;margin:14px auto;border:1px solid #d5ddcf;border-radius:6px}code{font-size:.9em;background:#e3e9df;padding:2px 5px}pre{padding:15px;background:#e3e9df;overflow:auto}pre code{padding:0}blockquote{border-left:3px solid #8ea48f;padding-left:14px}.actions{display:flex;gap:10px;flex-wrap:wrap}footer{margin:50px 0;font-size:13px;color:#64796c}@media(max-width:650px){main{padding:0 16px}h1{font-size:25px}.meta{grid-template-columns:1fr}select{max-width:100%}header{font-size:14px}}'''
def page(title,body,depth=0):
 pre='../'*depth;nav=[('index.html','INTRADAY / 日内 Setup'),('sources.html','来源'),('taxonomy.html','分类'),('comparisons.html','比较'),('learning.html','学习路线'),('glossary.html','术语'),('quality.html','质量报告'),('recognition.html','程序化识别')]
 header=''.join('<a href="'+pre+url+'">'+label+'</a>' for url,label in nav)
 return '<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>'+html.escape(title)+'</title><style>'+CSS+'</style></head><body><header>'+header+'</header><main>'+body+'<footer>Markdown 为正文主记录 · reviewed 表示规则记录审查 · 来源及真实图表外链需要联网。正文、原创示意、检索和比较可离线使用。<br><a href="'+pre+'readme.html">使用与维护</a> · <a href="'+pre+'research.html">研究报告</a></footer></main></body></html>'
def write(path,text):
 p=ROOT/path;p.parent.mkdir(parents=True,exist_ok=True);p.write_text(text,encoding='utf-8')
def visual_status(x,vd,strict=False):
 return any(vd[i]['material_type']=='real_source_chart' and vd[i]['verification'].startswith('chart_viewed') and (not strict or vd[i]['case_scope'] not in ['context_only','cross_session_context_only','related_countertrend_context_only']) for i in x['visual_ids'])
def main():
 ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('--output',type=pathlib.Path,help='build a complete clean distribution in a new or empty directory');ap.add_argument('--clean',action='store_true',help='remove generated artifacts before rebuilding');args=ap.parse_args()
 delivery_files(ROOT,include_generated=False) # reject source symlinks before reading or writing artifacts
 if args.output:
  target=args.output.expanduser().resolve()
  if target==ROOT or target.is_relative_to(ROOT):raise ValueError('isolated output must be outside the source project')
  if target.exists() and (not target.is_dir() or any(target.iterdir())):raise ValueError('isolated output must be a new or empty directory')
  target.mkdir(parents=True,exist_ok=True)
  for source in delivery_files(ROOT,include_generated=False):
   destination=target/source.relative_to(ROOT);destination.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(source,destination)
  return subprocess.call([sys.executable,str(target/'scripts/build.py'),'--clean'])
 if args.clean:
  for directory in ('site','exports'):
   path=ROOT/directory
   if path.is_symlink():raise ValueError('generated directory must not be symlink')
   if path.exists():shutil.rmtree(path)
  for rel in GENERATED_FILES:
   path=ROOT/rel
   if path.exists():path.unlink()
 (ROOT/'exports').mkdir(parents=True,exist_ok=True)
 sys.path.insert(0,str(ROOT))
 from recognition.specs import catalog_document,catalog_markdown
 rec=catalog_document();write('recognition/catalog.json',json.dumps(rec,ensure_ascii=False,indent=2)+'\n');write('recognition/catalog.md',catalog_markdown(rec))
 xs=records();ss=rows('sources.jsonl');ev=rows('evidence.jsonl');vs=rows('visuals.jsonl');rs=rows('relations.jsonl');sd={s['id']:s for s in ss};vd={v['id']:v for v in vs};xd={x['id']:x for x in xs}
 tax=json.loads((ROOT/'data/taxonomy.yaml').read_text());labels={d:{v['id']:v['label'] for v in q['values']} for d,q in tax['dimensions'].items()}
 def val(x):return html.escape(str(x))
 write('exports/setups.json',json.dumps(xs,ensure_ascii=False,indent=2)+'\n')
 fields=['id','version','name_zh','name_en','aliases','method_families','mechanism','structure_locations','environment_tags','market_types','markets_observed','markets_context_only','session','session_tags','trigger_timeframe','data_requirements','execution_mode','pa_role','status','independent_corroboration','profit_evidence','source_ids','evidence_ids','visual_ids','visual_status','file_path']+['fingerprint_'+str(i) for i in range(1,8)]
 with (ROOT/'exports/setups.csv').open('w',newline='',encoding='utf-8-sig') as f:
  w=csv.DictWriter(f,fieldnames=fields);w.writeheader()
  for x in xs:
   y={k:x.get(k) for k in fields};y['visual_status']='case_viewed' if visual_status(x,vd,True) else 'context_viewed' if visual_status(x,vd) else 'gap'
   for j,v in enumerate(x['logic_fingerprint'],1):y['fingerprint_'+str(j)]=v
   for k,v in y.items():
    if isinstance(v,list):v=' | '.join(v)
    if isinstance(v,str) and v.startswith(('=','+','-','@','\t','\r')):v="'"+v
    y[k]=v
   w.writerow(y)
 cat='# 条目目录\n\n仅 reviewed 独立ID计入合格数量；多个标签不重复计数。\n\n'+'\n'.join(f'- [{x["id"]} · {x["name_zh"]} / {x["name_en"]}](../{x["file_path"]}) — {x["status"]} · PA:{x["pa_role"]} · {x["independent_corroboration"]}' for x in xs)+'\n';write('indexes/catalog.md',cat)
 taxonomy='# 多轴分类索引\n\n分类来自主metadata和taxonomy值表；各轴和多标签重叠，独立数量按reviewed ID。首页可交叉筛选，此处供Markdown阅读。\n\n'
 for k,d in tax['dimensions'].items():
  taxonomy+='## '+d['label']+' / '+k+'\n\n| 分类值 | 独立条目数 | 条目 |\n|---|---|---|\n'
  for q in d['values']:
   items=[x for x in xs if q['id'] in (x[k] if isinstance(x[k],list) else [x[k]])]
   taxonomy+='| '+q['label']+' | '+str(len(items))+' | '+ '、'.join('['+x['id']+'](../'+x['file_path']+')' for x in items)+' |\n'
  taxonomy+='\n'
 write('indexes/taxonomy.md',taxonomy)
 aliases='# 别名与合并去向\n\n名称和多空镜像不另计数。相关位置变体的重定向不表示全部执行规则完全相同；以目标条目第9、13节和去重记录为准。\n\n'
 for x in xs:aliases+='- '+', '.join(x['aliases'] or [x['name_en']])+f' → [{x["id"]}](../{x["file_path"]})\n'
 for q in rows('../research/merge-decisions.jsonl'):
  if q['decision']=='merge':aliases+=f'- {q["candidate_name"]} → [{q["target"]}](../setups/{q["target"]}.md)；{q["reason"]}\n'
 write('indexes/aliases.md',aliases)
 comparisons='# 入场前的结构比较\n\n比较前提、位置和行为顺序，不根据后来盈亏区分。首页可选择1–3条逐轴比较。\n\n'
 guide=ROOT/'indexes/comparison-guide.md'
 if guide.exists():comparisons+=guide.read_text()+'\n\n'
 for r in rs:
  if r['type']=='contrasts_with':comparisons+=f'## {r["from"]} ↔ {r["to"]}\n\n[{r["from"]} · {xd[r["from"]]["name_zh"]}](../setups/{r["from"]}.md) / [{r["to"]} · {xd[r["to"]]["name_zh"]}](../setups/{r["to"]}.md)：{r["reason"]}\n\n'
 write('indexes/comparisons.md',comparisons)
 source_index='# 来源反向索引\n\n实际阅读和下载分别计数，同族内容不自动构成独立核验。\n\n'
 for s in ss:
  used=[x['id'] for x in xs if s['id'] in x['source_ids']]
  source_index+=f'- [{s["id"]} · {s.get("title") or "标题未核实"}]({s["url"]}) — {s["source_family"]} / {s["actual_read_scope"]}；引用：'+(', '.join(f'[{i}](../setups/{i}.md)' for i in used) or '无最终条目引用')+'\n'
 write('indexes/sources.md',source_index)
 for doc,out in DOCS.items():
  if out=='index.html':continue
  p=ROOT/doc
  if p.exists():write('site/'+out,page(p.read_text().splitlines()[0].lstrip('# '),render(p.read_text(),doc,'site/'+out)))
 for x in xs:
  out='site/setups/'+x['id']+'.html';toc='<nav class="toc" aria-label="章节">'+''.join('<a href="#sec'+str(i)+'">'+val(h)+'</a>' for i,h in enumerate(x['sections']))+'</nav>'
  cards='<h1>'+val(x['name_zh'])+'</h1><p>'+val(x['name_en'])+' · '+x['id']+' · '+x['status']+' · v'+x['version']+'</p><div class="meta"><div>PA：'+x['pa_role']+'<br>市场：'+val(' / '.join(x['markets_observed']))+'</div><div>数据：'+val(' / '.join(x['data_requirements']))+'<br>独立佐证：'+x['independent_corroboration']+'</div></div><p class="notice">盈利证据：'+val(x['profit_evidence'])+'。审查范围与来源版本见第13节；未知字段保留缺失原因。</p>'+toc
  for i,(h,md) in enumerate(x['sections'].items()):
   cards+='<section id="sec'+str(i)+'"><h2>'+val(h)+'</h2>'+render(md,x['file_path'],out)
   if i==11:
    cards+='<details><summary>视觉材料元数据与查看范围</summary><ul>'
    for vid in x['visual_ids']:
     v=vd[vid];cards+='<li>'+vid+' · '+val(v['material_type'])+' / '+val(v['case_scope'])+' / '+val(v['verification'])+'<br>品种：'+val(v['symbol'] or '未知/不适用')+'；日期：'+val(v['market_date'] or '未知/不适用')+'；周期：'+val(v['timeframe'] or '未知/不适用')+'；时区：'+val(v['timezone'] or '未知/不适用')+'<br>'+val('；'.join(v['missing']))+'</li>'
    cards+='</ul></details>'
   cards+='</section>'
  linked=[]
  for r in rs:
   if x['id'] in [r['from'],r['to']]:
    other=r['to'] if r['from']==x['id'] else r['from'];linked.append('<li><a href="'+other+'.html">'+other+' '+val(xd[other]['name_zh'])+'</a> '+val(r['reason'])+'</li>')
  cards+='<h2>双向相关导航</h2><ul>'+''.join(linked)+'</ul><p><a href="../../'+x['file_path']+'">Markdown 主记录</a></p><p>本条来源：'+' · '.join('<a href="../source/'+sid+'.html">'+sid+'</a>' for sid in x['source_ids'])+'</p>'
  write(out,page(x['name_zh'],cards,1))
 sourcepage='<h1>来源与反向索引</h1><p>获取、实际阅读、正文规则与图表查看分别标记；同一作者、同机构和转载属于同一来源族。</p>'
 for s in ss:
  used=[x for x in xs if s['id'] in x['source_ids']];sourcepage+='<p><a href="source/'+s['id']+'.html">'+s['id']+' · '+val(s.get('title') or '标题未核实')+'</a> · '+val(s['source_family'])+' · '+val(s['actual_read_scope'])+f' · {len(used)}条引用</p>'
  b='<h1>'+val(s.get('title') or '访问障碍／标题未核实')+'</h1><p>'+val(s['author'])+' · '+val(s['source_family'])+'</p><p><a href="'+html.escape(s['url'],quote=True)+'" rel="noopener noreferrer">原始公开来源</a></p>'
  for k,label in [('source_type','资料类型'),('published','发布日期'),('accessed','检查日期'),('link_state','本次访问情况'),('actual_read_scope','实际阅读范围'),('read_locator','阅读定位'),('summary','研究摘要'),('limitations','限制'),('commercial_interest','潜在利益关联')]:
   v=s.get(k);b+='<p><strong>'+label+'</strong>：'+val('；'.join(v) if isinstance(v,list) else v if v is not None else '未知/未核实')+'</p>'
  b+='<h2>反向引用条目</h2>'
  for x in used:b+='<p><a href="../setups/'+x['id']+'.html">'+x['id']+' '+val(x['name_zh'])+'</a></p>'
  if not used:b+='<p>无最终条目引用；保留为发现/访问日志。</p>'
  b+='<h2>具体证据及支持范围</h2>'
  for e in ev:
   if e['source_id']==s['id']:b+='<p id="'+e['id']+'">'+e['id']+' · '+val(e['locator'])+'<br>'+val(e['claim'])+'<br>主张类型：'+val(e['claim_type'])+'；关系：'+val(e['relationship'])+'；'+val(e['notes'])+'</p>'
  write('site/source/'+s['id']+'.html',page(s.get('title') or s['id'],b,1))
 write('site/sources.html',page('来源索引',sourcepage))
 data=[dict(x,visual_status='case_viewed' if visual_status(x,vd,True) else 'context_viewed' if visual_status(x,vd) else 'gap') for x in xs]
 blob=json.dumps(data,ensure_ascii=False).replace('</','<\\/');labelblob=json.dumps(labels,ensure_ascii=False).replace('</','<\\/')
 home='<h1>日内交易 Setup 知识库</h1><p class="lead">先识别环境、位置、行为顺序、触发与失效，再比较方法。搜索支持中英文名称、别名与完整正文；多个筛选维度取交集。</p><p class="notice">本版 '+str(sum(x['status']=='reviewed' for x in xs))+' 个 reviewed 独立条目。reviewed 表示规则记录审查，盈利状态单独标记。真实图表以原站外链交付，本地原创示意清楚标明非市场数据。</p><label for="search">全文搜索</label><input id="search" type="search" placeholder="例如：吸收、failed breakout、H2、VWAP"><div id="filters" class="filters"></div><p id="count" class="muted" aria-live="polite"></p><div class="actions"><button id="compareBtn">比较已选条目</button><button id="clearCompare">清空选择</button><button id="resetFilters">重置搜索与筛选</button></div><p id="compareStatus" class="muted" aria-live="polite">已选 0 / 3</p><div id="compare" class="compare"></div><div id="cards" class="grid"></div><script id="dataset" type="application/json">'+blob+'</script><script id="labels" type="application/json">'+labelblob+'</script>'
 js=r'''<script>
const data=JSON.parse(document.getElementById('dataset').textContent),labels=JSON.parse(document.getElementById('labels').textContent),selected=new Set();
const dims={method_families:'方法家族',structure_locations:'位置',environment_tags:'环境',market_types:'市场类别',session_tags:'时段',pa_role:'PA作用',data_requirements:'数据',execution_mode:'执行',status:'状态',visual_status:'视觉'};
const visualLabels={case_viewed:'行情图已看（含模型示例）',context_viewed:'仅背景行情图已看',gap:'真实案例缺口'};
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
Object.entries(dims).forEach(([key,label])=>{const sel=document.createElement('select');sel.id=key;sel.setAttribute('aria-label',label);sel.innerHTML='<option value="">'+label+'：全部</option>'+[...new Set(data.flatMap(x=>Array.isArray(x[key])?x[key]:[x[key]]))].sort().map(v=>'<option value="'+esc(v)+'">'+esc(key==='visual_status'?visualLabels[v]:labels[key]?.[v]??v)+'</option>').join('');sel.addEventListener('change',draw);document.getElementById('filters').append(sel)});
function status(note=''){document.getElementById('compareStatus').textContent='已选 '+selected.size+' / 3'+(note?' · '+note:'')}
function draw(){const tokens=document.getElementById('search').value.toLowerCase().trim().split(/\s+/).filter(Boolean);const items=data.filter(x=>tokens.every(t=>(x.id+' '+x.name_zh+' '+x.name_en+' '+x.aliases.join(' ')+' '+x.body_markdown).toLowerCase().includes(t))&&Object.keys(dims).every(k=>{const v=document.getElementById(k).value;return !v||(Array.isArray(x[k])?x[k].includes(v):x[k]===v)}));document.getElementById('count').textContent='显示 '+items.length+' / '+data.length+' 个独立主记录';document.getElementById('cards').innerHTML=items.map(x=>'<article class="card"><label><input type="checkbox" data-id="'+x.id+'" '+(selected.has(x.id)?'checked':'')+'> 比较</label><p class="muted">'+x.id+' · '+esc(x.status)+'</p><h3><a href="setups/'+x.id+'.html">'+esc(x.name_zh)+'</a></h3><div class="muted">'+esc(x.name_en)+'</div><p>'+esc(x.summary)+'</p><span class="tag">'+esc(labels.method_families?.[x.method_families[0]]??x.mechanism)+'</span><span class="tag">PA: '+esc(x.pa_role)+'</span><span class="tag">'+esc(visualLabels[x.visual_status])+'</span></article>').join('');document.querySelectorAll('[data-id]').forEach(c=>c.addEventListener('change',()=>{if(c.checked){if(selected.size>=3){c.checked=false;status('最多比较3条，请先取消一个');return}selected.add(c.dataset.id)}else selected.delete(c.dataset.id);status()}));status()}
document.getElementById('search').addEventListener('input',draw);
document.getElementById('resetFilters').addEventListener('click',()=>{document.getElementById('search').value='';Object.keys(dims).forEach(k=>document.getElementById(k).value='');draw()});
document.getElementById('clearCompare').addEventListener('click',()=>{selected.clear();document.getElementById('compare').innerHTML='';draw()});
document.getElementById('compareBtn').addEventListener('click',()=>{const xs=data.filter(x=>selected.has(x.id));const fields=[['PA作用','pa_role'],['来源佐证','independent_corroboration'],['必要数据','data_requirements'],['七轴指纹','logic_fingerprint'],['入场','6. Entry trigger 与执行方式'],['失效','7. Invalidation 与止损逻辑'],['管理','8. Exit、target 与 trade management']];document.getElementById('compare').innerHTML=xs.length?'<table><thead><tr><th>观察维度</th>'+xs.map(x=>'<th><a href="setups/'+x.id+'.html">'+esc(x.id+' '+x.name_zh)+'</a></th>').join('')+'</tr></thead><tbody>'+fields.map(([label,key])=>'<tr><th>'+esc(label)+'</th>'+xs.map(x=>{const v=x[key]??x.sections[key]??'';return '<td>'+(Array.isArray(v)?'<ol>'+v.map(z=>'<li>'+esc(z)+'</li>').join('')+'</ol>':esc(v).replaceAll('\n','<br>'))+'</td>'}).join('')+'</tr>').join('')+'</tbody></table>':'<p>先选择1–3个条目。</p>'});draw();
</script>'''
 write('site/index.html',page('日内交易 Setup 知识库',home+js))
 counts=dict(collections.Counter(x['status'] for x in xs));read=[s for s in ss if s['actual_read_scope']!='not_read'];used=set(q for x in xs for q in x['source_ids'])
 manifest=dict(schema_version=SCHEMA_VERSION,version=VERSION,date=DATE,counts=counts,source_count=len(ss),actually_read_source_count=len(read),used_source_count=len(used),actually_read_families=len(set(s['source_family'] for s in read)),source_families=len(set(s['source_family'] for s in ss)),evidence_count=len(ev),visual_count=len(vs),real_chart_record_coverage=sum(visual_status(x,vd) for x in xs),case_chart_record_coverage=sum(visual_status(x,vd,True) for x in xs),main_record='setups/*.md',recognition=dict(rule_version=rec['rule_version'],base_count=len(rec['setups']),variant_count=len(rec['variant_graphs']),market_validation='not_run'),generated=['exports','indexes/catalog.md','indexes/aliases.md','indexes/comparisons.md','indexes/sources.md','indexes/taxonomy.md','recognition/catalog.json','recognition/catalog.md','site'],files=[])
 for p in delivery_files(ROOT):manifest['files'].append(dict(path=p.relative_to(ROOT).as_posix(),sha256=hashlib.sha256(p.read_bytes()).hexdigest(),bytes=p.stat().st_size))
 write('manifest.json',json.dumps(manifest,ensure_ascii=False,indent=2)+'\n');print(json.dumps({k:v for k,v in manifest.items() if k not in ['files','generated']},ensure_ascii=False))
if __name__=='__main__':sys.exit(main() or 0)
