"""公开资料获取助手；原文仅保存在临时目录，不写入交付包。
用法 python3 scripts/fetch_sources.py urls.json [--show]。
人工实际阅读后必须单独更新 actual_read_scope 与证据记录。
"""
import argparse, datetime, json, sys, pathlib, urllib.request, urllib.error, urllib.parse, hashlib, concurrent.futures, tempfile
ROOT=pathlib.Path(__file__).resolve().parents[1]
TMP=pathlib.Path(tempfile.gettempdir())/'intraday-source-review'
ACCESS_DATE=datetime.date.today().isoformat()
DB=ROOT/'data/sources.jsonl'
def load(): return [json.loads(l) for l in DB.read_text().splitlines() if l.strip()] if DB.exists() else []
def fetch(s):
    s=dict(s); s.update(schema_version='1.0.0',accessed=ACCESS_DATE,canonical_url=s['url'],language=s.get('language','en'),actual_read_scope='not_read',access_method='public_http',limitations=s.get('limitations',[]),commercial_interest=s.get('commercial_interest','教育、平台或研究品牌推广；独立审计未知'))
    try:
        if urllib.parse.urlsplit(s['url']).scheme not in ['http','https']:raise ValueError('Only public HTTP(S) sources are accepted')
        req=urllib.request.Request(s['url'],headers={'User-Agent':'Mozilla/5.0'})
        with urllib.request.urlopen(req,timeout=22) as response:
            raw=response.read(12000000); s['http_status']=response.status; s['canonical_url']=response.url; ct=response.headers.get('Content-Type','')
        suffix='.pdf' if raw[:4]==b'%PDF' else '.html'
        p=TMP/(s['id']+suffix); p.write_bytes(raw)
        s.update(access_status='accessible',retrieved_sha256=hashlib.sha256(raw).hexdigest())
        if suffix=='.html':
            from bs4 import BeautifulSoup
            soup=BeautifulSoup(raw,'html.parser')
            title=soup.find('h1') or soup.find('title'); s['title']=title.get_text(' ',strip=True) if title else s.get('title',s['url'])
            date=soup.find('meta',property='article:published_time')
            s['published']=s.get('published') or (date.get('content') if date else None)
            core=soup.select_one('.entry-content') or soup.select_one('.post-content') or soup.select_one('article') or soup.select_one('main') or soup.body
            imgs=[]
            if core:
                for im in core.find_all('img'):
                    u=im.get('data-src') or im.get('src'); imgs.append({'url':urllib.parse.urljoin(s['canonical_url'],u) if u else None,'alt':im.get('alt'), 'width':im.get('width'),'height':im.get('height')})
                videos=[v.get('src') for v in core.find_all('iframe')]
                for node in core.select('script,style,nav,footer,header,aside,form'): node.decompose()
                txt=core.get_text('\n',strip=True)
            else: txt=''; videos=[]
            (TMP/(s['id']+'.txt')).write_text(txt)
            (TMP/(s['id']+'-media.json')).write_text(json.dumps({'images':imgs,'videos':videos},ensure_ascii=False,indent=2))
            s['extracted_characters']=len(txt)
        else:
            from pypdf import PdfReader
            pdf=PdfReader(p); parts=['PDF PAGE '+str(i+1)+'\n'+(page.extract_text() or '') for i,page in enumerate(pdf.pages)]
            (TMP/(s['id']+'.txt')).write_text('\n\n'.join(parts)); s['pdf_pages']=len(parts); s['published']=s.get('published')
    except Exception as e:
        s.update(access_status='access_failed',http_status=getattr(e,'code',None),access_error=str(e),published=s.get('published'))
    s.setdefault('title',None);s.setdefault('publisher',s.get('source_family','unknown'))
    s.setdefault('read_locator',None);s.setdefault('summary','已获取或发现，尚未人工阅读，不支持最终规则。')
    s['unknown_fields']={k:'公开内容未核实' for k in ['title','published'] if s.get(k) is None}
    code=s.get('http_status')
    s['link_state']='checked_accessible' if s['access_status']=='accessible' else 'restricted_access' if code in [402,403] else 'http_404_at_check' if code==404 else 'temporary_network_failure'
    return s
if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input',help='JSON array of public source metadata')
    parser.add_argument('--out',type=pathlib.Path,default=TMP,help='Temporary source review directory; never packaged')
    parser.add_argument('--date',default=ACCESS_DATE,help='Access date, YYYY-MM-DD')
    parser.add_argument('--show',action='store_true',help='Show extracted text for human review; does not mark it read')
    args=parser.parse_args();TMP=args.out;TMP.mkdir(parents=True,exist_ok=True);ACCESS_DATE=args.date
    inputs=json.loads(pathlib.Path(args.input).read_text()); records=load(); by={x['id']:x for x in records}
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        for s in pool.map(fetch,inputs):
            if s['id'] in by and by[s['id']].get('actual_read_scope')!='not_read':
                old=by[s['id']]
                for key in ['actual_read_scope','summary','read_locator','publisher','limitations']:s[key]=old.get(key)
                for key in ['read_detail','read_at']:
                    if key in old:s[key]=old[key]
                s.setdefault('read_at',old['accessed'])
                if s.get('retrieved_sha256') and old.get('retrieved_sha256')!=s.get('retrieved_sha256'):
                    s['previous_read_sha256']=old.get('retrieved_sha256');s['requires_reread']=True
                    s['limitations']=(s.get('limitations') or [])+['重新获取的内容哈希改变/未获取；原阅读记录不能核验新版本，需人工复读']
            by[s['id']]=s
            print(json.dumps({k:s.get(k) for k in ['id','title','access_status','extracted_characters','pdf_pages','access_error']},ensure_ascii=False))
    DB.write_text(''.join(json.dumps(x,ensure_ascii=False)+'\n' for x in sorted(by.values(),key=lambda x:x['id'])))
    if args.show:
        for s in inputs:
            p=TMP/(s['id']+'.txt')
            if p.exists(): print('\nSOURCE '+s['id']+'\n'+p.read_text()[:24000])
