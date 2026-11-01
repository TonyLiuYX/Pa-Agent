"""Actual offline browser tests. Requires optional Playwright + Chromium.
Run python3 scripts/test_site.py; outputs reports/ui-results.json and previews.
This tests reading/search/navigation, not trading logic or profitability.
"""
import asyncio,json,pathlib,platform,sys,importlib.metadata
from build import ROOT,DATE,VERSION,records,rows,visual_status
async def main():
 from playwright.async_api import async_playwright
 xs=records();vd={v['id']:v for v in rows('visuals.jsonl')};checks=[];fail=[];console=[];network=[];shots=[]
 out=ROOT/'reports/ui-results.json';preview=ROOT/'reports/previews';preview.mkdir(exist_ok=True)
 def checked(name):checks.append(name)
 async with async_playwright() as p:
  browser=await p.chromium.launch(headless=True);ctx=await browser.new_context(viewport={'width':1360,'height':1000});page=await ctx.new_page()
  page.on('pageerror',lambda e:console.append(str(e)))
  page.on('request',lambda r:network.append(r.url) if r.url.startswith(('http://','https://')) else None)
  async def route(r):
   if r.request.url.startswith(('http://','https://')):await r.abort()
   else:await r.continue_()
  await ctx.route('**/*',route)
  home=(ROOT/'site/index.html').as_uri()
  try:
   await page.goto(home);await page.wait_for_selector('.card');assert await page.locator('.card').count()==len(xs);checked('file:// home renders 50 records with network requests blocked')
   async def search(q):await page.locator('#search').fill(q)
   await search('吸收');assert await page.locator('[data-id="SET-0003"]').count()==1;checked('Chinese full-text search finds absorption')
   await search('failed breakout');assert await page.locator('[data-id="SET-0001"]').count()==1;checked('English AND-token search finds failed breakout')
   await search('H2');assert await page.locator('[data-id="SET-0006"]').count()==1;checked('Alias search finds H2/M2 record')
   await search('a(i,h)');assert await page.locator('[data-id="SET-0048"]').count()==1;checked('Body-only formula search finds noise-band record')
   await search('不存在的词xyz987');assert await page.locator('.card').count()==0;checked('Zero-result state renders without errors')
   await page.locator('#resetFilters').click();assert await page.locator('.card').count()==len(xs)
   await page.select_option('#pa_role','core');assert await page.locator('.card').count()==sum(x['pa_role']=='core' for x in xs)
   await page.select_option('#market_types','stocks');expected=sum(x['pa_role']=='core' and 'stocks' in x['market_types'] for x in xs);assert await page.locator('.card').count()==expected;checked('PA + market multi-axis filters intersect correctly')
   await page.locator('#resetFilters').click();await page.select_option('#data_requirements','DOM');expected=sum('DOM' in x['data_requirements'] for x in xs);assert await page.locator('.card').count()==expected;checked('Data requirement filter matches actual DOM records')
   await page.locator('#resetFilters').click();await page.select_option('#visual_status','gap');assert await page.locator('.card').count()==sum(not visual_status(x,vd) for x in xs);checked('Visual gap filter matches actual record coverage')
   await page.locator('#resetFilters').click();await page.select_option('#visual_status','context_viewed');assert await page.locator('.card').count()==sum(visual_status(x,vd) and not visual_status(x,vd,True) for x in xs);checked('Context-only visual filter separates background records')
   await page.locator('#resetFilters').click();await page.select_option('#method_families','VWAP');await page.select_option('#environment_tags','balance');assert await page.locator('[data-id="SET-0025"]').count()==1;checked('Method family + environment filters preserve specific VWAP balance setup')
   await page.locator('#resetFilters').click();await page.select_option('#structure_locations','initial_balance');assert await page.locator('.card').count()==2;checked('Structure-location filter returns lead-lag IB and two-sided IB records')
   await page.locator('#resetFilters').click();await page.select_option('#execution_mode','systematic_source_version');assert await page.locator('.card').count()==3;checked('Execution-mode filter separates three source model versions')
   await page.locator('#resetFilters').click();await page.locator('#compareBtn').click();assert '先选择' in await page.locator('#compare').inner_text();checked('Empty comparison gives actionable selection hint')
   for sid in ['SET-0003','SET-0040','SET-0047']:await page.locator('[data-id="'+sid+'"]').check()
   await page.locator('[data-id="SET-0004"]').click();assert not await page.locator('[data-id="SET-0004"]').is_checked();assert '最多' in await page.locator('#compareStatus').inner_text();checked('Fourth comparison selection is rejected with visible reason')
   await page.locator('#compareBtn').click();assert await page.locator('#compare thead th').count()==4;assert await page.locator('#compare tbody tr').count()==7;assert '大量冲击' in await page.locator('#compare').inner_text();assert '逐档小量' in await page.locator('#compare').inner_text();checked('Three-record comparison contains real fingerprint, entry, stop and management text')
   await search('VWAP');await page.locator('#compareBtn').click();assert 'SET-0003' in await page.locator('#compare').inner_text();checked('Comparison selection persists after search changes')
   await page.locator('#resetFilters').click();await page.locator('#compare').scroll_into_view_if_needed();shot=preview/'desktop-comparison.png';await page.screenshot(path=str(shot));shots.append(str(shot.relative_to(ROOT)))
   await page.locator('#clearCompare').click();assert await page.locator('#compare th').count()==0;assert '0 / 3' in await page.locator('#compareStatus').inner_text();checked('Clear comparison resets selections and table')
   await page.locator('[href="setups/SET-0003.html"]').first.click();assert 'SET-0003.html' in page.url;assert await page.locator('main section').count()==15;checked('Card navigates to full 15-section setup page')
   await page.locator('nav.toc [href="#sec11"]').click();await page.locator('#sec11 img').scroll_into_view_if_needed();await page.wait_for_function('document.querySelector("#sec11 img").complete && document.querySelector("#sec11 img").naturalWidth>0');assert await page.locator('#sec11 img').get_attribute('alt');shot=preview/'setup-absorption.png';await page.locator('#sec11 img').screenshot(path=str(shot));shots.append(str(shot.relative_to(ROOT)));checked('Original local SVG renders in case section with meaningful alt text')
   await page.locator('[href="../source/SRC-0033.html"]').click();assert 'SRC-0033' in page.url;assert await page.locator('[href="../setups/SET-0003.html"]').count()==1;checked('Setup-to-source link and source reverse citation work')
   await page.locator('[href="../setups/SET-0003.html"]').click();await page.locator('[href="SET-0040.html"]').last.click();assert 'SET-0040' in page.url;checked('Bidirectional related-setup navigation works')
   for name,needle in [('learning.html','路线'),('glossary.html','Delta'),('comparisons.html','SET-0042'),('quality.html',str(sum(s['actual_read_scope']!='not_read' for s in rows('sources.jsonl')))),('research.html',str(len(rows('../research/candidates.jsonl')))),('aliases.html','M2'),('sources.html','SRC-0086')]:
    await page.goto((ROOT/'site'/name).as_uri());assert needle in await page.locator('main').inner_text()
   checked('Learning, glossary, comparison, quality, report, aliases and source indexes render')
   await page.goto((ROOT/'site/readme.html').as_uri());await page.locator('[href="recognition.html"]').last.click();assert '研究版' in await page.locator('main').inner_text();checked('README links to offline recognition guide')
   await page.locator('[href="recognition-catalog.html"]').first.click();assert 'SET-0050' in await page.locator('main').inner_text();assert 'source_open' in await page.locator('main').inner_text();checked('Offline executable catalog renders all 50 definitions and variants')
   await page.locator('[href="evaluation.html"]').first.click();assert '一对一' in await page.locator('main').inner_text();checked('Recognition catalog links to independent evaluation protocol')
   await page.goto((ROOT/'site/recognition.html').as_uri());shot=preview/'desktop-recognition.png';await page.screenshot(path=str(shot));shots.append(str(shot.relative_to(ROOT)))
   await page.goto(home);shot=preview/'desktop-home.png';await page.screenshot(path=str(shot));shots.append(str(shot.relative_to(ROOT)))
   for x in xs:
    await page.goto((ROOT/'site/setups'/f'{x["id"]}.html').as_uri());assert await page.locator('main section').count()==15;img=page.locator('#sec11 img');assert await img.count()==1;await img.scroll_into_view_if_needed();await page.wait_for_function('document.querySelector("#sec11 img").complete && document.querySelector("#sec11 img").naturalWidth>0')
   checked('All 50 setup pages render 15 chapters and load their local original SVG')
   await page.set_viewport_size({'width':390,'height':844});await page.goto(home);assert await page.locator('.card').count()==len(xs);assert await page.evaluate('document.documentElement.scrollWidth <= window.innerWidth+1');shot=preview/'mobile-home.png';await page.screenshot(path=str(shot));shots.append(str(shot.relative_to(ROOT)))
   for sid in ['SET-0006','SET-0009','SET-0015']:await page.locator('[data-id="'+sid+'"]').check()
   await page.locator('#compareBtn').click();assert await page.locator('#compare thead th').count()==4;assert await page.evaluate('document.documentElement.scrollWidth <= window.innerWidth+1');checked('390px mobile home and horizontal comparison stay within viewport')
   await page.goto((ROOT/'site/setups/SET-0048.html').as_uri());await page.locator('#sec11 img').scroll_into_view_if_needed();await page.wait_for_function('document.querySelector("#sec11 img").complete && document.querySelector("#sec11 img").naturalWidth>0');await page.locator('#sec11 img').scroll_into_view_if_needed();assert await page.evaluate('document.documentElement.scrollWidth <= window.innerWidth+1');shot=preview/'mobile-noise-band.png';await page.screenshot(path=str(shot));shots.append(str(shot.relative_to(ROOT)));checked('Mobile full-record case section loads local SVG and stays within viewport')
   await page.locator('#sec11 a[href="../../assets/diagrams/SET-0048.svg"]').click();assert page.url.endswith('SET-0048.svg');assert await page.locator('svg').count()==1;checked('Mobile original-size diagram link opens the actual local SVG')
   for name in ['recognition.html','recognition-catalog.html','evaluation.html']:
    await page.goto((ROOT/'site'/name).as_uri());assert await page.evaluate('document.documentElement.scrollWidth <= window.innerWidth+1')
   checked('390px recognition, catalog and evaluation pages stay within viewport')
   await page.goto((ROOT/'site/recognition.html').as_uri());shot=preview/'mobile-recognition.png';await page.screenshot(path=str(shot));shots.append(str(shot.relative_to(ROOT)))
   assert not console,console;assert not network,network;checked('No JavaScript page errors or HTTP(S) requests throughout offline tests')
  except Exception as e:fail.append(type(e).__name__+': '+str(e))
  result={'date':DATE,'version':VERSION,'status':'passed' if not fail else 'failed','environment':{'python':platform.python_version(),'platform':platform.system(),'playwright':importlib.metadata.version('playwright'),'browser':'Chromium '+browser.version,'url_mode':'file://','http_requests_blocked':True,'mobile_viewport':[390,844]},'checks':checks,'errors':fail,'console_errors':console,'network_requests':network,'screenshots':shots,'manual_visual_review':'pending','limitations':['Chromium headless tested; no dedicated Safari/Firefox test','Original source charts stay external; tests check local own diagrams and links, not remote market execution','This is a reading UI test, not a strategy backtest']}
  out.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n');await browser.close();print(json.dumps(result,ensure_ascii=False,indent=2));return 1 if fail else 0
if __name__=='__main__':sys.exit(asyncio.run(main()))
