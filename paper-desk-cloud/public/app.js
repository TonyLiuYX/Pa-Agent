const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => Array.from(root.querySelectorAll(selector));
const PAGE_TOKEN = $('meta[name="pa-page-token"]').content;
const ROUTES = new Set(['/', '/trade', '/strategies', '/create-setup', '/settings']);
const ACTIVE_LIFECYCLES = new Set(['starting', 'running', 'waiting_market', 'recovery', 'model_error', 'stopping']);

const ICONS = {
  home: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="m4 10 8-6 8 6v9H4z"/><path d="M9 19v-6h6v6"/></svg>',
  trade: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 18 9 12l4 3 7-9"/><path d="M16 6h4v4"/></svg>',
  strategies: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 5h14v4H5zM5 13h6v6H5zM15 13h4v6h-4z"/></svg>',
  create: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 5v14M5 12h14"/><circle cx="12" cy="12" r="9"/></svg>',
  settings: '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="3"/><path d="M12 3.5v2M12 18.5v2M3.5 12h2M18.5 12h2M6 6l1.4 1.4M16.6 16.6 18 18M18 6l-1.4 1.4M7.4 16.6 6 18"/></svg>',
};

const COPY = {
  'zh-CN': {
    navHome: '总览', navTrade: '交易', navStrategies: '策略', navCreate: '创建 Setup', navSettings: '设置',
    kickerHome: 'HOUSE PERFORMANCE', titleHome: '项目总览', kickerTrade: 'LIVE WORKSPACE', titleTrade: '即时交易',
    kickerStrategies: 'SETUP LIBRARY', titleStrategies: '策略工作区', kickerCreate: 'BUNDLE STUDIO', titleCreate: '创建 Setup',
    kickerSettings: 'PRIVATE CLOUD CONFIGURATION', titleSettings: '设置', idle: '空闲', starting: '启动中', running: '运行中',
    waiting_market: '等待行情', recovery: '恢复中', model_error: '模型异常', blocked: '已阻止', stopping: '停止中', stopped: '已停止',
    homeHeadline: '长期结果，独立呈现。', homeLead: '这里仅展示项目在云端长期运行后的总权益，不混入任何用户临时交易。',
    totalEquity: '总权益', cumulativePnl: '累计模拟盈亏', totalReturn: '总收益率', noData: '尚无长期运行数据',
    noDataBody: 'House Performance 接口已预留。部署长期交易服务后，真实序列会在这里出现；当前不生成演示曲线。',
    performanceTitle: '项目总权益曲线', sourceBound: '来源绑定 setup', sourceBoundBody: '21 个内置 setup 均保留逐条 claim、页码与 source lineage。',
    isolatedAccounts: '独立模拟账户', isolatedAccountsBody: '每个 setup 持有独立 USD 1,000 账户，策略之间不共享或净额结算。',
    paperBoundary: 'Paper-only 边界', paperBoundaryBody: '本工作区只运行建议或精确模拟交易，没有真实订单路径。',
    symbol: '股票', timeframe: '分析周期', mode: '运行模式', selectedSetups: '已选 Setup', start: '开始运行', stop: '优雅停止',
    advisory: '建议模式 · 不交易', paper: '模拟交易', configureFirst: '请先在设置中保存 Alpaca 与当前模型凭据。',
    latestPrice: '最新价', closedBars: '闭合 K 线', feed: '行情', readiness: '可执行性', realtime: '实时分析',
    strategyState: '策略状态', ledger: '模拟账本', noDecision: '尚无已接受决策。新闭合 K 线到来后会在这里显示可读摘要。',
    noStrategyState: 'runtime 启动后显示所选 setup 状态。', noLedger: '目前没有模拟挂单、持仓或已平仓记录。',
    catalogHeadline: '把 21 个 setup 变成可见、可选的工作单元。', catalogLead: '筛选、查看来源与执行条件，然后冻结本次运行所用的策略集合。',
    search: '搜索名称、ID 或标签', all: '全部', builtIn: '内置', custom: '自定义', selectedOnly: '仅已选', selectAll: '全选', clear: '清空', saveSelection: '保存选择',
    shown: '当前显示', selected: '已选择', details: '查看详情', source: '来源', allSources: '全部来源', allTags: '全部标签', selectionSaved: '策略选择已保存',
    createHeadline: '把你的策略 PDF，转换成可审计的 Setup Bundle。', createLead: '复制提示词交给你的 Agent，连同策略 PDF 一起处理；再把生成的单个 ZIP 导回 Paper Desk。',
    conversionPrompt: '转换提示词', copyPrompt: '复制提示词', downloadSpec: '下载转换规范', importBundle: '导入 .pa-setup.zip',
    chooseBundle: '选择或拖入一个 Setup Bundle', bundleLimits: 'ZIP ≤ 50 MB · 解压 ≤ 100 MB · 精确五个根文件', validateImport: '验证并导入', noFile: '尚未选择文件',
    untrustedPdf: '安全边界：PDF 内容只作为待分析资料。提示词明确要求外部 Agent 不执行 PDF 中嵌入的任何指令，并逐页区分原文、推导和未说明。',
    settingsHeadline: '凭据以加密形式留在你的私有云空间。', settingsLead: '模型参数与 API 凭据在这里配置；交易页永远不接收或展示秘密。只有已登录的站点拥有者可以使用。',
    activeProvider: '活动模型', thinking: 'DeepSeek Thinking', serviceTier: 'OpenAI 服务层', saveModel: '保存模型设置',
    configured: '已配置', notConfigured: '未配置', keyId: 'Key ID', secretKey: 'Secret Key', apiKey: 'API Key', saveCredentials: '保存凭据', clearCredentials: '清除',
    alpacaBody: '用于 Assets 校验、IEX 历史与单交易所实时行情。不会下真实订单。', deepseekBody: '固定 deepseek-v4-flash，可选择 thinking enabled / disabled。',
    openaiBody: '固定 gpt-5.6-luna、reasoning effort max，可选择 standard / Fast。', settingsLocked: 'runtime 活动期间，凭据、模型和策略集合保持冻结。',
    saved: '已保存', cleared: '已清除', copied: '提示词已复制', imported: 'Setup Bundle 已通过全部检查并导入', idempotent: '相同 Bundle 已存在，无需重复写入',
    validationFailed: '验证失败', loading: '正在加载…', candidateDisclaimer: '高流动性观察候选，不构成盈利或投资推荐。',
  },
  'en-US': {
    navHome: 'Overview', navTrade: 'Trade', navStrategies: 'Strategies', navCreate: 'Create Setup', navSettings: 'Settings',
    kickerHome: 'HOUSE PERFORMANCE', titleHome: 'Project overview', kickerTrade: 'LIVE WORKSPACE', titleTrade: 'On-demand trading',
    kickerStrategies: 'SETUP LIBRARY', titleStrategies: 'Strategy workspace', kickerCreate: 'BUNDLE STUDIO', titleCreate: 'Create Setup',
    kickerSettings: 'PRIVATE CLOUD CONFIGURATION', titleSettings: 'Settings', idle: 'Idle', starting: 'Starting', running: 'Running',
    waiting_market: 'Waiting for market', recovery: 'Recovering', model_error: 'Model error', blocked: 'Blocked', stopping: 'Stopping', stopped: 'Stopped',
    homeHeadline: 'Long-run results, kept separate.', homeLead: 'This page is reserved for the project’s long-running cloud equity. It never mixes in a user’s on-demand run.',
    totalEquity: 'Total equity', cumulativePnl: 'Cumulative paper P&L', totalReturn: 'Total return', noData: 'No long-run data yet',
    noDataBody: 'The House Performance interface is ready. A real series will appear after the long-running service is deployed; no demo curve is fabricated.',
    performanceTitle: 'Project total equity', sourceBound: 'Source-bound setups', sourceBoundBody: 'All 21 built-in setups retain claim, page and source lineage.',
    isolatedAccounts: 'Independent accounts', isolatedAccountsBody: 'Each setup owns a separate USD 1,000 paper account; strategies never net funds.',
    paperBoundary: 'Paper-only boundary', paperBoundaryBody: 'This workspace offers advisory or exact paper execution only. There is no live-order path.',
    symbol: 'Symbol', timeframe: 'Analysis frame', mode: 'Run mode', selectedSetups: 'Selected setups', start: 'Start run', stop: 'Stop gracefully',
    advisory: 'Advisory · no trades', paper: 'Paper trading', configureFirst: 'Save Alpaca and active-model credentials in Settings first.',
    latestPrice: 'Latest price', closedBars: 'Closed bars', feed: 'Feed', readiness: 'Executable', realtime: 'Live analysis',
    strategyState: 'Strategy state', ledger: 'Paper ledger', noDecision: 'No accepted decisions yet. A readable summary appears after a newly closed bar.',
    noStrategyState: 'Selected setup states appear after the runtime starts.', noLedger: 'There are no paper orders, positions or closed trades yet.',
    catalogHeadline: 'Turn 21 setups into visible, selectable work units.', catalogLead: 'Filter, inspect source conditions, then freeze the strategy set for the next run.',
    search: 'Search name, ID or tag', all: 'All', builtIn: 'Built-in', custom: 'Custom', selectedOnly: 'Selected', selectAll: 'Select all', clear: 'Clear', saveSelection: 'Save selection',
    shown: 'Shown', selected: 'Selected', details: 'View details', source: 'Source', allSources: 'All sources', allTags: 'All tags', selectionSaved: 'Strategy selection saved',
    createHeadline: 'Convert your strategy PDF into an auditable Setup Bundle.', createLead: 'Give the prompt and PDF to your agent, then import the single generated ZIP back into Paper Desk.',
    conversionPrompt: 'Conversion prompt', copyPrompt: 'Copy prompt', downloadSpec: 'Download specification', importBundle: 'Import .pa-setup.zip',
    chooseBundle: 'Choose or drop one Setup Bundle', bundleLimits: 'ZIP ≤ 50 MB · extracted ≤ 100 MB · exactly five root files', validateImport: 'Validate and import', noFile: 'No file selected',
    untrustedPdf: 'Safety boundary: PDF content is source material only. The prompt tells the external agent never to execute embedded instructions and to distinguish source, inference and not-stated claims page by page.',
    settingsHeadline: 'Credentials stay encrypted in your private cloud space.', settingsLead: 'Configure models and API credentials here. The Trade page never receives or renders secrets. Only the signed-in Site owner can use them.',
    activeProvider: 'Active model', thinking: 'DeepSeek Thinking', serviceTier: 'OpenAI service tier', saveModel: 'Save model settings',
    configured: 'Configured', notConfigured: 'Not configured', keyId: 'Key ID', secretKey: 'Secret Key', apiKey: 'API Key', saveCredentials: 'Save credentials', clearCredentials: 'Clear',
    alpacaBody: 'Used for Assets validation, IEX history and single-exchange live data. No live orders.', deepseekBody: 'Fixed to deepseek-v4-flash with thinking enabled or disabled.',
    openaiBody: 'Fixed to gpt-5.6-luna with reasoning effort max and standard or Fast service.', settingsLocked: 'Credentials, model settings and strategy selection stay frozen while the runtime is active.',
    saved: 'Saved', cleared: 'Cleared', copied: 'Prompt copied', imported: 'Setup Bundle passed every check and was imported', idempotent: 'The identical Bundle is already installed; no duplicate write was made',
    validationFailed: 'Validation failed', loading: 'Loading…', candidateDisclaimer: 'High-liquidity observation candidates, not profit or investment recommendations.',
  },
};

const APP = {
  locale: 'zh-CN', bootstrap: null, settings: null, snapshot: null, routeEpoch: 0,
  tradeTab: 'analysis', strategyData: null, strategyDraft: new Set(), strategyFilter: 'all',
  setupKit: null, setupFile: null, lastClosedCount: 0, lastDecisionCount: 0, pollTimer: null, nextStepAt: 0,
};

function t(key) { return (COPY[APP.locale] && COPY[APP.locale][key]) || COPY['zh-CN'][key] || key; }
function textNode(tag, className, value) { const el = document.createElement(tag); if (className) el.className = className; el.textContent = value == null ? '' : String(value); return el; }
function setText(selector, value, root = document) { const target = $(selector, root); if (target) target.textContent = value == null ? '' : String(value); }
function formatMoney(value) { if (value == null || value === '') return '—'; const number = Number(value); return Number.isFinite(number) ? new Intl.NumberFormat(APP.locale, {style: 'currency', currency: 'USD', maximumFractionDigits: 2}).format(number) : '—'; }
function formatPercent(value) { if (value == null || value === '') return '—'; const number = Number(value); return Number.isFinite(number) ? `${number >= 0 ? '+' : ''}${number.toFixed(2)}%` : '—'; }
function shortTime(value) { if (!value) return '—'; const date = new Date(value); return Number.isNaN(date.valueOf()) ? '—' : date.toLocaleTimeString(APP.locale, {hour: '2-digit', minute: '2-digit', second: '2-digit'}); }
function activeLifecycle() { return ACTIVE_LIFECYCLES.has(APP.snapshot?.lifecycle); }
function configurationLocked() { return Boolean(APP.bootstrap?.runtime?.configuration_locked || activeLifecycle()); }

async function api(path, options = {}) {
  const response = await fetch(path, {cache: 'no-store', ...options});
  let value = {};
  try { value = await response.json(); } catch { value = {}; }
  if (!response.ok) {
    const error = new Error(value?.error?.message || `HTTP ${response.status}`);
    error.code = value?.error?.code || 'request_failed';
    throw error;
  }
  return value;
}

function mutation(path, payload) {
  return api(path, {method: 'POST', headers: {'Content-Type': 'application/json', 'X-PA-Page-Token': PAGE_TOKEN}, body: JSON.stringify(payload)});
}

function toast(message, kind = '') {
  const region = $('#toast-region');
  const item = textNode('div', `toast ${kind}`.trim(), message);
  region.replaceChildren(item);
  window.setTimeout(() => item.remove(), 3200);
}

function loading() {
  $('#page-content').innerHTML = `<div class="loading-block"><div class="loading-mark" aria-hidden="true"></div><span>${t('loading')}</span></div>`;
}

function navItems() {
  return [
    ['/', 'home', 'navHome', '01'], ['/trade', 'trade', 'navTrade', '02'], ['/strategies', 'strategies', 'navStrategies', '03'],
    ['/create-setup', 'create', 'navCreate', '04'], ['/settings', 'settings', 'navSettings', '05'],
  ];
}

function renderNavigation() {
  const current = location.pathname;
  const desktop = $('#primary-nav'); const mobile = $('#mobile-nav'); desktop.replaceChildren(); mobile.replaceChildren();
  navItems().forEach(([path, icon, label, number]) => {
    const link = document.createElement('a'); link.href = path; link.dataset.route = ''; link.className = `nav-link${current === path ? ' active' : ''}`;
    link.innerHTML = ICONS[icon]; link.append(textNode('span', '', t(label)), textNode('small', '', number)); desktop.append(link);
    const compact = document.createElement('a'); compact.href = path; compact.dataset.route = ''; compact.className = `mobile-link${current === path ? ' active' : ''}`;
    compact.innerHTML = ICONS[icon]; compact.append(textNode('span', '', t(label))); mobile.append(compact);
  });
}

function pageMeta(path) {
  const values = {
    '/': ['kickerHome', 'titleHome'], '/trade': ['kickerTrade', 'titleTrade'], '/strategies': ['kickerStrategies', 'titleStrategies'],
    '/create-setup': ['kickerCreate', 'titleCreate'], '/settings': ['kickerSettings', 'titleSettings'],
  };
  const [kicker, title] = values[path] || values['/'];
  setText('#page-kicker', t(kicker)); setText('#page-title', t(title)); document.title = `${t(title)} · Paper Desk`;
}

function updateShell(snapshot = APP.snapshot) {
  if (!snapshot) return;
  APP.snapshot = snapshot;
  if (APP.bootstrap) APP.bootstrap.runtime = {lifecycle: snapshot.lifecycle, configuration_locked: activeLifecycle()};
  const lifecycle = snapshot.lifecycle || 'idle';
  const pill = $('#runtime-pill'); pill.className = 'runtime-pill';
  if (lifecycle === 'running') pill.classList.add('running'); else if (['starting', 'waiting_market', 'recovery', 'stopping'].includes(lifecycle)) pill.classList.add('waiting'); else if (['model_error', 'blocked'].includes(lifecycle)) pill.classList.add('error');
  setText('#runtime-label', t(lifecycle)); setText('#spine-state', lifecycle.toUpperCase());
  const closed = snapshot.bars?.closed_count || 0; const decisions = snapshot.recent_decisions?.length || 0;
  const feedNode = $('.node-feed'); const barNode = $('.node-bar'); const decisionNode = $('.node-decision');
  feedNode.classList.toggle('active', Boolean(snapshot.market?.stream?.started || lifecycle === 'running'));
  barNode.classList.toggle('active', closed > 0); decisionNode.classList.toggle('active', decisions > 0);
  document.documentElement.style.setProperty('--spine-progress', decisions ? '1' : closed ? '.58' : snapshot.market?.stream?.started ? '.27' : '.12');
  if (closed > APP.lastClosedCount) { barNode.classList.remove('pulse'); void barNode.offsetWidth; barNode.classList.add('pulse'); }
  if (decisions > APP.lastDecisionCount) { decisionNode.classList.remove('pulse'); void decisionNode.offsetWidth; decisionNode.classList.add('pulse'); }
  APP.lastClosedCount = closed; APP.lastDecisionCount = decisions;
  setText('#spine-detail', `${snapshot.run?.symbol || 'IEX'} · ${snapshot.run?.execution_policy || 'PAPER'}`.toUpperCase());
  if (location.pathname === '/trade' && $('#trade-chart')) renderTradeSnapshot();
  updateLockedControls();
}

function updateLockedControls() {
  const locked = configurationLocked();
  $$('[data-config-control]').forEach((el) => { el.disabled = locked; });
  $$('[data-stop-control]').forEach((el) => { el.disabled = !activeLifecycle(); });
  const start = $('#start-run'); if (start) start.disabled = locked || !(APP.settings?.credentials?.alpaca && APP.settings?.credentials?.[APP.settings?.active_provider]);
  const note = $('#runtime-lock-note'); if (note) note.hidden = !locked;
}

async function refreshBootstrap() {
  const value = await api('/api/bootstrap'); APP.bootstrap = value; APP.locale = value.locale || 'zh-CN'; APP.settings = value.settings; document.documentElement.lang = APP.locale;
  $('#language-toggle').textContent = APP.locale === 'zh-CN' ? 'EN' : '中';
  return value;
}

function go(path) {
  if (!ROUTES.has(path)) path = '/';
  if (location.pathname !== path) history.pushState({}, '', path);
  renderRoute();
}

async function renderRoute() {
  const path = ROUTES.has(location.pathname) ? location.pathname : '/';
  const epoch = ++APP.routeEpoch; renderNavigation(); pageMeta(path); loading();
  try {
    if (path === '/') await renderHome(epoch);
    else if (path === '/trade') await renderTrade(epoch);
    else if (path === '/strategies') await renderStrategies(epoch);
    else if (path === '/create-setup') await renderCreateSetup(epoch);
    else if (path === '/settings') await renderSettings(epoch);
  } catch (error) {
    if (epoch !== APP.routeEpoch) return;
    $('#page-content').innerHTML = '';
    const block = textNode('div', 'inline-notice', error.message || 'Unable to load page'); $('#page-content').append(block); toast(error.message, 'error');
  }
}

async function renderHome(epoch) {
  const home = await api('/api/home'); if (epoch !== APP.routeEpoch) return;
  const summary = home.summary || {};
  $('#page-content').innerHTML = `
    <section class="section-intro"><div><span class="eyebrow">HOUSE / USER SEPARATION</span><h2>${t('homeHeadline')}</h2><p>${t('homeLead')}</p></div></section>
    <section class="metric-grid" aria-label="House performance summary">
      <div class="metric"><span class="metric-label">${t('totalEquity')}</span><strong id="home-equity">—</strong><small>USD · HOUSE ACCOUNT</small></div>
      <div class="metric"><span class="metric-label">${t('cumulativePnl')}</span><strong id="home-pnl">—</strong><small>SIMULATED · LONG-RUN ONLY</small></div>
      <div class="metric"><span class="metric-label">${t('totalReturn')}</span><strong id="home-return">—</strong><small>NO USER RUNTIME MIXING</small></div>
    </section>
    <section class="card home-performance"><header class="chart-toolbar"><h3>${t('performanceTitle')}</h3><span>HOUSE PERFORMANCE SNAPSHOT · ${home.available ? 'AVAILABLE' : 'EMPTY'}</span></header><div class="house-chart" id="house-chart"></div></section>
    <section class="project-grid" id="project-grid"></section>`;
  if (home.available) {
    setText('#home-equity', formatMoney(summary.current_equity)); setText('#home-pnl', formatMoney(summary.total_pnl)); setText('#home-return', formatPercent(summary.total_return_pct));
    renderHouseSeries(home.series || []);
  } else {
    const empty = textNode('div', 'empty-performance'); empty.innerHTML = '<div class="empty-mark" aria-hidden="true">∿</div>';
    empty.append(textNode('h3', '', t('noData')), textNode('p', '', t('noDataBody'))); $('#house-chart').append(empty);
  }
  const projects = [
    ['21', t('sourceBound'), t('sourceBoundBody')], ['1K', t('isolatedAccounts'), t('isolatedAccountsBody')], ['0', t('paperBoundary'), t('paperBoundaryBody')],
  ];
  projects.forEach(([number, title, body]) => { const card = textNode('article', 'card project-card'); card.append(textNode('span', 'project-number', number)); const group = document.createElement('div'); group.append(textNode('h3', '', title), textNode('p', '', body)); card.append(group); $('#project-grid').append(card); });
}

function renderHouseSeries(series) {
  const host = $('#house-chart'); host.replaceChildren();
  if (!series.length) return;
  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg'); svg.setAttribute('viewBox', '0 0 1000 330'); svg.setAttribute('role', 'img'); svg.setAttribute('aria-label', t('performanceTitle'));
  const values = series.map((item) => Number(item.equity)).filter(Number.isFinite); if (!values.length) return;
  const low = Math.min(...values); const high = Math.max(...values); const range = high - low || 1;
  const points = values.map((value, index) => `${40 + index * 920 / Math.max(1, values.length - 1)},${290 - (value - low) * 250 / range}`).join(' ');
  const line = document.createElementNS(svg.namespaceURI, 'polyline'); line.setAttribute('points', points); line.setAttribute('fill', 'none'); line.setAttribute('stroke', '#315BFF'); line.setAttribute('stroke-width', '3'); svg.append(line); host.append(svg);
}

async function renderTrade(epoch) {
  if (!APP.bootstrap) await refreshBootstrap(); if (epoch !== APP.routeEpoch) return;
  const defaults = APP.bootstrap.defaults; const counts = APP.bootstrap.strategy_counts;
  $('#page-content').innerHTML = `
    <section class="trade-page">
      <div class="trade-controls">
        <div class="field"><label for="trade-symbol">${t('symbol')}</label><select id="trade-symbol" data-config-control></select></div>
        <div class="field"><label for="trade-timeframe">${t('timeframe')}</label><select id="trade-timeframe" data-config-control></select></div>
        <div class="field"><label for="trade-policy">${t('mode')}</label><select id="trade-policy" data-config-control><option value="advisory">${t('advisory')}</option><option value="paper">${t('paper')}</option></select></div>
        <div class="field"><span class="field-label">${t('selectedSetups')}</span><div class="setup-count-box"><strong id="trade-setup-count">${counts.selected}</strong><span>/ ${counts.total}</span></div></div>
        <div class="trade-actions"><button class="button button-primary" id="start-run" type="button">${t('start')}</button><button class="button button-ghost" id="stop-run" data-stop-control type="button">${t('stop')}</button></div>
      </div>
      <div class="inline-notice" id="credential-notice" hidden>${t('configureFirst')} <a href="/settings" data-route class="cobalt">${t('navSettings')} →</a></div>
      <div class="trade-stage">
        <section class="market-panel"><header class="market-header"><div class="symbol-lockup"><strong id="market-symbol">${defaults.symbol}</strong><span>US EQUITY · IEX</span></div><div class="last-price"><strong id="market-price">—</strong><span id="market-price-at">${t('latestPrice')}</span></div></header>
          <div class="market-meta"><span>${t('closedBars')} <b id="market-bar-count">0</b></span><span>${t('feed')} <b id="market-feed">IEX</b></span><span>${t('readiness')} <b id="market-ready">WAITING</b></span><span>${t('mode')} <b id="market-policy">ADVISORY</b></span></div>
          <div class="chart-shell"><svg id="trade-chart" viewBox="0 0 820 500" role="img" aria-label="Candlestick chart"></svg><div class="chart-empty-dark" id="trade-chart-empty"><strong>${t('closedBars')}</strong><span>${t('noData')}</span></div></div>
        </section>
        <aside class="decision-panel"><div class="decision-tabs" role="tablist"><button class="decision-tab active" data-tab="analysis" role="tab">${t('realtime')}</button><button class="decision-tab" data-tab="strategies" role="tab">${t('strategyState')}</button><button class="decision-tab" data-tab="ledger" role="tab">${t('ledger')}</button></div><div class="decision-content" id="decision-content"></div></aside>
      </div>
    </section>`;
  const symbol = $('#trade-symbol'); APP.bootstrap.symbols.forEach((item) => { const option = textNode('option', '', item.symbol); option.value = item.symbol; symbol.append(option); }); symbol.value = APP.snapshot?.run?.symbol || defaults.symbol;
  const frame = $('#trade-timeframe'); APP.bootstrap.timeframes.forEach((item) => { const option = textNode('option', '', item); option.value = item; frame.append(option); }); frame.value = typeof APP.snapshot?.run?.timeframe === 'string' ? APP.snapshot.run.timeframe : defaults.timeframe;
  $('#trade-policy').value = APP.snapshot?.run?.execution_policy || defaults.execution_policy;
  const hasCredentials = APP.settings?.credentials?.alpaca && APP.settings?.credentials?.[APP.settings.active_provider]; $('#credential-notice').hidden = Boolean(hasCredentials);
  $('#start-run').addEventListener('click', startRun); $('#stop-run').addEventListener('click', stopRun);
  $$('.decision-tab').forEach((button) => button.addEventListener('click', () => { APP.tradeTab = button.dataset.tab; $$('.decision-tab').forEach((item) => item.classList.toggle('active', item === button)); renderDecisionPane(); }));
  renderTradeSnapshot(); updateLockedControls();
}

async function startRun() {
  const payload = {symbol: $('#trade-symbol').value, timeframe: $('#trade-timeframe').value, execution_policy: $('#trade-policy').value};
  try { const result = await mutation('/api/start', payload); APP.nextStepAt = 0; updateShell(result.snapshot); toast(t('starting')); }
  catch (error) { toast(error.message, 'error'); }
}
async function stopRun() { try { const result = await mutation('/api/stop', {}); updateShell(result.snapshot); toast(t('stopped'), 'success'); } catch (error) { toast(error.message, 'error'); } }

function renderTradeSnapshot() {
  if (!$('#trade-chart')) return; const snapshot = APP.snapshot || {};
  const symbol = snapshot.run?.symbol || $('#trade-symbol')?.value || APP.bootstrap?.defaults?.symbol || 'NVDA';
  setText('#market-symbol', symbol); setText('#market-price', snapshot.market?.latest_price ? `$${snapshot.market.latest_price}` : '—'); setText('#market-price-at', snapshot.market?.latest_price_at ? `${t('latestPrice')} · ${shortTime(snapshot.market.latest_price_at)}` : t('latestPrice'));
  setText('#market-bar-count', snapshot.bars?.closed_count || 0); setText('#market-feed', snapshot.market?.stream?.state || 'IEX'); setText('#market-ready', snapshot.market?.readiness?.executable ? 'READY' : 'WAITING'); setText('#market-policy', (snapshot.run?.execution_policy || $('#trade-policy')?.value || 'advisory').toUpperCase());
  renderCandles(snapshot.bars || {}); renderDecisionPane();
}

function renderCandles(bars) {
  const svg = $('#trade-chart'); if (!svg) return; svg.replaceChildren();
  const values = [...(bars.closed || []).slice(-72)]; if (bars.forming) values.push({...bars.forming, forming: true});
  $('#trade-chart-empty').hidden = values.length > 0; if (!values.length) return;
  const prices = values.flatMap((bar) => [Number(bar.high), Number(bar.low)]).filter(Number.isFinite); const low = Math.min(...prices); const high = Math.max(...prices); const range = high - low || 1;
  const left = 36, right = 70, top = 24, bottom = 40, width = 820 - left - right, height = 500 - top - bottom;
  for (let i = 0; i <= 5; i += 1) {
    const y = top + height * i / 5; const line = document.createElementNS(svg.namespaceURI, 'line'); line.setAttribute('x1', left); line.setAttribute('x2', 820 - right); line.setAttribute('y1', y); line.setAttribute('y2', y); line.setAttribute('class', 'grid-line'); svg.append(line);
    const label = document.createElementNS(svg.namespaceURI, 'text'); label.setAttribute('x', 820 - right + 8); label.setAttribute('y', y + 3); label.setAttribute('class', 'axis-label'); label.textContent = (high - range * i / 5).toFixed(2); svg.append(label);
  }
  const step = width / values.length; const bodyWidth = Math.max(2, Math.min(9, step * .56)); const yFor = (price) => top + (high - Number(price)) * height / range;
  values.forEach((bar, index) => {
    const open = Number(bar.open), close = Number(bar.close), barHigh = Number(bar.high), barLow = Number(bar.low); if (![open, close, barHigh, barLow].every(Number.isFinite)) return;
    const x = left + step * (index + .5); const className = bar.forming ? 'candle-forming' : close >= open ? 'candle-up' : 'candle-down';
    const wick = document.createElementNS(svg.namespaceURI, 'line'); wick.setAttribute('x1', x); wick.setAttribute('x2', x); wick.setAttribute('y1', yFor(barHigh)); wick.setAttribute('y2', yFor(barLow)); wick.setAttribute('class', `${className} candle-wick`); svg.append(wick);
    const rect = document.createElementNS(svg.namespaceURI, 'rect'); rect.setAttribute('x', x - bodyWidth / 2); rect.setAttribute('y', Math.min(yFor(open), yFor(close))); rect.setAttribute('width', bodyWidth); rect.setAttribute('height', Math.max(1, Math.abs(yFor(open) - yFor(close)))); rect.setAttribute('class', className); svg.append(rect);
  });
  const latest = Number(bars.latest_price); if (Number.isFinite(latest)) { const y = yFor(latest); const line = document.createElementNS(svg.namespaceURI, 'line'); line.setAttribute('x1', left); line.setAttribute('x2', 820 - right); line.setAttribute('y1', y); line.setAttribute('y2', y); line.setAttribute('class', 'price-line'); svg.append(line); }
}

function renderDecisionPane() {
  const host = $('#decision-content'); if (!host) return; host.replaceChildren(); const snapshot = APP.snapshot || {};
  if (APP.tradeTab === 'analysis') {
    const status = textNode('div', 'desk-status'); const left = document.createElement('div'); left.append(textNode('span', '', 'RUNTIME'), textNode('strong', '', t(snapshot.lifecycle || 'idle'))); const right = document.createElement('div'); right.append(textNode('span', '', 'PROVIDER CALLS'), textNode('strong', 'data', snapshot.run?.provider_calls || 0)); status.append(left, right); host.append(status);
    const decisions = [...(snapshot.recent_decisions || [])].reverse(); if (!decisions.length) { host.append(textNode('div', 'dark-empty', t('noDecision'))); return; }
    decisions.forEach((decision) => { const card = textNode('article', 'decision-card'); const head = textNode('div', 'decision-card-head'); head.append(textNode('strong', '', decision.strategy_id || 'SETUP'), textNode('span', '', shortTime(decision.occurred_at))); card.append(head, textNode('span', 'decision-branch', decision.branch || 'accepted'), textNode('p', '', decision.reason || 'Validated decision accepted.')); const strategy = (snapshot.strategies || []).find((item) => item.strategy_id === decision.strategy_id); if (strategy?.gate_trace) card.append(renderGateTrace(strategy.gate_trace)); host.append(card); });
  } else if (APP.tradeTab === 'strategies') {
    const strategies = snapshot.strategies || []; if (!strategies.length) { host.append(textNode('div', 'dark-empty', t('noStrategyState'))); return; }
    strategies.forEach((strategy) => { const item = textNode('article', 'dark-list-item'); const group = document.createElement('div'); group.append(textNode('strong', '', strategy.name), textNode('span', '', strategy.strategy_id)); item.append(group, textNode('small', '', String(strategy.state || '').toUpperCase()), textNode('p', '', strategy.accepted_reason || strategy.last_reason || '—')); host.append(item); });
  } else {
    const entries = [...(snapshot.open_trades || []), ...(snapshot.pending_entries || []), ...(snapshot.closed_trades || []).slice(-20).reverse()]; if (!entries.length) { host.append(textNode('div', 'dark-empty', t('noLedger'))); return; }
    entries.forEach((entry) => { const item = textNode('article', 'dark-list-item'); const group = document.createElement('div'); group.append(textNode('strong', '', `${entry.strategy_id || 'setup'} · ${entry.direction || 'paper'}`), textNode('span', '', `${entry.entry_price || '—'} → ${entry.exit_reason || entry.status || 'open'}`)); item.append(group, textNode('small', entry.paper_pnl && Number(entry.paper_pnl) < 0 ? 'risk' : 'profit', entry.paper_pnl ? formatMoney(entry.paper_pnl) : entry.quantity ? `${entry.quantity} sh` : '—')); host.append(item); });
  }
}

function renderGateTrace(trace) { const list = textNode('div', 'gate-list'); trace.forEach((gate) => { const row = textNode('div', `gate-row ${gate.status || ''}`); row.append(document.createElement('i'), textNode('span', '', gate.label || gate.gate), textNode('small', '', gate.status || 'waiting')); list.append(row); }); return list; }

async function renderStrategies(epoch) {
  const data = await api('/api/strategies'); if (epoch !== APP.routeEpoch) return; APP.strategyData = data; APP.strategyDraft = new Set(data.selected_ids); APP.strategyFilter = 'all';
  const sources = [...new Set(data.items.map((item) => item.source_id).filter(Boolean))].sort(); const tags = [...new Set(data.items.flatMap((item) => item.tags || []))].sort();
  const catalogHeadline = t('catalogHeadline').replace('21', String(data.counts.total));
  $('#page-content').innerHTML = `
    <section class="section-intro"><div><span class="eyebrow">21 BUILT-IN + LOCAL BUNDLES</span><h2>${catalogHeadline}</h2><p>${t('catalogLead')}</p></div></section>
    <section class="strategy-toolbar"><input class="search-input" id="strategy-search" type="search" placeholder="${t('search')}" aria-label="${t('search')}"><div class="filter-row" id="type-filters"></div><div class="selection-actions"><button class="button button-compact" id="select-all" data-config-control>${t('selectAll')}</button><button class="button button-compact" id="clear-all" data-config-control>${t('clear')}</button><button class="button button-primary button-compact" id="save-selection" data-config-control>${t('saveSelection')}</button></div></section>
    <div class="filter-row" style="margin-bottom:14px"><div class="field"><select id="source-filter" aria-label="${t('source')}"><option value="">${t('allSources')}</option></select></div><div class="field"><select id="tag-filter" aria-label="Tags"><option value="">${t('allTags')}</option></select></div></div>
    <div class="inline-notice" id="runtime-lock-note" hidden>${t('settingsLocked')}</div>
    <div class="strategy-summary"><span><strong id="shown-count">0</strong> ${t('shown')}</span><span><strong id="draft-count">${APP.strategyDraft.size}</strong> / ${data.counts.total} ${t('selected')}</span></div>
    <section class="strategy-catalog" id="strategy-catalog"></section>`;
  [['all','all'], ['builtIn','built-in'], ['custom','custom'], ['selectedOnly','selected']].forEach(([label, value]) => { const button = textNode('button', `filter-chip${value === 'all' ? ' active' : ''}`, t(label)); button.type = 'button'; button.dataset.filter = value; $('#type-filters').append(button); });
  sources.forEach((source) => { const option = textNode('option', '', source); option.value = source; $('#source-filter').append(option); }); tags.forEach((tag) => { const option = textNode('option', '', tag); option.value = tag; $('#tag-filter').append(option); });
  $('#strategy-search').addEventListener('input', renderCatalog); $('#source-filter').addEventListener('change', renderCatalog); $('#tag-filter').addEventListener('change', renderCatalog);
  $$('.filter-chip').forEach((button) => button.addEventListener('click', () => { APP.strategyFilter = button.dataset.filter; $$('.filter-chip').forEach((item) => item.classList.toggle('active', item === button)); renderCatalog(); }));
  $('#select-all').addEventListener('click', () => { APP.strategyDraft = new Set(data.items.map((item) => item.strategy_id)); renderCatalog(); }); $('#clear-all').addEventListener('click', () => { APP.strategyDraft.clear(); renderCatalog(); }); $('#save-selection').addEventListener('click', saveStrategySelection);
  renderCatalog(); updateLockedControls();
}

function renderCatalog() {
  const host = $('#strategy-catalog'); if (!host || !APP.strategyData) return; host.replaceChildren(); const query = ($('#strategy-search')?.value || '').trim().toLowerCase(); const source = $('#source-filter')?.value || ''; const tag = $('#tag-filter')?.value || '';
  const items = APP.strategyData.items.filter((item) => {
    const haystack = [item.name, item.strategy_id, ...(item.aliases || []), ...(item.tags || [])].join(' ').toLowerCase();
    return (!query || haystack.includes(query)) && (!source || item.source_id === source) && (!tag || (item.tags || []).includes(tag)) && (APP.strategyFilter === 'all' || APP.strategyFilter === 'custom' && item.custom || APP.strategyFilter === 'built-in' && !item.custom || APP.strategyFilter === 'selected' && APP.strategyDraft.has(item.strategy_id));
  });
  items.forEach((item) => {
    const card = textNode('article', `strategy-item${APP.strategyDraft.has(item.strategy_id) ? ' selected' : ''}`); const top = textNode('div', 'strategy-item-top'); const names = document.createElement('div'); names.append(textNode('h3', '', item.name), textNode('span', 'strategy-id', item.strategy_id)); const checkbox = document.createElement('input'); checkbox.type = 'checkbox'; checkbox.className = 'strategy-check'; checkbox.checked = APP.strategyDraft.has(item.strategy_id); checkbox.disabled = configurationLocked(); checkbox.setAttribute('aria-label', `${t('selected')} ${item.name}`); checkbox.addEventListener('change', () => { if (checkbox.checked) APP.strategyDraft.add(item.strategy_id); else APP.strategyDraft.delete(item.strategy_id); renderCatalog(); }); top.append(names, checkbox); card.append(top);
    const tags = textNode('div', 'strategy-tags'); (item.tags || []).slice(0, 4).forEach((value) => tags.append(textNode('span', 'tag', value))); if (item.custom) tags.prepend(textNode('span', 'tag custom', t('custom'))); card.append(tags, textNode('p', '', item.summary || 'Source-linked price-action setup.'));
    const foot = textNode('div', 'strategy-item-foot'); foot.append(textNode('span', 'strategy-source', item.source_id || t('source'))); const detail = textNode('button', 'button button-compact', t('details')); detail.type = 'button'; detail.addEventListener('click', () => openStrategy(item.strategy_id)); foot.append(detail); card.append(foot); host.append(card);
  });
  if (!items.length) host.append(textNode('div', 'empty-catalog', 'No setups match the current filters.'));
  setText('#shown-count', items.length); setText('#draft-count', APP.strategyDraft.size); updateLockedControls();
}

async function saveStrategySelection() {
  try { const data = await mutation('/api/strategies/selection', {strategy_ids: [...APP.strategyDraft]}); APP.strategyData = data; APP.strategyDraft = new Set(data.selected_ids); await refreshBootstrap(); renderCatalog(); toast(t('selectionSaved'), 'success'); }
  catch (error) { toast(error.message, 'error'); }
}

async function openStrategy(strategyId) {
  try {
    const item = await api(`/api/strategies/${encodeURIComponent(strategyId)}`); const content = $('#drawer-content'); content.replaceChildren();
    const head = textNode('header', 'drawer-head'); head.append(textNode('span', 'eyebrow', item.custom ? 'CUSTOM SETUP' : 'BUILT-IN SETUP'), textNode('h2', 'drawer-title', item.name), textNode('p', '', `${item.strategy_id} · ${item.source_id || ''}`)); head.querySelector('h2').id = 'drawer-title'; content.append(head);
    Object.entries(item.claims || {}).forEach(([field, claims]) => { if (!claims.length) return; const section = textNode('section', 'drawer-section'); section.append(textNode('h3', '', field.replaceAll('_', ' ').toUpperCase())); claims.forEach((claim) => { const block = textNode('div', 'claim'); block.append(textNode('span', 'claim-type', claim.declaration_type), textNode('p', '', claim.statement)); const pages = (claim.source_refs || []).map((ref) => `${ref.source_file || 'source'} p.${ref.page_start}${ref.page_end !== ref.page_start ? `–${ref.page_end}` : ''}`).join(' · '); block.append(textNode('small', '', pages)); section.append(block); }); content.append(section); });
    $('#drawer-backdrop').hidden = false; $('#detail-drawer').classList.add('open'); $('#detail-drawer').setAttribute('aria-hidden', 'false'); $('#drawer-close').focus();
  } catch (error) { toast(error.message, 'error'); }
}

function closeDrawer() { $('#detail-drawer').classList.remove('open'); $('#detail-drawer').setAttribute('aria-hidden', 'true'); $('#drawer-backdrop').hidden = true; }

async function renderCreateSetup(epoch) {
  const kit = APP.setupKit || await api('/api/setup-kit'); APP.setupKit = kit; if (epoch !== APP.routeEpoch) return; const prompt = kit.prompts[APP.locale] || kit.prompts['zh-CN'];
  $('#page-content').innerHTML = `
    <section class="section-intro"><div><span class="eyebrow">PDF → SOURCE-LINKED PACKET</span><h2>${t('createHeadline')}</h2><p>${t('createLead')}</p></div></section>
    <section class="setup-layout"><article class="card prompt-card"><header class="card-heading"><h3>${t('conversionPrompt')}</h3><span>${APP.locale}</span></header><div class="prompt-body"><pre class="prompt-text" id="setup-prompt"></pre><div class="prompt-actions"><button class="button button-dark" id="copy-prompt">${t('copyPrompt')}</button><button class="button" id="download-spec">${t('downloadSpec')}</button></div></div></article>
      <article class="card import-card"><header class="card-heading"><h3>${t('importBundle')}</h3><span>BUNDLE V1</span></header><label class="drop-zone" id="drop-zone"><input id="bundle-file" type="file" accept=".zip,.pa-setup.zip,application/zip" data-config-control><span class="drop-icon" aria-hidden="true">↓</span><strong>${t('chooseBundle')}</strong><span>${t('bundleLimits')}</span></label><div class="import-meta"><div class="import-meta-row"><span>FILE</span><strong id="bundle-name">${t('noFile')}</strong></div><button class="button button-primary" id="import-button" data-config-control disabled>${t('validateImport')}</button></div><div id="validation-result" hidden></div></article></section>
    <div class="security-note">${t('untrustedPdf')}</div><div class="inline-notice" id="runtime-lock-note" hidden>${t('settingsLocked')}</div>`;
  $('#setup-prompt').textContent = prompt; $('#copy-prompt').addEventListener('click', () => copyPrompt(prompt)); $('#download-spec').addEventListener('click', () => downloadSpec(kit));
  const input = $('#bundle-file'); const zone = $('#drop-zone'); input.addEventListener('change', () => chooseBundle(input.files?.[0])); zone.addEventListener('dragover', (event) => { event.preventDefault(); zone.classList.add('dragging'); }); zone.addEventListener('dragleave', () => zone.classList.remove('dragging')); zone.addEventListener('drop', (event) => { event.preventDefault(); zone.classList.remove('dragging'); chooseBundle(event.dataTransfer?.files?.[0]); }); $('#import-button').addEventListener('click', importBundle); updateLockedControls();
}

async function copyPrompt(prompt) { try { await navigator.clipboard.writeText(prompt); toast(t('copied'), 'success'); } catch { const area = document.createElement('textarea'); area.value = prompt; document.body.append(area); area.select(); document.execCommand('copy'); area.remove(); toast(t('copied'), 'success'); } }
function downloadSpec(kit) { const blob = new Blob([JSON.stringify(kit.specification, null, 2)], {type: 'application/json'}); const href = URL.createObjectURL(blob); const link = document.createElement('a'); link.href = href; link.download = kit.download_filename; link.click(); URL.revokeObjectURL(href); }
function chooseBundle(file) { APP.setupFile = file || null; setText('#bundle-name', file ? `${file.name} · ${(file.size / 1024 / 1024).toFixed(2)} MB` : t('noFile')); const button = $('#import-button'); if (button) button.disabled = !file || configurationLocked(); const result = $('#validation-result'); if (result) result.hidden = true; }
async function importBundle() {
  if (!APP.setupFile) return; const result = $('#validation-result'); result.hidden = false; result.className = 'validation-result'; result.textContent = t('loading');
  try { const value = await api('/api/setups/import', {method: 'POST', headers: {'Content-Type': 'application/zip', 'X-PA-Page-Token': PAGE_TOKEN}, body: APP.setupFile}); result.textContent = value.idempotent ? t('idempotent') : `${t('imported')} · ${value.bundle.setup_ids.length} setup`; toast(value.idempotent ? t('idempotent') : t('imported'), 'success'); APP.setupFile = null; const input = $('#bundle-file'); if (input) input.value = ''; setText('#bundle-name', t('noFile')); $('#import-button').disabled = true; await refreshBootstrap(); }
  catch (error) { result.className = 'validation-result error'; result.textContent = `${t('validationFailed')}: ${error.message}`; toast(error.message, 'error'); }
}

async function renderSettings(epoch) {
  const settings = await api('/api/settings'); APP.settings = settings; if (epoch !== APP.routeEpoch) return;
  $('#page-content').innerHTML = `
    <section class="section-intro"><div><span class="eyebrow">ENCRYPTED OWNER VAULT</span><h2>${t('settingsHeadline')}</h2><p>${t('settingsLead')}</p></div></section>
    <section class="card model-settings-bar"><div class="field"><span class="field-label">${t('activeProvider')}</span><div class="radio-group"><label class="radio-choice"><input type="radio" name="active-provider" value="deepseek" data-config-control>DeepSeek</label><label class="radio-choice"><input type="radio" name="active-provider" value="openai" data-config-control>OpenAI</label></div></div><div class="field"><label for="deepseek-thinking">${t('thinking')}</label><select id="deepseek-thinking" data-config-control><option value="enabled">Enabled</option><option value="disabled">Disabled</option></select></div><div class="field"><label for="openai-speed">${t('serviceTier')}</label><select id="openai-speed" data-config-control><option value="standard">Standard</option><option value="fast">Fast</option></select></div><button class="button button-dark" id="save-model" data-config-control>${t('saveModel')}</button></section>
    <div class="inline-notice" id="runtime-lock-note" hidden>${t('settingsLocked')}</div><section class="settings-grid" id="settings-grid"></section><p class="settings-disclaimer">Secrets are never written to browser storage, logs or HTTP responses. They are encrypted before durable storage. Saving clears every password field immediately.</p>`;
  $(`input[name="active-provider"][value="${settings.active_provider}"]`).checked = true; $('#deepseek-thinking').value = settings.models.deepseek.thinking; $('#openai-speed').value = settings.models.openai.speed; $('#save-model').addEventListener('click', saveModelSettings);
  renderCredentialCards(); updateLockedControls();
}

function renderCredentialCards() {
  const host = $('#settings-grid'); if (!host || !APP.settings) return; host.replaceChildren();
  [
    {id:'alpaca', name:'Alpaca', model:'BASIC IEX', body:t('alpacaBody'), fields:[['key_id',t('keyId')],['secret_key',t('secretKey')]]},
    {id:'deepseek', name:'DeepSeek', model:'V4 FLASH', body:t('deepseekBody'), fields:[['api_key',t('apiKey')]]},
    {id:'openai', name:'OpenAI', model:'LUNA / MAX', body:t('openaiBody'), fields:[['api_key',t('apiKey')]]},
  ].forEach((provider) => {
    const card = textNode('article', 'card settings-card'); const head = textNode('header', 'settings-card-head'); const lockup = textNode('div', 'provider-lockup'); lockup.append(textNode('span', 'provider-icon', provider.name.slice(0,2).toUpperCase())); const names = document.createElement('div'); names.append(textNode('h3', '', provider.name), textNode('span', '', provider.model)); lockup.append(names); const configured = Boolean(APP.settings.credentials[provider.id]); const status = textNode('span', `config-status${configured ? ' configured' : ''}`); status.append(document.createElement('i'), textNode('span', '', configured ? t('configured') : t('notConfigured'))); head.append(lockup, status); card.append(head);
    const body = textNode('form', 'settings-card-body'); body.autocomplete = 'off'; body.append(textNode('p', '', provider.body)); provider.fields.forEach(([field, label]) => { const wrap = textNode('div', 'field'); const inputId = `${provider.id}-${field}`; const labelEl = textNode('label', '', label); labelEl.htmlFor = inputId; const input = document.createElement('input'); input.id = inputId; input.name = field; input.type = 'password'; input.autocomplete = 'new-password'; input.spellcheck = false; input.dataset.configControl = ''; wrap.append(labelEl, input); body.append(wrap); });
    if (provider.id === 'alpaca' && APP.settings.alpaca_assets?.length) body.append(textNode('p', 'profit data', `${APP.settings.alpaca_assets.length}/6 assets validated · non-OTC · tradable`));
    const actions = textNode('div', 'settings-card-actions'); const save = textNode('button', 'button button-primary', t('saveCredentials')); save.type = 'submit'; save.dataset.configControl = ''; const clear = textNode('button', 'button button-danger', t('clearCredentials')); clear.type = 'button'; clear.dataset.configControl = ''; clear.addEventListener('click', () => clearProvider(provider.id)); actions.append(save, clear); body.append(actions); body.addEventListener('submit', (event) => saveProvider(event, provider)); card.append(body); host.append(card);
  });
}

async function saveModelSettings() { const payload = {active_provider: $('input[name="active-provider"]:checked').value, deepseek_reasoning: $('#deepseek-thinking').value, openai_speed: $('#openai-speed').value}; try { APP.settings = await mutation('/api/settings/model', payload); toast(t('saved'), 'success'); renderCredentialCards(); } catch (error) { toast(error.message, 'error'); } }
async function saveProvider(event, provider) { event.preventDefault(); const form = event.currentTarget; const payload = {provider: provider.id}; provider.fields.forEach(([field]) => { payload[field] = form.elements[field].value; }); try { await mutation('/api/settings/credentials', payload); APP.settings = await api('/api/settings'); toast(t('saved'), 'success'); renderCredentialCards(); } catch (error) { toast(error.message, 'error'); } finally { provider.fields.forEach(([field]) => { if (form.elements[field]) form.elements[field].value = ''; }); } }
async function clearProvider(provider) { try { await mutation('/api/settings/credentials/clear', {provider}); APP.settings = await api('/api/settings'); toast(t('cleared'), 'success'); renderCredentialCards(); updateLockedControls(); } catch (error) { toast(error.message, 'error'); } }

async function switchLanguage() {
  const locale = APP.locale === 'zh-CN' ? 'en-US' : 'zh-CN';
  try { await mutation('/api/preferences', {locale}); APP.locale = locale; document.documentElement.lang = locale; $('#language-toggle').textContent = locale === 'zh-CN' ? 'EN' : '中'; if (APP.bootstrap) APP.bootstrap.locale = locale; renderRoute(); }
  catch (error) { toast(error.message, 'error'); }
}

async function pollSnapshot() {
  try {
    const snapshot = await api('/api/snapshot'); updateShell(snapshot);
    if (activeLifecycle() && Date.now() >= APP.nextStepAt) {
      APP.nextStepAt = Date.now() + 15000;
      try { const stepped = await mutation('/api/step', {}); updateShell(stepped.snapshot); }
      catch (error) { toast(error.message, 'error'); APP.nextStepAt = Date.now() + 30000; }
    }
  }
  catch (error) { setText('#runtime-label', error.message); }
  finally { APP.pollTimer = window.setTimeout(pollSnapshot, 1000); }
}

document.addEventListener('click', (event) => { const link = event.target.closest('a[data-route]'); if (!link) return; const url = new URL(link.href, location.href); if (url.origin !== location.origin) return; event.preventDefault(); go(url.pathname); });
window.addEventListener('popstate', renderRoute);
$('#language-toggle').addEventListener('click', switchLanguage); $('#drawer-close').addEventListener('click', closeDrawer); $('#drawer-backdrop').addEventListener('click', closeDrawer); document.addEventListener('keydown', (event) => { if (event.key === 'Escape') closeDrawer(); });

async function boot() {
  try { await refreshBootstrap(); APP.snapshot = await api('/api/snapshot'); updateShell(); await renderRoute(); pollSnapshot(); }
  catch (error) { $('#page-content').replaceChildren(textNode('div', 'inline-notice', error.message)); toast(error.message, 'error'); }
}

boot();
