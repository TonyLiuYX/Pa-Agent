/* Local research workbench. Rendering accepts persisted summaries; it never
   infers an unknown capability, order fill, or market validation result. */
"use strict";
const $ = (id) => document.getElementById(id);
const escape = (value) => String(value ?? "—").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const names = {demo:"演示",live:"实时",replay:"回放",running:"运行中",ready:"就绪",paused:"已暂停",stopped:"已停止",connecting:"连接中",connected:"已连接",disconnected:"连接中断",source_error:"数据源错误",auth_required:"需要行情凭证",capacity_blocked:"容量阻断",candidate:"候选",formed:"形成",triggered:"触发",invalidated:"失效",expired:"过期",completed:"结束",complete:"完整",partial:"部分资料",unavailable:"不可用",unknown:"未知",pending:"等待报价",pending_entry:"待入场",pending_exit:"待退出",open:"持仓中",filled:"已成交",cancelled:"已取消",canceled:"已取消",rejected:"已拒绝",blocked:"已阻断",stale:"报价陈旧",not_run:"未运行",migration_unvalidated:"市场迁移未验证",satisfied:"满足",failed:"未满足",not_satisfied:"未满足",measured:"已观测",entry:"入场",exit:"退出",true:"是",false:"否"};
const text = value => names[typeof value==="string"?value.toLowerCase():value] ?? (value == null ? "未知" : String(value));
const money = value => Number.isFinite(Number(value)) && value != null ? Number(value).toLocaleString("en-US", {style:"currency", currency:"USD", maximumFractionDigits:2}) : "—";
const num = value => Number.isFinite(Number(value)) && value != null ? Number(value).toLocaleString("en-US", {maximumFractionDigits:2}) : "—";
const date = value => { if (!value) return "—"; const d = new Date(value); return Number.isNaN(d.getTime()) ? "时间未知" : d.toLocaleString("zh-CN", {timeZone:"America/New_York",hour12:false}) + " ET"; };
const badge = (value, type="") => `<span class="badge ${type}">${escape(text(value))}</span>`;
const collection = value => Array.isArray(value) ? {items:value,next_cursor:null} : (value && Array.isArray(value.items) ? value : {items:[],next_cursor:null});
Object.assign(names,{source_ended:"资料源已结束",processing_blocked:"处理受阻",halted:"停牌",same_batch_opposite_signals:"同批出现多空信号",position_already_open:"已有同品种持仓",pending_entry_exists:"已有待入场订单",entry_timeout:"入场报价等待超时",budget_below_one_share:"预算不足一股",daily_loss_limit:"达到当日亏损阈值",session_close_buffer:"进入日终退出窗口",max_hold:"达到最长持有时间",stop_loss:"触及止损阈值",take_profit:"触及目标阈值",origin_invalidated:"发起判断已失效",missing_quote:"缺少有效报价",runtime_paused:"运行已暂停",runtime_stopped:"运行已停止"});
Object.assign(names,{paused:"新入场已暂停",runtime_paused:"新入场已暂停",confirmed:"形成",version_mismatch:"版本不一致，未评分",invalid_annotations:"标注需要修正，未评分",invalid_predictions:"检测来源无效，未评分",no_committed_labels:"没有已提交标注",no_eligible_committed_labels:"没有可评价标注",committed_label_limit_exceeded:"标注数量超过评价容量上限",related_prediction_limit_exceeded:"相关检测数量超过评价容量上限",annotation_and_detection_rule_versions_must_agree:"标注、检测和当前规则版本需要一致",related_prediction_provenance_is_invalid:"相关检测缺少有效资料来源",synthetic_logic_only:"合成逻辑验证",insufficient_sample:"真实样本不足",target_not_met:"该切片未达到目标",target_met_on_this_slice:"该切片点估计达到目标"});
const state = {view:"overview",status:{},runEpoch:0,account:{},symbol:"",datasets:{detections:[],marketDetections:[],orders:[],fills:[],positions:[],coverage:[],labels:[],reports:[],decisions:[]},cursors:{},historyLoaded:{},selected:null,chart:null,candles:null,levels:[],replay:[],replayIndex:-1,replayCursor:null,refreshing:false,stream:null,connected:false,eventCursor:null,token:null,errors:[],barSignature:"",chartSymbol:""};
function staleRunError() {const error=new Error("运行已变更，旧运行的返回资料已忽略。请核对当前运行后再操作。");error.staleRun=true;return error;}
function annotationProvenance(status) {
  const source=status.source || {},kind=status.source_kind || (status.mode==="demo"?"synthetic":status.mode==="live"?"real_market":"unknown");
  return JSON.stringify([status.mode,kind,status.source_scope,typeof source==="string"?source:[source.id,source.name,source.feed,source.source_scope,source.market_scope,source.path,source.data_sha256,source.sha256]]);
}
function hasAnnotationDraft() {const form=$("label-form");return state.annotationDraftDirty && ["case_id","setup_id","symbol","venue","start","end","trigger_time","source","annotator","rule_version","review_note","candidate_start","candidate_end","formed_start","formed_end","triggered_start","triggered_end"].some(name=>form.elements[name]?.value.trim());}
function adoptStatus(status) {
  const changed=state.status.run_id && status.run_id && state.status.run_id!==status.run_id;
  if(changed){
    const reviewDraft=hasAnnotationDraft() && (state.draftNeedsReview || annotationProvenance(state.status)!==annotationProvenance(status));
    state.runEpoch++;clearTimeout(connectStream.timer);state.stream?.close();state.stream=null;
    for(const name of Object.keys(state.datasets))state.datasets[name]=[];
    state.account={};state.cursors={};state.historyLoaded={};state.collectionRequests={};state.watchlistBars={};state.selected=null;state.visibleDetections=[];state.visibleReplayDetections=[];state.errors=[];
    state.symbol="";state.barSignature="";state.chartSymbol="";drawLevels({});state.candles?.setData([]);
    $("chart-status").textContent="等待当前运行行情";$("chart-description").textContent="运行已变更，旧行情已清空。";
    for(const name of ["detections","orders","fills","labels","decisions"])$("more-"+name).hidden=true;
    state.replayRequest=(state.replayRequest || 0)+1;state.replay=[];state.replayIndex=-1;state.replayCursor=null;state.replaySnapshotSeq=null;state.replaySymbol="";resetReplayConclusions();
    $("replay-start").value="";$("replay-end").value="";$("show-conclusions").checked=false;$("show-existing-labels").checked=false;$("replay-detections").innerHTML="";$("replay-evidence").innerHTML="";$("evidence").innerHTML=empty("运行已变更，请选择当前运行的判断查看依据。");
    state.draftNeedsReview=Boolean(reviewDraft);$("label-run-change").hidden=!reviewDraft;
  }
  state.status=status;state.token=status.session_token || (changed?null:state.token);
  if(changed || state.eventCursor==null)state.eventCursor=Number.isInteger(status.outbox_cursor)?status.outbox_cursor:null;
  if(changed){renderStatus();renderAccount();renderDetections();renderResearch();renderLabels();renderReplay();connectStream();toast("已切换到新运行。旧记录已清空。"+(state.draftNeedsReview?"未提交草稿已保留，请重新确认资料出处。":hasAnnotationDraft()?"未提交草稿已保留。":""));}
}
function sourceScope() {
  const source=state.status.source || {}, identity=typeof source==="string"?source:[source.id,source.name,source.source_scope].filter(Boolean).join(" ");
  return state.status.source_scope || (identity.toLowerCase().includes("iex")?"IEX交易所覆盖":state.status.mode==="demo"?"合成演示资料":source.source_scope || source.market_scope || "资料范围未知");
}
function knownSourceKind() {return state.status.source_kind || (state.status.mode==="demo"?"synthetic":state.status.mode==="live"?"real_market":"unknown");}
function renderAnnotationSource() {
  const kind=knownSourceKind(),select=$("label-form").elements.source_kind,identity=annotationProvenance(state.status);
  if(!state.draftNeedsReview && state.annotationSourceIdentity!==identity){select.value=["synthetic","real_market"].includes(kind)?kind:"";state.annotationSourceIdentity=identity;}
  for(const option of select.options)option.disabled=kind!=="unknown"&&option.value!==kind;
  $("label-source-note").textContent=kind==="unknown"?"当前回放的资料来源未知。请依据真实出处明确选择合成或真实资料，再保存标注。":"资料类型与当前运行一致："+(kind==="synthetic"?"合成资料":"真实市场资料")+"。";
}
function syncCoverageClaim() {
  const form=$("label-form"),input=form.elements.complete_coverage;
  input.disabled=form.elements.disputed.checked || form.elements.data_eligible.value!=="true";
  if(input.disabled)input.checked=false;
}

async function api(path, options={}) {
  const epoch=state.runEpoch;
  const controller = new AbortController(); const timeout = setTimeout(() => controller.abort(), 10000);
  try {
    const response = await fetch(path, {...options, signal:controller.signal, credentials:"same-origin", headers:{Accept:"application/json", ...(options.body ? {"Content-Type":"application/json","X-Session-Token":state.token || ""} : {}), ...options.headers}});
    if(epoch!==state.runEpoch)throw staleRunError();
    if (!response.ok) { const detail = await response.json().catch(() => ({}));if(epoch!==state.runEpoch)throw staleRunError();const errors={session_token_required:"页面会话已失效，请刷新后重试。",invalid_request:"资料不符合要求，请检查时间、必填字段和当前运行的资料类型。",operation_failed:"本地服务未完成操作，请查看运行健康后重试。",not_found:"所需资料暂不可用。",invalid_origin:"请从当前本地服务重新打开工作台。"};throw new Error(errors[detail.error] || detail.error || `请求失败 (${response.status})`); }
    const result=await response.json();if(epoch!==state.runEpoch)throw staleRunError();return result;
  } finally { clearTimeout(timeout); }
}
async function waitForCommit(receipt) {
  if(!Number.isInteger(receipt?.event_seq))return;
  const runId=state.status.run_id,deadline=Date.now()+15000;
  while(Date.now()<deadline){
    const status=await api("/api/status");
    if(runId&&status.run_id!==runId)throw new Error("运行已变更，请查看保存状态后再操作。");
    if(Number.isInteger(status.processed_cursor)&&status.processed_cursor>=receipt.event_seq)return;
    await new Promise(resolve=>setTimeout(resolve,250));
  }
  throw new Error("等待保存完成；后台仍可能继续处理，请查看最新记录。系统不会自动再次提交。");
}
function toast(message) { $("toast").textContent=message; $("toast").hidden=false; clearTimeout(toast.timer); toast.timer=setTimeout(()=>$("toast").hidden=true,5500); }
function notice(message) { $("notice").textContent=message; $("notice").hidden=!message; }
function empty(message) { return `<p class="empty">${escape(message)}</p>`; }
function table(columns, rows, options={}) {
  if (!rows.length) return empty(options.empty || "当前没有记录。");
  return `<div class="table-wrap"><table><thead><tr>${columns.map(c=>`<th scope="col">${escape(c.label)}</th>`).join("")}</tr></thead><tbody>${rows.map((r,i)=>`<tr ${options.select ? `class="selectable ${state.selected === r ? "selected" : ""}" tabindex="0" data-${options.select==="replay"?"replay-":""}detection-index="${i}" aria-label="查看 ${escape(r.setup_id)} ${escape(text(r.state))} 的证据"` : ""}>${columns.map(c=>`<td class="${c.class || ""}">${c.render ? c.render(r) : escape(r[c.key])}</td>`).join("")}</tr>`).join("")}</tbody></table></div>`;
}
function metrics(target, rows) { $(target).innerHTML=rows.map(([label,value,note,negative])=>`<article class="metric"><span class="metric-label">${escape(label)}</span><strong class="metric-value ${negative ? "negative" : ""}">${escape(value)}</strong><p class="metric-note">${escape(note)}</p></article>`).join(""); }
function direction(row) { return row.direction === 1 || row.side === "buy" || row.side === "long" ? '<span class="direction-up">多头 ↑</span>' : row.direction === -1 || row.side === "sell" || row.side === "short" ? '<span class="direction-down">空头 ↓</span>' : "未知"; }
function statusBadge(value) { const normalized=typeof value==="string"?value.toLowerCase():value;return badge(value,["triggered","filled","complete","running","open",true].includes(normalized)?"good":["invalidated","rejected","stale","blocked","pending_exit","unavailable",false].includes(normalized)?"warn":""); }
function memoryMaintenanceSummary(value) {
  if(!value || typeof value!=="object" || Array.isArray(value))return "未报告";
  const label={ready:"待首次维护",completed:"完成",unsupported:"环境不支持",error:"错误"}[String(value.status || "").toLowerCase()] || "未知";
  return label+(Number.isInteger(value.attempts)&&value.attempts>=0?" · "+num(value.attempts)+"次":"");
}
function detectionRow(row) { return row.detection || row.payload?.detection || row; }
const detectionColumns=[{label:"Setup / 变体",render:r=>`<strong class="mono">${escape(r.setup_id)}</strong><br><span class="caption">${escape(r.variant_id || "base")}</span>`},{label:"品种",key:"symbol",class:"mono"},{label:"方向",render:direction},{label:"阶段",render:r=>statusBadge(r.state)},{label:"资料",render:r=>statusBadge(r.data_status)},{label:"截止时刻",render:r=>`<span class="mono">${escape(date(r.evidence_cutoff))}</span>`}];

function showView(view, focus=false) {
  if (!$("view-"+view)) view="overview";
  state.view=view;
  document.querySelectorAll(".view").forEach(el=>el.hidden=el.id!=="view-"+view);
  document.querySelectorAll(".nav-button").forEach(el=>{const active=el.dataset.view===view;el.classList.toggle("active",active);if(active)el.setAttribute("aria-current","page");else el.removeAttribute("aria-current");});
  history.replaceState(null,"","#"+view);
  if(focus) $("main").focus({preventScroll:true});
  if(view==="market"){loadBars();loadMarketDetections();}
}

function renderStatus() {
  const s=state.status, health=s.health || {}, scope=sourceScope();
  const runtimeState=(s.state || s.status || "unknown").toLowerCase();
  $("mode-badge").textContent=text(s.mode);$("source-badge").textContent=scope;$("footer-scope").textContent=scope+"。";
  $("run-id").textContent=s.run_id || "等待运行";$("runtime-state").textContent=text(runtimeState);$("runtime-state").className="badge "+(runtimeState==="running"?"good":"");
  $("last-event").textContent="最新资料 "+date(s.last_event_time || s.last_arrival_time || health.last_received_at);
  $("received-count").textContent=num(s.event_count ?? s.received_events ?? s.stats?.events ?? health.events_processed ?? s.last_seq);
  $("detection-count").textContent=s.detection_count ?? s.stats?.detections ?? `${state.datasets.detections.length}${state.cursors.detections!=null?"+":""}`;
  $("order-count").textContent=s.order_count ?? s.stats?.orders ?? `${state.datasets.orders.length}${state.cursors.orders!=null?"+":""}`;
  $("commit-count").textContent=num(s.committed_events ?? s.last_seq ?? s.processed_seq ?? s.processed_cursor);
  $("model-version").textContent=s.execution_model_version || state.account.execution_version || "1.0.0";
  $("pause").hidden=runtimeState!=="running";$("resume").hidden=runtimeState!=="paused";$("stop").disabled=runtimeState==="stopped";
  const watchlist=s.watchlist || s.symbols || s.config?.watchlist || [];
  const symbols=watchlist.map(x=>typeof x==="string"?x:x.symbol).filter(Boolean);
  if (!state.symbol || !symbols.includes(state.symbol)) state.symbol=symbols[0] || state.datasets.detections[0]?.symbol || "";
  $("symbol").innerHTML=(symbols.length?symbols:[state.symbol].filter(Boolean)).map(x=>`<option ${x===state.symbol?"selected":""}>${escape(x)}</option>`).join("");
  const coverageSymbol=$("coverage-symbol").value;
  $("coverage-symbol").innerHTML='<option value="">观察名单汇总</option>'+symbols.map(x=>`<option ${x===coverageSymbol?"selected":""}>${escape(x)}</option>`).join("");
  $("watchlist").innerHTML=symbols.length?symbols.map(symbol=>{const last=state.watchlistBars?.[symbol];return `<button class="symbol-card" data-symbol="${escape(symbol)}"><strong>${escape(symbol)}</strong><span class="caption">${last?"最近完成K线 "+escape(money(last.close)):"尚无完成K线"}</span>${last?`<span class="caption">${escape(date(last.time))}</span>`:""}<span class="caption">${escape(scope)}</span></button>`;}).join(""):empty("运行尚未提供观察名单。");
  const elapsed=health.processing_ms ?? health.p95_feed_ms;
  const entries=[["数据连接",text(health.connection || s.connection_state || (health.source_ended?"资料源已结束":runtimeState==="disconnected"?"disconnected":["demo","replay"].includes(s.mode)?"读取记录流":"unknown"))],["队列积压",num(health.queue_depth ?? s.queue_depth)],["P95识别耗时",elapsed==null?"未知":num(elapsed)+" ms"],["日志积压",num(health.journal_backlog)],["内存维护",memoryMaintenanceSummary(health.memory_maintenance)],["运行错误",health.error || "无"],["事件序号",num(s.last_seq ?? s.processed_seq ?? s.processed_cursor)]];
  $("health").innerHTML=entries.map(([k,v])=>`<dt>${escape(k)}</dt><dd${k==="内存维护"?' id="memory-maintenance-state" aria-live="polite"':""}>${escape(v)}</dd>`).join("");
  const maintenance=health.memory_maintenance;
  $("memory-maintenance-details").hidden=!maintenance || typeof maintenance!=="object" || Array.isArray(maintenance);
  $("memory-maintenance-json").textContent=maintenance?JSON.stringify(maintenance,null,2):"";
  $("health-badge").textContent=text(health.state || health.status || (health.error?"source_error":health.source_ended?"资料源已结束":"unknown"));
  $("updated-at").textContent="更新 "+new Date().toLocaleTimeString("zh-CN",{hour12:false});
  renderAnnotationSource();
}
function renderAccount() {
  const a=state.account, equity=a.equity ?? a.net_liquidation, cash=a.available_cash ?? a.available ?? a.cash, pnl=a.unrealized_pnl ?? a.unrealized;
  const occupied=a.reserved!=null&&a.locked!=null?Number(a.reserved)+Number(a.locked):a.reserved ?? a.reserved_cash ?? a.locked;
  const rows=[["账户净值",money(equity),"美元 · 当前资料估值"],["可用资金",money(cash),"未占用的模拟资金"],["资金占用",money(occupied),"含待入场及虚拟空头担保"],["未实现盈亏",money(pnl),a.valuation_stale?"估值资料陈旧":"按最新有效行情估值",Number(pnl)<0]];
  metrics("account-metrics",rows);metrics("overview-metrics",rows);
  const times=Object.values(a.valuation_times || {}).sort();
  $("valuation-time").textContent="估值时刻 "+date(a.valuation_time || a.updated_at || times.at(-1))+(a.valuation_stale?" · 资料陈旧":"");
  $("positions").innerHTML=table([{label:"品种",key:"symbol",class:"mono"},{label:"方向",render:direction},{label:"数量",render:r=>num(r.quantity ?? r.qty)},{label:"入场价格",render:r=>money(r.entry_price)},{label:"当前价格",render:r=>money(r.mark_price ?? r.last_price)},{label:"状态",render:r=>statusBadge(r.status || r.state || "open")},{label:"发起规则",render:r=>escape(r.setup_id || r.origin?.setup_id || r.profile?.setup_id || r.detection_id || "—")},{label:"退出原因",render:r=>escape((r.exit_reasons || r.reasons || []).join("、")),class:"wrap"}],state.datasets.positions,{empty:"账户当前没有持仓。触发后的下一条有效报价才能产生模拟成交。"});
  $("orders").innerHTML=table([{label:"订单",render:r=>`<span class="mono">${escape(r.order_id || r.id)}</span>`},{label:"品种",key:"symbol",class:"mono"},{label:"方向",render:direction},{label:"目的",render:r=>escape(text(r.purpose || r.kind || r.intent))},{label:"状态",render:r=>statusBadge(r.status || r.state)},{label:"数量",render:r=>num(r.quantity ?? r.qty)},{label:"提交时刻",render:r=>escape(date(r.submitted_at || r.created_at || r.decision_time))},{label:"原因",render:r=>escape((r.reasons || r.exit_reasons || []).map(text).join("、") || text(r.cancel_reason || r.reason)),class:"wrap"}],state.datasets.orders,{empty:"当前没有模拟订单。缺必要资料或信号冲突时会记录阻断原因。"});
  $("fills").innerHTML=table([{label:"成交",render:r=>`<span class="mono">${escape(r.fill_id || r.id)}</span>`},{label:"品种",key:"symbol",class:"mono"},{label:"数量",render:r=>num(r.quantity ?? r.qty)},{label:"成交价格",render:r=>money(r.price ?? r.fill_price)},{label:"滑点",render:r=>money(r.slippage ?? r.slippage_amount ?? (r.slippage_cents!=null?r.slippage_cents/100:null))},{label:"费用",render:r=>money(r.fee ?? r.fee_amount ?? r.commission ?? (r.fee_cents!=null?r.fee_cents/100:null))},{label:"成交时刻",render:r=>escape(date(r.filled_at || r.timestamp || r.event_time || r.market_time))},{label:"执行模型",render:r=>escape((r.execution_model || "")+" "+(r.execution_model_version || r.execution_version || r.model_version || state.status.execution_model_version || "未知"))}],state.datasets.fills,{empty:"当前没有模拟成交。系统不会用断线前的价格补造成交。"});
}
function renderDetections() {
  const all=state.datasets.detections;
  $("overview-detections").innerHTML=table(detectionColumns,all.slice(0,7),{empty:"尚未产生判断。候选、形成与触发会分别记录。"});
  const stage=$("stage-filter").value;
  const rows=state.datasets.marketDetections.filter(d=>(!state.symbol||d.symbol===state.symbol)&&(stage==="all"||d.state===stage));
  state.visibleDetections=rows;
  $("detections").innerHTML=table(detectionColumns.filter(c=>c.key!=="symbol"),rows,{select:true,empty:"当前筛选中没有识别事件。可以切换品种或阶段。"});
  if(state.selected) renderEvidence(state.selected);
}
function renderEvidence(r,target="evidence") {
  const conditions=r.conditions || [], missing=r.missing || [], captured=r.anchor?.captures || {};
  $(target).innerHTML=`<div class="evidence-header"><div><div class="evidence-title">${escape(r.setup_id)} / ${escape(r.variant_id || "base")}</div><span class="caption">${escape(r.symbol)} · ${escape(r.scale_seconds)}秒 · ${escape(text(r.state))}</span></div>${direction(r)}</div><h3>观察截止</h3><p class="mono">${escape(date(r.evidence_cutoff))}</p><p class="caption">识别收到时刻 ${escape(date(r.identified_at))}</p>${missing.length?`<p class="evidence-missing">缺少资料：${missing.map(escape).join("、")}</p>`:""}<h3>逐条件结果</h3><ul class="evidence-conditions">${conditions.length?conditions.map(c=>{const raw=c.passed ?? c.result ?? c.met;const pass=raw==="satisfied"?true:raw==="not_satisfied"||raw==="failed"?false:raw;const observed=c.actual ?? c.value, expected=c.expected ?? c.threshold;return `<li><span class="condition-icon ${pass===true?"":"unknown"}">${pass===true?"✓":pass===false?"×":"?"}</span><div><strong>${escape(c.feature || c.condition || c.name || c.stage)}</strong><p>${escape(c.stage || "")} · ${escape(text(pass))}${observed!==undefined?" · 观测 "+escape(typeof observed==="object"?JSON.stringify(observed):observed):""}${c.reason?" · "+escape(c.reason):""}${c.operation?" · "+escape(c.operation)+" "+escape(typeof expected==="object"?JSON.stringify(expected):expected):""}</p>${c.observed_at?`<p>观察时刻 ${escape(date(c.observed_at))}</p>`:""}${c.source_ids?.length||c.provenance?`<details><summary class="caption">条件来源与证据</summary><p>${escape(c.provenance || "出处未提供")}</p><pre class="event-json">${escape(JSON.stringify(c.source_ids || [],null,2))}</pre></details>`:""}</div></li>`;}).join(""):empty("此事件没有提供逐条件明细。")}</ul>${Object.keys(captured).length?`<details><summary class="caption">关键位与结构记录</summary><pre class="event-json">${escape(JSON.stringify(captured,null,2))}</pre></details>`:""}<h3>规则与数据版本</h3><dl class="detail-list"><dt>来源版本</dt><dd>${escape(r.source_version)}</dd><dt>规则版本</dt><dd>${escape(r.rule_version)}</dd><dt>市场状态</dt><dd>${escape(text(r.market_status))}</dd><dt>数据状态</dt><dd>${escape(text(r.data_status))}</dd></dl><p><a class="caption" href="/library/setups/${encodeURIComponent(r.setup_id)}.html" target="_blank" rel="noopener">查看原始规则与来源 ↗</a></p>`;
  if(target==="evidence")drawLevels(captured);
}
function drawLevels(captures) {
  if(!state.candles)return;
  state.levels.forEach(l=>state.candles.removePriceLine(l));state.levels=[];
  for(const [key,value] of Object.entries(captures)){
    if(/price|level|high|low|vwap|stop|target|risk/i.test(key)&&typeof value==="number"&&value>0&&state.levels.length<8){state.levels.push(state.candles.createPriceLine({price:value,color:"#a87861",lineWidth:1,lineStyle:2,axisLabelVisible:true,title:key}));}
  }
}
function renderResearch() {
  let rows=state.datasets.coverage;
  const symbol=$("coverage-symbol").value;
  if(symbol)rows=rows.map(r=>{
    const scoped=r.per_symbol?.[symbol];if(!scoped)return r;
    const gate=r.auto_simulate,eligible=scoped.execution_eligible;
    const automatic=gate===false || eligible===false?false:gate===undefined || gate===true?eligible ?? "unknown":"unknown";
    return {...r,recognition_eligible:scoped.recognition_eligible,auto_simulate:automatic,missing_capabilities:scoped.missing};
  });
  $("coverage").innerHTML=table([{label:"Setup / 变体",render:r=>`<span class="mono">${escape(r.setup_id || (r.rule_id || r.key)?.split(":")[0])}</span><br><span class="caption">${escape(r.variant_id || (r.rule_id || r.key)?.split(":")[1] || "base")}</span>`},{label:"可识别",render:r=>statusBadge(r.recognizable ?? r.can_recognize ?? r.recognition_eligible ?? "unknown")},{label:"自动模拟",render:r=>statusBadge(r.auto_simulate ?? r.can_execute ?? r.execution_eligible ?? "unknown")},{label:"缺少資料",render:r=>escape((r.missing || r.missing_capabilities || r.required_missing || []).join("、") || ((r.missing || r.missing_capabilities || r.required_missing)?"无":"未提供")),class:"wrap"},{label:"来源范围",render:r=>escape(r.source_scope || r.scope || state.status.source_scope || (state.status.mode==="demo"?"合成资料":"未知"))},{label:"市场迁移",render:r=>escape(text(r.market_status || r.migration_status || "migration_unvalidated"))}],rows,{empty:"运行尚未提供规则能力矩阵。"});
  const reports=state.datasets.reports;
  $("reports").innerHTML=reports.length?reports.map(renderReport).join(""):empty("尚未生成评价报告。真实标签不足时，市场验证保持未运行。");
  $("validation-badge").textContent=state.status.market_validation && state.status.market_validation!=="not_run" ? "按切片验证，请查看报告" : "市场验证未运行";
  const a=state.account;
  $("execution-summary").innerHTML=`<dl class="detail-list"><dt>执行模型</dt><dd>${escape(a.execution_model || "未知")} / ${escape(a.execution_version || "未知")}</dd><dt>已实现盈亏</dt><dd>${escape(money(a.realized_pnl))}</dd><dt>累计费用</dt><dd>${escape(money(a.fees))}</dd><dt>当日新入场</dt><dd>${a.daily_entry_blocked===true?"已因风险阈值停止":a.daily_entry_blocked===false?"未因当日亏损阈值阻断":"未知"}</dd></dl><p class="caption">账户结果属于独立实验执行模型；识别正确性和来源策略表现另行验证。</p>`;
  $("decisions").innerHTML=table([{label:"品种",key:"symbol",class:"mono"},{label:"发起判断",key:"detection_id",class:"mono"},{label:"执行决定",render:r=>escape(text(r.reason || r.action || r.status)),class:"wrap"},{label:"事件序号",render:r=>num(r.event_seq)}],state.datasets.decisions,{empty:"尚无执行阻断记录。"});
}
function renderReport(report) {
  const score=report.recognition_evaluation || (report.kind==="recognition_evaluation"?report:null);
  let content="";
  if(score){
    const groups=score.metrics?.groups || [],stages=[["candidate","候选"],["confirmed","形成"],["triggered","触发"]],percent=value=>value==null?"未知":(Number(value)*100).toFixed(1)+"%";
    content=`<p class="evaluation-state">${badge(score.status==="completed"?"评分已完成":score.status,score.status==="completed"?"blue":"warn")} · 已提交标注 ${escape(score.label_count ?? 0)} · 可评价 ${escape(score.eligible_label_count ?? "未知")}</p>${score.reason?`<p class="evaluation-reason">${escape(text(score.reason))}</p>`:""}${score.version_mismatches?.length?`<p class="evaluation-reason">${escape(score.version_mismatches.length)}项规则版本不一致；修正并复核后再评分。</p>`:""}`;
    if(groups.length){
      const rows=groups.flatMap(group=>stages.map(([key,label])=>({...group,stage_label:label,stats:group[key] || {}})));
      content+=`<h3>三阶段事件匹配</h3><p class="caption">按 Setup、变体、资料、切分、品种、场所和环境分别报告；未定义的精确率或召回率保持未知。</p>`+table([{label:"评价范围",render:r=>`<span class="mono">${escape(r.setup_id)} / ${escape(r.variant_id)}</span><br><span class="caption">${escape(r.symbol)} · ${escape(r.venue)} · ${escape(r.source_kind==="synthetic"?"合成":"真实")}<br>${escape(r.split)} · ${escape(r.regime)}</span>`},{label:"阶段",key:"stage_label"},{label:"TP",render:r=>num(r.stats.tp)},{label:"FP",render:r=>num(r.stats.fp)},{label:"FN",render:r=>num(r.stats.fn)},{label:"精确率",render:r=>escape(percent(r.stats.precision))},{label:"召回率",render:r=>escape(percent(r.stats.recall))},{label:"验证范围",render:r=>escape(text(r.acceptance)),class:"wrap"}],rows,{empty:"尚无评价切片。"});
    }else content+=`<p class="caption">${score.status==="not_run"?"尚未运行识别评分；无标签时不会显示零误报或满分。":"当前状态无法评分，三阶段指标保持未知。"}</p>`;
    if(score.disputed_cases?.length||score.data_ineligible_cases?.length)content+=`<p class="caption">排除争议 ${escape(score.disputed_cases?.length || 0)} 个，必要资料不足 ${escape(score.data_ineligible_cases?.length || 0)} 个。</p>`;
  }
  return `<article class="report-card"><strong>${escape(report.title || score?.title || report.name || report.kind || report.report_type || "运行报告")}</strong><p>${escape(report.summary || report.scope || score?.scope || "按资料与运行版本记录，详见数据。")}</p>${content}<details><summary>查看完整报告</summary><pre class="event-json">${escape(JSON.stringify(report,null,2))}</pre></details></article>`;
}
function renderLabels() {
  const showing=$("show-existing-labels").checked;$("labels").hidden=!showing;$("more-labels").hidden=!showing || state.cursors.labels==null;
  $("labels").innerHTML=table([{label:"案例",key:"case_id",class:"mono"},{label:"Setup",key:"setup_id",class:"mono"},{label:"品种",key:"symbol",class:"mono"},{label:"判断",render:r=>escape(r.positive?"正例":"负例")},{label:"资料类型",render:r=>escape(r.source_kind==="real_market"?"真实市场":"合成")},{label:"标注者",key:"annotator"},{label:"开始",render:r=>escape(date(r.start))},{label:"争议",render:r=>escape(r.disputed?"待复核":"无")}],state.datasets.labels,{empty:"尚未保存事件标注。正例和困难负例均需独立标注。"});
}
async function loadCollection(name, append=false) {
  const epoch=state.runEpoch;
  state.collectionRequests ??={};const request=(state.collectionRequests[name] || 0)+1;state.collectionRequests[name]=request;
  const cursor=append?state.cursors[name]:null;
  const suffix=cursor!=null?`&before=${encodeURIComponent(cursor)}`:name==="orders"?"&before=9223372036854775807":"";
  const topic=name==="marketDetections"?"detections":name;const symbol=name==="marketDetections"?`&symbol=${encodeURIComponent(state.symbol)}`:"";
  const response=await api(`/api/${topic}?limit=${name==="coverage"?200:50}${suffix}${symbol}`);
  if(epoch!==state.runEpoch || request!==state.collectionRequests[name])return;
  let group=collection(response);
  if(name==="coverage"&&!Array.isArray(response)&&!response.items){group={items:Object.entries(response).filter(([,v])=>v&&typeof v==="object").map(([key,v])=>({key,...v})),next_cursor:null};}
  if(name==="reports"&&!Array.isArray(response)&&!response.items)group={items:[response],next_cursor:null};
  const rows=["detections","marketDetections"].includes(name)?group.items.map(detectionRow):group.items;
  const combined=append?[...state.datasets[name],...rows]:state.historyLoaded[name]?[...rows,...state.datasets[name]]:rows;
  const unique=new Map();for(const row of combined){const key=name==="orders"?row.order_id ?? row.record_id:name==="labels"?row.case_id ?? row.record_id:row.record_id ?? JSON.stringify(row);if(!unique.has(key))unique.set(key,row);}
  state.datasets[name]=[...unique.values()];if(["detections","marketDetections","orders","fills","decisions","labels"].includes(name))state.datasets[name].sort((a,b)=>(b.record_id||0)-(a.record_id||0));
  if(append||!state.historyLoaded[name])state.cursors[name]=group.items.length && group.next_cursor!==cursor && group.next_cursor!==0 ? group.next_cursor : null;
  if(append)state.historyLoaded[name]=true;
  const more=name==="detections"?null:$("more-"+(name==="marketDetections"?"detections":name));if(more)more.hidden=state.cursors[name]==null || name==="labels"&&!$("show-existing-labels").checked;
}
async function refresh(full=true) {
  if(state.refreshing)return;state.refreshing=true;
  try {
    const status=await api("/api/status");adoptStatus(status);const epoch=state.runEpoch;state.connected=true;
    const requests=[api("/api/account").then(a=>{if(epoch===state.runEpoch)state.account=a;}),loadWatchlistBars(),loadCollection("detections"),loadCollection("orders"),loadCollection("positions"),loadCollection("fills"),loadCollection("decisions")];
    if(full||state.view==="research")requests.push(loadCollection("coverage"),loadCollection("reports"));
    if(full||["research","replay"].includes(state.view))requests.push(loadCollection("labels"));
    const results=await Promise.allSettled(requests);if(epoch!==state.runEpoch)return;state.errors=results.filter(r=>r.status==="rejected"&&!r.reason.staleRun).map(r=>r.reason.message);
    renderStatus();renderAccount();renderDetections();renderResearch();renderLabels();
    $("connection-label").textContent=state.stream?.readyState===EventSource.OPEN?"实时更新":"定时刷新";$("connection-dot").className="status-dot good";
    notice(state.errors.length?"部分资料暂时无法读取："+[...new Set(state.errors)].join("；"):"");
    if(state.view==="market") await Promise.all([loadBars(),loadMarketDetections()]);
  }catch(error){if(error.staleRun)return;state.connected=false;if(window.EventSource)$("reconnect-stream").hidden=false;$("connection-label").textContent="连接中断";$("connection-dot").className="status-dot warn";notice("无法读取本地运行状态。请确认后台仍在运行；已展示资料保留原时间。 "+error.message);}
  finally{state.refreshing=false;}
}
async function loadWatchlistBars() {
  const epoch=state.runEpoch;
  const symbols=(state.status.watchlist || state.status.symbols || []).map(row=>typeof row==="string"?row:row.symbol).filter(Boolean);
  if(!state.watchlistBars)state.watchlistBars={};
  const results=await Promise.allSettled(symbols.map(async symbol=>{const row=collection(await api(`/api/bars?symbol=${encodeURIComponent(symbol)}&limit=1`)).items.at(-1),event=row?.event || row,data=event?.payload || event;if(epoch===state.runEpoch)state.watchlistBars[symbol]=data&&Number.isFinite(Number(data.close))?{close:data.close,time:event.event_time || event.end || event.time || data.end || data.time}:null;}));
  if(epoch!==state.runEpoch)return;const errors=results.filter(row=>row.status==="rejected"&&!row.reason.staleRun);if(errors.length)throw new Error("部分观察品种的完成K线暂不可读取；保留原资料时间。");
}
async function loadMarketDetections(append=false) {
  if(!state.symbol)return;
  const epoch=state.runEpoch;try{await loadCollection("marketDetections",append);if(epoch===state.runEpoch)renderDetections();}catch(error){if(!error.staleRun)toast("判断读取失败："+error.message);}
}
async function loadBars() {
  if(!state.symbol)return;
  const epoch=state.runEpoch,symbol=state.symbol;
  try {
    const rows=collection(await api(`/api/bars?symbol=${encodeURIComponent(symbol)}&limit=300`)).items;if(epoch!==state.runEpoch || symbol!==state.symbol)return;
    const candles=new Map();for(const item of rows){const row=item.event || item;const b=row.payload || row;const time=Date.parse(row.event_time || row.end || row.time || b.end || b.time)/1000;if(Number.isFinite(time)&&[b.open,b.high,b.low,b.close].every(x=>typeof x==="number"&&Number.isFinite(x)))candles.set(time,{time,open:b.open,high:b.high,low:b.low,close:b.close});}
    const data=[...candles.values()].sort((a,b)=>a.time-b.time);
    $("chart-symbol").textContent=state.symbol;$("chart-status").textContent=data.length?`${data.length} 根已完成K线`:"等待完整K线";
    $("chart-description").textContent=data.length?`最近K线结束于 ${date(new Date(data.at(-1).time*1000).toISOString())}。行情范围：${sourceScope()}。`:"没有可绘制的完整K线。没有收到分钟柱不代表零成交。";
    if(!window.LightweightCharts){$("market-chart").innerHTML=empty("本地图表文件无法载入。仍可查看下方的事件与证据。");return;}
    if(!state.chart){const element=$("market-chart");state.chart=LightweightCharts.createChart(element,{width:element.clientWidth,height:element.clientHeight,layout:{background:{type:"solid",color:"#ffffff"},textColor:"#677e94",fontFamily:"Menlo, monospace",fontSize:10,attributionLogo:true},grid:{vertLines:{color:"#eef3f7"},horzLines:{color:"#eef3f7"}},rightPriceScale:{borderColor:"#d6e2ec"},timeScale:{borderColor:"#d6e2ec",timeVisible:true,secondsVisible:false,tickMarkFormatter:(time,type)=>new Intl.DateTimeFormat("zh-CN",{timeZone:"America/New_York",...(type<=2?{month:"2-digit",day:"2-digit"}:{hour:"2-digit",minute:"2-digit",hour12:false})}).format(new Date(Number(time)*1000))},localization:{locale:"zh-CN",timeFormatter:time=>date(new Date(Number(time)*1000).toISOString())}});state.candles=state.chart.addSeries(LightweightCharts.CandlestickSeries,{upColor:"#25816d",downColor:"#ac7561",wickUpColor:"#25816d",wickDownColor:"#ac7561",borderVisible:false});new ResizeObserver(()=>{if(element.clientWidth&&element.clientHeight)state.chart.resize(element.clientWidth,element.clientHeight);}).observe(element);}
    const signature=state.symbol+":"+JSON.stringify(data);
    if(signature!==state.barSignature){state.candles.setData(data);if(state.chartSymbol!==state.symbol){state.chart.timeScale().fitContent();state.chartSymbol=state.symbol;}state.barSignature=signature;}
    if(state.selected?.symbol===state.symbol)drawLevels(state.selected.anchor?.captures || {});
  }catch(error){if(error.staleRun || epoch!==state.runEpoch)return;$("chart-status").textContent="行情读取失败";$("chart-description").textContent=error.message;}
}
function connectStream() {
  if(!window.EventSource){$("reconnect-stream").hidden=true;return;}
  state.stream?.close();
  const epoch=state.runEpoch,stream=new EventSource("/api/events/stream"+(state.eventCursor!=null?"?after="+encodeURIComponent(state.eventCursor):""));state.stream=stream;
  const active=()=>epoch===state.runEpoch && state.stream===stream;
  const update=event=>{if(!active())return;if(event.lastEventId)state.eventCursor=event.lastEventId;clearTimeout(connectStream.timer);connectStream.timer=setTimeout(()=>{if(active())refresh(false);},350);};
  stream.onmessage=update;
  ["commit","event","update","snapshot","runtime","committed"].forEach(name=>stream.addEventListener(name,update));
  stream.onopen=()=>{if(!active())return;$("reconnect-stream").hidden=true;$("connection-label").textContent="实时更新";$("connection-dot").className="status-dot good";};
  stream.onerror=()=>{if(!active())return;$("reconnect-stream").hidden=false;$("connection-label").textContent="定时刷新 · 推送重连中";$("connection-dot").className="status-dot warn";};
}
async function control(action) {
  const button=$(action);button.disabled=true;
  try{const receipt=await api("/api/runtime/"+action,{method:"POST",body:"{}"});toast("操作处理中，请稍候。");await waitForCommit(receipt);toast({pause:"已暂停新入场并取消等待入场；报价、识别和已有持仓退出继续处理。",resume:"已恢复新入场。报价和已有持仓退出持续处理。",stop:"已停止采集和运行。持久化记录已保留。"}[action]);await refresh();}catch(error){toast(error.message);}finally{button.disabled=false;}
}
function replayEvent(row) {return row?.event || row?.payload?.event || row?.payload || row?.data || row;}
function replayKnownTime(row) {const event=replayEvent(row);return row?.known_at || event?.arrival_time || event?.at || row?.recorded_at || row?.at || event?.event_time;}
function resetReplayConclusions() {
  state.replayDetectionRequest=(state.replayDetectionRequest || 0)+1;state.replayDetections=[];state.replayDetectionNext=0;state.replayDetectionHasMore=true;state.replayDetectionBusy=false;state.replayDetectionCapacityBlocked=false;state.replayDetectionReadThrough=null;state.replaySelected=null;
}
async function loadReplay(append=false) {
  const epoch=state.runEpoch;
  const start=$("replay-start").value.trim(),end=$("replay-end").value.trim(),offset=/([+-]\d\d:\d\d|Z)$/;
  for(const value of [start,end])if(value&&(!offset.test(value)||Number.isNaN(Date.parse(value))))throw new Error("回放范围需要有效ISO时间及明确时区。");
  if(start&&end&&Date.parse(start)>Date.parse(end))throw new Error("回放开始时间不能晚于结束时间。");
  if(!append){state.replaySnapshotSeq=state.status.processed_cursor;resetReplayConclusions();}
  const after=append?state.replayCursor:null;
  const query=new URLSearchParams({limit:"100"});if(after!=null)query.set("after",after);if(start)query.set("start",start);if(end)query.set("end",end);
  const request=state.replayRequest=(state.replayRequest || 0)+1,result=collection(await api("/api/events?"+query));if(epoch!==state.runEpoch || request!==state.replayRequest)return;
  const rows=result.items.filter(row=>!Number.isInteger(state.replaySnapshotSeq)||!Number.isInteger(row.seq)||row.seq<=state.replaySnapshotSeq);
  const unique=new Map();for(const row of append?[...state.replay,...rows]:rows)unique.set(row.seq ?? row.event_id ?? JSON.stringify(row),row);
  state.replay=[...unique.values()];state.replayCursor=rows.length && rows.length===result.items.length && result.next_cursor!==after && result.next_cursor!==0 ? result.next_cursor : null;
  if(!append)state.replayIndex=state.replay.length?0:-1;
  renderReplay();if($("show-conclusions").checked)await loadReplayConclusions();
}
function renderReplayChart() {
  const symbols=(state.status.watchlist || state.status.symbols || []).map(row=>typeof row==="string"?row:row.symbol).filter(Boolean);
  for(const row of state.replay.slice(0,state.replayIndex+1)){const event=replayEvent(row);if(event?.symbol&&!symbols.includes(event.symbol))symbols.push(event.symbol);}
  state.replaySymbol=state.replaySymbol && symbols.includes(state.replaySymbol)?state.replaySymbol:symbols[0] || "";
  $("replay-symbol").innerHTML=symbols.map(symbol=>`<option ${symbol===state.replaySymbol?"selected":""}>${escape(symbol)}</option>`).join("");
  const bars=new Map();
  for(const row of state.replay.slice(0,state.replayIndex+1)){
    const event=replayEvent(row),b=event?.payload || event,time=Date.parse(event?.event_time || b?.end)/1000;
    if(event?.kind==="bar"&&event.symbol===state.replaySymbol&&Number.isFinite(time)&&[b.open,b.high,b.low,b.close].every(value=>typeof value==="number"&&Number.isFinite(value)))bars.set(time,{time,open:b.open,high:b.high,low:b.low,close:b.close});
  }
  const data=[...bars.values()].sort((a,b)=>a.time-b.time);state.replayChartData=data;
  $("replay-chart-description").textContent=data.length?`${state.replaySymbol} · ${data.length}根已观察K线，末根结束于 ${date(new Date(data.at(-1).time*1000).toISOString())}。未前进的行情与引擎结论保持隐藏。`:`${state.replaySymbol || "当前品种"}尚无已前进到的完整K线；仅绘制选定范围内已观察的资料，不补零柱或范围前历史。`;
  if(!window.LightweightCharts)return;
  if(!state.replayChart){
    const element=$("replay-chart");state.replayChart=LightweightCharts.createChart(element,{width:element.clientWidth,height:element.clientHeight,layout:{background:{type:"solid",color:"#ffffff"},textColor:"#677e94",fontFamily:"Menlo, monospace",fontSize:10,attributionLogo:true},grid:{vertLines:{color:"#eef3f7"},horzLines:{color:"#eef3f7"}},rightPriceScale:{borderColor:"#d6e2ec"},timeScale:{borderColor:"#d6e2ec",timeVisible:true,secondsVisible:false,tickMarkFormatter:(time,type)=>new Intl.DateTimeFormat("zh-CN",{timeZone:"America/New_York",...(type<=2?{month:"2-digit",day:"2-digit"}:{hour:"2-digit",minute:"2-digit",hour12:false})}).format(new Date(Number(time)*1000))},localization:{locale:"zh-CN",timeFormatter:time=>date(new Date(Number(time)*1000).toISOString())}});
    state.replayCandles=state.replayChart.addSeries(LightweightCharts.CandlestickSeries,{upColor:"#25816d",downColor:"#ac7561",wickUpColor:"#25816d",wickDownColor:"#ac7561",borderVisible:false});new ResizeObserver(()=>{if(element.clientWidth&&element.clientHeight)state.replayChart.resize(element.clientWidth,element.clientHeight);}).observe(element);
  }
  state.replayCandles.setData(data);state.replayChart.timeScale().fitContent();
}
function replayDetectionVisible(d) {
  const row=state.replay[state.replayIndex],cutoff=replayKnownTime(row);
  if(!row || d.symbol!==state.replaySymbol)return false;
  if(Number.isInteger(row.seq)&&Number.isInteger(d.event_seq))return d.event_seq<=row.seq;
  return cutoff && Number.isFinite(Date.parse(d.identified_at))&&Date.parse(d.identified_at)<=Date.parse(cutoff);
}
function replayConclusionsComplete() {
  if(state.replayDetectionCapacityBlocked)return false;
  if(!state.replayDetectionHasMore)return true;
  const row=state.replay[state.replayIndex],cutoff=replayKnownTime(row);
  const d=state.replayDetectionReadThrough;return d&&(Number.isInteger(row?.seq)&&Number.isInteger(d.event_seq)?d.event_seq>row.seq:Date.parse(d.identified_at)>Date.parse(cutoff));
}
function renderReplayConclusions() {
  const showing=$("show-conclusions").checked;$("replay-conclusions").hidden=!showing;
  if(!showing)return;
  const rows=(state.replayDetections || []).filter(replayDetectionVisible);state.visibleReplayDetections=rows;
  const complete=replayConclusionsComplete();
  $("replay-conclusion-status").textContent=state.replayDetectionCapacityBlocked?"当前范围超过浏览器5000条判断上限，尚未读取完整。请缩短时间范围后重新载入；当前记录不能冒充完整评价。":state.replayDetectionBusy?"正在分页读取当前时点的已提交判断…":complete?`${state.replaySymbol}所选范围内、当前已查看时点之前的判断已读取，共${rows.length}条。`:`已读取${rows.length}条；历史尚未读取完整，请继续读取。未读取的范围不能视为没有判断。`;
  $("replay-detections").innerHTML=table(detectionColumns,rows,{select:"replay",empty:complete?"当前已查看时点没有此品种的引擎判断。":"尚未读取到当前范围的判断；请继续读取。"});
  $("replay-more-conclusions").hidden=complete;$("replay-more-conclusions").disabled=state.replayDetectionBusy || state.replayDetectionCapacityBlocked;
  if(state.replaySelected && rows.includes(state.replaySelected))renderEvidence(state.replaySelected,"replay-evidence");else $("replay-evidence").innerHTML=rows.length?empty("选择一次已查看时点的判断，展开逐条件证据。"):"";
}
async function loadReplayConclusions() {
  if(!$("show-conclusions").checked || state.replayIndex<0 || state.replayDetectionBusy || state.replayDetectionCapacityBlocked)return;
  const epoch=state.runEpoch,request=state.replayDetectionRequest=(state.replayDetectionRequest || 0)+1;state.replayDetectionBusy=true;renderReplayConclusions();
  try{
    // Each action reads at most three pages. The explicit continuation keeps
    // large histories bounded per request and never calls a recent snapshot complete.
    for(let page=0;page<3 && !replayConclusionsComplete();page++){
      const query=new URLSearchParams({after:String(state.replayDetectionNext ?? 0),limit:"100",symbol:state.replaySymbol});
      const group=collection(await api("/api/detections?"+query));if(epoch!==state.runEpoch || request!==state.replayDetectionRequest)return;
      const unique=new Map((state.replayDetections || []).map(row=>[row.record_id ?? JSON.stringify(row),row]));
      const first=state.replay[0];
      for(const item of group.items){const row=detectionRow(item);state.replayDetectionReadThrough=row;
        const inWindow=Number.isInteger(first?.seq)&&Number.isInteger(row.event_seq)?row.event_seq>=first.seq:Date.parse(row.identified_at)>=Date.parse(replayKnownTime(first));
        if(inWindow)unique.set(row.record_id ?? JSON.stringify(row),row);
      }
      if(unique.size>5000){state.replayDetectionCapacityBlocked=true;state.replayDetections=[...unique.values()].slice(0,5000);break;}
      state.replayDetections=[...unique.values()];state.replayDetectionHasMore=group.items.length>0 && group.next_cursor!=null && group.next_cursor!==state.replayDetectionNext;
      state.replayDetectionNext=group.next_cursor;
    }
  }catch(error){if(!error.staleRun)toast("回放判断读取失败："+error.message);}
  finally{if(epoch===state.runEpoch && request===state.replayDetectionRequest){state.replayDetectionBusy=false;renderReplayConclusions();}}
}
function renderReplay() {
  const row=state.replay[state.replayIndex],event=replayEvent(row);
  $("replay-step").disabled=state.replayIndex<0||(state.replayIndex>=state.replay.length-1&&state.replayCursor==null);$("replay-prev").disabled=state.replayIndex<=0;$("replay-reset").disabled=state.replayIndex<0;$("label-use-current").disabled=state.replayIndex<0;
  $("replay-progress").textContent=state.replayIndex<0?"日志中没有事件":`已查看 ${state.replayIndex+1} / ${state.replay.length}${state.replayCursor!=null?"+":""} 个事件`;
  const range=[$("replay-start").value.trim(),$("replay-end").value.trim()],loaded=state.replay.length?`已载入 ${date(replayKnownTime(state.replay[0]))} 至 ${date(replayKnownTime(state.replay.at(-1)))}`:"尚未载入符合条件的事件";
  $("replay-range").textContent=(range.some(Boolean)?`筛选 ${range[0]?date(range[0]):"起点不限"} 至 ${range[1]?date(range[1]):"终点不限"}（含边界）。`:"未限定时间。")+loaded+"；按系统知道资料的时间。";
  $("replay-event").innerHTML=event?`<div class="panel-heading"><h3>${escape(event.symbol || "运行控制")} · ${escape(event.kind || row.kind || "事件")}</h3><span class="caption">市场时刻 ${escape(date(event.event_time || event.at))}<br>收到时刻 ${escape(date(replayKnownTime(row)))}</span></div><details><summary class="caption">查看当前事件原始资料</summary><pre class="event-json">${escape(JSON.stringify(event,null,2))}</pre></details>`:empty("当前运行日志中没有事件。");
  renderReplayChart();renderReplayConclusions();
  if(event){const form=$("label-form");if(event.symbol)form.elements.symbol.value=event.symbol;if(event.venue)form.elements.venue.value=event.venue;if(event.event_time&&!form.elements.start.value){form.elements.start.value=event.event_time;form.elements.end.value=event.event_time;}}
}
async function saveLabel(event) {
  event.preventDefault();if(state.draftNeedsReview){toast("运行已变更，请核对草稿并重新确认资料出处后保存。");return;}const form=event.currentTarget,row=Object.fromEntries(new FormData(form));row.direction=Number(row.direction);row.positive=row.positive==="true";row.data_eligible=row.data_eligible==="true";row.disputed=form.elements.disputed.checked;row.complete_coverage=form.elements.complete_coverage.checked;row.data_source=row.source;row.review_records=row.review_note?[{note:row.review_note,reviewer:row.annotator}]:[];delete row.review_note;
  if(row.complete_coverage&&(row.disputed||!row.data_eligible)){toast("完整覆盖只能声明已复核、无争议且必要资料可观察的窗口。");return;}
  if(!row.trigger_time)delete row.trigger_time;
  const sourceKind=knownSourceKind();if(!["synthetic","real_market"].includes(row.source_kind)||(sourceKind!=="unknown"&&row.source_kind!==sourceKind)){toast("标注资料类型必须与当前运行一致；来源未知的回放需要先确认资料出处。");return;}
  const offset=/([+-]\d\d:\d\d|Z)$/;if(!offset.test(row.start)||!offset.test(row.end)||Number.isNaN(Date.parse(row.start))||Number.isNaN(Date.parse(row.end))){toast("开始和结束时间需要有效的ISO时间及明确时区。例：2026-10-02T09:40:00-04:00");return;}
  if(Date.parse(row.start)>Date.parse(row.end)){toast("窗口开始不能晚于结束。");return;}
  if(row.trigger_time&&(!offset.test(row.trigger_time)||Number.isNaN(Date.parse(row.trigger_time))||Date.parse(row.trigger_time)<Date.parse(row.start)||Date.parse(row.trigger_time)>Date.parse(row.end))){toast("触发时刻需要有效时区，并位于标注窗口内。");return;}
  row.stage_windows={};
  for(const stage of ["candidate","formed","triggered"]){const lo=row[stage+"_start"],hi=row[stage+"_end"];delete row[stage+"_start"];delete row[stage+"_end"];
    if(lo||hi){if(!lo||!hi||!offset.test(lo)||!offset.test(hi)||Number.isNaN(Date.parse(lo))||Number.isNaN(Date.parse(hi))||Date.parse(lo)<Date.parse(row.start)||Date.parse(hi)>Date.parse(row.end)||Date.parse(lo)>Date.parse(hi)){toast("各阶段窗口需要完整、有效的开始和结束时刻，并处于外层标注窗口内。");return;}row.stage_windows[stage]={start:lo,end:hi};}
  }
  const button=form.querySelector("button[type=submit]"),draftRevision=state.annotationDraftRevision;button.disabled=true;
  let accepted=false;
  try{const receipt=await api("/api/labels",{method:"POST",body:JSON.stringify(row)});accepted=true;toast("保存中，等待后台完成。");await waitForCommit(receipt);await loadCollection("labels");renderLabels();await loadCollection("reports");renderResearch();if(draftRevision===state.annotationDraftRevision)state.annotationDraftDirty=false;toast("标注已保存。");}catch(error){toast((accepted?"等待保存完成或刷新最新记录：":"标注保存未完成：")+error.message);}finally{button.disabled=false;}
}

document.querySelectorAll("[data-view]").forEach(el=>el.addEventListener("click",()=>showView(el.dataset.view,true)));
document.querySelectorAll("[data-open]").forEach(el=>el.addEventListener("click",()=>showView(el.dataset.open,true)));
$("watchlist").addEventListener("click",e=>{const card=e.target.closest("[data-symbol]");if(card){state.symbol=card.dataset.symbol;state.datasets.marketDetections=[];state.historyLoaded.marketDetections=false;$("symbol").value=state.symbol;renderDetections();showView("market",true);}});
$("symbol").addEventListener("change",e=>{state.symbol=e.target.value;state.datasets.marketDetections=[];state.historyLoaded.marketDetections=false;state.selected=null;$("evidence").innerHTML=empty("选择一次判断查看依据。");drawLevels({});renderDetections();loadBars();loadMarketDetections();});
$("stage-filter").addEventListener("change",renderDetections);
$("coverage-symbol").addEventListener("change",renderResearch);
function selectDetection(e){const row=e.target.closest("[data-detection-index]");if(row){state.selected=state.visibleDetections[Number(row.dataset.detectionIndex)];renderDetections();}}
$("detections").addEventListener("click",selectDetection);$("detections").addEventListener("keydown",e=>{if(e.key==="Enter"||e.key===" "){e.preventDefault();selectDetection(e);}});
$("reconnect-stream").addEventListener("click",()=>{connectStream();toast("正在恢复实时更新；已提交操作不会重新发送。");});
$("refresh").addEventListener("click",()=>refresh());["pause","resume","stop"].forEach(action=>$(action).addEventListener("click",()=>control(action)));
["detections","orders","fills","labels","decisions"].forEach(name=>$("more-"+name).addEventListener("click",async e=>{e.target.disabled=true;try{await loadCollection(name==="detections"?"marketDetections":name,true);renderDetections();renderAccount();renderLabels();renderResearch();}catch(error){toast(error.message);}finally{e.target.disabled=false;}}));
$("replay-load").addEventListener("click",()=>loadReplay().catch(e=>toast(e.message)));
for(const id of ["replay-start","replay-end"])$(id).addEventListener("change",()=>{state.replayRequest=(state.replayRequest || 0)+1;state.replay=[];state.replayIndex=-1;state.replayCursor=null;renderReplay();loadReplay().catch(e=>toast(e.message));});
$("replay-step").addEventListener("click",async()=>{try{if(state.replayIndex===state.replay.length-1&&state.replayCursor!=null)await loadReplay(true);state.replayIndex=Math.min(state.replay.length-1,state.replayIndex+1);renderReplay();if($("show-conclusions").checked)await loadReplayConclusions();}catch(error){toast(error.message);}});
$("replay-prev").addEventListener("click",()=>{state.replayIndex=Math.max(0,state.replayIndex-1);renderReplay();});
$("replay-reset").addEventListener("click",()=>{state.replayIndex=state.replay.length?0:-1;renderReplay();});
$("show-conclusions").addEventListener("change",()=>{renderReplayConclusions();if($("show-conclusions").checked)loadReplayConclusions();});
$("replay-symbol").addEventListener("change",event=>{state.replaySymbol=event.target.value;resetReplayConclusions();renderReplay();if($("show-conclusions").checked)loadReplayConclusions();});
$("replay-more-conclusions").addEventListener("click",loadReplayConclusions);
function selectReplayDetection(event){const row=event.target.closest("[data-replay-detection-index]");if(row){state.replaySelected=state.visibleReplayDetections[Number(row.dataset.replayDetectionIndex)];renderReplayConclusions();}}
$("replay-detections").addEventListener("click",selectReplayDetection);$("replay-detections").addEventListener("keydown",event=>{if(event.key==="Enter"||event.key===" "){event.preventDefault();selectReplayDetection(event);}});
$("label-use-current").addEventListener("click",()=>{const event=replayEvent(state.replay[state.replayIndex]),form=$("label-form"),time=event?.event_time || replayKnownTime(state.replay[state.replayIndex]);if(event?.symbol)form.elements.symbol.value=event.symbol;if(event?.venue)form.elements.venue.value=event.venue;if(time){form.elements.start.value=time;form.elements.end.value=time;}toast("已采用当前事件时点；请独立填写规则及结构判断。");});$("label-form").addEventListener("submit",saveLabel);
$("show-existing-labels").addEventListener("change",renderLabels);
for(const event of ["input","change"])$("label-form").addEventListener(event,()=>{state.annotationDraftDirty=true;state.annotationDraftRevision=(state.annotationDraftRevision || 0)+1;});
$("label-confirm-run").addEventListener("click",()=>{const kind=knownSourceKind(),select=$("label-form").elements.source_kind;if(kind==="unknown" && !["synthetic","real_market"].includes(select.value)){toast("请先依据资料出处选择合成或真实资料。");return;}if(kind!=="unknown")select.value=kind;state.draftNeedsReview=false;state.annotationSourceIdentity=annotationProvenance(state.status);$("label-run-change").hidden=true;renderAnnotationSource();toast("已确认当前运行的资料类型；请继续核对草稿窗口与规则版本。");});
$("label-form").elements.disputed.addEventListener("change",syncCoverageClaim);$("label-form").elements.data_eligible.addEventListener("change",syncCoverageClaim);
$("export-reports").addEventListener("click",async event=>{const button=event.currentTarget;button.disabled=true;try{const report=await api("/api/reports"),blob=new Blob([JSON.stringify(report,null,2)],{type:"application/json"}),url=URL.createObjectURL(blob),link=document.createElement("a");link.href=url;link.download="intraday-committed-reports-"+(state.status.run_id || "current")+".json";link.click();setTimeout(()=>URL.revokeObjectURL(url),5000);toast("已下载当前已提交报告；资料范围与版本保留在JSON中。");}catch(error){toast("报告下载失败："+error.message);}finally{button.disabled=false;}});
$("export").addEventListener("click",async()=>{try{const result=await api("/api/export",{method:"POST",body:"{}"});if(result.download_url){const url=new URL(result.download_url,location.href);if(url.origin!==location.origin)throw new Error("导出地址必须来自本地服务。");location.assign(url.href);}else{const blob=new Blob([JSON.stringify(result,null,2)],{type:"application/json"});const url=URL.createObjectURL(blob),a=document.createElement("a");a.href=url;a.download="intraday-run-export.json";a.click();setTimeout(()=>URL.revokeObjectURL(url),5000);}toast("运行导出已生成。");}catch(error){toast("导出失败："+error.message);}});
window.addEventListener("hashchange",()=>showView(location.hash.slice(1)));window.addEventListener("beforeunload",()=>state.stream?.close());
async function boot() {
  try {
    const status=await api("/api/status");adoptStatus(status);
    renderStatus();connectStream();await refresh();
  }catch(error){await refresh();if(!state.stream&&state.connected)connectStream();}
}
showView(location.hash.slice(1) || "overview");boot();setInterval(()=>{if(!document.hidden)refresh(false);},5000);
