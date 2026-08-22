import { CloudError } from './cloud';
import type { Strategy } from './product';

type Bar = {t:string;o:number;h:number;l:number;c:number;v?:number};
type PublicBar = {start_at:string;end_at:string;open:string;high:string;low:string;close:string;volume:string;forming?:boolean};
type Decision = {strategy_id:string;branch:string;reason:string;occurred_at:string};

const frameMinutes: Record<string,number> = {'1m':1,'5m':5,'15m':15,'30m':30,'1h':60};
const decimal = (value:number) => Number.isFinite(value) ? value.toFixed(4).replace(/0+$/,'').replace(/\.$/,'') : '0';

export async function fetchIexBars(symbol: string, timeframe: string, credential: Record<string,string>) {
  const start = new Date(Date.now() - 36 * 60 * 60 * 1000).toISOString();
  const url = new URL(`https://data.alpaca.markets/v2/stocks/${encodeURIComponent(symbol)}/bars`);
  url.searchParams.set('feed','iex'); url.searchParams.set('timeframe','1Min'); url.searchParams.set('adjustment','raw');
  url.searchParams.set('sort','asc'); url.searchParams.set('limit','1000'); url.searchParams.set('start',start);
  const response = await fetch(url,{headers:{'APCA-API-KEY-ID':credential.key_id,'APCA-API-SECRET-KEY':credential.secret_key,'Accept':'application/json'}});
  if (!response.ok) throw new CloudError(response.status === 401 ? 401 : 502,'market_data_unavailable',response.status === 401 ? 'Alpaca credentials were rejected.' : 'IEX market data is temporarily unavailable.');
  const value = await response.json() as {bars?:Bar[]};
  if (!Array.isArray(value.bars) || !value.bars.length) throw new CloudError(422,'market_data_empty','No eligible IEX bars are available for this interval.');
  return aggregateBars(value.bars,timeframe).slice(-200);
}

function aggregateBars(bars: Bar[], timeframe: string): PublicBar[] {
  const minutes = frameMinutes[timeframe] || 5; const buckets = new Map<number,Bar[]>();
  for (const bar of bars) {
    const time = new Date(bar.t).getTime(); if (!Number.isFinite(time)) continue;
    const bucket = Math.floor(time / (minutes * 60000)) * minutes * 60000;
    const group = buckets.get(bucket) || []; group.push(bar); buckets.set(bucket,group);
  }
  return [...buckets.entries()].sort(([a],[b])=>a-b).map(([start,group]) => ({
    start_at:new Date(start).toISOString(), end_at:new Date(start+minutes*60000).toISOString(), open:decimal(group[0].o),
    high:decimal(Math.max(...group.map(item=>item.h))), low:decimal(Math.min(...group.map(item=>item.l))),
    close:decimal(group[group.length-1].c), volume:String(group.reduce((sum,item)=>sum+(item.v||0),0)),
  })).filter(bar => new Date(bar.end_at).getTime() <= Date.now());
}

function decisionSchema() {
  return {type:'object',additionalProperties:false,required:['decisions'],properties:{decisions:{type:'array',items:{type:'object',additionalProperties:false,required:['strategy_id','branch','reason'],properties:{strategy_id:{type:'string'},branch:{type:'string',enum:['wait','observe-long','observe-short']},reason:{type:'string',maxLength:240}}}}}};
}

function contextFor(strategies: Strategy[], bars: PublicBar[]) {
  const strategyContext = strategies.map(item => ({strategy_id:item.strategy_id,name:item.name,source_id:item.source_id,
    claims:Object.values(item.claims).flat().slice(0,5).map((claim) => (claim as {statement?:string}).statement).filter(Boolean)}));
  return JSON.stringify({market:{bars:bars.slice(-60),latest_close:bars.at(-1)?.close},strategies:strategyContext});
}

function parseOpenAI(value: Record<string,unknown>) {
  const output = Array.isArray(value.output) ? value.output : [];
  for (const item of output as Array<{type?:string;content?:Array<{type?:string;text?:string}>}>) for (const part of item.content || []) if (part.type === 'output_text' && typeof part.text === 'string') return part.text;
  throw new CloudError(502,'model_response_invalid','OpenAI returned no structured decision output.');
}

export async function callModel(provider: string, settings: {deepseek_reasoning:string;openai_speed:string}, credential: Record<string,string>, strategies: Strategy[], bars: PublicBar[]): Promise<Decision[]> {
  const instructions = 'You are a source-grounded price-action analyst. Evaluate every supplied strategy independently against only the closed bars and supplied source claims. Return JSON only. Use wait when evidence is insufficient. Never mention accounts, money, or real orders.';
  const context = contextFor(strategies,bars); let response: Response; let text: string;
  if (provider === 'openai') {
    response = await fetch('https://api.openai.com/v1/responses',{method:'POST',headers:{Authorization:`Bearer ${credential.api_key}`,'Content-Type':'application/json'},body:JSON.stringify({model:'gpt-5.6-luna',instructions,input:context,reasoning:{effort:'max'},text:{format:{type:'json_schema',name:'paper_desk_decisions',strict:true,schema:decisionSchema()}},max_output_tokens:4096,service_tier:settings.openai_speed === 'fast' ? 'fast':'default',store:false})});
    if (!response.ok) throw new CloudError(response.status === 401 ? 401 : 502,'model_unavailable',response.status === 401 ? 'OpenAI credentials were rejected.' : 'OpenAI analysis is temporarily unavailable.');
    text = parseOpenAI(await response.json() as Record<string,unknown>);
  } else {
    response = await fetch('https://api.deepseek.com/chat/completions',{method:'POST',headers:{Authorization:`Bearer ${credential.api_key}`,'Content-Type':'application/json'},body:JSON.stringify({model:'deepseek-v4-flash',messages:[{role:'system',content:instructions},{role:'user',content:context}],thinking:{type:settings.deepseek_reasoning},response_format:{type:'json_object'},max_tokens:4096,...(settings.deepseek_reasoning === 'disabled' ? {temperature:0}: {})})});
    if (!response.ok) throw new CloudError(response.status === 401 ? 401 : 502,'model_unavailable',response.status === 401 ? 'DeepSeek credentials were rejected.' : 'DeepSeek analysis is temporarily unavailable.');
    const value = await response.json() as {choices?:Array<{message?:{content?:string}}>}; text = value.choices?.[0]?.message?.content || '';
  }
  try {
    const parsed = JSON.parse(text) as {decisions?:Array<{strategy_id?:string;branch?:string;reason?:string}>};
    const allowed = new Set(strategies.map(item=>item.strategy_id)); const timestamp = new Date().toISOString();
    return (parsed.decisions || []).filter(item => typeof item.strategy_id==='string' && allowed.has(item.strategy_id) && ['wait','observe-long','observe-short'].includes(item.branch || '') && typeof item.reason==='string').map(item => ({strategy_id:item.strategy_id!,branch:item.branch!,reason:item.reason!.slice(0,240),occurred_at:timestamp}));
  } catch { throw new CloudError(502,'model_response_invalid','The model response did not match the required decision contract.'); }
}

export function makeSnapshot(config:{symbol:string;timeframe:string;execution_policy:string;provider_calls:number},bars:PublicBar[],decisions:Decision[],selected:Strategy[]) {
  const latest = bars.at(-1); const strategyState = selected.map(strategy => ({strategy_id:strategy.strategy_id,name:strategy.name,state:'analysis_complete',last_reason:decisions.find(item=>item.strategy_id===strategy.strategy_id)?.reason || null,gate_trace:[{gate:'closed_bar',label:'Closed bar',status:'passed'},{gate:'source_binding',label:'Source binding',status:'passed'}]}));
  const ledger = config.execution_policy === 'paper' ? decisions.filter(item=>item.branch!=='wait').map(item=>({strategy_id:item.strategy_id,status:'paper signal',direction:item.branch==='observe-long'?'long':'short',entry:latest?.close,paper_pnl:'0.00'})) : [];
  return {lifecycle:'running',paper_only:true,iex_only:true,no_live_orders:true,run:{symbol:config.symbol,timeframe:config.timeframe,execution_policy:config.execution_policy,provider_calls:config.provider_calls},market:{latest_price:latest?.close||null,latest_price_at:latest?.end_at||null,stream:{state:'IEX REST',started:true},readiness:{executable:true}},bars:{closed:bars,closed_count:bars.length,forming:null,latest_price:latest?.close||null},recent_decisions:decisions.slice(-100),strategies:strategyState,ledger};
}
