import knowledge from './pa-records.json';

export type Strategy = {
  strategy_id: string; name: string; source_id: string; tags: string[];
  aliases: string[]; summary: string; custom: boolean; claims: Record<string, unknown[]>;
};

const rows: Array<[string, string, string, string[]]> = [
  ['nanqiao-h2-l2','趋势回调 H2 / L2（顺大逆小）','nanqiao-checklist',['南桥','Breakout','趋势回调','一 tick']],
  ['nanqiao-breakout-failure-breakout','突破-失败-突破（二次突破确认）','nanqiao-checklist',['南桥','Breakout','确认','时间限制']],
  ['nanqiao-key-level-reversal','关键位结构反转突破单','nanqiao-checklist',['南桥','双顶','双底','楔形','头肩','OB','IB']],
  ['nanqiao-range-extreme-limit','区间前高/前低限价反转','nanqiao-checklist',['南桥','Limit','Trading Range','1:1']],
  ['nanqiao-point3-limit','宽通道 Point 3 限价反转','nanqiao-checklist',['南桥','Limit','Weak Channel','Point 3']],
  ['nanqiao-trend-50-pullback','趋势 50% 回调限价顺势','nanqiao-checklist',['南桥','Limit','趋势','50%']],
  ['nanqiao-trapped-pullback','真突破后的回踩限价顺势（Trapped 对手盘）','nanqiao-checklist',['南桥','Limit','突破','跟随']],
  ['nanqiao-always-in-opposite-1','Always in：逆1失败限价单','nanqiao-checklist',['南桥','Always in','Limit','scalp']],
  ['nanqiao-always-in-third-bar','Always in：第三根未遂（Third-Bar Failure）','nanqiao-checklist',['南桥','Always in','Limit','逆势失败']],
  ['nanqiao-always-in-ema20','Always in：均线回补 EMA20','nanqiao-checklist',['南桥','Always in','EMA20','Limit']],
  ['nanqiao-always-in-breakout-scalp','Always in：顺势突破 scalp','nanqiao-checklist',['南桥','Always in','Breakout','scalp','ATR']],
  ['rose-caveman','Caveman（野人）策略','rose-caveman',['Rose','失败突破','反转','scalp','HR','1R','2R']],
  ['rose-gap-bar','Gap Bar 顺势回调策略','rose-gap-bar',['Rose','趋势','斐波那契','回调','供需']],
  ['rose-mother-bar','Mother Bar（孕线）区域反转与突破','rose-mother-bar',['Rose','孕线','限价','Trading Range','Trap Zone']],
  ['rose-mtr','MTR（主趋势反转）失败突破与被套盘','rose-mtr',['Rose','MTR','失败突破','被套交易者','实体确认']],
  ['rose-three-pushes','三推的正确画法','rose-strategy',['Rose','三推','楔形','推段距离']],
  ['rose-mother-bar-scalp','Rose 幻灯片 Mother Bar 刮头皮','rose-strategy',['Rose','Mother Bar','Trap Zone']],
  ['rose-fofs-swing','FOFS 失败的失败是波段','rose-strategy',['Rose','FOFS','TTR','双顶','双底']],
  ['rose-range-breakout','区间突破与价格控制','rose-strategy',['Rose','区间','突破']],
  ['rose-flag-pullback','大影线旗形回调','rose-strategy',['Rose','旗形','内包线','大影线']],
  ['rose-missed-trend','错过趋势后的十字星补救','rose-strategy',['Rose','趋势','十字星','1MM','2MM']],
];

const records = new Map((knowledge.records as Array<Record<string, unknown>>).map(record => [record.id, record]));
const claimFields = ['market_context','prerequisites','recognition_features','trigger','entry','invalidation_stop','targets_exit','prohibited','risk_notes','exceptions'];

export const STRATEGIES: Strategy[] = rows.map(([id,name,source_id,tags]) => {
  const record = records.get(id) || {};
  const claims = Object.fromEntries(claimFields.map(field => [field, Array.isArray(record[field]) ? record[field] : []]));
  const firstClaim = claimFields.flatMap(field => claims[field] as Array<{statement?:string}>).find(item => typeof item?.statement === 'string');
  return { strategy_id:`pa:${id}`, name, source_id, tags, aliases:Array.isArray(record.aliases) ? record.aliases as string[] : [], custom:false,
    summary:firstClaim?.statement || 'Source-linked price-action setup with preserved claim and page lineage.', claims };
});

export const SYMBOLS = ['NVDA','TSLA','AMD','AAPL','AMZN','META'].map(symbol => ({symbol}));

export function emptySnapshot() {
  return { lifecycle:'idle', paper_only:true, iex_only:true, no_live_orders:true,
    run:{symbol:'NVDA',timeframe:'5m',execution_policy:'advisory',provider_calls:0},
    market:{latest_price:null,latest_price_at:null,stream:{state:'IEX',started:false},readiness:{executable:false}},
    bars:{closed:[],closed_count:0,forming:null,latest_price:null}, recent_decisions:[], strategies:[], ledger:[] };
}

export function settings() {
  return {active_provider:'deepseek',models:{deepseek:{thinking:'enabled'},openai:{speed:'standard'}},credentials:{alpaca:false,deepseek:false,openai:false},alpaca_assets:[]};
}
