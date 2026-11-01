# 双语术语与数据口径

下表是本库使用的定义。平台、合约和来源的不同实现必须另记版本；定义清楚不等于有交易优势。

| 术语 | 本库含义与常见误读 |
|---|---|
| Setup / Trigger / Fill | 条件结构 / 发单触发 / 实际成交。触发不保证价格或数量成交 |
| Invalidation / Stop | 假说被否认 / 执行风险订单。模型固定风险止损可以先于或晚于自然结构失效 |
| Session / Trading day | 固定的数据时间段与交易日归属。期货近全天电子交易与现金活动段分开；连续BTC须自定UTC日/段 |
| Timezone / DST | 用IANA时区如America/New_York、Europe/Berlin。图上本地时钟与CET/EST文字不能未经核对直接换算夏令时 |
| PA / Price action | 价格结构和先后顺序主导决策。仅展示K线不使事件、模型或订单流条目变成PA |
| OHLC / Bar | 开高低收；同一柱不能证明触发、止损、目标的实际先后 |
| ORB / Opening Range Breakout | 先冻结开盘窗口再越界；窗口长短通常是参数。IB的特殊失败顺序/跨市场前提另审 |
| IB / Initial Balance | 来源固定的首段拍卖区间，常见首小时但非本库默认所有产品一小时；7:00数据段不自动等于cash open |
| PMH / PML | 事先已知盘前高/低，不能用开盘后重画高低冒充盘前水平 |
| H2/L2 / M2B/M2S | 趋势内两段回撤第二次恢复；均线接触与信号端点有来源版本 |
| ii / Double Inside Bar | 两个连续内包柱的压缩；影线/收盘内包与母柱/内柱触发必须固定 |
| NR7 / NR4 | 单柱范围小于前6/3柱。不是自动买入，也不等于ii完整定义；本库保守按压缩变体处理 |
| MTR / Major Trend Reversal | 旧趋势受损后再测旧极值再给反向信号；不同于只出现一根反向柱 |
| Anti / First Snap Pullback | 首强反向冲击后首弱回撤的新方向恢复；不必再测旧极值 |
| Wedge / Three Pushes | 三次分开的推进加衰退背景；三根同向柱不自动算三推 |
| Re-entry | 原模式触发、其模式保护位被穿、前提仍有效再恢复；不是亏损后的无限加仓 |
| VWAP | 所选段累计价×量/累计量。逐笔价量与典型价×柱量代理不同，重置及是否含盘前需记 |
| Anchored / Historical VWAP | 从特定事件/历史日定锚；26是历史最高量日版本，不是普通当日VWAP |
| RVOL / Relative volume | 相对过去可比时刻/时期的量；不能拿当天最终总量筛选早盘 |
| Relative Strength / Weakness | 本库指股票相对市场的同时响应，不等于RSI振荡指标 |
| Volume Profile / VPOC | 价格档位累计真实成交量分布 / 最大成交量价位。最终当日VPOC不能用于早期已知信号 |
| TPO / TPO POC | 每个固定时间bracket在哪些价格成交过的分布 / 时间出现数最多价位；不是成交量POC |
| VAH / VAL | 指定算法、权重与百分比的价值区上/下边；Volume和TPO不能混接 |
| HVN / LVN | 成交量分布的高/低量节点；节点位置自身不是完整setup |
| Single prints / Poor high-low | 时间分布单印/未充分结束的边缘。作为背景不自动带来可执行入口 |
| Acceptance / Rejection | 在事前定义窗口、持续交易/收盘与流响应的接受/否认；不能仅凭后来走势命名 |
| TPO ledge | 时间分布平齐台阶边；33关注解决并保持，34要求多次失败后反边快速退出 |
| 80% Rule / Traverse | 历史名称与旧VA横穿假说；touch、clean与交易净胜率是三种口径 |
| DOM / Depth | 显示挂单深度与队列信息；挂单可撤，不等于成交，更不直接证明机构身份 |
| Tape / Time & Sales | 已成交逐笔时间、价格、量；完整性、合并与成交方向分类影响解释 |
| Bid/Ask volume | 在bid主动卖出的成交 / 在ask主动买入的成交；平台列位置和算法须核对 |
| Delta / CVD | Ask成交量−Bid成交量 / 从指定reset累计；每笔仍有买卖双方，正delta不等于净多仓 |
| Footprint / Cluster | 每价成交分类的柱内显示；相同颜色可在不同趋势背景有相反含义 |
| Diagonal imbalance | Bid某价与Ask上一档的比率（或镜像）；不是同价两列简单比较。300%例和150%默认不同 |
| Stacked imbalance | 连续多档对角失衡；最小成交量和零分母处理需固定，42中的最少档数为编辑规则 |
| Absorption | 持续主动冲击却低位移且被动承接；仅大挂单、单次Delta背离不够 |
| Exhaustion | 主动成交减少、推进失去接力；区别于大量主动成交被持续吸收 |
| Iceberg | 隐藏量订单机制的可能解释；静态图不直接证明同一机构或完整隐藏量 |
| Stop run / Liquidation | 来源对穿极值和急促流的库存解释；没有订单/账户证据时按行为假说处理 |
| Liquidity gap / Jump | 若干价档可成交深度弱、价格跳过；必须确认真实成交和区域保持，不从静态空白推定 |
| Roll / Initiative | 同向主动流实际推进；不是大挂单展示就代表进攻 |
| Passive grind | 支持持续而主动offer逐档小量消耗的慢速推进；不能把没动静都叫承接 |
| News surprise / Consensus | 新信息相对事前可获取预期的差；官方公告不能独自证实交易者当时共识 |
| Lead-lag / Correlation | 同步产品先后响应 / 样本关系；先行者要事前可辨，不能用后来领先者选择 |
| ATR / Noise band | ATR是特定历史波幅统计；48用同刻绝对开盘偏离均值，σ符号不代表标准差 |
| Look-ahead / Survivorship | 用未来信息或只保留后来存活资产；日最终高低、最终Profile和最终成交量都是常见泄漏 |
| R / Expectancy | 相对计划风险的收益 / 成本后分布期望。图上一笔R、触及率和声誉均不证明正期望 |
| Reviewed / Profit evidence | 本库内容和规则自审状态 / 盈利证据单独标签。未做独立复现与账户审计 |
