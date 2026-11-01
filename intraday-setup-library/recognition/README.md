# Setup 程序化识别 · 研究版 v0.2.2

当前交付：全部 50 个基础 setup 有可运行的数值特征与状态路径，另有 34 个独立标识的变体。使用标准库 Python 3.10+，无模型服务依赖；优先按美股现金日接入数据。原规则主记录仍为 `setups/SET-*.md`，本模块中的数值解释明确标为编辑操作化，尚未完成真实行情验证。

0.2.0 的审计修复及回归边界见[修复说明](../reports/recognition-fixes.md)。`same_frame` 只允许已在规则目录注明的兼容证据共用观测；新信号柱端点继续等待后续观测。SET-0002 已形成的分钟路径可由真实逐笔事件窗口续行；SET-0024 同时接受1/5/10秒观测，必要数据要求保持完整。

0.2.1 按判断截止时间关联基准历史，修复未来基准柱先到时误报缺数；保留 SET-0019 的有效父路径能力与精确两 tick 跳价边界。精确 VWAP 必须从实际现金开盘起连续完整，缓存截断后的开盘、盘前和累计能力保留明确未知。网络恢复和新交易日不能解除已观察停牌。输入类型、数值与状态校验、特征注册、版本化 JSON 恢复及有限缓存策略见下文；原始事件矩阵覆盖 84 图正向、50 基础版的 100 条核心负例及 31 个同目标机械变体边界、1 个来源时钟与1 个注册周期选择边界，并明确 SET-0027 数据代理的语义身份例外。完整修订记录见 [CHANGELOG](../CHANGELOG.md)。

0.2.2 使用明确交易日历检查连续完整历史，缺失实际前一交易日时不会将更旧日期用作前日位置；ATR与同刻历史也保留缺数。显式空历史清除旧值；省略替代历史可保留真实、完整且日期正确的本地前日资料。

覆盖范围见 [逐条可执行目录](catalog.md)，机器规格见 [catalog.json](catalog.json)，测试执行结果见 [recognition-validation.json](../reports/recognition-validation.json)，真实样本的评价方法见 [评价协议](../evaluation/README.md)。84 条图是规则及变体的数量，不代表 84 个独立 setup，也不代表所有市场形态已被覆盖。

## 立即运行

以下命令均在项目根目录执行。输出目录放在项目外；每次回放使用新目录，已有结果不会被覆盖。

```sh
python3 scripts/test_recognition.py --require-jsonschema
python3 scripts/build.py
python3 scripts/validate.py --require-jsonschema

python3 -m recognition demo --output ../recognition-runs/demo-input
python3 -m recognition replay --input ../recognition-runs/demo-input/events.jsonl --output ../recognition-runs/demo-run --variants
python3 -m recognition evaluate --detections ../recognition-runs/demo-run/detections.jsonl --labels ../recognition-runs/demo-input/labels.jsonl --output ../recognition-runs/demo-run/evaluation.json
```

`demo` 仅生成合成数据及两个手写示例标签。通过示例说明输入、识别、报告可以连通，不证明 50 个 setup 的市场召回率或精确率。这些离线识别命令只生成检测和评价；统一模拟账户由独立运行平台启动。

默认回放 50 条基础图；`--variants` 增加全部变体。可限定 `--setups SET-0005 SET-0010`；包含再入场 SET-0019 时，还须启用可能的父 setup，父事件与关联关系保留在输出中。Python 可直接复用增量接口：

```python
from recognition.engine import RecognitionEngine
from recognition.models import MarketEvent

engine = RecognitionEngine(["SET-0005", "SET-0010"], include_variants=True)
for raw_event in arrival_ordered_events:
    for detection in engine.feed(MarketEvent.from_dict(raw_event)):
        consume(detection.to_dict())
```

`arrival_ordered_events` 与 `consume` 是调用方的数据流和处理函数。实时/供应商连接器负责转换、订阅和完整性声明；本版已提供 JSONL 回放与通用 OHLCV CSV 转换器。

## 如何避免把形态写死

- 核心行为先后顺序独立于形态评分。高分不能跳过前提、回测、流转或触发，也不能补齐缺失数据。
- 普通价格图并行扫描 1/3/5/15 分钟；短线图使用实际 1/5/10 秒数据。1 分钟数据无法恢复未提供的秒线。
- swing 同时维护 0.35/0.7/1.4 个历史噪声尺度的转折；转折只在反向幅度实际出现后确认，后来的波动变化不重画旧转折。
- 位置容差取 tick、已收到的 spread、历史波动率的较大值。允许贴近、略越界、inside/beyond/equal 等分别解释的回测版本，保持原有结构身份。
- SET-0003/0010/0016/0039 会分别检查输入中的 key_level、前日高低点、历史 VWAP。相同价位合并；不同参考价位的事件独立跟踪，避免只检查最近位置。
- DOM、Footprint、新闻短窗同时使用实际到达事件组成的窗口，默认最短 0.5 秒。标记 `observation_kind=event_window`、`scale_seconds=0`；这不是提前读取尚未结束的分钟柱。source_open 的 0 则明确标记 `open_price`。
- 候选层允许趋势/冲击/压缩略低于确认阈值；继续跟踪时必须重新满足核心条件，才进入严格确认。候选图不设静默截断上限，不会因另一个 setup 同时出现就删除标签。
- 已捕获的边界、信号端点与风险参考冻结在当次事件中。跨周期只合并同刻且锚点相同的事件；不同周期先后识别出的事件可能仍需人工/评价窗口归组。

这是提高出现形式覆盖的机制，不能保证发现全部行情实例。三推、平衡、主动性、耗尽等仍是数值代理；需要用漏报及误报样本持续校准。

## 数据层与可确认范围

| 输入 | 可以支持 | 限制 |
|---|---|---|
| 已收盘 OHLCV | 内外包、Hikkake、趋势/回撤、突破回测等价格序列 | 不推断柱内先后或订单流 |
| 前日/历史/盘前资料 | 前日失败、开盘、隔夜、ORB、历史量、固定时钟 | 必须是当日开盘前已知资料，保留出处和 as_of |
| 实际成交 notional 或完整逐笔 | 精确累计 VWAP | 不用典型价 × volume 冒充实际 VWAP |
| buy/sell 已分类逐笔 | Footprint、CVD、主动流和实际成交 Profile | unknown side、断档或不同 venue 不能当作完整主动流 |
| DOM snapshot | 被动承接、offer 稳定与队列补充的数值观察 | 供应商先重建增量书；快照之间仍不等于真实排队/成交审核 |
| 已收到新闻/事前预案 | 次日消息、60/90 秒首反应、宏观分支与再评估 | 人工/连接器给结构化事实，本版不自动读文本判利多利空 |
| 同步相关产品及事前日线/leadership | 相对强弱、先行 IB、跨产品分支确认 | 不用当日最终涨跌选“领导者” |

仅有 K 线时，订单流 setup 保留数据不足状态；没有候选也可能表示其核心观测无法计算。`run.json.coverage` 为每条图列出要求和各 stream/scale 最后观测的缺项，不能把“未触发”一律解释成 setup 没出现。

Volume Profile 来自实际逐价成交量；value area 从 POC 向相邻价格档连续扩展到 70%，相邻同权同时纳入。TPO 用实际成交访问的 30 分钟 bracket 计数，不用 K 线高低点虚构档位访问。SET-0037 保留 TPO/60 分钟内部收盘基础版，两个 bracket touch 和 Volume 代理各有 variant_id。

Footprint 使用对角比率：buy Ask(p)/Bid(p−tick)，sell Bid(p)/Ask(p+tick)。零分母保持未知，默认至少 3 个连续档、每档最小量 10；300% 与 150% 分别输出。CVD 在现金日重置，比较同一累计序列与历史价格极值；流窗口和成交 Profile 不跨 venue 混合。

## JSONL 输入契约

每行一个对象，统一字段为 `event_id, symbol, venue, kind, event_time, arrival_time, payload`；可选 `quality=complete|partial|unavailable`。时间必须有 UTC offset。输入按 arrival_time 升序；相同 event_id 的重传去重，内容变更会报错。同一种市场数据内部的过时/乱序事件不补写旧判断，而记断档状态。

先输入每个 symbol/venue 当日 session，显式提供该日实际开收盘时间、tick 和 asset_class（equity/ETF/futures）。日历由连接器提供；半日市/假日不能默认填成完整交易日。以下仅是合成格式示意：

```jsonl
{"event_id":"calendar-1","symbol":"SYN","venue":"SYNTHETIC","kind":"session","event_time":"2026-09-30T07:00:00-04:00","arrival_time":"2026-09-30T07:00:00-04:00","payload":{"open_time":"2026-09-30T09:30:00-04:00","close_time":"2026-09-30T16:00:00-04:00","tick_size":0.01,"asset_class":"equity","references":{"prev_close":{"value":100,"source":"synthetic_example","as_of":"2026-09-29T16:00:00-04:00"}}}}
{"event_id":"bar-1","symbol":"SYN","venue":"SYNTHETIC","kind":"bar","event_time":"2026-09-30T09:31:00-04:00","arrival_time":"2026-09-30T09:31:01-04:00","payload":{"seconds":60,"closed":true,"open":100,"high":100.2,"low":99.9,"close":100.1,"volume":1000,"notional":100050}}
```

bar.event_time 是区间结束时刻，seconds 定义区间长度；只接受完整收盘柱。聚合必须有连续且对齐的所有组成柱，缺口不插值。notional 可缺省，但对应 VWAP 能力可能不可用。

其他 payload 格式：

```json
{
  "trade": {"price":100.01,"size":200,"side":"buy","sequence":123},
  "quote": {"bid":100,"ask":100.01},
  "depth": {"mode":"snapshot","bids":[[100,1000],[99.99,600]],"asks":[[100.01,800],[100.02,500]],"sequence":321},
  "status": {"gap":true,"stream":"trade","halted":false},
  "news": {"source":"synthetic_event","event_key":"release-1","is_new":true,"values":{"actual":0.3}},
  "consensus": {"source":"synthetic_preplan","event_key":"release-1","branches":[{"name":"above","conditions":[["actual","gt",0.2]],"direction_by_symbol":{"SYN":1,"BENCH":-1}}]}
}
```

depth 的 bids 降序、asks 升序且价格不重复。sequence 是各 kind 自己的连续序号；检测到跳号或 status.gap 后，局部流能力可在干净窗口后恢复，整日 VWAP/Profile/CVD 的完整性不会被局部恢复伪造。新闻 event_time 保留原始发布时间，arrival_time 保留实际收到时间；两者均须处于 60/90 秒窗口，旧闻延迟送达不能变成“最新事件”。consensus 必须在新闻发布及收到前已知，在回放中不可修改。

SET-0028保留前一交易session的已知新消息；当天无关更新不会覆盖它。同一`event_key`的真实更正可显式提供`invalidates_prior: true`，撤销该前日消息前提。更正只在其真实发布时间和到达时间均已知后生效。

多维预案可额外提供 `consensus.payload.dimensions`，每个维度含 `consensus, scale, weight`；生成的 `_net` 是归一化加权意外，用于事前明确的数值分支。新增新闻可带同一 event_key 的后续阶段，分支改变后才满足再评估条件。

session.payload 的其他按需字段：

| 字段 | 含义 |
|---|---|
| references | 值为 `{value, source, as_of}`；as_of 必须早于本日 open |
| history | 已结束日的升序摘要，至少有 cash_open/as_of/source；同一日不能重复 |
| coverage | 如 `{"trades":{"complete":true,"start_time":"…"},"classified_trades":{"complete":true,"start_time":"…"},"depth":{"complete":true,"start_time":"…"}}`；不是填数指令 |
| premarket_complete | 盘前数据确实完整时为 true；需实际盘前分钟柱 |
| benchmarks | 同 venue 的相关产品 symbol 列表，产品需各自 session 与数据 |
| ib_minutes | IB 长度，默认 60 分钟；与来源/市场定义一致 |

常用 references 包括 `prev_high, prev_low, prev_close, atr14, average_daily_volume14, key_level, pm_support, historical_vwap, float_shares, daily_direction, prev_close_location, leadership`。前日 Profile 值为 `kind=volume|tpo, venue, val, vah, poc`；Volume 尾部版本还需 `low, high, lower_tail_fraction, upper_tail_fraction`，显著 POC 需 prominence。leadership 为事前 `{symbol, correlation, lag_bars}` 资料。

SET-0028 可提供事前已知的 `references.prior_acceptance_level`（沿用 `{value, source, as_of}`）；它表示前日消息后的内部接受位。缺省时退回前日高/低，不从次日最终行情倒推位置。该setup使用独立的 `second_day_edge/second_day_break` 特征，其他setup的前日极值保持原定义。

history 可含 `high, low, close, volume, vwap, volume_profile, tpo`，以及按纽约钟点或明确 offset 时间戳索引的 `closes, volumes`。ORB ATR 推导需 15 个完成日（含首个参考昨收）；SET-0048 必须有前 14 个完成日的同刻 cash_open 与 close，缺一不确认。历史切分与复权方法由数据端保持一致，日线拆并股调整与新闻撤回重放尚需在具体连接器验证。

OHLCV CSV 接入：

```sh
python3 -m recognition convert-bars --csv ../data/bars.csv --sessions ../data/calendar.jsonl --output ../recognition-runs/imported-events.jsonl
```

CSV 必需列：`symbol,venue,event_time,open,high,low,close,volume`。可选 `event_id,arrival_time,seconds,notional,quality`；seconds 默认 60。不提供 arrival_time 时用 event_time 作回放代理，报告中明确说明，不能据此验证真实新闻/交易延迟。每个品种的 session 来自 calendar.jsonl，不自动猜日历。

## 状态、证据与审计

| state | 含义 |
|---|---|
| candidate | 前提接近成立或必要数据尚不足，等待进一步证据 |
| formed | 按序满足形成条件，进入最终触发阶段；两阶段图首阶段可直接形成 |
| triggered | 当前路径的全部核心条件成立；只有 data_status=complete 才计入可评价确认 |
| invalidated | 对应结构失效，先前 triggered 记录仍保留 |
| expired | 未触发路径超过规则期限、session 结束或关键上下文断档 |
| completed | 已触发路径在收到现金收盘数据后结束 |

`detections.jsonl` 是状态转换日志，先按 detection_id 归组，勿将每行当独立事件。每条保留 setup/variant/source/rule 版本、原文/算法/config 哈希、冻结锚点、逐条条件 actual/expected/result、evidence_cutoff 与 identified_at、missing、related_ids。证据中的 source_ids 是定位示例，完整复现使用 run.json 中的完整输入 SHA-256，不声称列表包含每一笔数据。

structure_score 是 70% 核心进度 + 30% 数值增强项；evidence_score 单列可观察程度。两个分数均不是胜率或置信概率。柱内同时越过触发与风险参考且无法确定顺序时，标 intrabar_order_unknown，评价不把它当完整确认。

普通失效按价格极值或连续接受判断；Profile/流等用两个连续收盘越过风险参考，作为编辑定义。SET-0002 用 ATR 风险触及，SET-0048 用原采样时刻的动态 band 收盘判断，SET-0049 按现金日时间结束。早期观测、限价边缘观测均不表示可成交。

文件中途结束时，活动路径在 run.json.unfinished_at_eof 单列，保留最后状态；不会虚构现金收盘或把未确认候选改成已触发。迟到 K 线所用 CVD/Profile/VWAP 冻结到该柱结束时刻；未完成柱和未来新闻不参与历史判断。

## 参数与维护

`--config` 接受 JSON，示例：

```json
{"location_noise_fraction":0.15,"swing_factors":[0.35,0.7,1.4],"candidate_slack":0.15,"event_window_seconds":0.5,"footprint_min_volume":10,"footprint_min_levels":3}
```

这些参数只调整编辑定义的容差、形态尺度、候选宽度及微观观察。Hikkake 基础版三柱、失败趋势柱下一柱、ORB 五分钟、60/90 秒新闻及 SET-0048/0049 的原时钟不能被通用 config 改写；有依据的变化另建 variant_id。event_window_seconds 范围 0.01–10 秒，需结合真实数据密度校准；改参数后保留 configuration_sha256 并用锁定的样本外数据重新测量。

source_open 只接受开盘时刻实际收到的 trade 或 quote，中点观测标 `price_basis=quote_mid`，成交观测标 trade；保留 observed_price。quote 中点不等于交易所开盘成交价，两种观测都不证明按该价成交。

代码分层：models/adapters/data 为输入与增量数据；numeric/features 为因果特征；specs 为 50+34 规则；engine 为序列跟踪；evaluation 为独立标注与事件评价。目录由 build 生成，原 setup 的 reviewed 身份不自动变成算法 validated。

进入真实验证的条件是获得合规可用的历史 OHLCV、逐笔/DOM、新闻和相应标签。实时连接与模拟执行位于 `connectors/`、`runtime/` 和 `paper/`；合成回归不证明美股迁移适用或识别准确率。真实验证仍需用具体来源、范围和标注核对。

## 运行缓存与检查点

离线 `RecognitionEngine.feed(MarketEvent)` 接口保持兼容。长期运行显式传入 `RuntimePolicy`，将近期去重、检测、审计、行情及归档缓存限制在配置上限内；完整原始输入和检测由外层运行器持久化。缓存外的重复输入必须由持久日志先行去重，不能只依赖有限的 `engine.seen`。

```python
import json
from recognition.engine import RecognitionEngine
from recognition.policy import RuntimePolicy

engine = RecognitionEngine(include_variants=True, runtime_policy=RuntimePolicy())
checkpoint = json.loads(json.dumps(engine.snapshot(), allow_nan=False))
resumed = RecognitionEngine.restore(checkpoint)
counts = resumed.resource_usage()
```

检查点使用白名单 JSON 类型编码，保存活动候选、冻结锚点、数据上下文、覆盖累计及运行缓存策略；恢复校验检查点格式、状态哈希、规则、算法、配置和执行图。外层运行器继续校验事件日志、处理位置和模拟执行状态。版本或策略不一致要求重新回放，不续跑旧内存对象。

缓存截断保留明确的质量状态。缺失实际开盘柱、盘前完整范围或整日连续行情时，相应 VWAP、盘前及累计能力不能继续声称完整。真实开盘价独立保存，不会被截断后的首柱替代。新闻预案、累计 Profile/TPO、最近有效父检测、活动候选或候选边界达到保护上限时抛出 `CapacityExceeded`，交由运行器记录容量阻断并恢复；不会静默删除活动候选。`resource_usage()` 给出状态计数、上限和截断次数，便于解释长期运行的缓存填充及峰值。

`halted` 表示已观察的交易所停牌。网络连接恢复、计时、修订和缺包状态，以及交易日切换，都不能解除停牌；必须收到明确的交易所恢复状态。

## 分层覆盖与原审计关联

`registry.py` 登记所有规则引用特征的类型、提供路径、最低数据依赖、周期和未知原因。`validation.py` 同时检查注册契约及真实可调用提供路径；登记本身不表示当前输入有此能力。

```sh
python3 -m unittest evaluation.tests.test_runtime_recognition evaluation.tests.test_raw_coverage -q
python3 -m evaluation.tests.raw_scenarios > /tmp/intraday-raw-coverage.json
```

原始事件矩阵经过实际 `MarketEvent → 数据 → 特征 → 引擎` 链路，覆盖 84 张图的完整正向路径。每个基础版有两条原始输入负例，共 100 条；它们实际破坏不同核心谓词，保留完整事件数量和必要数据能力，在同一事件位置、周期与阶段记录正例为真、负例为假或阈值失败的条件证据，并验证目标品种和方向没有完整触发。输入哈希及冻结条件证据位于 `evaluation/tests/raw_negative_designs.json`，测试会重新处理原始输入校验这些证据。另有 68 条变体的缺观察、几何或缺资料补充控制，报告单独标明，不能替代基础版核心负例。

33 个变体有实际触发有无、因果触发位置或冻结锚点几何差异。SET-0027 的 public_price_proxy 与基础版使用相同公开价格谓词，单独标为可观测边界 `not_applicable`；报告验证实际谓词哈希和触发轨迹相等，保留声明的数据代理与研究身份，不虚构专有指标或价格行为差异。这些是合成工程回归，所有条目继续标 `market_validation=not_run`。

`audit_links.json` 为原 250 情形中 14 项待核查建立冻结输入、原始文件哈希、v1.2 复核证据、分层回归和边界解释。原审计文件保持原样；图层和特征层证据不会改称原始事件证据。
