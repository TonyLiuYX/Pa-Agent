# 识别评价与标注协议

工程测试回答“规则图和因果计算能否按预期运行”，真实数据标注回答“是否识别出行情中的 setup”。[测试报告](../reports/recognition-validation.json) 中的合成结果与后者单独记录。当前真实行情标签数为 0，精确率、召回率均未知；不声称目标已经达成。

## 验收目标

| 层次 | 目标 | 样本边界 |
|---|---|---|
| 宽候选 | recall ≥ 0.95 | 条件可观察的 setup 出现窗口 |
| 严格形成 | recall ≥ 0.85、precision ≥ 0.85 | 按序成立的 formed/triggered 事件 |
| 明确触发 | 独立报告 precision/recall、延迟 | 与手工 trigger_time 对照 |
| 单条基础版的首轮样本 | 至少 30 个正例 + 30 个困难负例 | 至少 10 正 + 10 负留给锁定 test |
| 每个可宣传变体 | 单独验证 | 不从基础版/其他数据权重继承成绩 |

样本数量是首轮最低要求，不足以精确证明长期识别性能。评价输出 Wilson 95% 区间；少量样本的 100% 不能代替跨时期验证。CLI 的 `target_met_on_this_slice` 仅表示该真实、test、单品种/场所/环境切片已有 10 正/10 负且点估计达标；完整验收仍需检查总样本 30/30、区间、全部 setup 与变体、覆盖的时期和环境。

## 建立独立标注集

固定原 Markdown 版本及其七轴边界，先看当时价格/流/背景，再判断事件和触发，不用未来盈亏决定有效性。标注者不从引擎输出倒推“正确标签”。建议两人独立标注，有争议交第三次复核；本版没有实际完成此人工过程。

每条保存完整识别窗、方向、品种、venue、来源数据位置、日期/事件家族、市场环境和触发时刻；负例优先覆盖单看形态相近但前提不满足、顺序错、回测失守、前后窗口超时等情况。全交易时段的 precision 需要连续且完整的标注，不能只抽取漂亮正例和少量负例。

JSONL 示例仅是合成标注格式：

```jsonl
{"case_id":"fixture-1","setup_id":"SET-0005","variant_id":"base","symbol":"SYN","venue":"SYNTHETIC","direction":1,"positive":true,"start":"2026-09-30T09:40:00-04:00","end":"2026-09-30T09:44:00-04:00","trigger_time":"2026-09-30T09:42:00-04:00","group_id":"synthetic-2026-09-30-event-1","split":"test","source_kind":"synthetic","source":"hand_authored_fixture","regime":"range_trap","data_eligible":true,"disputed":false}
```

真实样本将 source_kind 设为 real_market 并填写真实数据出处，不通过修改示例标签制造真实验证。positive=false 为负例；disputed=true 或 data_eligible=false 会单列并排除指标。缺必要 DOM、历史或事件发布时间时，应明确 data_eligible=false 与原因，不能把数据缺失视作 setup 反例。

## 切分与防泄漏

group_id 至少识别交易日和相关事件家族。同一消息、同一天相关产品、同一事件的周期/多空/参数变体不跨 development/validation/test；系统拒绝同 group_id 指向不同 split。省略 split 时按 group_id 哈希分配约 60/20/20，这是可复现分组，不保证小样本恰有该比例。后续时段最好另外做完全向前的时间验证。

调参只看 development 与 validation，test 锁定后不再反复择优。使用未调整到未来的日历、证券池、流通股、新闻发布时间及一致的拆并股调整；对应数据与配置的哈希随回放保存。若日内同一事件跨多个日期，应使用能覆盖全部相关记录的共同 group_id。

## 评价规则

运行：

```sh
python3 -m recognition evaluate --detections ../recognition-runs/run/detections.jsonl --labels ../data/labels.jsonl --output ../recognition-runs/run/evaluation.json
```

先按 detection_id 归并状态日志，对 setup/variant/symbol/venue/direction 和标注时间窗做一对一最大匹配。一个手工事件最多计一个 TP；窗口里的额外完整识别计 FP。不同 setup 可以同时成立，不采用互斥分类。跨周期先后产生的重复事件由标注窗揭示并计入 precision，不能自动拿多次预测增加 recall。

只对 data_status=complete 的候选/形成/触发统计可评价事件；完整性不足不计为 TP。没有覆盖到的时段标 `ignored_unannotated_detections`，不自动计 FP。结果按 setup、variant、synthetic/real_market、split、品种、场所、环境分别报告 TP/FP/FN、Wilson 区间、每已标注小时误报、确认延迟；不同版本和来源类型不合并成一个漂亮总数字。

延迟以 identified_at 减手工 trigger_time（未提供则用 start）计算，候选可早于 trigger_time，因此允许负值；真实低延迟验证须使用实际到达时间，CSV 的 event-time 代理不具备该条件。精准的阶段级评价需要分别标注 formation 与 trigger 窗口；同一宽窗口报告只说明事件级覆盖。

### 阶段窗口与完整覆盖

`evaluate(predictions, labels)` 保留原有抽样窗口语义。可选第三个参数 `coverage` 保存连续完整标注区间：在这些区间内未匹配到正例的完整预测计FP，区间外继续保持未标注。只有显式 `complete=true` 的覆盖可参与评价；覆盖与争议或资料不足的案例相交时拒绝运行，避免把未知当作反例。方向可省略表示覆盖多空两侧，提供时必须是整数1或-1。

```json
{"coverage_id":"day-1-set-0005","setup_id":"SET-0005","variant_id":"base","symbol":"SPY","venue":"IEX","source_kind":"real_market","source":"具体数据出处及复核记录","group_id":"2026-10-02-correlated-event-family","split":"test","start":"2026-10-02T09:30:00-04:00","end":"2026-10-02T16:00:00-04:00","complete":true}
```

标注的可选 `stage_windows` 分别提供 `candidate`、`formed` 和 `triggered` 的 `{start,end}`。各窗口必须位于外层窗口内；省略某阶段则沿用外层窗口。形成报告的字段仍为 `confirmed`，兼容已有消费端。工作台标注 `complete_coverage` 默认false，显式true声明当前窗口内所选Setup/变体/方向已完整标注；只有无争议、必要资料可观察的标注可声明。运行研究报告将该声明转换为独立coverage记录，并分别展示三阶段TP/FP/FN；无标签、版本错或容量超限时不伪造指标。触发时刻必须是有效的带时区时间且位于外层窗口内；方向布尔值不作为整数处理。

### 冻结数据集

`evaluation.dataset.freeze_dataset` 将标签、完整覆盖、数据/规则/配置/算法SHA-256和相关组切分锁写成带自身校验和的manifest。冻结标签额外要求 `annotator`、`rule_version` 和 `data_source`。保存采用独占创建，防止覆盖锁定test；输入顺序不改变标签与覆盖哈希。冻结只锁定标签与声明身份；评分还必须提供实际运行的 `--run-report`。`evaluate_dataset` 先核验manifest，再核对运行报告 `input_provenance` 与实际行情、规则、检测文件的字节SHA-256；行情和规则还须等于冻结数据集身份，算法与识别配置须与每条预测一致。规则集合校验Setup/变体及版本，检测文件按序逐条等于完整预测批次，不能只提供另一个批次或选择性子集。行情格式逐行校验、文件哈希分块计算，评分结束再次核对输入未变。报告保留输入文件哈希、运行ID、处理游标与来源范围。

```sh
python3 -m evaluation.dataset freeze --help
python3 -m evaluation.dataset verify --manifest ../data/frozen-dataset.json
python3 -m evaluation.dataset evaluate --manifest ../data/frozen-dataset.json --run-report ../runtime-runs/run/run.json --detections ../runtime-runs/run/detections.jsonl --output ../runtime-runs/run/locked-evaluation.json
```

`run.json.input_provenance` 使用 `data_file`、`rule_file`、`detections_file`（相对该报告目录）和对应三个 `*_sha256`，以及 `algorithm_sha256`、`configuration_sha256`、`scope` 与可选整数 `processed_cursor`。`configuration_sha256` 是识别配置指纹，区别于整个运行平台配置。离线识别replay和runtime export都保存实际 `events.jsonl`、`rules.json`（versioned完整规则集合）、`detections.jsonl`。运行导出只包括该一致事务快照下已处理的market事件；完整journal另行保存控制与计时事件。移动文件可显式传 `--events`、`--rules`、`--detections`，哈希身份仍不可覆盖。`recognition evaluate --dataset` 同样要求 `--run-report`；原来的非冻结 `--labels` 接口继续可用。

文件追溯证明这些评分资料一致，不作为独立签名或识别正确性的证明。冻结与工程测试本身不构成市场验证。账户表现使用独立实验执行模型报告，不能混入识别精确率或来源策略业绩。

## 当前完成与后续数据工作

工程测试包含50个手写基础特征路径及34个变体路径；原始MarketEvent矩阵包含84张图的正向路径，50个基础版的100条真正核心谓词反例（正反输入保留同事件及同周期的已知真假观察），以及68条变体补充阻断控制。同品种、同方向、共同注册周期的机械边界为31条；source_open的独立来源时钟及ross_10s的注册周期选择各另计1条，不能冒充共同周期的形态差异。SET-0027 public_price_proxy的可观察价格谓词与基础版相同，保留来源身份并将observable boundary明确标为not_applicable。SET-0028提前触发以同方向60s的变体event3对基础版event6验证，不比较相反方向的首次触发。另核对固定时钟、严格内包、微观吸收、缺项、延迟到达、不可重绘、同刻多参考价位和重复计数。特征图单测直接输入数值，不证明所有 setup 的原始特征检测已经准确。

美股接入按三个批次进行：价格/时间与历史 → 实际逐笔/DOM/Profile → 新闻及同步跨产品。逐条检查数据能力和 migration_unvalidated；源于期货/外汇时钟的结构在美股上仍需另测。新闻语义由人工或事前注册的结构化分支提供。

免费IEX连接器、行情质量状态、模拟成本与执行审计已经实现；首次真实连接、实际行情标签、市场质量与样本外结果、真实行情容量/延迟基准及十小时固定版本稳定性验收仍待真实证据。缺这些数据时，工程验收可以通过，市场验收保持 not_run。

独立回补研究报告只有 `status=completed`、`input_selection_complete=true`、`analysis_completed=true` 时才能进入冻结评分。容量阻断、缺少历史上下文或输入被拒绝均不产生评分文件；freeze命令仅锁定声明与标签，不替代评分入口的资料完整性检查。
