# 质量审查报告 · v1.3.0 · 2026-10-03

完成50个独立reviewed ID，正文逐条保留前提、位置、时序、触发、失效和管理。这里的审查是本研究AI辅助逐条语义自审及工程检查；没有独立专家复核、真实市场策略回测或实盘账户审计。新增模拟账户单独进行工程检查，不能继承来源策略收益。

## 完成数量与去重

| 主记录状态 | 独立ID数 |
|---|---|
| reviewed | 50 |
| draft | 0 |
| provisional | 0 |
| candidate | 0 |
| merged | 0 |
| deprecated | 0 |

主记录总数50；候选116条，其状态为{'merged': 11, 'candidate_unread': 47, 'retained': 50, 'provisional': 4, 'related_not_counted': 1, 'rejected': 3}。候选、别名、合并去向和多标签不增加合格数。69份候选决策与50份逐条语义审查记录可追溯；74条关系双向浏览。

每条第11节比较最近1–3条，在入场前写明差异。重复判断采用七轴而非文本聚类：M2/H2、ORB窗口、poor high/low、压缩阈值、吸收显示工具和量化仓位参数保留为变体；位置模块重定向不宣称完整执行规则同义。最接近的结构见[比较指南](../indexes/comparisons.md)。

## PA、领域、市场与数据覆盖

PA core=20（40%），supporting=23，non_primary=7。按价格顺序是否主导必要决策判断，不因画在K线上就归PA。

| 可重叠方法家族 | 条目数 |
|---|---|
| opening_gap | 10 |
| price_action_reversal | 9 |
| liquidity_inventory | 9 |
| auction_profile | 9 |
| tape_DOM | 8 |
| price_action_trend | 6 |
| event_driven | 5 |
| footprint_delta | 5 |
| VWAP | 4 |
| quantitative_time | 3 |
| compression_breakout | 2 |
| relative_strength_lead_lag | 2 |

| 来源实际讨论的市场类别 | 条目数 |
|---|---|
| futures | 35 |
| stocks | 14 |
| ETF | 4 |
| FX_venue_unconfirmed | 3 |
| unconfirmed | 2 |
| options | 1 |
| crypto | 1 |

市场类别可重叠；futures含外汇标的期货，FX_venue_unconfirmed明确未确认场所，不算额外现汇验证。unconfirmed保留源图品种缺口。日线、跨日、概念补充另列markets_context_only。各市场、家族、数据轴不能相加当独立ID。没有把任何规则机械迁移为已验证的跨市场适用。

| 必要数据标签 | 条目数 |
|---|---|
| OHLC | 41 |
| volume | 16 |
| DOM | 12 |
| trade_prints | 5 |
| volume_at_price | 3 |
| session_profile | 3 |
| volume_optional | 3 |
| TPO | 3 |
| premarket | 3 |
| VWAP | 3 |
| footprint | 3 |
| session_clock | 3 |
| volume_profile | 3 |
| EMA_optional | 2 |
| news | 2 |
| initial_balance | 2 |
| official_release | 2 |
| bid_ask_classified_trades | 2 |
| diagonal_footprint | 2 |
| previous_close | 2 |
| previous_session_levels | 1 |
| daily_ATR | 1 |
| time_and_sales | 1 |
| bid_ask_volume | 1 |
| trend_structure | 1 |
| prior_session_levels_optional | 1 |
| original_pattern_stop_reference | 1 |
| session_volume_profile | 1 |
| historical_session_VWAP | 1 |
| stock_OHLC | 1 |
| index_OHLC | 1 |
| prior_day_levels | 1 |
| news_timestamp | 1 |
| synchronized_market_OHLC | 1 |
| consensus_notes | 1 |
| synchronized_prices | 1 |
| news_sequence | 1 |
| prices | 1 |
| prior_session_TPO | 1 |
| cumulative_delta | 1 |
| trade_classification | 1 |
| classified_trades | 1 |
| minute_OHLCV | 1 |
| VWAP_optional | 1 |
| intraday_prices | 1 |
| overnight_OHLC | 1 |
| cash_open | 1 |
| prior_cash_close | 1 |

session、时区、前日/当前段Profile及现金/隔夜边界在各条目明示。未知触发周期保留null与原因；不能默认为纽约现金日。

## 来源与独立性

来源记录86，实际阅读76，被最终条目引用64，实际阅读来源族23；全部记录包含25族。未读10条不支持最终核心规则。正文相关页、全文、公开转写和缓存作者稿分别标阅读范围，获取与阅读分开。

44条引用至少两份已读实质资料。6条富信息单份原始来源例外：[SET-0001](../setups/SET-0001.md)、[SET-0002](../setups/SET-0002.md)、[SET-0004](../setups/SET-0004.md)、[SET-0011](../setups/SET-0011.md)、[SET-0048](../setups/SET-0048.md)、[SET-0049](../setups/SET-0049.md)。例外可还原规则，但都标独立佐证有限，并分别注明编辑管理模块。

| 独立佐证类别 | 条目数 |
|---|---|
| limited_single_family | 24 |
| independent_context_only | 15 |
| multiple_families | 7 |
| independent_event_facts_only | 2 |
| limited_derivative_corroboration | 1 |
| independent_counter_evidence_only | 1 |

multiple_families只用于多个独立族的具体核心说明。independent_context_only是概念/背景互补；independent_event_facts_only只补官方事实；independent_counter_evidence_only补反证；limited_derivative_corroboration含来源继承。这些类别不能宣传为“全部已交叉验证”。

| 来源族 | 实读资料 | 最终条目引用数 | 核心规则覆盖数 |
|---|---|---|---|
| axia | 25 | 20 | 20 |
| galen-woods | 13 | 7 | 6 |
| atas | 6 | 4 | 2 |
| brooks | 3 | 8 | 7 |
| oneoption | 3 | 1 | 1 |
| smb | 3 | 3 | 2 |
| concretum-bbt | 2 | 2 | 2 |
| adam-grimes | 2 | 3 | 1 |
| tradingsim | 2 | 4 | 2 |
| bookmap | 2 | 3 | 1 |
| rayner-teo | 2 | 1 | 0 |
| ecb | 2 | 2 | 0 |
| jigsaw | 1 | 4 | 2 |
| sai-trades | 1 | 8 | 7 |
| warrior | 1 | 2 | 2 |
| bulkowski | 1 | 1 | 1 |
| quantconnect | 1 | 0 | 0 |
| ninjatrader | 1 | 1 | 1 |
| amt-balance | 1 | 1 | 0 |
| luxalgo | 1 | 1 | 1 |
| gao-han-li-zhou | 1 | 1 | 1 |
| lebaron-zhao | 1 | 0 | 0 |
| federal-reserve | 1 | 1 | 0 |

Axia实读25/76（32.9%），被20/50条引用（40.0%），是主要集中风险。Galen/Brooks也在PA中集中。网站域名和托管者未当成独立作者；OneOption/Gao的大小写及转载家族已归一。约100份实质资料的探索方向未达，本版实际76；没有把未读页/搜索摘要凑入。

## 分歧、数字与盈利主张

- 13母柱/内柱、影线/收盘内包分版本；16的1或2ATR缓冲不择优拼接；19原文空头买卖方向文字冲突已解释。
- 26历史高量日与SPI持续大买量反例保留，来源年份冲突不猜日期；42/43的300%例与150%默认、连续档数编辑阈值分开。
- 37的TPO选择、默认Volume、60分钟收盘和两TPO bracket触及接受不是同版；AMT2008–2026、4662 session、1090触发、62%touch/33%clean为作者统计，成本/原数据未核，不是本条净胜率。
- 45来源将被套买方下跌称short liquidation的文字冲突已注明，未照抄为事实；Jigsaw/不同产品的厚薄是相对比较，不能从一篇建立永久排名。
- 48同刻绝对偏离均值与半小时检查，49含隔夜首段与原模型时间风险已核对；新增硬价格止损、杠杆或分钟检查不能继承原回测。
- source/evidence保留原规则、编辑操作化、官方事实及反证支持范围。重要编辑模块标editor_proposal_not_tested，未归因给作者。

盈利状态：{'not_established': 47, 'author_backtest_specific_version': 1, 'author_backtest_not_reproduced': 2}。只有作者特定历史研究，独立复现与账户审计数量均为0；无利润排行，图表与资料完整性不等于正期望。

## 视觉材料与实际查看

107份视觉记录：外部真实价格/成交图57，本地原创非行情SVG50。原始来源示意、重绘行情、绩效表、实际观看视频均为0份计入本版覆盖。

- 广义已看行情覆盖46/50（92%），包括背景图。
- 排除背景的结构/片段/历史模型覆盖44/50（88%），仍不代表全部触发与订单核验；36只是Footprint片段，2/48是历史价格叠加模型。
- 只有背景：[SET-0046](../setups/SET-0046.md)、[SET-0050](../setups/SET-0050.md)。真实价格案例缺口：[SET-0025](../setups/SET-0025.md)、[SET-0028](../setups/SET-0028.md)、[SET-0037](../setups/SET-0037.md)、[SET-0049](../setups/SET-0049.md)。
- 原创结构示意覆盖50/50，单列，不提高真实案例率。

真实原图再分发授权未知，local_path为null；仅精确外链、PDF页序、哈希/尺寸与原创解读交付。日期/周期/时区/合约未知保持null和原因。全部source chart_viewed标签来自实际静态画面查看；没有将下载未看的图片或文字案例计入。图是否成交、机构身份、真实止损/收益未核。

在57份外部图中，品种未核实9、日期未核实42、周期未核实26、时区未核实53；这些字段缺口有重叠。57份均保存被查看图像/渲染页的哈希和尺寸，但哈希不证明金融数据来源或订单真实。

## 验收与工程验证

逐条语义自审记录了来源支持范围、日内/跨日边界、近邻区别、风险/管理版本和视觉限制；重点复核了单源、相近结构、精确数字与源文冲突。机械检查覆盖UTF-8/JSON Schema、ID、引用、15节、指纹、来源实际阅读、素材权限和路径、双向关系、候选决策、全文JSON/CSV一致性、全部HTML内部链接/图片/anchors和manifest哈希。

结构验证报告版本：1.3.0，状态：passed；JSON Schema：passed。离线浏览器报告版本：1.3.0，状态：passed，完成32个测试场景。实际测试细节和工具环境见[验证记录](verification.md)。最后交付前仍以最终validate与ZIP完整性结果为准；报告不将未执行检查标为通过。

程序化识别包含50个基础路径与34个独立变体；工程测试报告版本：1.3.0，状态：passed，实际执行171个测试方法。手写特征路径与原始格式合成行情分别说明范围，均不能替代真实行情标注。原规则正文版本仍为1.0.0，数值解释为编辑操作化。新增运行平台、免费IEX、统一模拟执行和五视图工作台的范围见[发布说明](release-notes.md)。真实行情精确率/召回率、美股迁移及首轮实时连接仍待执行。十小时固定版本合成稳定性测试及独立全日志重放已通过，实际观察时间与冻结身份以soak-results.json为准；真实市场性能仍未验证。已保留各次运行证据：`soak-v1.3.0-20261003-candidate-1`中断，观察355.378755秒；`soak-v1.3.0-20261003-candidate-2`中断，观察2045.628135秒；`soak-v1.3.0-20261003-candidate-3`失败，观察1754.653483秒，错误为SoakFailure/rss_budget_exceeded；`soak-v1.3.0-20261003-candidate-4`中断，观察28133.869205秒；`soak-v1.3.0-20261003-candidate-5`失败，观察36001.827709秒，错误为SoakFailure/unexplained_late_rss_growth；`soak-v1.3.0-20261004-candidate-6`未计划退出，最后保存的观察样本30828.070387秒，缺少最终报告，未通过；该样本不是最终观察或准确退出时长，秒数不累计到新候选。各次失败或中断时长分别记录，不累计到新候选。详情见[使用指南](../recognition/README.md)、[评价协议](../evaluation/README.md)及[实际测试记录](recognition-validation.json)。

## 未执行、未知及接续

没有独立盈利回测、当下市场适用验证、动态DOM/视频/字幕审核、账户单据、所有外链的长期监控、Safari/Firefox专项兼容或第三方专家复审。没有外部发布或私人数据。已读网页的发布日、部分图的时区/周期/日期、部分交易场所、费用及订单排队仍未知，逐条保持缺失。

访问分类：{'checked_accessible': 77, 'restricted_access': 4, 'wrong_or_empty_content': 1, 'http_404_at_check': 1, 'temporary_network_failure': 2, 'direct_restricted_cached_text_read': 1}。403/402是限制，404是本次响应，DNS/网关属于临时网络，错误/空页面另记；不能将一次失败判永久死链。YouTube字幕轨道返回空内容，CUA控制超时；本地阅读UI使用Playwright验证。

下阶段优先补4个视觉缺口、46/50触发画面、Axia独立失败资料及37可比反证；pairs/纽约FX未明确全部日终风险边界，不计50。研究候选与具体问题保存在[state](../research/state.json)及[handoff](../research/handoff.md)。本版满足至少50个实质不同规则记录目标，来源独立性、视频和盈利核验仍有上述明确局限。
