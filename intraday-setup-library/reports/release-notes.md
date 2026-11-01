# 本轮交付与验收边界

当前验收状态（2026-10-05）：候选7的固定版本十小时合成稳定性和只读门禁均为 `passed`，有效观察36000.103071秒，83046条完整日志独立重放通过289.654239秒，39次故障、18次恢复，107文件f1核心未变。原报告结束于2026-10-05T06:50:27.833981Z，含收尾校验的单调总历时36308.589248秒；任何旧候选、压力、smoke、休眠或轮询时间均未累计，也不作为开发人时。正式归档身份、最终CRC/manifest、重建、CLI及提交/tag结果以项目外 `intraday-setup-library-v1.3.0-release-checks.json` 和 `runtime-runs/candidate7-final-release-20261005/delivery-completion.json` 为准。

候选6历史状态为 `unplanned_exit_report_missing`：发生未计划退出且缺少最终报告，十小时未通过。最后保存的状态来自19:32:21.694927Z，观察30828.070387秒、处理游标99814，低于36000秒；这不是准确退出时长，原 `running` 字段已陈旧。退出原因尚未确定，不能归因为OOM、应用重启或系统重启。原始证据保留，不以恢复或重放补足观察。

候选5历史验收为 `failed`，原因为 `SoakFailure/unexplained_late_rss_growth`。有效观察36001.827709秒、总历时36390.691123秒，124619事件的完整日志独立重放通过378.332637秒；资源检查第二四分位区间RSS中位数557.53125MiB至尾段785.875MiB，增长228.34375MiB，超过128MiB预算，因此未通过十小时稳定性验收。最高采样当前RSS887.28125MiB与进程峰值1215.953125MiB分别记录。候选4因历史日期及执行venue缺陷中断，实际观察28133.869205秒，97889事件的原版本全日志重放通过；候选3的RSS超限失败及全部中断原证据也保留。

候选5的[失败原报告](../../runtime-runs/soak-v1.3.0-20261003-candidate-5/soak-report.json)及[收尾状态](../../runtime-runs/soak-v1.3.0-20261003-candidate-5/soak-status.json)原字节保持不变：各402510 bytes，SHA-256均为 `47e0b43b907d3781c30818ee04326874699bcf9bda33a80895b71a527bf78340`。主机暂停、电源和有效计时边界见 [稳定性记录](../runtime/SOAK.md)，不以UTC历时填补观察或覆盖资源失败。

候选5的识别155方法/783子测试、运行152方法/169子测试通过记录继续保留；该核心上的最终前端产品30、实际13、研究8和受控30检查通过，包含8项新运行身份、旧缓存和SSE游标恢复边界。它们不冒充后继核心的重新验收。修复、新增独立回补分析及工作台操作缺口的实际范围见 [计划验收对照](implementation-acceptance.md)；当前身份和诊断状态见 [开发检查点](../research/development-checkpoint.json)，以实际外部报告为准。

资源修复核心现已冻结为 `f1dc3d6d042a3ae8d408b032e60197741fbaea902fad5af937371800d9aeb325`。识别171方法/833子测试、运行171方法/182子测试全部通过，0失败、错误及跳过，起止核心一致；[实际回归摘要](../../runtime-runs/candidate6-regression-integration-20261004/final-core-regression/regression-summary.json)单列工程范围。规则0.2.2、schema和实验执行模型1.0.0保持。[两阶段全日志压力诊断](../../runtime-runs/candidate6-resource-pressure-20261004/final-utf8-full-journal-pressure/pressure-report.json)已为 `passed_pre_soak_diagnostic`，原报告SHA-256为 `57991bef6ef7f75259957d89284ef0885d9fe0675deb36663ef1523d61f45a69`。两阶段各124619事件、22会话，总3159.317570375秒；第一阶段12次恢复、增长104.78125MiB、最高采样当前RSS671.296875MiB，第二阶段无恢复、增长4.09375MiB、最高采样当前RSS700.65625MiB。状态、派生记录、journal及原三个文件SHA一致，640笔成交、1294条账本记录和资金不变量通过。累积进程峰值1026.265625MiB（1076117504 bytes）已超过1GiB，不能把采样当前RSS预算通过写成全时刻低于1GiB或长期验收通过。旧组件对照及malloc负结果保留，默认allocator和原验收预算未调整。

候选6于2026-10-04T10:23:36.106592Z在项目外目录 `runtime-runs/soak-v1.3.0-20261004-candidate-6/`启动独立观察，约19:39Z检查发现PID58714、caffeinate58830及8781预览已不在。[原状态](../../runtime-runs/soak-v1.3.0-20261004-candidate-6/soak-status.json)停留在19:32:21.694927Z，最后观察30828.070387秒、游标99814；尚无最终报告或已确定退出原因，也没有系统重启证据。原目录不作为可写Runner打开，恢复及完整重放仅使用副本，诊断不能将缺失报告改为通过。此前20:23:36.106592Z下界已经不是当前完成边界；新运行不自动重启，须另建目录重新完整36000秒观察和门禁，所有旧候选、压力及smoke时长不累计。正式发布未完成，真实市场验证仍为 `not_run`。

[退出审计摘要](../../runtime-runs/candidate6-unplanned-exit-audit-20261004/failure-summary.json)SHA-256为 `00f766e8e0d3dc5ce86e5299c8bab49b4ac95c4afbaf9a441d4838cb51b662af`；[副本恢复及完整重放](../../runtime-runs/candidate6-unplanned-exit-recovery-20261004/recovery-report.json)SHA-256为 `989dc97041a748051cc2137c8460fca5729bba4c588853049404441626755c5d`。同一f1核心严格恢复4.906186秒、独立CLI重放99857条完整日志341.567466458秒，游标99857、pending0，账户及全部派生、引擎、模拟状态相等，515笔成交和1047条账本记录唯一。提交日志比陈旧状态多43条，不能据此补算观察时间；原目录、不可变快照及核心哈希保持不变。该恢复证据不改变候选6不足36000秒、缺少正式报告及原因未知的未通过结论。

候选7使用项目外独立目录 `runtime-runs/soak-v1.3.0-20261004-candidate-7/`，[原报告](../../runtime-runs/soak-v1.3.0-20261004-candidate-7/soak-report.json)SHA-256为 `4a8bf15a84122ebbb8f488c0692d0bc9086216b7fdeb8171280f2d49d5d6a450`，[只读门禁](soak-validation.json)绑定同一原报告、核心和完整数据库重放证据。[实际启动记录](../../runtime-runs/candidate7-launch-20261004/launch-record.json)保留2026-10-04T19:50:45.126572Z开始、launchd单次服务、`KeepAlive=false`且无自动重启。原1024MiB采样当前RSS和128MiB增长预算未调整；最高采样当前RSS687.671875MiB、累积进程峰值1016.078125MiB、后段增长10.265625MiB，处理P50/P95/P99为10.67/29.56/49.538毫秒，最大积压300。账户、完整派生、引擎、模拟状态和游标重放一致，425笔成交及869条账本记录通过核对。停止观察时仍有5个未关闭的合成持仓，无待入场或待退出，不补造成交；结果仅是本次合成输入、配置和环境的工程验收，不是已平仓策略回测或真实市场收益。全部旧时长单独保留，候选6的最后样本与缺少最终报告不因本次通过而改写。

该核心已完成 [新候选包干净解压验收](../../runtime-runs/candidate6-clean-delivery-20261004/validation-summary.json)：50项CLI检查、46方法/467子测试及1364条完整日志重放通过；459个manifest文件和三个fixture哈希、模块来源及恢复无重复成交均核对通过。[两次隔离重建](../../runtime-runs/candidate6-regression-integration-20261004/pre-soak-final-rebuild-results.json)与实际工程候选归档字节相同。[实际API及合成研究21项检查](../../runtime-runs/workbench-candidate6-api-20261004/actual-api-summary.json)另在新demo目录通过，未重跑未变化的前端浏览器测试。这些记录与旧候选和十小时验收分开。

现行版本、内容日期和实验执行模型版本以 [release.json](../release.json) 为准。50份setup正文继续保留来源版本；84张识别规则图分别检查数据及自动模拟能力。

已实现的链路是：行情事件接收与持久化 → 因果识别 → 同批信号仲裁 → 下一有效报价模拟执行 → 账本提交 → 本地工作台 → 按事件复盘与独立标注。数据源覆盖、未知资料、等待报价及待退出状态必须可见。

## 运行与恢复

从 [README](../README.md) 启动演示或免费IEX行情。运行目录和导出存放项目之外。账户只进行本地模拟；凭证只在本地环境加载，不能进入配置、日志、报告、导出或发布包。

恢复需要相同算法、规则、配置和执行模型身份；不同版本使用新运行目录回放。合成演示、记录流回放、实时IEX以及仅K线的成交代理模型分别保存运行身份，评价不得合并。

检查点编码与恢复降低共享证据的重复分配，Store仅序列化一次并分块计算UTF-8校验和。v1 JSON/校验和字节与冻结旧前缀fixture保持兼容；日历历史修复已将识别行为登记为0.2.2。生产恢复继续校验当前算法与核心哈希，不能用格式兼容跳过版本保护。新的核心需要重新完整计时，此前候选时长不累计。

最新修复采用v1流式规范编码及UTF-8外层缓冲，限制单次字符串转义缓存，减少完整中间AST暂存；检查点通过独立短连接提交或回滚后关闭，soak先释放旧Runner再恢复。session容量纯预检只在明确opt-in时启用，先于签名及状态修改检查完整记录和scalar预算，超限明确阻断而不剪数据。进程环境证据只保存白名单请求值、启动标志及构建/平台信息，有效allocator保持unknown。具体上限、同态诊断及验证局限见 [稳定性手册](../runtime/SOAK.md)的“候选5失败后的资源修复与验证范围”。

## 验收记录

- [recognition-validation.json](recognition-validation.json)：实际发现并运行的识别与评价测试、子场景、算法哈希和范围。
- [ui-results.json](ui-results.json)：离线知识库检索、导航、原始示意图和移动布局检查。
- [rebuild-results.json](rebuild-results.json)：两个隔离副本的文件/manifest及ZIP一致性。
- [validation-results.json](validation-results.json)：结构与Schema验证历史记录；最终发布再运行只读validate核验manifest。
- [runtime-validation.json](runtime-validation.json)：模拟执行、连接器、运行恢复、公共契约及发布边界的实际工程测试。
- [release-validation.json](release-validation.json)：候选5核心对应的原666f89cf候选包解包CLI50检查、原始路径/检查点30方法417子测试及隔离ZIP精确复现；前端追加修复由独立工作台报告和新候选manifest记录，测试时间均不计十小时。
- [workbench-validation.json](workbench-validation.json)：实际本地API运行的五视图、图表、原始日志、键盘和移动布局验证；截图哈希可关联项目外浏览器证据。

实际合成demo标注到报告的端到端检查使用 `python3 workbench/tests/research_browser_check.py --url 独立demo地址 --output 项目外截图目录`，它拒绝live/replay，标签仅为UI/API工程核对而构造，不计独立盲标或真实市场标签。

增加 `--require-active` 可要求在采集仍进行时提交标签。页面等后台实际完成后才显示已保存，延迟或超时明确保留等待状态且不自动重发；合成fixture覆盖这两条路径。

工作台浏览器测试命令为 `python3 workbench/tests/browser_check.py --url 本地运行地址 --output 项目外截图目录`。标注保存及暂停/恢复新入场的写操作另用明确合成fixture验证，不向真实行情运行写入测试标签。

原始识别矩阵区分84条正向图路径、100条基础版核心谓词反例和68条变体补充阻断控制；31条同目标共同周期机械边界、1条注册来源open时钟及1条注册周期选择分别计数，SET-0027公开价格代理可观察边界明确not_applicable。冻结评分通过run.json追溯实际行情、完整规则集合和完整检测批次，不能只凭声明的hash评分。工作台三阶段指标及完整标注范围与账户实验结果分层展示。

报告中的测试数以该次实际记录为准，不用预先写死的总数代替检查。较早版本的审计和验证记录保留其原日期及范围。

真实市场标签目前为0，精确率、召回率、跨市场迁移和盈利验证保持未运行。首轮真实IEX连接验收需有效凭证及开市时段，仍为 `not_run`。本次至少十小时固定版本合成稳定性及只读门禁已经通过；加速回放、短测及旧候选时长仍不能替代这次独立观察。知识库内容日期继续来自release.json，实际验收日期为2026-10-05，不将两种日期混用。

## 干净构建与发布

1. 运行必要测试，记录真实通过、失败及未执行项目。
2. 执行 `python3 scripts/report.py`，更新自动计数并保留人工研究问题。
3. 执行 `python3 scripts/build.py --clean`，重建知识库、目录、导出和manifest。
4. 执行 `python3 scripts/validate.py --require-jsonschema` 进行只读验证。
5. 执行 `python3 scripts/verify_rebuild.py`，验证两个隔离构建及ZIP的一致性。它会更新可复现性报告，随后再次build并只读validate。
6. 工程候选执行 `python3 scripts/package.py --candidate --output 项目外的新候选ZIP路径`；正式发布使用 `--soak-report 项目外稳定性目录/soak-report.json`，检查旁边的release-checks报告。

正式发布先执行 `python3 scripts/verify_soak.py --report 项目外稳定性目录/soak-report.json --output reports/soak-validation.json`。门禁要求真实十小时观察、当前核心身份、故障与资源证据、原始日志、原数据库及独立重放数据库全部一致；拒绝short、interrupted、running、failed及旧核心。候选5虽达到观察时长并通过重放，仍因failed状态被拒绝。只有门禁通过后，将原soak报告原字节保存为 `reports/soak-results.json`，再更新报告、构建、只读验证及正式包。较早候选包保留各自当时未通过的边界；本次候选7门禁已经通过。正式归档身份、最终CRC/manifest、重建、CLI及提交/tag结果以项目外 `intraday-setup-library-v1.3.0-release-checks.json` 和 `runtime-runs/candidate7-final-release-20261005/delivery-completion.json` 为准。

隔离构建使用 `python3 scripts/build.py --output 项目外的新空目录`。发布边界由 `scripts/release_support.py` 的具体目录和文件模式维护，排除隐藏文件、Git、虚拟环境、缓存、凭证、数据库和运行目录。新交付素材必须显式加入边界后验证；不能通过递归扫描整个项目来收集发布文件。

必要测试资料明确包含 `evaluation/tests/raw_fixtures.json`、`raw_negative_designs.json` 及 `fixtures/checkpoint_v1.json`。从空目录解包候选后复跑原始覆盖与检查点兼容性测试，确认测试模块和fixture来自实际分发内容；该工程检查不计十小时或真实市场验证。

ZIP按稳定路径排序，时间来自内容日期，权限固定为普通文件模式；打包后核对CRC、文件集合、manifest、每个文件大小与SHA-256。相同冻结输入及相同Python/zlib环境应生成相同归档字节。源码、报告或运行规则变动后需要重新冻结和核验。
