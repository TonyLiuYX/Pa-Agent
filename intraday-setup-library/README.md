# 日内交易 Setup 知识库与模拟研究工作台

本版完成50个实质不同的 reviewed 主记录，以简体中文说明前提、位置、行为顺序、触发、失效、管理及证据边界。reviewed 表示本研究规则记录审查通过；没有独立证明这些方法的净盈利能力。

v1.1 新增全部50条的程序化识别研究引擎及34个变体，保留原文规则、独立版本和可追溯条件。算法采用多周期、多转折尺度、位置容差、逐笔窗口和多个关键位的独立序列跟踪；真实行情召回率、精确率及美股迁移仍待验证。

v1.2 根据250种定向情形审计修复端点触发、兼容同帧阶段、显式回测变体、数值特征及噪声带止损后重入，算法规则升至0.2.0。修复范围、保留限制和实际复验见[修复说明](reports/recognition-fixes.md)；原50条setup正文保持原版本。

本轮增加免费Alpaca IEX行情连接、持久化运行日志、崩溃恢复、统一自动模拟账户和本地浏览器工作台。现行项目版本、内容日期和实验执行模型版本以 [release.json](release.json) 为准。工程检查、真实市场验证和实验账户表现分别报告。

## 启动研究工作台

Python标准库即可运行无密钥演示，使用固定观察名单SPY、QQQ、AAPL、MSFT、NVDA。运行目录必须在本项目之外：

```sh
python3 -m runtime serve --mode demo --data-dir ../runtime-runs/demo
```

默认演示只有一个合成交易会话，结束后显示SOURCE_ENDED。需要跨会话观察可用 `--sessions 20 --pace 0.25`；它按合成会话推进，跳过周末但不冒充真实交易日历。会话数和实时历史天数也可在配置中设置为 `demo_sessions`、`alpaca_history_sessions`。

打开终端返回的本地地址，默认是 `http://127.0.0.1:8765`。工作台包含总览、行情与证据、模拟账户、逐事件回放与标注、能力矩阵及研究报告。演示资料始终标为合成资料；关闭浏览器不会停止后台。终端Ctrl+C结束进程，日志和账本保留。

接入免费实时IEX行情时，先按 [连接器说明](connectors/README.md) 在本地环境设置Alpaca密钥，再使用Python 3.11或更新版本安装固定依赖 `websockets==17.1`。根目录与连接器的依赖清单使用同一版本；安装、诊断与运行应使用同一个Python环境。环境诊断只显示是否配置，不打印凭证：

```sh
python3 -m pip install -r requirements-runtime.txt
python3 -m runtime doctor
python3 -m runtime serve --mode live --data-dir ../runtime-runs/live
```

实时启动可用 `--history-sessions 30` 请求最多30个已完成会话，默认15，0表示不请求历史。明确前一交易日缺资料时，其高低点和收盘参考保持未知；ATR及同刻校准只使用所需连续、完整的交易日，不用更早日期替代。

系统显式使用IEX，其成交量和覆盖不能当作全美市场完整观测。深度、真实买卖分类、新闻等资料缺失时保持未知；所有84张规则图都列入能力检查，必要资料及执行条件满足才参与自动模拟。真实连接需本地有效凭证及实际开市时段；首轮实时连接验收目前仍待执行。

恢复时使用原目录；算法、规则、配置或执行模型改变后，恢复会拒绝续跑，需要新建运行并回放原始资料：

```sh
python3 -m runtime resume --data-dir ../runtime-runs/demo
python3 -m runtime replay --input ../data/events.jsonl --data-dir ../runtime-runs/replay
python3 -m runtime export --data-dir ../runtime-runs/demo
python3 -m runtime analyze-backfill --data-dir ../runtime-runs/live --output ../recognition-runs/backfill-study
```

回补分析只读取已提交资料并使用独立识别器，不修改原运行或创建模拟账户。输出目录必须是项目外的新目录；缺对应品种、场所、日期及当时已知的session上下文时保存缺项并返回blocked，不借用当前日资料。

统一账户默认初始资金100,000美元、每次预算净值10%、最多5个品种、不加仓或自动反手；采用触发后下一条有效报价、1 tick不利滑点、默认零费用、1%止损阈值、2%目标阈值和60分钟最长持有。缺报价时订单等待或超时，不补造成交。详细冲突、日终和当日风险规则见 [模拟执行说明](paper/README.md)。这些是独立实验执行规则，不能继承来源策略收益。

使用 [配置样例](config/runtime.example.json) 调整观察名单和模拟参数。密钥只放本地环境，不能写入配置、日志、导出或发布包。不会发送真实交易订单，也不会调用收费LLM。

完整运行接口、事务和恢复操作见 [运行平台说明](runtime/README.md)，实际墙钟十小时稳定性方法见 [稳定性操作手册](runtime/SOAK.md)。原发布包、Git基线及校验范围保留在 [开发基线](reports/baseline.md)。行情、检测、账户、日志、标注与冻结评价数据集的公共契约位于 [公共契约说明](schema/runtime-contracts.md)、[数据字典](schema/data-dictionary.md) 及相应 `*.schema.json`；Schema还需配合时序、资金和哈希语义校验。

当前检查点优化保留v1 JSON与校验和格式兼容，生产恢复继续校验核心身份；实际容量、重放及备份恢复的分阶段内存结果见 [验收记录](reports/verification.md)。短时资源诊断不计十小时。候选3因RSS超限失败，候选4在独立复审发现历史日期与执行venue缺陷后正常中断；候选4实际观察28133.869205秒，97889事件的原版本全日志重放通过。原证据均保留，旧时长不累计。修复版本必须在新目录重新完整计时，最新恢复位置见 [开发检查点](research/development-checkpoint.json)，逐项实现与缺口见 [计划验收对照](reports/implementation-acceptance.md)。

## 验证状态

候选7的固定版本十小时合成稳定性验收及只读门禁已通过：有效观察36000.103071秒，83046条完整日志独立重放通过289.654239秒，覆盖39次故障和18次恢复，107文件的冻结核心保持不变。实际结果见 [十小时原报告副本](reports/soak-results.json)、[只读门禁](reports/soak-validation.json) 和 [稳定性操作手册](runtime/SOAK.md)。停止测试时仍有5个未关闭的合成持仓，没有补造平仓；该结果仅说明本次输入、配置和环境下的工程稳定性。候选1–6的失败或中断记录保留，任何旧时长、压力、smoke或轮询时间均未累计至本次观察，也不作为开发人时。

真实市场标签仍为0，首轮真实IEX连接、识别精确率/召回率、美股迁移和盈利验证均为 `not_run`。工程测试和合成模拟结果不代替市场验证。正式归档身份、最终CRC/manifest、重建、CLI及提交/tag结果以项目外 `intraday-setup-library-v1.3.0-release-checks.json` 和 `runtime-runs/candidate7-final-release-20261005/delivery-completion.json` 为准。当前边界与流程见 [发布说明](reports/release-notes.md)。

## 程序化识别与回放

从 [使用指南](recognition/README.md)、[全部50条可执行目录](recognition/catalog.md) 和 [评价协议](evaluation/README.md) 开始。工程测试结果见 [识别测试报告](reports/recognition-validation.json) 与 [合成回放摘要](reports/recognition-smoke.json)。实时增量接口与JSONL回放使用同一引擎；通用CSV适配器需要显式交易日历。

```sh
python3 scripts/test_recognition.py --require-jsonschema
python3 -m recognition demo --output ../recognition-runs/demo-input
python3 -m recognition replay --input ../recognition-runs/demo-input/events.jsonl --output ../recognition-runs/demo-run --variants
```

宽候选、严格形成、触发和失效单独输出；数据不足不会被补成订单流确认。合成示例只用于工程复现。输入与结果置于项目外，每次回放使用新目录；原有50条正文不因算法化而自动变更为已验证策略。

## 打开与检索

双击 [离线首页](site/index.html)。普通桌面浏览器即可使用，无需服务器、账号或联网；英文名、中文名、别名和完整正文均参与搜索。空格分隔关键词取交集，十个筛选维度也取交集。清空筛选可恢复50条。

勾选1–3个条目后点击“比较已选条目”，检查七轴指纹、入场、失效和管理。选择在筛选变化后保留，使用“清空选择”重置。点击条目进入15节全文，再通过双向相关导航比较最近结构。来源页反向显示被引用条目和证据的具体支持范围。

先阅读 [学习路线](indexes/learning-paths.md)、[术语表](indexes/glossary.md)、[多轴分类](indexes/taxonomy.md) 和 [结构比较](indexes/comparisons.md)。只需要Markdown时从 [目录](indexes/catalog.md) 开始。

## 证据与图表的读法

86份来源记录中，76份有实际阅读范围，覆盖23个来源族；64份用于最终条目。来源、阅读、引用和独立核心核验分别计数。7条有多个独立族的核心说明，其余大多为同族或只有背景补充，逐条如实标明。

46/50条有实际查看过的行情图；44/50条有对应结构片段或模型示例，两条只有背景图。SET-0025、0028、0037、0049保留真实案例缺口。历史行情上的假设订单不是实盘审核，Footprint片段也不等于完整触发验证。

因未确认原图再分发授权，真实图表只提供原站图片/精确PDF页链接、原创解读及核对状态，需要联网。包内50张原创SVG七轴示意可离线显示，均标“非真实市场数据”，不增加真实案例覆盖。source_schematic、reconstructed_chart等类型预留在schema中，本版没有将它们冒充行情。

## 文件职责

| 路径 | 职责 |
|---|---|
| setups/SET-*.md | 唯一正文主记录；JSON兼容YAML front matter + 15节 |
| data/sources.jsonl | 来源作者、家族、时间、访问和实际阅读范围 |
| data/evidence.jsonl | 主张性质、支持/背景/限制关系、定位、核对状态 |
| data/visuals.jsonl | 视觉来源、权利、日期/周期/时区缺失、查看范围 |
| data/relations.jsonl | 条目之间双向浏览的语义关系 |
| data/taxonomy.yaml | 多轴分类；JSON兼容YAML，避免额外解析依赖 |
| assets/diagrams | 本项目原创SVG，非真实价格图 |
| indexes | 目录、别名、比较、来源、术语、学习路线 |
| exports | 全文JSON与UTF-8 BOM目录CSV，均由主记录生成 |
| site | 可直接file://打开的离线HTML，没有外部脚本或追踪资源 |
| schema | 主记录、行情、检测、日志、执行、报告、标注和冻结数据集的公共Schema及数据字典 |
| recognition | 输入契约、因果特征、50条规则/34变体、增量识别与回放CLI |
| evaluation | 手工标注评价、独立事件匹配、合成数据与回归测试 |
| runtime | SQLite WAL事件日志、单写运行器、版本检查、恢复和本地API |
| connectors | 免费IEX、合成演示和JSONL回放数据源 |
| paper | 统一模拟资金、信号冲突、报价成交、风险及退出 |
| workbench | 原生浏览器工作台及本地图表文件、LICENSE和NOTICE |
| research | 候选、合并决策、语义自审、搜索路径、状态和交接 |
| reports | 研究报告、质量审查、实际执行的验证记录 |
| templates | setup编写、候选审核、通用交易复盘模板 |
| manifest.json | 版本、统计及交付文件SHA-256清单 |

## 复盘与扩充

复制 [复盘模板](templates/trade-review-template.md)，保留setup ID、版本、纽约/交易所时区、入场前截图和当时规则。分别评估“识别是否成立”“是否按计划成交”和“单笔盈亏”，不要把亏损自动改标为无效setup。

维护者先读 [AGENTS.md](AGENTS.md)、[state.json](research/state.json)、[handoff.md](research/handoff.md) 与别名索引。读取状态 → 对照七轴去重 → 实读来源、记录证据 → 编写/更新主记录 → 处理视觉资料 → 语义审查 → 重建 → 验证 → 记版本变化。新增ID继续递增，不复用合并/弃用ID。

在项目目录运行：

```sh
python3 scripts/test_recognition.py --require-jsonschema
python3 scripts/test_runtime.py --require-jsonschema
python3 scripts/report.py
python3 scripts/build.py --clean
python3 scripts/validate.py --require-jsonschema
```

构建只需Python标准库；验证也可用标准库完成跨文件和结构检查。安装 `jsonschema` 后会另外执行完整Draft 2020-12 schema检查，缺少时输出明确未执行标记，可加 `--require-jsonschema` 强制要求。浏览器验证需可选的Playwright与Chromium，见 `scripts/test_site.py`。`scripts/fetch_sources.py` 仅是可选获取助手，BeautifulSoup/pypdf依赖见requirements；下载不改变实际阅读状态。

修改报告或测试结果后再构建，最后验证manifest。不要直接修改生成的HTML、JSON或CSV。外链本次可访问不保证以后持续可用；403、404、空内容和网络故障分别记录，不将一次请求失败当永久死链。

使用 `python3 scripts/build.py --output ../新建空目录` 构建完整的隔离交付副本，源目录不会被写入。交付采用显式文件allowlist，排除隐藏目录、Git、虚拟环境、缓存、凭证、数据库、运行目录和ZIP；不会递归把整个工作目录收进manifest。`python3 scripts/verify_rebuild.py` 检查两个隔离构建和ZIP的可复现性。

候选包使用 `python3 scripts/package.py --candidate --output ../新候选名称.zip`。正式发布使用 `python3 scripts/package.py --soak-report 项目外稳定性目录/soak-report.json --output ../新正式名称.zip`，脚本先执行真实十小时及当前核心的只读门禁。随后只读验证项目，仅收录allowlist与manifest共同确认的文件，固定ZIP条目时间、权限和顺序，检查CRC和逐文件哈希；旁边保存release-checks.json，明确candidate或formal。现有ZIP会保留，请为新发布使用新文件名。

本版提供识别、模拟执行、恢复及浏览器的工程验收路径；实际检查记录保存在reports。研究数据不足的地方已留在报告和状态中，后续真实市场验证需要数据及独立标注。
