# 本地持续运行平台

当前验收状态（2026-10-03）：候选4在独立复审发现历史日期和执行venue缺陷后正常中断，实际观察28133.869205秒，原版本97889事件全日志重放通过；该结果未达到十小时。修复版本必须新建目录重新完整计时。候选3的RSS超限失败及三个中断候选原证据均保留，不累计旧时长。当前身份、运行目录、修复和保存状态见 [开发检查点](../research/development-checkpoint.json)，实际进度以外部报告为准。

运行平台保留 `RecognitionEngine.feed(MarketEvent)` 的旧接口，在外层添加顺序日志、统一模拟账户和已提交事件接口。全部执行都是本地实验模拟，默认不调用模型。

```sh
python3 -m runtime serve --mode demo --data-dir ../runtime-runs/demo
python3 -m runtime doctor --config config/runtime.example.json
python3 -m runtime resume --data-dir ../runtime-runs/demo
python3 -m runtime export --data-dir ../runtime-runs/demo
```

观察名单、识别参数、执行规则、容量政策见 `config/runtime.example.json`。实时模式使用 `python3 -m runtime serve --mode live`，密钥和固定版本依赖的设置见 `connectors/README.md`。无密钥演示和文件回放只依赖标准库；完整JSON Schema验证使用可选jsonschema。IEX范围、历史不足及未知能力在工作台列出。

`--sessions N`或配置`demo_sessions`指定正整数个合成演示会话，默认1；`--history-sessions N`或`alpaca_history_sessions`指定IEX历史请求会话数0–30，默认15。二者属于冻结观察配置，resume不能更改。演示会话会自然结束，SSE和HTTP仍可读取；连续演示也不等于真实交易时段或墙钟稳定性证明。

运行数据必须在知识库项目目录之外。SQLite采用WAL、FULL同步和单写者，`run.lock`阻止两个进程占用同一运行。原始行情先提交到journal；识别的完整批次随后进入统一仲裁，检测、订单、成交、资金、账户快照、处理进度及界面outbox在同一事务内提交。SSE仅从已提交outbox读取。

## 恢复与版本

默认每60秒或10,000个事件保存版本化JSON检查点，并保留最近两个检查点和完整journal。恢复校验配置、算法、规则、执行模型、运行代码及检查点前日志哈希。恢复后重放已提交尾段，逐项比较检测、订单、成交、资金和账户输出。提交失败后丢弃已变动的内存处理器。版本改变会阻止原运行续跑；新版本应使用新目录重新回放。

编码在单次调用内复用共享证据的投影，恢复只池化不可变字符串并保持可变容器独立；Store仅序列化一次并分块计算UTF-8校验和。v1 JSON及校验和字节保持兼容，规范JSON读取保留旧格式回退；格式兼容不能跳过算法与核心身份检查。实际容量/备份恢复阶段的内存证据见`../reports/verification.md`，短诊断不替代十小时验收。

相同来源、相同事件ID及市场内容的重传保持首次收到时间，不重复处理；相同ID不同市场内容保留原始记录并登记隔离错误。检测输出的持久键为 `(topic,event_seq,output_index)`，检测ID只表示事件身份，不替代状态转换键。控制请求可带`Idempotency-Key`，相同请求复用首次决策时间，不同请求内容返回409。

暂停和停止禁止新入场并撤销等待入场。已开持仓及等待退出保留，后续报价仍可完成真实的模拟退出。断线或停牌阻止报价成交，连接恢复不会解除真实交易停牌。退出阈值被触及只提交退出命令，成交取后续有效报价，跳空损失可能大于阈值。

操作状态包含READY、RUNNING、PAUSED、STOPPED、DISCONNECTED、AUTH_REQUIRED、SOURCE_ERROR、SOURCE_ENDED、CAPACITY_BLOCKED、PROCESSING_BLOCKED。自然文件/演示结束保持后台可读并显示SOURCE_ENDED。源错误有分类及日志，处理错误保留原始事件和恢复入口。

## 完整日志回放

旧MarketEvent JSONL仍可使用：

```sh
python3 -m runtime replay --input ../data/events.jsonl --source-kind synthetic --data-dir ../runtime-runs/replay
```

回放来源默认unknown；确认真实资料出处后可明确指定`--source-kind real_market`。unknown不能被当作已验证的真实市场输入。仅有K线时，配置独立的`paper.model=next_bar_proxy_experimental`；其结果不能与报价执行模型合并。

运行导出同时包含兼容的`events.jsonl`和完整`journal.jsonl`。后者保存行情、控制、计时、连接、回补及标注的顺序。导出包含配置、账户、能力、输入/输出哈希及逐文件manifest，通过ZIP CRC检查。恢复跨控制指令的账本应使用完整journal：

```sh
python3 -m runtime replay-journal --input ../runtime-runs/demo/exports/运行名/journal.jsonl --run-report ../runtime-runs/demo/exports/运行名/run.json --data-dir ../runtime-runs/audit-copy
```

命令核对版本、manifest、完整日志哈希、已提交进度、所有派生输出哈希及账户。导出时尚未处理的原始事件仍保留为pending，不冒充已经成交。journal审计运行保持冻结，不能接入新的实时源。版本改变时，可以用兼容MarketEvent流新建独立研究运行，结果记录新版本。

导出固定一个已提交WAL读快照，再释放采集/处理写锁；行情采集继续运行。`events.jsonl`仅包含此快照已经处理的行情，完整journal也保留未处理输入。`rules.json`保存本次实际规则集合，`run.json.input_provenance`记录行情、规则及检测文件字节哈希、算法与识别配置身份。哈希按块计算，原始事件逐行导出。冻结数据集评价还必须核对这些实际文件，操作见评价指南。

`GET /api/reports`返回已提交运行信息、独立识别评价及实验账户。评价读取当前案例的最新标注版本，按候选/形成/触发窗口一对一匹配；只有显式`complete_coverage=true`的完整窗口才将未匹配预测计为连续覆盖误报。该声明不允许争议或数据不足案例。没有可用标签显示`not_run`，规则版本不符或超出1000条标签/10000条相关预测时返回具体阻断原因并停止评分；资料不会因此删除。评分与导出使用相同提交前缀，市场验证始终保持未运行。

## 容量与健康

采集队列默认上限10,000；通知入队前原始事件已落库。容量满会停止源并标CAPACITY_BLOCKED，已落库事件不静默丢弃。检测及交易历史落库，HTTP通过after/before游标分页，浏览器关闭不停止后台。JSON检查点不使用pickle。

`retention`可配置`RuntimePolicy`：seen缓存10,000、转换历史2,000、audit1,000、archive2,000、active20,000、每个流事件50,000、会话累计证据50,000、symbol64、每周期bars3,000、每候选证据引用1,000。活动候选和仍可用的父事件超限时停止，不静默删除。流或K线截断登记具体能力降级；新闻、consensus、Profile累计资料超限停止。修改容量也属于新配置，需要新运行。

新闻和consensus证据跨会话保留，不按交易日自动淘汰；达到配置的50,000事件上限时明确阻断。演示资料不提供这些数据，相应能力保持未知。缓存尚未饱和时允许增长，JSON检查点复制与编码会产生临时内存峰值；有界设计和只读资源审查不能代替实际十小时RSS曲线及预算验收。

健康接口显示已处理进度、journal积压、队列长度、最近行情时间、错误分类、磁盘剩余空间及近期P50/P95/P99识别耗时。账户显示每个估值的市场时间；实时模式超过30秒显示陈旧。输入的历史回补进入独立backfill通道，不补开当前账户订单。

`python3 -m runtime analyze-backfill --data-dir 原运行目录 --output 项目外的新分析目录`从只读WAL快照提取已提交backfill。它只运行独立识别器，不创建Paper或修改原库；原seq、arrival顺序及输入、配置、规则和算法身份保留。真实session上下文须同symbol/venue、对应市场时段，并在资料序号及接收时钟上已经可用。缺失或歧义保存missing_context并返回blocked（CLI退出码2），不能借当前日session给历史柱。产物包括events/detections/rules/configuration/source-inputs及run报告，source_processed_cursor和来源范围固定在该提交前缀。此报告不含账户表现，也不宣称真实市场评价通过。 默认分析输入上限为64MiB、已提交backfill上限为10000条；超限后仍流式核对完整来源前缀，但不构造识别器、不导出残缺输入冒充全量。selection或analysis未完成的run报告不能进入冻结评分。

`health.memory_maintenance`单独记录检查点/恢复后平台分配器维护的状态、次数、耗时及native返回值；不进入引擎、账户、journal或检查点，不能将它作为确定性策略结果。环境不支持或调用错误保留明确遥测，不破坏已经提交的检查点与恢复。native返回0仍可表示调用完成；API的字节数或flag与观察到的RSS变化分别保存，不互相推算。工作台在总览显示简洁状态，精确backend只放折叠详情。该措施不强制GC，也不清空活动候选、证据或账户。

## 本地接口

HTTP仅绑定127.0.0.1，写请求要求同源及会话token。静态文件受各自workbench/site/assets目录边界限制。

| 接口 | 内容 |
|---|---|
| `GET /api/status` | 模式、来源、健康、身份哈希、session_token与outbox_cursor |
| `GET /api/account`、`/api/positions` | 已提交账户、持仓及估值时间 |
| `GET /api/bars`、`/api/detections` | 默认最近一页；before向历史分页、after向前、symbol过滤 |
| `GET /api/orders`、`/api/fills`、`/api/account_events`、`/api/decisions` | 订单、成交、账本及阻断解释；orders默认每订单最新状态 |
| `GET /api/events` | 按原始journal事件顺序逐事件回放 |
| `GET /api/events/stream` | 已提交outbox SSE；after及Last-Event-ID断线续传 |
| `GET /api/coverage`、`/api/profiles` | 全84图的数据能力及独立执行登记 |
| `GET /api/labels`、`/api/reports` | 标注、运行研究结果 |
| `POST /api/runtime/pause|resume|stop` | 持久化控制 |
| `POST /api/labels`、`/api/export` | 校验标注并保存；导出可校验运行包 |

JSON公共契约位于`schema/*-event.schema.json`、`detection.schema.json`、`paper.schema.json`、`runtime-envelope.schema.json`、`annotation.schema.json`、`evaluation-dataset.schema.json`及`run-report.schema.json`。窗口、时序、资金和哈希还须通过语义校验，不能仅靠Schema证明交易正确。

`runtime.advisor`提供带截止时刻、接收时间及证据ID的模型扩展接口。未来资料及未知证据引用会被拒绝；默认`DisabledAdvisor`只返回未知项，没有网络或模型调用。

## 验收边界

```sh
python3 scripts/test_runtime.py --require-jsonschema
python3 scripts/test_recognition.py --require-jsonschema
```

真实连接需有效本地凭证和开市时段；真实市场标签当前为0，market_validation保持not_run。十小时稳定性使用独立实际计时报告，短时回归、加速行情及盈利结果均不能代替它。持续测试操作见`SOAK.md`。
