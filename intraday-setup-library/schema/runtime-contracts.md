# 运行与评价公共契约

所有时间使用显式时区的ISO 8601；行情事件保持市场时间与接收时间两个时钟。市场事件区间端点遵循现有识别契约。序号是非负整数，数值不接受bool、NaN、Infinity。必要条件未知不转换为false，也不补成完整观测。

| 文件 | 公共数据 |
|---|---|
| market-event.schema.json | 标准MarketEvent；bar、quote、trade、session条件字段及status类型 |
| detection.schema.json | 候选/形成/触发/失效，条件证据、方向、周期及算法/配置/来源身份 |
| paper.schema.json | 84执行profile、订单、成交、仓位、整数分账本和账户 |
| runtime-envelope.schema.json | run_id、顺序、来源、原始payload及接收日志元数据；原MarketEvent不加运行字段 |
| runtime-config.schema.json | 无密钥配置；观察名单、模式、参数及资源上限 |
| annotation.schema.json | 人工事件窗、阶段窗、方向、来源、标注者和复核字段 |
| evaluation-dataset.schema.json | 冻结输入哈希、切分锁、完整标注覆盖和manifest自校验 |
| run-report.schema.json | 已提交运行、配置、实验账户、能力与完整输入/派生输出哈希 |

`runtime.contracts.validate_contract`始终拒绝非JSON数值。完整验证可使用jsonschema，并只读取本地Schema，不在运行时下载外部定义。

Schema负责形状；MarketEvent.validate、评价窗口校验、模拟账户状态机与检查点恢复负责语义。跨字段OHLC、quote交叉、日期日历、方向与bool区分、连续覆盖、合法触发时刻、分组泄漏、资金守恒及因果执行必须另外检验。

可选session.history_calendar为最多30个有出处、日期唯一且升序、早于当前开盘的已完成交易日。历史summary须匹配该日历close/as_of且显式complete，才能提供对应前日或连续历史统计；显式空history清除旧值。旧输入省略history_calendar保持其原参考接口。runtime-config增加正整数demo_sessions（默认1）和整数alpaca_history_sessions（0–30，默认15），属于恢复配置身份。模拟session及报价保存venue，不能跨venue替换订单的执行上下文。

账户以整数分记账，显示金额是十进制字符串；资金锁定/预约不是现金损益。`balance_delta_cents`只有实际费用及平仓损益改变余额，必须满足初始余额加完整账本增量等于当前余额。原订单生命周期可以有多条更新记录，不能把这些更新误报为重复下单；成交及资金事件ID应唯一。

检测ID表示一个识别episode。持久化幂等键包含行情event_seq及完整批次内output_index，不以检测ID删掉后续失效状态。相同来源事件重传保留首次接收时钟，相同ID异内容被隔离。识别正确性、执行一致性和实验账户表现分别评价。

标注可显式设置`complete_coverage=true`，表示整个外层时间窗已经完整审查所选setup、变体、品种、场所与方向；默认不作此声明。有争议、必要数据不可观察或与这些案例重叠的区间不能作为完整覆盖。只有这些完整区间内未匹配的预测才增加连续覆盖误报；候选、形成和触发使用各自阶段窗。

运行导出的`input_provenance`将实际`events.jsonl`、版本化`rules.json`及完整`detections.jsonl`文件字节绑定到数据、规则、检测、算法与识别配置哈希。冻结评价必须同时提供运行报告和对应实际文件，不能仅复述manifest里的哈希。`events.jsonl`仅含已处理行情；完整`journal.jsonl`继续保留尚未处理的原始记录。研究报告、账户和上述文件来自同一个已提交WAL读快照。
