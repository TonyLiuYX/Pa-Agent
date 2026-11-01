# 当前工程验收记录 · v1.3.0 · 2026-10-03

此页由实际报告生成，不将旧版本通过结果计为本版验收。现行身份以 [release.json](../release.json) 为准，源规则和算法身份分别记录。测试证明相应工程边界，不证明真实市场识别质量、盈利能力或独立专家复审。

| 验收资料 | 状态 | 报告版本 | 实际方法/场景数 | 适用范围 |
|---|---|---|---:|---|
| [识别与评价](recognition-validation.json) | passed | 1.3.0 | 171 | 本版实际记录 |
| [运行、行情、模拟执行及公共契约](runtime-validation.json) | passed | 1.3.0 | 171 | 本版实际记录 |
| [离线知识库浏览器](ui-results.json) | passed | 1.3.0 | 32 | 本版实际记录 |
| [实际本地API工作台浏览器](workbench-validation.json) | passed | 1.3.0 | 13 | 本版实际记录 |
| [结构与Schema](validation-results.json) | passed | 1.3.0 | 9 | 本版实际记录 |
| [隔离构建与ZIP可复现性](rebuild-results.json) | passed | 1.3.0 | 见报告 | 本版实际记录 |

识别测试保留算法、配置、输入哈希及子测试记录；运行测试覆盖资金、报价、冲突、退出、持久化和恢复。工作台浏览器资料保留运行ID、服务身份、截图SHA-256、浏览器环境与实际检查项。离线页面验证与有后台服务的工作台验证分别报告。

## 分层识别证据

图层手写特征路径与原始MarketEvent路径分别报告。当前原始管线矩阵状态：passed；84张图达到完整正向触发。50个基础版共有100条已知为假的核心谓词负例，证据同时保留正反输入在相同事件、周期下的特征观察与必要能力。另有68条变体补充阻断控制，不能计为核心谓词反例。总计运行168个负向控制、通过168个。通过的33个变体差异中，31个是同品种/方向/共同周期的机械边界，1个为独立注册的来源open时钟，1个为注册周期选择；1个可观察边界不适用。不同方向的首次触发不能作为同目标的提前触发证据；来源时钟和周期选择不冒充共同周期下的形态行为差异。按验证层划分的破坏条件分布为 `{"raw_pipeline_known_false_core": {"prior_reclaim": 1, "signal": 9, "orb_direction": 1, "directional_extreme": 10, "resume": 19, "flow_turn": 3, "poc_retest": 1, "hikkake_start": 1, "three_push": 1, "old_extreme_retest": 1, "first_micro": 1, "late_flag": 1, "flag_failure": 1, "retrace_start": 1, "trend": 3, "impulse": 2, "pressure": 1, "trend_bar_failure": 1, "range_edge": 1, "prior_tail_open": 1, "tail_reject": 1, "near_pm_edge": 1, "pm_wash": 1, "weak_open_drop": 1, "vwap_reclaim": 1, "overnight_failure": 3, "vwap_cross": 1, "vwap_stretch": 1, "historical_vwap_reject": 1, "relative_strength": 1, "second_day_break": 1, "accepted": 3, "flow_resume": 3, "cross_confirm": 2, "ledge_break": 1, "ledge_failed": 1, "ib_first_failure": 1, "ib_break": 1, "absorption": 1, "value_return": 1, "value_inside60": 1, "failed_extension": 1, "attack_energy": 1, "stacked": 1, "cvd_divergence": 1, "weak_long_context": 1, "drive_recovery": 1, "passive_support": 1, "noise_break": 1, "half_hour": 1, "early_sign": 1, "last_half_hour": 1, "directional_price": 1}, "raw_pipeline_supplemental_blocking_control": {"final_observation_absent": 34, "geometric_premise_removed": 33, "required_previous_close_removed": 1}}`。

SET-0027的public_price_proxy保留独立身份与公开价格代理语义，实际可执行价格谓词哈希及触发序列与基础版相同；可观察机械边界标为not_applicable，不计入通过的机械变体数。所有矩阵证据是合成工程路径，真实市场验证保持not_run。

干净候选解包后的原始覆盖及检查点兼容性复验尚未记录。

原250情形中14个待核查结果由 [audit_links.json](../recognition/audit_links.json) 映射；验证层分布 `{"rule_graph": 9, "raw_pipeline": 2, "feature_provider": 3}`。原输入、原证据及修复后证据分别保留哈希和解释，源摘要SHA-256为 `db2f9efaf580a45dd4ce19bd7fe16b5061712ceb2b96cddf10c9d00a7ac43819`。特征层或图层证据不能冒充原始管线证据，尚待真实标注的规则仍保留未验证状态。

## 运行和发布复验

当前冻结核心的实际十小时资源与恢复验收已通过：有效观察36000.103071秒，83046条完整日志独立重放289.654239秒且状态一致。最高定期RSS样本687.671875MiB，累计进程峰值1016.078125MiB，后段增长10.265625MiB；原1024MiB当前RSS与128MiB增长预算未改变。处理耗时P50/P95/P99为10.67/29.56/49.538ms，18次恢复耗时0.828979–3.121418秒，最大处理积压300。这是本次合成输入、配置和主机环境的有限验收，采样RSS不证明每个瞬时值或无限期稳定，未关闭模拟持仓按原状态保留。真实市场验证仍为not_run；旧候选、容量诊断与轮询时长不累计。原始字节副本见 [soak-results.json](../reports/soak-results.json)，独立只读门禁见 [soak-validation.json](../reports/soak-validation.json)。

```sh
python3 scripts/test_recognition.py --require-jsonschema
python3 scripts/test_runtime.py --require-jsonschema
python3 workbench/tests/browser_check.py --url 本地运行地址 --output 项目外截图目录
python3 scripts/test_site.py
python3 scripts/report.py
python3 scripts/build.py --clean
python3 scripts/validate.py --require-jsonschema
```

浏览器及测试报告写入后再build，最终validate只读检查全部manifest文件。两个隔离构建及确定性ZIP的复验见 [rebuild-results.json](rebuild-results.json)，归档最终身份由项目外release-checks报告提供。发布检查和运行方法见 [发布说明](release-notes.md)。

## 尚未完成的真实验证

真实市场标签目前为0；识别精确率/召回率、美国股票迁移和策略盈利保持未运行。首轮实时IEX接收、历史初始化与重连需有效本地凭证及实际市场窗口，尚待执行。十小时固定版本合成稳定性测试及独立全日志重放已通过，实际观察时间与冻结身份以soak-results.json为准；真实市场性能仍未验证。已保留各次运行证据：`soak-v1.3.0-20261003-candidate-1`中断，观察355.378755秒；`soak-v1.3.0-20261003-candidate-2`中断，观察2045.628135秒；`soak-v1.3.0-20261003-candidate-3`失败，观察1754.653483秒，错误为SoakFailure/rss_budget_exceeded；`soak-v1.3.0-20261003-candidate-4`中断，观察28133.869205秒；`soak-v1.3.0-20261003-candidate-5`失败，观察36001.827709秒，错误为SoakFailure/unexplained_late_rss_growth；`soak-v1.3.0-20261004-candidate-6`未计划退出，最后保存的观察样本30828.070387秒，缺少最终报告，未通过；该样本不是最终观察或准确退出时长，秒数不累计到新候选。各次失败或中断时长分别记录，不累计到新候选。短时测试及加速回放不能替代独立墙钟计时资料；本页不预设通过。

Safari/Firefox专项兼容、动态DOM原始材料、独立策略收益及外链长期存活仍未验证。研究待核查问题保留在 [state.json](../research/state.json)，合成、真实和未知来源资料不合并评价。

## 历史资料

原v1.2实际记录保留在 [verification-v1.2.0.md](verification-v1.2.0.md)。旧审计与冻结输入保持原范围，本轮新增证据不会改写此前判断。
