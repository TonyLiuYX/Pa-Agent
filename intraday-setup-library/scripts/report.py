"""Refresh descriptive quality counts and maintenance state from the records.
Does not perform semantic review or change setup status. Run build after this.
"""
import collections,hashlib,json,math,re
from datetime import datetime,timedelta
from build import ROOT,VERSION,DATE,records,rows,visual_status
import sys
sys.path.insert(0,str(ROOT))
from recognition.specs import RULE_VERSION
def table(counter):return '\n'.join('| '+str(k)+' | '+str(v)+' |' for k,v in counter.most_common())
def links(ids):return '、'.join('['+i+'](../setups/'+i+'.md)' for i in ids)
def evidence(name):
 path=ROOT/'reports'/name
 return json.loads(path.read_text()) if path.exists() else {'status':'not_executed'}
def soak_history_note(state,exclude=None):
 runs={}
 for row in [*state.get('previous_soak_runs',[]),state.get('soak',{})]:
  if row.get('status') not in ('interrupted','failed','unplanned_exit_report_missing'):continue
  key=(row.get('directory'),row.get('started_at'))
  if key==exclude:continue
  runs[key]=row
 if not runs:return '此前失败或中断候选的时长不累计。'
 notes=[]
 for row in runs.values():
  name=row.get('directory','未记录目录').rsplit('/',1)[-1]
  if row['status']=='unplanned_exit_report_missing':
   note=f"`{name}`未计划退出，最后保存的观察样本{row.get('observation_seconds','未记录')}秒，缺少最终报告，未通过；该样本不是最终观察或准确退出时长，秒数不累计到新候选"
  else:
   status='失败' if row['status']=='failed' else '中断'
   note=f"`{name}`{status}，观察{row.get('observation_seconds','未记录')}秒"
  error=row.get('error',{})
  if error:note+='，错误为'+str(error.get('code','未记录'))+'/'+str(error.get('detail','未记录'))
  notes.append(note)
 return '已保留各次运行证据：'+'；'.join(notes)+'。各次失败或中断时长分别记录，不累计到新候选。'
def soak_note():
 checkpoint=ROOT/'research/development-checkpoint.json'
 state=json.loads(checkpoint.read_text()) if checkpoint.exists() else {}
 soak=state.get('soak',{})
 if soak.get('status')=='failed':
  error=soak.get('error',{})
  prior=soak_history_note(state,exclude=(soak.get('directory'),soak.get('started_at')))
  return f"十小时固定版本稳定性验收尚未通过。当前候选已于{soak.get('ended_at','未记录结束时间')}失败，错误为{error.get('code','未记录')}/{error.get('detail','未记录')}；运行总历时{soak.get('elapsed_seconds','未记录')}秒，观察计时{soak.get('observation_seconds','未记录')}秒。原失败证据保留在`{soak.get('directory','见检查点')}`。{prior}修复后须重新冻结核心，在新目录完整计时36000秒并进行独立全日志重放和只读门禁；当前不承诺完成时刻。此处状态来自保存的开发检查点，故障详情以原外部报告为准。"
 report=evidence('soak-results.json');observed=report.get('observation_seconds',0)
 gate=evidence('soak-validation.json');source=ROOT/'reports/soak-results.json'
 current=evidence('runtime-validation.json').get('core_manifest',{}).get('sha256')
 previous_note=soak_history_note(state)
 if current and source.exists() and report.get('core',{}).get('sha256')==current and report.get('status')=='passed' and type(observed) in (int,float) and math.isfinite(observed) and observed>=36000 and gate.get('status')=='passed' and gate.get('validation')=='actual_ten_hour_soak_and_persisted_full_replay' and gate.get('core_sha256')==current and gate.get('report_sha256')==hashlib.sha256(source.read_bytes()).hexdigest():
  return '十小时固定版本合成稳定性测试及独立全日志重放已通过，实际观察时间与冻结身份以soak-results.json为准；真实市场性能仍未验证。'+previous_note
 if soak.get('status')=='running' and soak.get('core_sha256')==current and soak.get('started_at'):
  earliest=(datetime.fromisoformat(soak['started_at'])+timedelta(seconds=36000)).isoformat()
  return f"十小时固定版本稳定性验收尚未通过。{previous_note}当前冻结候选已于{soak['started_at']}启动，外部目录为`{soak.get('directory','见检查点')}`；不得早于{earliest}完成十小时观察，随后仍须完成独立全日志重放及只读验收门禁。此处状态来自保存的开发检查点，实际进度以外部报告为准。"
 return f'十小时固定版本稳定性验收尚未通过。{previous_note}优化后须重新冻结核心、使用项目外新目录完整计时及独立重放。'
def resource_note(runtime):
 report=evidence('soak-results.json');gate=evidence('soak-validation.json');source=ROOT/'reports/soak-results.json'
 current=runtime.get('core_manifest',{}).get('sha256')
 if current and source.exists() and report.get('status')=='passed' and report.get('core',{}).get('sha256')==current and gate.get('status')=='passed' and gate.get('validation')=='actual_ten_hour_soak_and_persisted_full_replay' and gate.get('core_sha256')==current and gate.get('report_sha256')==hashlib.sha256(source.read_bytes()).hexdigest():
  samples=report['resource_samples'];mib=1024**2;recovery=report['recovery_seconds'];latency=report['processing_latency']
  return f"当前冻结核心的实际十小时资源与恢复验收已通过：有效观察{report['observation_seconds']}秒，{report['journal_events']}条完整日志独立重放{report['replay']['seconds']}秒且状态一致。最高定期RSS样本{max(row['rss_bytes'] for row in samples)/mib:.6f}MiB，累计进程峰值{max(row['peak_rss_bytes'] for row in samples)/mib:.6f}MiB，后段增长{report['late_rss_growth_bytes']/mib:.6f}MiB；原1024MiB当前RSS与128MiB增长预算未改变。处理耗时P50/P95/P99为{latency['p50_ms']}/{latency['p95_ms']}/{latency['p99_ms']}ms，{len(recovery)}次恢复耗时{min(recovery):.6f}–{max(recovery):.6f}秒，最大处理积压{report['max_backlog']}。这是本次合成输入、配置和主机环境的有限验收，采样RSS不证明每个瞬时值或无限期稳定，未关闭模拟持仓按原状态保留。真实市场验证仍为not_run；旧候选、容量诊断与轮询时长不累计。原始字节副本见 [soak-results.json](../reports/soak-results.json)，独立只读门禁见 [soak-validation.json](../reports/soak-validation.json)。"
 pressure=runtime.get('checkpoint_pressure_resource_validation',{})
 if pressure.get('status')=='passed' and pressure.get('core_sha256')==runtime.get('core_manifest',{}).get('sha256'):
  def measurement(key):
   value=pressure.get(key)
   return f'{value:.2f}' if type(value) in (int,float) and math.isfinite(value) else '未记录'
  measurements=[('密集检查点观察期最高RSS','observation_max_rss_mib'),('密集检查点观察期进程峰值','observation_peak_rss_mib'),('完整重放后当前RSS','after_replay_rss_mib'),('完整重放后进程峰值','after_replay_peak_rss_mib'),('独立新进程恢复后当前RSS','restored_rss_mib'),('独立新进程恢复峰值','restore_peak_rss_mib'),('恢复后哈希验证当前RSS','restore_hash_rss_mib'),('恢复后哈希验证进程峰值','restore_hash_peak_rss_mib')]
  rows='\n'.join('| '+label+' | '+measurement(key)+' |' for label,key in measurements)
  post=pressure.get('post_saturation',{})
  post_seconds=post.get('seconds',0);post_min=post.get('minimum_bytes',0)/1024**2;post_max=post.get('maximum_bytes',0)/1024**2
  margin=pressure.get('budget_mib',0)-pressure.get('after_replay_peak_rss_mib',0)
  return f'''当前冻结核心的独立密集检查点压力诊断处理{pressure.get('events')}个事件、{pressure.get('sessions')}个会话，每{pressure.get('checkpoint_every_events')}个事件保存检查点，共{pressure.get('checkpoint_count')}次；实际处理耗时{measurement('observation_seconds')}秒。seen、transitions、archive缓存均达到上限，完整日志重放核对{pressure.get('derived_records_compared')}条派生记录、识别、模拟与账户相等；SQLite备份在独立新进程恢复也相等，源资料保持原字节。该诊断不计十小时。

| 测量阶段 | MiB |
|---|---:|
{rows}

独立恢复耗时{measurement('restore_seconds')}秒；资源预算仍为{pressure.get('budget_mib')}MiB。archive饱和后的事件范围为{post.get('first_cursor')}–{post.get('last_cursor')}，约{post_seconds:.2f}秒，保留{pressure.get('post_saturation_samples')}个检查点后样本，RSS范围{post_min:.2f}–{post_max:.2f}MiB；这是有限窗口，不证明无限期稳定。完整重放阶段峰值{measurement('after_replay_peak_rss_mib')}MiB距资源预算约{margin:.2f}MiB。分配器维护只保存健康遥测，native返回0仍保留为完成，不从RSS差推算API释放字节。密集检查点补充此前加速诊断检查点密度不足的范围；不同输入、会话相位和处理节奏不能直接合并为A/B改善率。有限压力诊断及恢复等价不证明十小时内存平台、真实行情表现或全部外部故障已通过。阶段数值、实际范围和项目外原报告哈希见 [runtime-validation.json](../reports/runtime-validation.json) 的 `checkpoint_pressure_resource_validation`。'''
 capacity_evidence=runtime.get('short_capacity_resource_validation',{});row=capacity_evidence.get('analysis',{})
 if row.get('status')!='smoke_passed' or row.get('core_sha256')!=runtime.get('core_manifest',{}).get('sha256'):
  return '当前核心的独立容量与备份恢复诊断尚未记录；长期资源验收仍须实际十小时资料。'
 old=row.get('old_baseline',{})
 def mib(source,key):
  value=source.get(key)
  return f'{value:.2f}' if type(value) in (int,float) and math.isfinite(value) else '未记录'
 metrics=[('观察期最高定期RSS样本','observation_max_sampled_rss_mib'),('观察期最高定期峰值样本','observation_max_sampled_peak_rss_mib'),('完整验证后当前RSS','post_validation_rss_mib'),('整个进程峰值RSS','total_peak_rss_mib'),('新进程恢复后当前RSS','restored_rss_mib'),('新进程恢复峰值RSS','restore_peak_rss_mib'),('恢复后哈希核验峰值RSS','restore_hash_validation_peak_rss_mib')]
 old=dict(old);old['observation_max_sampled_peak_rss_mib']=old.get('observation_peak_rss_mib')
 rows='\n'.join('| '+label+' | '+mib(old,key)+' | '+mib(row,key)+' |' for label,key in metrics)
 return f'''独立合成容量诊断实际观察{row.get('observation_seconds')}秒、处理{row.get('events')}个事件和{row.get('sessions')}个会话；seen、transitions、archive缓存均达到上限，完整日志重放与备份恢复等价，核心始终一致。备份恢复耗时{row.get('restore_seconds',0):.3f}秒；其源数据库和报告保持原字节。该六分钟诊断不计十小时。

| 资源阶段（MiB） | 先前核心诊断 | 优化后核心诊断 |
|---|---:|---:|
{rows}

当前诊断预算为{row.get('budget_mib')}MiB。完整验证及恢复的内存下降，但观察期样本高于旧测量，不能声称所有阶段都改善。两版采用各自连续流，结束时事件量和会话位置不同，不能当作同输入A/B基准。饱和后没有足够的定期RSS样本，尚不能建立稳定平台或长期增长结论；这些仍由固定版本十小时验收判断。重复来源ID的{row.get('restored_source_string_sharing',{}).get('occurrences')}个引用已共享一个不可变字符串对象。具体范围、输入身份与原报告哈希见 [runtime-validation.json](../reports/runtime-validation.json) 的独立资源证据。'''
def handoff_report(recognition,runtime,workbench,ui):
 path=ROOT/'research/handoff.md';history=ROOT/'research/handoff-v1.2.0.md';previous=path.read_text() if path.exists() else ''
 if not history.exists() and previous.startswith('# 接续研究交接 · 2026-10-02 · v1.2.0'):
  history.write_bytes(previous.encode())
 marker='\n## 来源研究事项\n\n'
 if marker in previous:manual=previous.split(marker,1)[1]
 elif '已完成50个reviewed' in previous:manual=previous[previous.index('已完成50个reviewed'):]
 else:manual=previous
 manual=manual.replace('CUA浏览器控制超时；最终本地file://验证用Python Playwright，结果见reports/verification及ui-results。','早期来源研究中的CUA浏览器控制超时属于历史记录；本轮本地工作台及file://验证使用Python Playwright，结果见reports/verification、workbench-validation及ui-results。')
 raw=recognition.get('raw_pipeline_coverage',{});distribution=recognition.get('distribution_fixture_validation',{});writes=workbench.get('synthetic_write_validation',{});actual_writes=workbench.get('actual_synthetic_research_validation',{})
 distribution_note=(f"必要的原始负例设计及旧检查点JSON已纳入明确发布白名单；干净候选ZIP解包后重新通过{distribution.get('test_methods_run')}个原始覆盖/运行识别/检查点兼容性测试方法及{distribution.get('subtests_passed')}个子测试，实际加载模块来自解包目录。该发布检查单独报告，不增加上述全套测试方法计数。" if distribution.get('status')=='passed' else '')
 header=f'''# 接续开发与研究交接 · {DATE} · v{VERSION}

当前链路包括免费IEX/演示/文件连接器、持久化顺序日志、因果识别、统一模拟账户、下一条有效报价执行、恢复与完整日志回放、本地五视图工作台、独立标注和冻结评价数据集。先读 [运行平台](../runtime/README.md)、[工作台](../workbench/README.md)、[识别指南](../recognition/README.md)、[评价协议](../evaluation/README.md) 和 [当前实际验收](../reports/verification.md)。算法规则为{RULE_VERSION}；来源规则与执行实验版本分别保存。

当前实际记录：识别/评价{recognition.get('test_methods_run','未记录')}个测试方法及{recognition.get('subtests_passed','未记录')}个子测试，运行/行情/模拟执行/公共契约{runtime.get('test_methods_run','未记录')}个测试方法。原始MarketEvent矩阵有{raw.get('raw_positive_graph_count','未记录')}张图的正向路径；{raw.get('raw_core_negative_base_count','未记录')}个基础版共有{raw.get('raw_core_negative_path_count','未记录')}条真正核心谓词已知为假的负例，另有{raw.get('raw_supplemental_negative_path_count','未记录')}条变体缺数、缺最终观察或几何补充控制。同目标机械变体边界为{raw.get('raw_same_target_mechanical_boundary_count','未记录')}条，另有{raw.get('raw_registered_source_clock_boundary_count','未记录')}条来源时钟及{raw.get('raw_registered_scale_selection_boundary_count','未记录')}条注册周期选择边界，另有{raw.get('raw_variant_observable_boundary_not_applicable_count','未记录')}条可观察边界不适用；分层范围、破坏条件和原14项审计证据映射见实际报告。SET-0027公开价格代理保留身份及资料语义，已证明可执行价格谓词和触发序列与基础版相同，observable boundary明确not_applicable。

{distribution_note}

{resource_note(runtime)}

实际运行工作台完成{len(workbench.get('checks',[]))}项界面检查，合成HTTP展示fixture完成{len(writes.get('checks',[]))}项界面检查，另在独立实际demo服务完成{len(actual_writes.get('checks',[]))}项标注提交、三阶段实际API指标、版本阻断及修订恢复检查；离线知识库完成{len(ui.get('checks',[]))}项浏览器场景。以上是检查项或场景数，不是市场案例数；实际demo标签由合成输出构造以核对UI/API，不计盲标或真实标签。图层、特征层、原始管线、执行一致性及账户结果分层解释，不相互替代。

{soak_note()}实际连续运行与独立全日志重放由 [稳定性工具](../runtime/SOAK.md) 及项目外的soak报告记录；短时smoke和加速回放不计十小时。当前真实市场标签为0，首轮实时IEX接收/历史初始化/重连及美国股票迁移仍待真实资料和市场窗口；不能继承来源策略成绩。下一阶段每条基础版至少30正例及30困难负例、变体独立验证并锁定相关组切分。

所有运行资料存项目外。恢复校验算法、规则、配置、执行模型与核心哈希；改变核心需要重新回放及重新进行固定版本稳定性计时。候选归档的工程检查不代表最终长期验收。发布按测试 → report → clean build → 只读validate → 可复现性 → ZIP核验推进。原版本交接保留在 [handoff-v1.2.0.md](handoff-v1.2.0.md)。
'''
 path.write_text(header+marker+manual)
def verification_report(reports):
 path=ROOT/'reports/verification.md';history=ROOT/'reports/verification-v1.2.0.md'
 if not history.exists() and path.exists() and path.read_text().startswith('# 实际验证记录 · v1.2.0'):
  history.write_bytes(path.read_bytes())
 rows=[]
 raw=reports['recognition-validation.json'].get('raw_pipeline_coverage',{})
 distribution=reports['recognition-validation.json'].get('distribution_fixture_validation',{})
 fixture_rows='；'.join(f"`{name}`（{row.get('bytes')} bytes，SHA-256 `{row.get('sha256')}`）" for name,row in distribution.get('fixtures',{}).items())
 distribution_note=(f"发布白名单中的必要fixture为{fixture_rows}。从空目录解包候选ZIP后，原始覆盖与运行识别/检查点兼容性实际重新通过{distribution.get('test_methods_run')}个方法及{distribution.get('subtests_passed')}个子测试，各测试模块的方法数为 `{json.dumps(distribution.get('test_modules',{}),ensure_ascii=False)}`，模块解析限定在解包目录。该证据属于白名单修复后的隔离工程快照；最终候选全部字节及相同fixture由项目外release-checks报告和manifest核验。此复验独立列示，不增加全套方法计数。" if distribution.get('status')=='passed' else '干净候选解包后的原始覆盖及检查点兼容性复验尚未记录。')
 audit=reports['recognition-validation.json'].get('original_audit_evidence_mapping',{})
 for name,label in [('recognition-validation.json','识别与评价'),('runtime-validation.json','运行、行情、模拟执行及公共契约'),('ui-results.json','离线知识库浏览器'),('workbench-validation.json','实际本地API工作台浏览器'),('validation-results.json','结构与Schema'),('rebuild-results.json','隔离构建与ZIP可复现性')]:
  row=reports[name];version=row.get('version','未记录');scope='本版实际记录' if version==VERSION else '历史记录或尚未执行'
  count=row.get('test_methods_run',len(row.get('checks',[])))
  rows.append(f"| [{label}]({name}) | {row['status']} | {version} | {count if count else '见报告'} | {scope} |")
 report=f'''# 当前工程验收记录 · v{VERSION} · {DATE}

此页由实际报告生成，不将旧版本通过结果计为本版验收。现行身份以 [release.json](../release.json) 为准，源规则和算法身份分别记录。测试证明相应工程边界，不证明真实市场识别质量、盈利能力或独立专家复审。

| 验收资料 | 状态 | 报告版本 | 实际方法/场景数 | 适用范围 |
|---|---|---|---:|---|
{chr(10).join(rows)}

识别测试保留算法、配置、输入哈希及子测试记录；运行测试覆盖资金、报价、冲突、退出、持久化和恢复。工作台浏览器资料保留运行ID、服务身份、截图SHA-256、浏览器环境与实际检查项。离线页面验证与有后台服务的工作台验证分别报告。

## 分层识别证据

图层手写特征路径与原始MarketEvent路径分别报告。当前原始管线矩阵状态：{raw.get('status','not_executed')}；{raw.get('raw_positive_graph_count','未记录')}张图达到完整正向触发。{raw.get('raw_core_negative_base_count','未记录')}个基础版共有{raw.get('raw_core_negative_path_count','未记录')}条已知为假的核心谓词负例，证据同时保留正反输入在相同事件、周期下的特征观察与必要能力。另有{raw.get('raw_supplemental_negative_path_count','未记录')}条变体补充阻断控制，不能计为核心谓词反例。总计运行{raw.get('raw_negative_controls_run','未记录')}个负向控制、通过{raw.get('raw_negative_controls_passed','未记录')}个。通过的{raw.get('raw_variant_boundary_count','未记录')}个变体差异中，{raw.get('raw_same_target_mechanical_boundary_count','未记录')}个是同品种/方向/共同周期的机械边界，{raw.get('raw_registered_source_clock_boundary_count','未记录')}个为独立注册的来源open时钟，{raw.get('raw_registered_scale_selection_boundary_count','未记录')}个为注册周期选择；{raw.get('raw_variant_observable_boundary_not_applicable_count','未记录')}个可观察边界不适用。不同方向的首次触发不能作为同目标的提前触发证据；来源时钟和周期选择不冒充共同周期下的形态行为差异。按验证层划分的破坏条件分布为 `{json.dumps(raw.get('negative_conditions_by_level',{}),ensure_ascii=False)}`。

SET-0027的public_price_proxy保留独立身份与公开价格代理语义，实际可执行价格谓词哈希及触发序列与基础版相同；可观察机械边界标为not_applicable，不计入通过的机械变体数。所有矩阵证据是合成工程路径，真实市场验证保持not_run。

{distribution_note}

原250情形中{audit.get('linked_case_count','未记录')}个待核查结果由 [audit_links.json](../recognition/audit_links.json) 映射；验证层分布 `{json.dumps(audit.get('verification_levels',{}),ensure_ascii=False)}`。原输入、原证据及修复后证据分别保留哈希和解释，源摘要SHA-256为 `{audit.get('source_summary_sha256','未记录')}`。特征层或图层证据不能冒充原始管线证据，尚待真实标注的规则仍保留未验证状态。

## 运行和发布复验

{resource_note(reports['runtime-validation.json'])}

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

真实市场标签目前为0；识别精确率/召回率、美国股票迁移和策略盈利保持未运行。首轮实时IEX接收、历史初始化与重连需有效本地凭证及实际市场窗口，尚待执行。{soak_note()}短时测试及加速回放不能替代独立墙钟计时资料；本页不预设通过。

Safari/Firefox专项兼容、动态DOM原始材料、独立策略收益及外链长期存活仍未验证。研究待核查问题保留在 [state.json](../research/state.json)，合成、真实和未知来源资料不合并评价。

## 历史资料

原v1.2实际记录保留在 [verification-v1.2.0.md](verification-v1.2.0.md)。旧审计与冻结输入保持原范围，本轮新增证据不会改写此前判断。
'''
 path.write_text(report)
def main():
 xs=records();ss=rows('sources.jsonl');es=rows('evidence.jsonl');vs=rows('visuals.jsonl');rs=rows('relations.jsonl');cs=rows('../research/candidates.jsonl');ds=rows('../research/merge-decisions.jsonl');vd={v['id']:v for v in vs};sd={s['id']:s for s in ss};read=[s for s in ss if s['actual_read_scope']!='not_read'];used={q for x in xs for q in x['source_ids']};gaps=[x['id'] for x in xs if not visual_status(x,vd)];context=[x['id'] for x in xs if visual_status(x,vd) and not visual_status(x,vd,True)];single=[x['id'] for x in xs if len(x['source_ids'])==1]
 fam=collections.Counter(s['source_family'] for s in read);primary=collections.Counter();cit=collections.Counter()
 for x in xs:
  for f in set(sd[q]['source_family'] for q in x['source_ids']):cit[f]+=1
  for f in set(sd[q]['source_family'] for q in x['source_coverage']['core_support_source_ids']):primary[f]+=1
 ui_path=ROOT/'reports/ui-results.json';ui=json.loads(ui_path.read_text()) if ui_path.exists() else {'status':'not_executed','checks':[]}
 valid_path=ROOT/'reports/validation-results.json';val=json.loads(valid_path.read_text()) if valid_path.exists() else {'status':'not_executed','checks':[]}
 recognition_path=ROOT/'reports/recognition-validation.json';recognition=json.loads(recognition_path.read_text()) if recognition_path.exists() else {'status':'not_executed'}
 runtime=evidence('runtime-validation.json');workbench=evidence('workbench-validation.json')
 handoff_report(recognition,runtime,workbench,ui)
 verification_report({'recognition-validation.json':recognition,'runtime-validation.json':runtime,'ui-results.json':ui,'workbench-validation.json':workbench,'validation-results.json':val,'rebuild-results.json':evidence('rebuild-results.json')})
 totals=collections.Counter(x['status'] for x in xs);states={k:totals.get(k,0) for k in ['reviewed','draft','provisional','candidate','merged','deprecated']};pa=collections.Counter(x['pa_role'] for x in xs);inds=collections.Counter(x['independent_corroboration'] for x in xs);markets=collections.Counter(v for x in xs for v in x['market_types']);methods=collections.Counter(v for x in xs for v in x['method_families']);req=collections.Counter(v for x in xs for v in x['data_requirements']);access=collections.Counter(s['link_state'] for s in ss)
 source_table='\n'.join('| '+f+' | '+str(fam[f])+' | '+str(cit[f])+' | '+str(primary[f])+' |' for f,_ in fam.most_common())
 reviewed=[x['id'] for x in xs if x['status']=='reviewed'];realn=sum(v['material_type']=='real_source_chart' for v in vs);synthn=sum(v['material_type']=='synthetic_structure_diagram' for v in vs);broad=sum(visual_status(x,vd) for x in xs);strict=sum(visual_status(x,vd,True) for x in xs)
 text=f'''# 质量审查报告 · v{VERSION} · {DATE}

完成{len(reviewed)}个独立reviewed ID，正文逐条保留前提、位置、时序、触发、失效和管理。这里的审查是本研究AI辅助逐条语义自审及工程检查；没有独立专家复核、真实市场策略回测或实盘账户审计。新增模拟账户单独进行工程检查，不能继承来源策略收益。

## 完成数量与去重

| 主记录状态 | 独立ID数 |
|---|---|
{table(collections.Counter(states))}

主记录总数{len(xs)}；候选{len(cs)}条，其状态为{dict(collections.Counter(c['state'] for c in cs))}。候选、别名、合并去向和多标签不增加合格数。{len(ds)}份候选决策与{len(rows('../research/semantic-review.jsonl'))}份逐条语义审查记录可追溯；{len(rs)}条关系双向浏览。

每条第11节比较最近1–3条，在入场前写明差异。重复判断采用七轴而非文本聚类：M2/H2、ORB窗口、poor high/low、压缩阈值、吸收显示工具和量化仓位参数保留为变体；位置模块重定向不宣称完整执行规则同义。最接近的结构见[比较指南](../indexes/comparisons.md)。

## PA、领域、市场与数据覆盖

PA core={pa['core']}（{pa['core']/len(xs):.0%}），supporting={pa['supporting']}，non_primary={pa['non_primary']}。按价格顺序是否主导必要决策判断，不因画在K线上就归PA。

| 可重叠方法家族 | 条目数 |
|---|---|
{table(methods)}

| 来源实际讨论的市场类别 | 条目数 |
|---|---|
{table(markets)}

市场类别可重叠；futures含外汇标的期货，FX_venue_unconfirmed明确未确认场所，不算额外现汇验证。unconfirmed保留源图品种缺口。日线、跨日、概念补充另列markets_context_only。各市场、家族、数据轴不能相加当独立ID。没有把任何规则机械迁移为已验证的跨市场适用。

| 必要数据标签 | 条目数 |
|---|---|
{table(req)}

session、时区、前日/当前段Profile及现金/隔夜边界在各条目明示。未知触发周期保留null与原因；不能默认为纽约现金日。

## 来源与独立性

来源记录{len(ss)}，实际阅读{len(read)}，被最终条目引用{len(used)}，实际阅读来源族{len(fam)}；全部记录包含{len(set(s['source_family'] for s in ss))}族。未读{len(ss)-len(read)}条不支持最终核心规则。正文相关页、全文、公开转写和缓存作者稿分别标阅读范围，获取与阅读分开。

{len(xs)-len(single)}条引用至少两份已读实质资料。{len(single)}条富信息单份原始来源例外：{links(single)}。例外可还原规则，但都标独立佐证有限，并分别注明编辑管理模块。

| 独立佐证类别 | 条目数 |
|---|---|
{table(inds)}

multiple_families只用于多个独立族的具体核心说明。independent_context_only是概念/背景互补；independent_event_facts_only只补官方事实；independent_counter_evidence_only补反证；limited_derivative_corroboration含来源继承。这些类别不能宣传为“全部已交叉验证”。

| 来源族 | 实读资料 | 最终条目引用数 | 核心规则覆盖数 |
|---|---|---|---|
{source_table}

Axia实读{fam['axia']}/{len(read)}（{fam['axia']/len(read):.1%}），被{cit['axia']}/{len(xs)}条引用（{cit['axia']/len(xs):.1%}），是主要集中风险。Galen/Brooks也在PA中集中。网站域名和托管者未当成独立作者；OneOption/Gao的大小写及转载家族已归一。约100份实质资料的探索方向未达，本版实际{len(read)}；没有把未读页/搜索摘要凑入。

## 分歧、数字与盈利主张

- 13母柱/内柱、影线/收盘内包分版本；16的1或2ATR缓冲不择优拼接；19原文空头买卖方向文字冲突已解释。
- 26历史高量日与SPI持续大买量反例保留，来源年份冲突不猜日期；42/43的300%例与150%默认、连续档数编辑阈值分开。
- 37的TPO选择、默认Volume、60分钟收盘和两TPO bracket触及接受不是同版；AMT2008–2026、4662 session、1090触发、62%touch/33%clean为作者统计，成本/原数据未核，不是本条净胜率。
- 45来源将被套买方下跌称short liquidation的文字冲突已注明，未照抄为事实；Jigsaw/不同产品的厚薄是相对比较，不能从一篇建立永久排名。
- 48同刻绝对偏离均值与半小时检查，49含隔夜首段与原模型时间风险已核对；新增硬价格止损、杠杆或分钟检查不能继承原回测。
- source/evidence保留原规则、编辑操作化、官方事实及反证支持范围。重要编辑模块标editor_proposal_not_tested，未归因给作者。

盈利状态：{dict(collections.Counter(x['profit_evidence'] for x in xs))}。只有作者特定历史研究，独立复现与账户审计数量均为0；无利润排行，图表与资料完整性不等于正期望。

## 视觉材料与实际查看

{len(vs)}份视觉记录：外部真实价格/成交图{realn}，本地原创非行情SVG{synthn}。原始来源示意、重绘行情、绩效表、实际观看视频均为0份计入本版覆盖。

- 广义已看行情覆盖{broad}/{len(xs)}（{broad/len(xs):.0%}），包括背景图。
- 排除背景的结构/片段/历史模型覆盖{strict}/{len(xs)}（{strict/len(xs):.0%}），仍不代表全部触发与订单核验；36只是Footprint片段，2/48是历史价格叠加模型。
- 只有背景：{links(context)}。真实价格案例缺口：{links(gaps)}。
- 原创结构示意覆盖{len(set(i for v in vs if v['material_type']=='synthetic_structure_diagram' for i in v['setup_ids']))}/{len(xs)}，单列，不提高真实案例率。

真实原图再分发授权未知，local_path为null；仅精确外链、PDF页序、哈希/尺寸与原创解读交付。日期/周期/时区/合约未知保持null和原因。全部source chart_viewed标签来自实际静态画面查看；没有将下载未看的图片或文字案例计入。图是否成交、机构身份、真实止损/收益未核。

在{realn}份外部图中，品种未核实{sum(v['symbol'] is None for v in vs if v['material_type']=='real_source_chart')}、日期未核实{sum(v['market_date'] is None for v in vs if v['material_type']=='real_source_chart')}、周期未核实{sum(v['timeframe'] is None for v in vs if v['material_type']=='real_source_chart')}、时区未核实{sum(v['timezone'] is None for v in vs if v['material_type']=='real_source_chart')}；这些字段缺口有重叠。57份均保存被查看图像/渲染页的哈希和尺寸，但哈希不证明金融数据来源或订单真实。

## 验收与工程验证

逐条语义自审记录了来源支持范围、日内/跨日边界、近邻区别、风险/管理版本和视觉限制；重点复核了单源、相近结构、精确数字与源文冲突。机械检查覆盖UTF-8/JSON Schema、ID、引用、15节、指纹、来源实际阅读、素材权限和路径、双向关系、候选决策、全文JSON/CSV一致性、全部HTML内部链接/图片/anchors和manifest哈希。

结构验证报告版本：{val.get('version','未记录')}，状态：{val['status']}；JSON Schema：{val.get('schema_checks','not_executed')}。离线浏览器报告版本：{ui.get('version','未记录')}，状态：{ui['status']}，完成{len(ui.get('checks',[]))}个测试场景。实际测试细节和工具环境见[验证记录](verification.md)。最后交付前仍以最终validate与ZIP完整性结果为准；报告不将未执行检查标为通过。

程序化识别包含50个基础路径与34个独立变体；工程测试报告版本：{recognition.get('version','未记录')}，状态：{recognition['status']}，实际执行{recognition.get('test_methods_run',0)}个测试方法。手写特征路径与原始格式合成行情分别说明范围，均不能替代真实行情标注。原规则正文版本仍为1.0.0，数值解释为编辑操作化。新增运行平台、免费IEX、统一模拟执行和五视图工作台的范围见[发布说明](release-notes.md)。真实行情精确率/召回率、美股迁移及首轮实时连接仍待执行。{soak_note()}详情见[使用指南](../recognition/README.md)、[评价协议](../evaluation/README.md)及[实际测试记录](recognition-validation.json)。

## 未执行、未知及接续

没有独立盈利回测、当下市场适用验证、动态DOM/视频/字幕审核、账户单据、所有外链的长期监控、Safari/Firefox专项兼容或第三方专家复审。没有外部发布或私人数据。已读网页的发布日、部分图的时区/周期/日期、部分交易场所、费用及订单排队仍未知，逐条保持缺失。

访问分类：{dict(access)}。403/402是限制，404是本次响应，DNS/网关属于临时网络，错误/空页面另记；不能将一次失败判永久死链。YouTube字幕轨道返回空内容，CUA控制超时；本地阅读UI使用Playwright验证。

下阶段优先补4个视觉缺口、46/50触发画面、Axia独立失败资料及37可比反证；pairs/纽约FX未明确全部日终风险边界，不计50。研究候选与具体问题保存在[state](../research/state.json)及[handoff](../research/handoff.md)。本版满足至少50个实质不同规则记录目标，来源独立性、视频和盈利核验仍有上述明确局限。
'''
 (ROOT/'reports/quality-audit.md').write_text(text)
 state={'schema_version':'1.0.0','version':'1.0.0','phase':'v1_release_validation' if ui['status']!='passed' or val['status']!='passed' else 'v1_complete_maintenance_ready','reviewed_ids':reviewed,'candidate_queue':[c['id'] for c in cs if c['state'] in ['candidate_unread','provisional','related_not_counted']],'pending_questions':['补25/28/37/49真实案例与46/50具体触发画面','Axia同族集中：寻找独立规则和失败案例','观看DOM动态过程并核对逐笔、队列与时间序列','37的Profile权重/接受/touch-clean版本与成本可比性','2/48/49原研究基础版独立复现与样本外','pairs/LeBaron FX日终风险边界和公开代码问题'],'visual_gaps':gaps,'context_only_visual_ids':context,'access_obstacles':[{'source_id':s['id'],'url':s['url'],'state':s['link_state'],'error':s.get('access_error')} for s in ss if s['link_state']!='checked_accessible'],'next_batch_priorities':['先补现有证据质量，避免仅扩数量','优先独立失败与no-trade案例','四条价格图缺口及动态执行核验','候选47未读需先查别名和指纹'],'counts':{'reviewed':len(reviewed),'candidates':len(cs),'sources':len(ss),'actually_read':len(read),'read_families':len(fam),'external_chart_record_coverage':broad,'structure_chart_record_coverage':strict},'engineering':{'structural_validation':val['status'],'browser_validation':ui['status']},'updated':DATE,'handoff':'research/handoff.md'}
 state_path=ROOT/'research/state.json'
 previous=json.loads(state_path.read_text()) if state_path.exists() else {}
 # Preserve human research questions, priorities and unrelated maintenance keys.
 questions=previous.get('pending_questions',[]);priorities=previous.get('next_batch_priorities',[])
 engineering={**previous.get('engineering',{}),**state['engineering']}
 state={**previous,**state};state['engineering']=engineering
 state['pending_questions']=list(dict.fromkeys(questions+state['pending_questions']))
 state['next_batch_priorities']=list(dict.fromkeys(priorities+state['next_batch_priorities']))
 state['version']=VERSION
 from runtime.soak import core_manifest
 current_core=core_manifest()['sha256']
 current_reports=all(r.get('version')==VERSION and r['status']=='passed' for r in [ui,val,recognition,runtime,workbench]) and all(r.get('core_manifest',{}).get('sha256')==current_core and r.get('core_unchanged_during_tests') is True for r in [recognition,runtime])
 state['engineering']['runtime_validation']=runtime['status'];state['engineering']['workbench_validation']=workbench['status']
 state['phase']='v'+VERSION.replace('.','_')+'_engineering_checks_passed_soak_and_market_validation_pending' if current_reports else 'v'+VERSION.replace('.','_')+'_engineering_validation_pending'
 state['engineering']['recognition']={'rule_version':RULE_VERSION,'base_graphs':50,'variant_graphs':34,'regressions':recognition['status'],'test_methods_run':recognition.get('test_methods_run',0),'market_validation':'not_run'}
 question='接入美股历史行情、逐笔/DOM与新闻，独立标注每条至少30正/30负并锁定样本外测试'
 if question not in state['pending_questions']:state['pending_questions'].append(question)
 (ROOT/'research/state.json').write_text(json.dumps(state,ensure_ascii=False,indent=2)+'\n')
 print(json.dumps({'reviewed':len(reviewed),'source_read':len(read),'candidate_states':dict(collections.Counter(c['state'] for c in cs)),'independence':dict(inds),'visuals':{'broad':broad,'structure':strict,'raw_source':realn,'synthetic':synthn}},ensure_ascii=False))
if __name__=='__main__':main()
