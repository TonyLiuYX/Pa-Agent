# 实际验证记录 · v1.2.0 · 2026-10-02

结构验证、完整JSON Schema和离线浏览器测试均通过。验证不替代50份逐条语义自审，不证明策略盈利。没有独立专家、回测或账户审计。

## 程序化识别工程验收

实际运行 `python3 scripts/test_recognition.py --require-jsonschema`：88个测试方法与300个子测试通过。全部50个基础规则和34个变体各有独立手写特征路径，另外核对原始格式的合成K线、逐笔、DOM、新闻、严格时钟、缺项、前后顺序、不可重绘、同刻多关键位、迟到CVD/Profile/VWAP、venue隔离、重复事件计数及CLI回放评价。路径单测不证明所有setup的原始行情检测质量。报告保留算法哈希与明确的验证范围：[recognition-validation.json](recognition-validation.json)。

与v1.1.0交付ZIP的原文SHA-256对照，50条setup源文均未改动。项目版本升为1.2.0，原setup源规则仍为1.0.0，算法规则为0.2.0。新增机器目录50基础+34变体的Schema、字段提供者和源文哈希检查通过；生成目录通过build维护。

391条合成输入在全部84条图上完成回放，产生201条状态转换，文件末尾无未结束路径；两个手写事件窗完成一对一匹配。这两个示例仅用于管线复现，不计真实精确率/召回率，未标注的其他事件不擅自当反例。实际摘要与算法/input/config哈希见[recognition-smoke.json](recognition-smoke.json)。完整运行数据保存在项目外，可用demo命令重建合成输入。

原250例独立复验目标确认44→63，新确认19例，原确认减少0；另有显式变体、内部接受位、纠正时钟及风险未知的补充证据。原审计保留，机器摘要见[recognition-situations.json](recognition-situations.json)，修复及保留边界见[修复说明](recognition-fixes.md)。

真实行情标签数仍为0；候选recall、确认precision/recall、美国股票迁移、实际延迟/容量、收益与执行验收均未完成。

## 已执行结构验证

实际运行 `python3 scripts/validate.py --require-jsonschema`，状态passed、errors=0、warnings=0。实际计数：50主记录、86来源、106证据、107视觉、74关系。

- JSONL/front matter UTF-8 parsing and unique stable IDs
- Five Draft 2020-12 JSON Schemas with format checks
- 15 sections, seven axes, nearest comparisons, taxonomy, review trail and body references
- Evidence/source ownership, read scopes, editor-rule attribution, visual rights/types and SVG assets
- Relations and candidate/merge targets
- Generated full JSON and UTF-8 CSV consistency
- All local HTML links, anchors, chapter pages, image paths and embedded offline data
- Manifest exact file set, SHA-256, sizes and count consistency

最初审查修正了章节解析、官方Web阅读器未留原字节的null哈希、来源核心/背景支持范围和未生成报告页面链接。最终完整Schema允许真实未知字段并要求原因，没有补造哈希。源文及精确数字另外分层复核，自动检查只审文件一致性。

## 已执行浏览器验证

Python 3.14.3 / Darwin，Playwright 1.62.0，Chromium 151.0.7922.34。使用file://，HTTP(S)请求被阻断；完成32个测试场景，无JS异常、无网络请求。桌面1360×1000、移动390×844。本次重新执行了原页面场景及新增程序化文档导航与移动布局检查。

- file:// home renders 50 records with network requests blocked
- Chinese full-text search finds absorption
- English AND-token search finds failed breakout
- Alias search finds H2/M2 record
- Body-only formula search finds noise-band record
- Zero-result state renders without errors
- PA + market multi-axis filters intersect correctly
- Data requirement filter matches actual DOM records
- Visual gap filter shows exactly four records
- Context-only visual filter separates two background records
- Method family + environment filters preserve specific VWAP balance setup
- Structure-location filter returns lead-lag IB and two-sided IB records
- Execution-mode filter separates three source model versions
- Empty comparison gives actionable selection hint
- Fourth comparison selection is rejected with visible reason
- Three-record comparison contains real fingerprint, entry, stop and management text
- Comparison selection persists after search changes
- Clear comparison resets selections and table
- Card navigates to full 15-section setup page
- Original local SVG renders in case section with meaningful alt text
- Setup-to-source link and source reverse citation work
- Bidirectional related-setup navigation works
- Learning, glossary, comparison, quality, report, aliases and source indexes render
- README links to offline recognition guide
- Offline executable catalog renders all 50 definitions and variants
- Recognition catalog links to independent evaluation protocol
- All 50 setup pages render 15 chapters and load their local original SVG
- 390px mobile home and horizontal comparison stay within viewport
- Mobile full-record case section loads local SVG and stays within viewport
- Mobile original-size diagram link opens the actual local SVG
- 390px recognition, catalog and evaluation pages stay within viewport
- No JavaScript page errors or HTTP(S) requests throughout offline tests

首次测试的第四选项被正确拒绝，但测试器check方法假定选中而退出；修正测试动作后通过。实际截图复查发现懒加载截屏时图片尚未加载，补等待与SVG宽高属性后重新验证，避免版面跳动。该修正与逻辑/收益无关。

## 实际视觉复查

本版重新执行32项离线浏览器场景，所有50条的本地SVG加载与15章节锚点均实际验证；保留桌面首页、比较、结构图和移动案例截图。上版已人工查看这五类页面，本次重点人工复查更新后的识别指南。

本次另外实际查看[桌面识别指南](previews/desktop-recognition.png)和[移动识别指南](previews/mobile-recognition.png)，中文内容、版本/真实验证边界可读，代码块自身滚动，没有页面横向溢出。

- [desktop-comparison.png](previews/desktop-comparison.png)
- [setup-absorption.png](previews/setup-absorption.png)
- [desktop-home.png](previews/desktop-home.png)
- [mobile-home.png](previews/mobile-home.png)
- [mobile-noise-band.png](previews/mobile-noise-band.png)

源市场图查看记录另在data/visuals，不混入页面预览截图。57份源图静态查看不等于核对动态DOM、参与者身份或订单。源码、SVG及Markdown主记录也实际读取；原图没有授权再分发，不打入包。

## 移动目录与可复现构建

将项目复制到独立临时目录，从/tmp运行build；160个生成文件（识别JSON/Markdown目录、全文JSON/CSV、所有HTML及自动索引）逐字节相同，差异0。临时副本已删除。构建只使用标准库；未依赖本机绝对路径、临时源PDF/图片或本机私密资料。结果见[rebuild-results.json](rebuild-results.json)。

## 未执行与限制

未专项测试Safari/Firefox，未读成功视频字幕、未观看原视频、未验证动态逐笔/队列、未做独立利润复现、当前市场迁移和所有外链长期存活。403/402、404、空/错页与网络错误分别在source记录。本次源资料阅读没有私人账号和外部写入。

最终文件校验在报告同步后再次运行只读validate；ZIP通过CRC和内含manifest逐文件SHA-256检查，结果保存项目目录外的release-checks.json，避免将ZIP自身哈希塞回ZIP形成循环。最终聊天只提供已生成文件入口。
