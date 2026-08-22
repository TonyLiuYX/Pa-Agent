# G003 PDF 渲染、OCR 与视觉检查记录

执行日期：2026-08-09。

## 检查结果

| 来源 | 页面 | 文本/图像处理 | 视觉结论 |
|---|---:|---|---|
| `Rose的策略.pdf` | 31 | 文本层 + 20 个图表页 OCR；全页缩略图渲染 | 以图表和黄色标题为主；买卖区域、三推、Mother Bar、FOFS、区间突破、旗形回调和十字星补救的图文页均可见。|
| `Rose 的 Caveman...pdf` | 14 | 文本层；图像页链接 | 图表与 bearish/bullish 规则、案例说明相邻，视觉标注可读。|
| `Rose 的 Gap Bar...pdf` | 4 | 文本层；图像页链接 | 两张趋势/斐波那契图与文字案例可读，图表不是无关装饰。|
| `Rose 的 MTR...pdf` | 未知 | `pdfinfo`、`pdftotext`、`pdftoppm`、`pypdf` 均失败 | 文件缺少 trailer/xref/EOF；未伪造页码或图像引用，问题写入 manifest/processing issues。|
| `Rose 的 Mother Bar...pdf` | 6 | 文本层；页面 4 OCR 补充；全页渲染 | 母线/子线示意、比例区域、三种背景、突破条件和 MB36 图表均可见；图注与规则文字被同一 chunk/相邻 chunk 关联。|
| `（公开版）PA日内交易setup清单...pdf` | 17 | 文本层；5 个图表/低文本页 OCR；全页缩略图渲染 | 突破单、限价单、Always in 和纪律检查表的表格与图例连续可读。|

## 方法与留存位置

- Poppler `pdftoppm` 用于全量页面渲染、缩略图检查和低文本图表页的 OCR 前置渲染。
- Tesseract `chi_sim+eng --psm 6` 用于图表页的 OCR 补充；OCR 文本和页级来源在 `knowledge/data/extracted/<source_id>/pages.jsonl` 中与文本层分栏保存。
- 图像数量和页面关联来自 `pdfimages -list`，汇总在 `knowledge/data/visual_index.json`，对应 chunk 的 `related_image_pages` 和 `visual_notes` 保留图文关系。
- `Rose的策略` 的代表性图表页 4、13、16、21、30，以及 Mother Bar 全部 6 页做了高分辨率视觉抽样检查；其余成功 PDF 做了全页缩略图检查并抽查图文连续页。

视觉检查用于确认图表确实承载规则含义，不用于从未标注的 K 线图推导额外价格参数。
