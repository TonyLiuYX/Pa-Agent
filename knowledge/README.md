# PA 策略知识库

这是 G003 的首版本地、模型无关 PA 知识库。它保留两层数据：

1. `data/extracted/`：按源文件和页码保存 PDF 文本层、必要的 OCR 补充和图像数量。
2. `data/chunks.json`：按标题/完整概念/setup/规则组/示例/例外切出的来源语义单元。每个 chunk 都有稳定 `chunk_id`、源文件、页码范围、标题路径、父级、相邻 chunk、原文和图文关联。
3. `data/pa_records.json`：规范化的 PA 记录，包含背景、方向、前置条件、识别、触发、入场、失效/止损、目标/退出、风险、禁用情形、例外、示例和来源引用。

`config/sources/<source_id>.json` 是每份来源唯一的手写配置边界，包含该文档的语义边界、视觉说明、normalized records 和该来源维护的 `terminology.terms`/`terminology.conflicts`；G007-G012 可以只编辑自己的文件。旧的 `config/document_specs.json`、`config/pa_records.json`、`config/terminology.json` 仅作为迁移兼容副本，构建器不再读取它们。跨来源冲突由一个 source-scoped 文件负责维护，再由 loader 确定性聚合。`schema.json` 是 schema 索引，实际版本化 schemas 位于 `knowledge/schemas/*-0.1.0.schema.json`。

## 运行环境

从项目根目录准备 Python、PDF 工具和 OCR 依赖。脚本会在 `build`/`update` 写入任何输出前检查这些依赖；缺依赖时返回退出码 `3`，不会生成半成品。

推荐三步 bootstrap（只用标准库 + pip）：

```bash
python3 --version                 # 已验证解释器：Python 3.14.3（Homebrew python@3.14，macOS arm64）
bash scripts/bootstrap_env.sh     # 1) 创建/复用 .venv，按 requirements-lock.txt 全量精确锁安装
bash scripts/smoke_env.sh         # 2) 逐项验证 venv/依赖版本/poppler/tesseract/kb.py validate/隔离构建/全量 pytest
```

`bootstrap_env.sh` 幂等：`.venv` 已存在则直接复用并核对锁安装。`smoke_env.sh` 每项打印 PASS/FAIL，任一项失败即以非零码退出；全量 pytest 当前为 813 用例，期望 `813 passed, 13 subtests passed`（0 failed / 0 errors）。锁文件头部记录了生成环境（Python 版本、平台、日期），验证不通过的版本组合会被 smoke 明确拒绝。

手动准备（或不需要测试依赖时按 `requirements.txt` 的运行范围声明）：

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-lock.txt
```

系统级依赖（macOS Homebrew）：

```bash
brew install poppler tesseract tesseract-lang

# Debian/Ubuntu 等 Linux（等价安装）
# sudo apt-get install poppler-utils tesseract-ocr tesseract-ocr-chi-sim
```

运行前可直接检查入口：

```bash
.venv/bin/python scripts/kb.py --help
```

注意：对正式 `knowledge/data` 运行 `validate` 会按设计以当前时间戳重写 `validation/quality_report.json` 与 `quality_report.md` 两份派生报告。`smoke_env.sh` 在验证后自动将其恢复为 G016 冻结字节，并按 `tests/fixtures/phase2/g016/protected-sha256.json` 的登记哈希自检；手动运行 `validate` 后请自行确认这两份报告与冻结哈希一致。

## 运行

在项目根目录执行全量构建：

```bash
python3 scripts/kb.py build
python3 scripts/kb.py query "Mother Bar"
python3 scripts/kb.py validate
```

`build` 会全量重建小规模语料库；遇到单个 PDF 损坏会记录到 `data/processing_issues.json`，继续处理其他文件。当前正式快照已用六份现有 PDF 重建：6/6 来源成功、82 个 chunk（65 个 retrieval chunk）、24 条 normalized record，`validate` 返回 `0` 且无 warning。7 页 MTR 替代文件会稳定映射到 canonical source ID `rose-mtr`，其本地 chunks、视觉引用和 source assertions 已取代旧的外部补充占位。文本层不足且页面含图时，脚本会调用 Poppler `pdftoppm` 和 Tesseract `chi_sim+eng` 做页面级 OCR。固定字符/token 长度不是切片规则；切片边界来自对应的 `config/sources/<source_id>.json`。

`Rose的策略.pdf` 的图表页使用 source-scoped 正向 OCR 白名单：逐页原始 `pdf_text`/`ocr_text` 和 document root 完整保留，只有 retrieval child chunk 会去掉 TradingView 工具栏、时间/价格坐标等界面噪声，并以明确的 `[OCR rule lines]`、`[Visual notes]` 标记保留规则文字和视觉转录。其他来源没有显式启用该 policy 时保持原抽取行为。

需要先验证而不改写正式快照时，使用一个空的隔离输出目录：

```bash
python3 scripts/kb.py build --output-dir tmp/pdfs/kb-runs/full-clean
python3 scripts/kb.py validate --output-dir tmp/pdfs/kb-runs/full-clean
python3 scripts/kb.py query "MTR 失败突破 1R 2R" \
  --output-dir tmp/pdfs/kb-runs/full-clean
```

隔离目录不存在时会自动创建；若已存在，先换用另一个空目录，避免把旧产物误当作本次干净构建证据。

构建先写入临时 staging 目录，质量检查通过后才交换正式输出目录。`quality_status=fail` 返回退出码 `4` 并保持上一份正式快照不变。

代表性文档闭环必须使用明确的隔离输出目录；`--only` 拒绝覆盖默认的 `knowledge/data`，也拒绝覆盖包含其他来源的已有输出：

```bash
python3 scripts/kb.py build \
  --only rose-mother-bar \
  --output-dir tmp/pdfs/kb-runs/mother-bar
python3 scripts/kb.py query "Mother Bar 区间 突破 止损" \
  --output-dir tmp/pdfs/kb-runs/mother-bar
```

查询结果中的 `citations` 直接返回 `chunk_id`、源文件、页码范围、标题路径和相关图像页，查询结果同时保留完整的背景—触发—入场—失效/退出逻辑摘要。

## 更新

小规模数据允许全量重建。`update` 与 `build` 使用同一流程，并在 `manifest.json` 的 `changes`（以及每个当前 document 的 `change_status`）中记录 `added`、`changed`、`deleted`、`renamed` 或 `unchanged`。每条变更都带有当前/上一版本的 SHA-256；删除项只留在变更台账，不会留在 active `documents`、chunks、records、citations 或 `extracted/<source_id>/`：

```bash
python3 scripts/kb.py update
```

最短维护流程：

1. 把新增/替换后的 PDF 放入 `策略/`；删除不再维护的文件，或直接改文件名完成同 SHA-256 重命名。
2. 运行 `update`，检查 JSON 输出和 `manifest.json` 的 `changes`/`change_summary`，确认 SHA-256 与预期一致。
3. 对 `changed` 的已知来源重新核对语义规则；在核源前，旧 normalized records 会保留为 `active: false`、`record_status: "invalidated"`、`source_fidelity: "pending_source_reverification"`，不会参与 query。
   核源完成后，把当前 PDF 的小写 64 位 SHA-256 写入该来源的 `knowledge/config/sources/<source_id>.json` → `document_spec.verified_source_sha256`，再运行一次 `update`；只有这个与当前文件内容精确匹配的 source-scoped marker 会将 manifest document 标为 `source_reverification_status: "source_reverified"` 并恢复 records。仅运行 unchanged no-op update 或同 SHA-256 rename 都不会解除 pending 状态。
4. 新文件若没有 `config/sources/<source_id>.json` 配置，只会生成页级 `extracted` 文本；validator 报告 incomplete、overall fail（退出码 `4`），不会替换上一份正式快照。补齐对应 source-scoped 文件后再次运行 `update`。
5. 连续对相同输入运行 `update` 应得到 `unchanged`，active chunks/records 的内容和 ID 保持不变（时间戳与 run metadata 可变化）。

可用 `--source-dir` 和 `--output-dir` 对测试目录做可逆更新演练，不会改写正式源文件：

```bash
python3 scripts/kb.py update --source-dir tmp/pdfs/update_fixture --output-dir tmp/pdfs/update_kb
```

## CLI 退出码

所有子命令的参数错误返回 `2`。构建类命令在依赖预检失败时返回 `3`；质量检查失败返回 `4`；输出目录不存在、不可读或无法交换时返回 `5`。

| 退出码 | 含义 |
| --- | --- |
| `0` | 成功；质量通过且没有来源处理 warning。对 `query`，合法但无命中也属于成功。 |
| `1` | warning；输出已提交且至少一个来源为 `partial`/`failed`；调用方必须检查每份 document 的最终状态，不能只看 `overall_status`。 |
| `2` | 参数/用法错误，包括非法 `--limit`、未匹配的 `--only`，或 `--only` 试图覆盖正式/不同来源输出。 |
| `3` | `build`/`update` 缺少 Python、`pypdf`、`jsonschema`、Poppler（`pdfinfo`、`pdfimages`、`pdftoppm`）或 Tesseract/语言包依赖。 |
| `4` | 质量报告为 `fail`；构建不会替换正式快照。 |
| `5` | 已有输出缺失/损坏或其他文件系统/运行时错误。 |

`query` 和 `validate` 只读取已生成数据，不需要重新解析 PDF，因此不强制 Poppler/Tesseract；`validate` 对带来源 warning 但整体质量通过的快照返回 `1`，对质量失败返回 `4`。

## 输出与质量检查

- `data/manifest.json`：6 份源文件的哈希、页数、文本/OCR/图像处理方式和状态。
- `data/extracted/<source_id>/pages.jsonl`：逐页抽取结果。
- `data/visual_index.json`：图像页、OCR 页和 chunk 视觉引用。
- `data/processing_issues.json`：局部失败、页级 OCR 问题和补充来源说明。
- `data/validation/quality_report.json` / `.md`：版本化 JSON Schema、声明类型/claim-level 引用、页码与哈希、chunk 父子/相邻关系和跨页连续性、精确引用回指、字段、图文链接、重复、术语冲突和代表性查询检查。

当前正式 manifest 中的 MTR 来源为 `策略/价格行为学 - Rose 的 MTR (主趋势反转) - forecho's Blog 2.pdf`（7 页，SHA-256 `9ade5b6c706cdeff09fba8effadffc1d0bda3c4a2004774232bae24ed094e5be`）。它以 `rose-mtr` 保存 7 个 retrieval chunk 和 1 条本地 normalized record；`processing_issues.json` 为空，`extracted/rose-mtr/` 只包含 `document.txt` 与 `pages.jsonl`，不再包含旧损坏文件的 diagnostic 或 `supplemental_external_due_to_pdf_failure` 记录。

## 维护约定

- 新文档先运行 `build`，确认 manifest 和逐页抽取，再新增 `config/sources/<source_id>.json`（不要修改共享聚合配置）。
- 不要按固定 token/字符数切割条件链；如果一个 setup 的背景、触发、入场、失效、目标或关键例外跨页，应扩大页面范围或用父/相邻 chunk 保持关联。
- 来源原文与规范化解释分开：原文留在 chunk，归纳逻辑写入记录，并在 `interpretation_notes` 标明推导性质。
- `market_context`、`trigger`、`entry`、`invalidation_stop`、`targets_exit`、`risk_notes`、`prohibited`、`exceptions`、`examples`、`prerequisites` 和 `interpretation_notes` 的每一项都是 claim 对象：必须有 `statement`、`claim_id`、`declaration_type`（`source_assertion`、`derived_interpretation` 或 `source_not_stated`）和解析到 chunk/page 的 `source_refs`。`derived_interpretation` 与 `source_not_stated` 还必须写非空 `rationale`。
- 这里的知识库不证明策略有效、不提供真实交易执行，也不替代后续风险/绩效验证。
