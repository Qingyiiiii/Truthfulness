# Claim warehouse（GDB1）

## 状态与边界

本页描述数据库前置改造 GDB1 的活动非商品 V02 合同与合成验收，不代表真实
S01 已启动。

- 权威事实仍是 append-only Artifact Registry 与外部存储中的 canonical
  `manifest.json` / `rows.jsonl`。
- Parquet 和 DuckDB 是可删除、可完整重建的查询投影。
- 当前只允许 invented synthetic 数据；真实媒体、真实 Claim、真实 Gold、V01
  payload、真实 S01 和 S02 的计数都必须为 `0`。
- `human_annotation` 逻辑层中的 Gold 只能是 `synthetic_contract_only` 合成
  Gold，不能宣传为真实人工标注。
- GDB1 通过后仍须由用户单独审核；本轮不会自动进入真实 S01。

## 模型前人工门与活动数据域

活动 V02 永久排除商品、商品混合和类别未确认素材。任何 ASR/OCR、Claim、检索
判断或其他业务模型读取正文前，调用方必须确定性验证有权限人工签发、并与
project/storage/source/Artifact/content hash 绑定的 `non_product_verified`
receipt。缺失、失效、不匹配或非通过状态必须 fail-closed，模型调用数保持 0；
模型不得承担素材分类。

Claim warehouse 不保留商品兼容表、taxonomy、视图、关系、空壳或
`not_applicable` 伪行。目标 catalog 固定为：

```text
BUSINESS_LOGICAL_TABLES=34
WAREHOUSE_CONTROL_LEDGERS=8
TOTAL_CATALOG_ENTITIES=42
PRODUCT_DOMAIN_TABLES=0
```

八张控制账本为 `warehouse_export`、`export_publication_journal`、
`warehouse_load_plan`、`warehouse_load_batch`、`warehouse_loaded_export`、
`warehouse_projection_attempt`、`warehouse_load_receipt` 和
`warehouse_watermark`。

## 权威链与五层隔离

一个逻辑 warehouse 使用五个明确命名的逻辑层：

1. `core_provenance`：source、run、Claim 稳定实体/修订与拆分关系；
2. `machine_screening`：Evidence、link 与初始 machine assessment；
3. `source_depth`：独立 retrieval batch 与 rebuilt assessment；
4. `human_annotation`：annotation task、authorized-human annotation 与合成
   Gold；
5. `analytics_mart`：本轮只冻结边界，不写业务事实。

Loader 只投影，不能改写业务语义。每一行在进入 Parquet 之前必须同时通过：

- table-specific strict data schema；
- canonical primary key、revision、writer role 和 logical layer 校验；
- source/run、父 Claim/revision/split/member/atomic Claim、Evidence/link、machine
  batch/assessment、annotation/Gold 的 FK 校验；
- 每父至少一子、每 approved annotation 恰一 Gold 等基数与唯一约束；
- machine candidate、source-depth 与 human canonical 写权限隔离。

## Immutable export 与 V02-only Loader

每个 export 的相对布局固定为：

```text
exports/<export_id>/manifest.json
exports/<export_id>/rows.jsonl
```

两份文件都使用 UTF-8 canonical JSON；`rows.jsonl` 以
`(logical_layer, table_code, canonical_primary_key)` 排序并以 LF 结尾。同输入
重放必须得到逐字节相同的 manifest、rows、logical hash。Registry 只记录逻辑
`storage_root_ref` 和相对路径，禁止私有绝对路径、`..`、UNC、盘符或 symlink
逃逸。

每个 manifest 冻结 source Registry 的 prefix count、prefix bytes hash、head
record ID/hash 与精确 input Artifact record。Loader 在读取 manifest/rows payload
前必须先验证 project/storage/run/Registry path 均属于 V02；任何 V01 身份、未知
major 或混合身份都在 payload read count 仍为 0 时拒绝。migration 只允许
V02→V02 side-by-side rebuild，禁止 V01→V02 导入、映射或修复。

Loader 的八个 fail-closed 阶段固定为：

1. `load_plan`
2. `export_validate`
3. `parquet_staging`
4. `parquet_validate`
5. `parquet_publish`
6. `duckdb_transaction`
7. `receipt_publish`
8. `registry_append`

每阶段故障恢复必须证明 no-clobber、幂等重放、attempt/receipt 可追踪以及再次恢复
新增 0 条业务行。S01 publication 的七阶段和 finalizer 的十二阶段由各自测试
证明；warehouse validator 只引用这些证据，不重复宣称由单一脚本覆盖。

## 规模任务延期与当前小规模验收

```text
SCALE_501_919_10=DEFERRED_UNTIL_SUFFICIENT_V02_DATA_AND_REAUTHORIZATION
```

501 source、919 export、10 load batch 不是当前 GDB1 的通过条件。只有 V02 原生
非商品数据达到足量条件、且用户重新授权规模/预算/fixture 后才可重新设计和
执行。当前回归不得生成、发布或加载该旧规模，也不得用乘法计数冒充执行。

当前只运行最小、schema-valid 的 invented fixture，并在报告中披露实际 source、
run、export、row、plan 与 receipt 数。验收必须覆盖：

- 34 张业务表 + 8 张控制账本 = 42 项，商品结构计数为 0；
- 父/Atomic Claim、dependency、writer permission 与七项 Gold；
- Evidence 七个正交轴；
- deterministic export、current/as-of、完整重建和幂等重放；
- 同 ID 异 hash、第二 writer、stale lock、未知 schema major 等拒绝路径；
- V02-only payload 前置门以及全部 V01 read/import/map 计数为 0；
- real media/model/S01/S02/human-Gold 计数全部为 0。

非商品长 Claim fixture 使用复杂政策、科研或跨时段统计分析：

- 一个父 Claim 精确为 65,536 个混合 Unicode code point，并有 128 个实际
  split member；
- 一个父 Claim 精确为 65,537 个四字节 code point，真正触发两个连续 chunk；
- 同时验证精确 262,144-byte inline 边界、超过 5,000 字符的 atomic 候选、
  `needs_human_split`、全链 SHA-256 与逐字符重组；
- 禁止用商品、SKU、报价、评论或促销内容生成该 fixture。

当前小规模测试入口应禁用仓库外缓存，并明确不收集已延期规模测试：

```powershell
$env:PYTHONDONTWRITEBYTECODE = "1"
pytest -p no:cacheprovider `
  tests/versions/v02/test_claim_taxonomy.py `
  tests/versions/v02/test_business_models.py `
  tests/versions/v02/test_warehouse_models.py `
  tests/versions/v02/test_warehouse_export.py `
  tests/versions/v02/test_warehouse_loader.py -q
```

## 查询、重建与兼容判定

DuckDB 必须实际查询并断言：

- `v_parent_claim_current`、`v_atomic_claim_current`、
  `v_claim_split_current`、`v_machine_verdict_current`、`v_evidence_current`、
  `v_claim_evidence_current`、`v_gold_current` 与
  `v_warehouse_projection_lag`；
- as-of 使用 schema 的 `warehouse_rows_as_of` table macro，不能在验收脚本中
  另造同名逻辑；
- machine、source-depth 与 human 表的 logical layer/namespace 违规数为 0；
- 删除投影并从 immutable exports 重建后，逐表计数、logical hash、current、
  as-of、跨 Claim 查询和长 Claim 重组结果不变。

删除投影时只能删除调用方明确提供的合成 root 内 DuckDB 与 `parquet/`，并先
验证目标仍位于该 root。旧 V02 schema 兼容验收采用 side-by-side rebuild：旧
DuckDB 只读且 byte hash 不变，successor 登记来源 schema/hash；rollback 只归档
successor。V01 永不进入该兼容链。

## 微型示例

`examples/versions/v02/claim_warehouse/` 只提供 invented micro fixture，帮助审阅
canonical 字节形态；它不是已延期规模任务的输出，不包含真实媒体、Claim、Gold、
私有绝对路径、Ubuntu 主机路径或商品 taxonomy，也不能替代小规模合同测试。
