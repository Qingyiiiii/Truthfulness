# V02 Annotation Schema

版本：`annotation_schema_v02.1.0`
通用 taxonomy：`truthfulness_taxonomy_v02.1.0`
业务 Artifact successor：`v02_business_artifact_v1.2.0`

## 1. 当前边界

本文件只固化活动非商品 V02 的 schema、标签语义、写权限和合成验收口径。当前阶段永久禁止读取/导入/映射 V01，不写真实人工 Gold，不进入 S02。

- `V01_IMPORT_STATUS=FORBIDDEN`
- `V01_MAPPING_STATUS=FORBIDDEN`
- `PRODUCT_DOMAIN_SCHEMA=ABSENT`
- S01 只产生 machine candidate、机器 Evidence/Verdict 与 `warehouse.export_batch`。
- human canonical 表当前可以建表和用合成数据验收，但真实行数保持 0。
- canonical code 使用英文稳定值；中文只来自版本化 taxonomy 显示名。
- 任何模型读取正文前，调用方必须验证有权限人工签发且与 source/Artifact/hash 绑定的 `non_product_verified` receipt；商品、商品混合、类别未确认、receipt 缺失/失效/不匹配均 fail-closed，模型调用数为 0，模型不得承担分类。

规范源：

- `configs/versions/v02/claim_taxonomy_v1.toml`
- `schemas/versions/v02/v02_claim_taxonomy_v1.schema.json`
- `schemas/versions/v02/v02_business_artifact_v1_2.schema.json`
- `src/video_truthfulness/versions/v02/business_models.py`

## 2. Claim 身份与无损文本

### 2.1 父子结构

父 Claim 保存来源中的完整表达；Atomic Claim 保存后续核验的原子断言。父、子 Claim 使用不同 canonical identity，禁止把父 Claim ID 直接当作 child ID。

```text
parent_claim_revision
  -> claim_split_set_revision
       -> split_set_member (ordinal 0..N-1)
            -> atomic_claim_revision
```

- `resolved_atomic`：至少一个 child，ordinal 从 0 连续，`coverage_reviewed=true`。
- `needs_human_split`：必须保留失败原因和已经安全拆出的 child，`coverage_reviewed=false`。
- 即使父 Claim 已经原子化，也必须产生一个内容等价、identity 独立的 child。
- 任一 current split 为 `needs_human_split` 时，run gate 为 `WAITING_FOR_HUMAN`，不得进入 S02。
- unresolved split 下的 child 不得标记 `machine_verdict_eligible=true`。

### 2.2 inline/chunk XOR

父、Atomic revision 都使用同一无损文本 envelope：

| 字段 | 类型 | 规则 |
|---|---|---|
| `text_char_count` | integer | Unicode code point 计数，至少 1 |
| `text_utf8_byte_count` | integer | UTF-8 byte 计数，至少 1 |
| `text_sha256` | sha256 | 完整 UTF-8 bytes 哈希 |
| `inline_text` | string/null | UTF-8 bytes `<=262144` 时使用 |
| `chunks` | array | UTF-8 bytes `>262144` 时使用 |

`inline_text` 与 `chunks` 恰好二选一。chunk 必须满足：

- `chunk_index` 从 0 连续；
- byte range 连续、无重叠、无空洞；
- owner kind/revision 与外层 revision 完全一致；
- 每块哈希正确；
- 按序拼接后的字符数、字节数和 SHA-256 与 envelope 完全一致。

Atomic 文本超过 5,000 字符不是序列化错误，不允许截断。它必须完整保存并带 `atomic_text_over_5000_chars` 质量告警；若仍不能安全拆分，由所属父 Claim 的 successor split revision 写 `needs_human_split`。

强制合成回归包含：65,536 个混合 Unicode 字符、128 个 child、精确 262,144-byte inline 边界，以及 65,537 个四字节 code point 的真正 chunk 路径。

## 3. Checkability、Verdict 与 Gold

### 3.1 Checkability

| code | 中文 | Machine verdict | Human Gold |
|---|---|---|---|
| `checkable` | 可核验 | 必须产生一项机器候选 verdict | 除 `gold_uncheckable` 外六项 |
| `context_only` | 仅语境依赖 | 禁止 truth verdict；至少一条 dependency | 禁止任何 Gold |
| `not_checkable` | 不可核验 | 只能 `unverifiable` | 只能 `gold_uncheckable` |

dependency 值：`qualifies`、`conditions`、`scope_of`、`compares_with`、`same_assertion_bundle`。自环和重复 triple 被拒绝。

### 3.2 Machine verdict

机器候选值只有：

```text
supported
refuted
mixed
insufficient
unverifiable
```

机器 assessment 必须绑定 atomic revision、Evidence link IDs、reason、uncertainty、model/prompt/config identity，并固定 `writer_role=machine_assessor`、`review_status=machine_pending`。机器不得写任何 `gold_*`。

### 3.3 Human Gold 七项

| code | 中文 | 额外必需字段/关系 |
|---|---|---|
| `gold_supports` | 黄金支持 | 至少一条正式 supports Evidence |
| `gold_partially_supports` | 黄金部分支持 | `supported_scope` + `unsupported_scope` |
| `gold_refutes` | 黄金反驳 | 至少一条正式 refutes Evidence；不得映射为 misleading |
| `gold_misleading` | 黄金误导性 | `misleading_mechanism` |
| `gold_missing_context` | 黄金缺失语境 | `missing_context` |
| `gold_insufficient_evidence` | 黄金证据不足 | 已闭合 `retrieval_batch_id` |
| `gold_uncheckable` | 黄金无法核实 | 仅 `not_checkable` |

Gold 固定 `writer_role=authorized_human` 和 `approval_status=approved`。一名有权限的人工明确批准即可产生 canonical Gold。

父 Claim 不继承 child Gold。父级只允许人工独立写 `gold_misleading` 或 `gold_missing_context`，并满足：

- `annotation_scope=parent_context`；
- 独立 reason；
- 独立 Evidence；
- 不替代任何 child Gold。

## 4. Inclusion 权限

Machine 只能写独立的 `machine_inclusion_recommendation`；human canonical 决定为 `included/excluded/pending`。

- `excluded` 必须有 reason；
- `pending`、`excluded` 的 train/eval flag 必须为 false；
- `included` 不自动成为数据集样本；任一 train/eval flag 为 true 时必须绑定 approved Gold batch；
- machine 与 human 使用不同字段、不同 writer role，禁止互相覆盖。

## 5. Evidence 正交轴

Evidence 不再使用混合“质量”枚举。七个维度分别保存：

| 轴 | 值 |
|---|---|
| `source_kind` | `official/primary_report/paper/database/high_quality_secondary/other` |
| `source_role` | `primary_source/secondary_source` |
| `access_status` | `accessible/source_blocked/not_found/access_error` |
| `use_status` | `evidence/clue_only/rejected` |
| `evidence_strength` | `high/medium/low` |
| `evidence_relation` | `supports/refutes/context/conflicts/unresolved` |
| `availability` | `pending/has_evidence/no_evidence` |

组合约束：

- `use_status=evidence`：role、strength、relation 全部必填。
- `use_status=clue_only`：role、relation 必填；strength 必须 null。
- `use_status=rejected`：strength 必须 null，`rejection_reason` 必填，role/relation 可 null。
- `pending` 对应未闭合批次。
- `has_evidence` 对应闭合批次且至少一个正式 Evidence link。
- `no_evidence` 对应闭合批次且零正式 Evidence link；可以与 clue-only 共存。
- `source_blocked` 只是一次 retrieval attempt 的访问结果，不能自动推出 `no_evidence`。
- blocked attempt 可与其他来源的 `has_evidence` 共存。
- Evidence collection 允许零 Evidence revision/零 link；禁止制造伪 Evidence。

## 6. 活动非商品边界

活动 V02 不定义或接受任何商品专用 table、taxonomy、Artifact payload、关系、
标签、视图或 `not_applicable` 兼容占位。通用 `human_annotation` 是事实标注，不能
重新解释为商品 Review。输入若出现旧商品专用字段或 taxonomy version，schema
必须拒绝，不能静默丢弃或映射到通用字段。

非商品复杂政策、科研和统计 Claim 仍必须完整保存主体、适用范围、地区/人群、
时间窗、统计口径、单位、分母、样本边界、比较基线、否定和条件；这些维度通过
通用 Claim/dependency/Evidence 结构表达，不新增领域专用表。

## 7. Artifact 与 warehouse 交接

`v02_business_artifact_v1.2.0` 保留 v1.0/v1.1 可读性，并为 Claim/Evidence/Verdict 使用上述 successor payload。新增 Artifact type：

```text
warehouse.export_batch
```

其 canonical payload 精确包含：

```text
export_id
run_id
storage_root_ref
manifest_relative_path
manifest_hash
rows_relative_path
rows_hash
logical_hash
row_count
row_counts
schema_versions
taxonomy_versions
exporter_versions
projection_status="pending"
```

额外约束：

- manifest/rows 为安全仓库相对路径，位于同一 package 目录，文件名分别为 `manifest.json`、`rows.jsonl`；
- `row_count=sum(row_counts)`；
- `taxonomy_versions` 只绑定通用 `label_taxonomy_version`，不得包含商品 taxonomy 或兼容空值；
- envelope/payload `run_id` 一致；
- Artifact 必须由 S01 `warehouse_export` 节点产生并绑定上游 Artifact；
- Loader 只消费 immutable export；`pending` 不回滚成功的 S01，也不能伪装成 projection success。

## 8. 合成验收与错误边界

当前测试只使用 synthetic IDs、`example.invalid` URL 和合成文本，覆盖：

1. taxonomy TOML 与 JSON Schema 一致；
2. 超长父/Atomic 文本零截断、UTF-8 chunk 可重组；
3. resolved/needs-human 的 S02 gate；
4. checkability—machine verdict—Gold 矩阵；
5. 七项 Gold，特别是 `gold_refutes`；
6. Evidence 空集合、clue-only/no-evidence、blocked/has-evidence；
7. 商品 table/taxonomy/字段/view/fixture 计数为 0，旧商品字段 fail-closed；
8. machine/human writer permission；
9. `warehouse.export_batch` schema/runtime/hash/row-count 交接；
10. 旧 V02 `v02_business_artifact_v1.0.0` 与 `v1.1.0` 兼容读取，但不允许任何 V01 payload 进入兼容链。
