# 版本与 canonical ID 规范

> 规范版本：`id_policy_v1.0.0`
> 项目范围：`truthfulness_v0.2_youtube_video`
> 权威状态：阶段一 canonical 规范；阶段四 WP1 补充 event/DAG 语法

本文只定义项目版本、组件版本、对象身份和物理路径之间的关系。Artifact Registry、DAG 业务拓扑、HANDOFF v2、Session 分支和自动调度不属于本规范；`dag_version` 与 `event_id` 仅在这里固定语法。

## 1. 固定版本身份

| 字段 | canonical 值 | 职责 |
|---|---|---|
| `project_version` | `v0.2` | 当前项目能力范围：YouTube 视频求真 |
| `storage_version` | `V02` | 本地运行目录分区：`runs/V02/` |
| `release_id` | `truthfulness_v0.2_youtube_video` | 人类可读的项目发布身份 |
| `primary_source_type` | `video` | 顶层待求真对象类型 |
| `primary_source_platform` | `youtube` | V02 顶层 run 的主来源平台 |
| `release_status` | `development` | 当前发布状态；以后可转为 `frozen` 或 `deprecated` |

固定边界：

- `v0.2` 只表示 YouTube 视频求真项目范围，不表示数据集已经冻结、发布或可以训练。
- `V02` 只是大写目录分区，不是 Semantic Versioning，也不是模型或数据集版本。
- 网页、新闻、公告、PDF、报告、数据库和截图可以成为 YouTube run 的证据来源，但不会因此自动成为新的 V02 顶层 run。
- 抖音及其他平台不占用 `v0.2`，其未来版本号另行决定。

## 2. 独立版本字段

| 字段 | canonical 格式 | 版本化对象 |
|---|---|---|
| `project_version` | `v0.2` | 项目能力和来源范围 |
| `release_id` | `truthfulness_v0.2_youtube_video` | 项目发布身份 |
| `id_policy_version` | `id_policy_v1.0.0` | ID 语法、唯一性与映射规则 |
| `dataset_version` | `truthfulness_youtube_video_ds_v<MAJOR>.<MINOR>.<PATCH>` | 已冻结的数据内容、标签和 split |
| `workflow_version` | `youtube_truthfulness_workflow_v<MAJOR>.<MINOR>.<PATCH>` | DAG、阶段顺序与阶段契约 |
| `dag_version` | `youtube_truthfulness_dag_v<MAJOR>.<MINOR>.<PATCH>` | 逻辑 DAG 声明及兼容代际 |
| `schema_version` | `<schema_name>_v<MAJOR>.<MINOR>.<PATCH>` | 单个结构化 Schema |
| `prompt_version` | `<stage_id>_prompt_v<MAJOR>.<MINOR>.<PATCH>` | 单阶段 Prompt |
| `agent_profile_version` | `<role>_agent_v<MAJOR>.<MINOR>.<PATCH>` | Agent 职责、工具和权限 |

这些字段独立演进。禁止因为项目版本是 `v0.2`，就把所有组件机械命名为 `v0.2.0`。实际运行必须固定具体版本，不能引用 `latest`。

组件版本变化规则：

- 兼容的说明或非行为性修订增加 `PATCH`。
- 新增兼容字段、兼容分支或可选能力增加 `MINOR`。
- 删除字段、改变字段语义、改变阶段输入输出或产生不兼容行为增加 `MAJOR`。

## 3. 数据集构建与冻结

构建态：

```text
dataset_build_id = dataset_build_<ulid>
dataset_status = draft
dataset_version = null
```

冻结态示例：

```text
dataset_build_id = dataset_build_<ulid>
dataset_status = frozen
dataset_version = truthfulness_youtube_video_ds_v0.1.0
```

数据集 Semantic Versioning：

- `MAJOR`：标签体系、任务定义或 split 规则发生不兼容变化。
- `MINOR`：兼容定义下新增已验证样本、来源范围或正式 split。
- `PATCH`：修正标签、证据元数据或非结构性错误；旧版本必须保留。

下载视频、完成 run 或形成标注批次都不会自动产生正式 `dataset_version`。正式指标必须同时绑定 `dataset_version`、`schema_version`、`workflow_version` 和 `exp_id`。

## 4. canonical ID 总规则

canonical ID 分为两类：

1. 语义稳定 ID：由外部稳定事实确定，例如 YouTube video ID 对应的 `source_id`。
2. 运行实体 ID：对象创建时分配，例如 `run_id`、`task_id`、`artifact_id`、`event_id` 和 `checkpoint_id`。

运行实体使用带类型前缀的 26 位小写 Crockford Base32 ULID：

```text
<kind>_<26-char-lowercase-ulid>
```

ULID 规则：

- 创建时只生成一次，之后不可重算、回收或复用。
- canonical 写出一律使用小写；读取旧数据时可以兼容大写。
- ULID 的时间排序只用于索引，审计时间仍使用显式 UTC 时间字段。
- 字符集排除 `i`、`l`、`o`、`u`；不能用当前秒数或递增整数替代随机唯一性。
- ID 只承担身份，不编码标题、状态、父子关系、阶段、URL 或物理路径。

字段名使用 `snake_case`；审计时间使用 UTC ISO-8601，例如 `2026-07-17T08:00:00Z`。

## 5. ID 目录

### 5.1 来源 ID

YouTube：

```text
source_external_id = <原始 11 位 video ID，大小写敏感>
source_id = youtube_<source_external_id>
```

校验表达式：

```regex
^youtube_[A-Za-z0-9_-]{11}$
```

V01 只读映射如果能从现有元数据可靠取得 BVID，可以使用：

```text
source_id = bilibili_<BVID>
```

```regex
^bilibili_BV[0-9A-Za-z]{10}$
```

无法可靠证明时必须使用 `null`，禁止从中文标题猜测平台 ID。

### 5.2 运行实体 ID

| 字段 | 格式 | 唯一范围 |
|---|---|---|
| `run_id` | `run_<ulid>` | 全项目 |
| `task_id` | `task_<ulid>` | 全项目 |
| `session_id` | `session_<ulid>` | 全项目 |
| `artifact_id` | `artifact_<ulid>` | 全项目 |
| `checkpoint_id` | `checkpoint_<ulid>` | 全项目 |
| `event_id` | `event_<ulid>` | 全项目 |
| `batch_id` | `batch_<ulid>` | 全项目 |
| `dataset_build_id` | `dataset_build_<ulid>` | 全项目 |
| `exp_id` | `exp_<ulid>` | 全项目 |
| `claim_id` | `claim_<ulid>` | 全项目 |
| `evidence_id` | `evidence_<ulid>` | 全项目 |

父子、阶段和尝试关系由 `parent_artifact_id`、`parent_checkpoint_id`、`stage_id`、`attempt_no` 等独立字段保存，禁止拼接超长层级 ID。

`claim_0001` 只可作为单个 run 内的 `claim_display_no`。V01 已存在的 `claim_001` 等编号继续作为 legacy 值，不在阶段一逐条回填。

## 6. run 目录和身份壳层

阶段一启用后，新 V02 run 必须先创建身份，再处理媒体：

```text
runs/V02/run_<ulid>/
  run.json
  source/
  media/
  transcript/
  claims/
  evidence/
  output/
  logs/
```

创建顺序：

1. 生成一次 canonical `run_id`。
2. 创建 `runs/V02/<run_id>/`。
3. 写入通过 `schemas/run_identity_v1.schema.json` 校验的最小 `run.json`。
4. 再开始下载、转录或其他处理。

标题只进入 `source_title` 和人类报告，不进入 canonical 目录名。

阶段一之前已经下载的 V02 试点目录不重命名。它使用：

- 目录内最小 `run.json`；
- `runs/V02/run_path_map.jsonl` 中的路径别名；
- `directory_mode = "legacy_alias"`。

目录名中的旧时间只能作为提示，不能在没有可靠日志时冒充已验证的 `started_at`。

## 7. V01 冻结与映射

V01 历史 run 保持原目录名、层级和文件内容不变。阶段一唯一允许增加的 V01 文件是：

```text
runs/V01/legacy_run_id_map.jsonl
```

每条记录为一个旧目录分配 canonical `run_id`，同时保存 `legacy_directory_name` 和 `legacy_relative_path`。该映射只提供新系统引用，不把 v0.2 的新字段追溯性强加给 v0.1。

## 8. 逻辑身份与物理路径

- `run_id` 是身份，`storage_path` 是位置，两者不能互相替代。
- canonical V02 路径必须严格等于 `runs/V02/<run_id>/`。
- legacy alias 可以使用旧物理目录名，但必须通过映射索引解析到唯一 canonical `run_id`。
- 结构化记录只保存仓库相对 POSIX 路径；禁止写入盘符、用户目录、Cookie 路径、Token 或请求头。

合法示例：

```text
run_01arz3ndektsv4rrffq69g5fav
youtube_A1b2C3d4E5F
runs/V02/run_01arz3ndektsv4rrffq69g5fav/
truthfulness_youtube_video_ds_v0.1.0
```

非法示例：

```text
youtube_某视频标题_20260717
run_latest
runs/v02/run_01arz3ndektsv4rrffq69g5fav/
C:\Users\name\private-run\
dataset_version = v0.2
```

## 9. 权威顺序

发生冲突时，读取优先级为：

1. 本文件；
2. `configs/version_id_policy.toml` 与对应 Schema；
3. 单个 `run.json` 和映射索引；
4. `runs/README.md`、`docs/file_layout.md`、`docs/interfaces.md`；
5. `Optmize/` 中标为历史、规划或候选的方案。

低优先级文件不能覆盖高优先级规则。校验命令：

```powershell
python -B scripts/validate_version_ids.py --root . --require-private --self-test
```

校验器只读取，不移动、重命名、删除或自动修复任何 run。
