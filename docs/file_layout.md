# 文件布局与公开策略

本项目采用“白名单式公开”原则：只有已经确认不含个人信息、访问凭据、真实运行材料和未清洗来源的数据，才进入公开快照。目录是否存在不等于允许提交；最终以本文件、`.gitignore` 和提交前审查共同决定。

版本、canonical ID 和 legacy 路径映射以 [version_and_id_system.md](version_and_id_system.md) 为权威；本文件只说明物理布局和公开边界。

## 一、三类公开边界

| 类别 | 默认 Git 行为 | 典型内容 | 处理要求 |
|---|---|---|---|
| 可完全公开展示 | 可提交 | 通用源码、中文文档、schema、测试、固定评测、合成示例、Docker 启动文件 | 提交前仍需执行密钥与路径检查 |
| 需要清洗后公开 | 默认忽略 | 原始数据、实验记录、方案草稿、来源特定构建脚本、教学材料、未经审查的报告 | 脱敏、去真实来源、确认许可后，逐文件显式放行 |
| 不可公开 | 始终忽略 | 凭据、cookies、token、真实媒体、运行产物、Chroma 库、复核数据库、模型权重、私有研究方案 | 不进入 Git；必要时仅在受控本地或私有存储中保存 |

`.gitignore` 只能降低误提交概率，不能替代提交前人工检查。已经被 Git 跟踪的文件不会因为新增忽略规则而自动移除。

## 二、仓库级目录

| 路径 | 用途 | 公开类别 |
|---|---|---|
| `README.md`、`LICENSE` | 项目入口和许可 | 可完全公开展示 |
| `src/video_truthfulness/` | 通用业务逻辑与 Agent/RAG 实现 | 可完全公开展示；来源特定脚本除外 |
| `app/` | 现有 Streamlit 页面和调用入口 | 可完全公开展示 |
| `tests/` | 自动化测试 | 可完全公开展示 |
| `evals/V01/` | 20 条固定合成评测 fixture | 可完全公开展示；冻结兼容用途 |
| `docs/` | 架构、接口、文件边界和演示说明 | 可完全公开展示 |
| `schemas/` | 数据契约 | 可完全公开展示 |
| `examples/` | 仅包含合成或已脱敏示例 | 可完全公开展示 |
| `Dockerfile`、`compose.yaml`、`.dockerignore` | 容器构建与一键启动 | 可完全公开展示 |
| `.env.example` | 不含真实值的环境变量模板 | 可完全公开展示 |
| `scripts/` | 不含凭据和私人路径的公共脚本 | 可完全公开展示 |
| `configs/` | 安全默认值、依赖清单和示例配置 | 可完全公开展示；本地覆盖配置除外 |
| `report/` | 报告及汇报材料 | 需要清洗后公开；仅显式白名单文件可提交 |
| `Optmize/` | 优化方案和研究草稿 | 需要清洗后公开；私有研究方案不可公开 |
| `teach/` | 教学和个人学习材料 | 需要清洗后公开，默认不提交 |
| `data/V01/`、`experiments/V01/` | 冻结的 V01 数据与实验记录 | 需要清洗后公开，默认不提交 |
| `项目方案.md` | 可能包含内部规划和真实来源背景 | 需要清洗后公开，默认不提交 |
| `runs/` | 每次真实处理运行的产物 | 不可公开；仅 `runs/README.md` 可提交 |
| `runtime/` | Chroma、人工复核数据库和运行时缓存 | 不可公开 |
| `models/` | 本地模型和权重 | 不可公开；仅 `models/README.md` 可提交 |
| `.agents/`、`.codex/`、`.tmp/` | 本地代理状态、工具状态和临时文件 | 不可公开 |

当前允许从受控目录显式公开的文件包括：

- `report/V01/v0.1成果汇报.md`
- `report/V01/Annotation-example.md`
- `Optmize/优化方案参考.md`
- `runs/README.md`
- `models/README.md`
- `.env.example`

新增例外必须逐文件审查，不应通过放开整个目录来实现。

## 三、版本目录与单次运行目录

阶段一启用后，新的 v0.2 真实运行产物使用固定存储分区和 canonical `run_id`：

```text
runs/V02/run_<ulid>/
  run.json
  source/
    source.json
    download_attempts.jsonl
    browser_fallback.json
  media/
    source.mp4
    source_audio.wav
    source_subtitles.vtt
  transcript/
    transcript.json
  claims/
    claims.jsonl
  evidence/
    evidence_records.jsonl
    source_text/
      <source_id>.txt
  screenshots/
    video/
      <claim_id>_<timestamp>.png
    sources/
      <claim_id>_<source_id>.png
  output/
    report.json
    report.md
  logs/
    events.jsonl
```

当前版本索引：

| 版本 | 路径 | 主要来源 | 状态 |
|---|---|---|---|
| v0.1 Seed | `runs/V01/` | B站 | 已于 2026-07-16 归档，共 27 个 run 目录 |
| v0.2 | `runs/V02/` | YouTube 视频求真 | 开发中；不代表数据集已冻结 |

规则：

- `project_version = v0.2` 与 `storage_version = V02` 分开保存；
- 新 V02 `<run_id>` 固定为 `run_` 加 26 位小写 Crockford Base32 ULID，不包含标题、URL、时间文本或路径；
- 新 V02 目录必须严格使用 `runs/V02/<run_id>/`，并在任何媒体处理前先写入最小 `run.json`；
- V01 的 27 个历史目录不改名，通过 `legacy_run_id_map.jsonl` 只读映射；阶段一前的 V02 试点通过 `run_path_map.jsonl` 解析；
- 每个运行目录必须自包含，便于本地复核；
- `run.json` 记录配置摘要、代码版本、开始和结束时间；
- `logs/events.jsonl` 只记录结构化事件，不记录 cookies、token、完整请求头或私人绝对路径；
- 整个 `runs/<storage_version>/<physical_directory>/` 属于不可公开内容，不通过“只挑几个看起来安全的文件”规避清洗流程。

## 四、Agent/RAG 运行时目录

建议的本地布局：

```text
runtime/
  chroma/
  review_tasks.sqlite3
  model_cache/

eval-results/
  <evaluation_run_id>.json
```

- `runtime/chroma/` 可能包含由来源文本生成的向量和元数据，不公开；
- `review_tasks.sqlite3` 可能包含人工复核上下文，不公开；
- `model_cache/` 只用于本地复用，不公开；
- `eval-results/` 是运行结果，不公开；固定的合成评测输入和预期结果应放在 `evals/`。

## 五、媒体命名与处理

建议文件名：

- `source.mp4`
- `source_audio.wav`
- `source_subtitles.vtt`

处理规则：

- 原始媒体保存到运行目录，不进入仓库；
- 除非复核确有需要，否则不保留重复的转码副本；
- 若转码，记录工具版本、参数和校验摘要；
- 媒体提取失败时保留失败状态，不创建空文件冒充成功产物。

## 六、截图命名与复核

视频截图：

```text
runs/<storage_version>/<physical_directory>/screenshots/video/<claim_id>_<timestamp>.png
```

外部来源截图：

```text
runs/<storage_version>/<physical_directory>/screenshots/sources/<claim_id>_<source_id>.png
```

规则：

- 文件名必须稳定且便于定位；
- 关键证据不能只依赖易失的外部 URL；
- 截图中可能出现账号名、头像、浏览器信息或个人路径，公开前必须单独清洗；
- 截图只证明“页面当时显示了什么”，不能替代来源真实性判断。

## 七、下载尝试与浏览器降级

自动下载只执行一次顺序尝试，并写入：

```text
runs/<storage_version>/<physical_directory>/source/download_attempts.jsonl
```

若直接下载失败，但用户可以在已登录浏览器中合法访问，则记录：

```text
runs/<storage_version>/<physical_directory>/source/browser_fallback.json
```

其中可以记录页面标题、规范 URL、可见元数据和降级原因，但不得记录会话密钥、完整 cookies、认证请求头或账号凭据。

## 八、Cookies 与凭据

Cookies 仅用于用户已获授权访问的来源，并遵循：

- cookies 文件始终存放在仓库外或被 Git 忽略的位置；
- 临时 Netscape 格式 cookies 文件在使用后立即删除或清空；
- 日志只记录“使用了临时凭据”，不记录其值；
- 错误消息、截图和报告必须隐藏 cookies 文件路径及账号标识；
- 需要长期保留的凭据交给系统密钥管理，不写入 `.env.example`。

## 九、模型和大文件

以下内容不可提交：

- 模型权重和检查点；
- 嵌入模型缓存；
- Chroma 持久化数据；
- 原始视频、音频和字幕；
- SQLite、DuckDB 等运行数据库；
- Parquet 或其他可能包含真实数据的批量文件。

仓库中只能保留下载说明、校验方法、许可证提示和不含真实权重的配置示例。

## 十、提交前检查

每次生成公开快照前至少确认：

1. `git status --short --ignored` 中没有意外放行的真实产物；
2. diff 中不存在 token、cookies、邮箱、账号、私人绝对路径和真实媒体链接；
3. `examples/` 与 `evals/` 仅包含合成或已明确授权的数据；
4. 所有“需要清洗后公开”的文件都有逐文件审查依据；
5. Docker 构建上下文没有复制不可公开目录。
