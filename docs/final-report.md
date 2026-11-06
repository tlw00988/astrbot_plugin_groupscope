# GroupScope 最终交付报告

> 项目：AstrBot 插件 **GroupScope（群聊观察与数字档案系统）**
> 版本：0.1.0 ｜ AstrBot 4.29.4 ｜ Python 3.12 ｜ 报告日期：2026-11-06
> 分阶段方案见 `docs/phase0-technical-proposal.md`，本报告是 Phase 0–6 全部完成后的总交付说明。

---

## 1. 结论摘要

- **Phase 1–6 全部完成**：消息档案 → 人物与关系 → 称呼与语义分析 → 群体记忆 → WebUI → 长期运行，功能代码均已落地并逐阶段提交。
- **全部验证手段通过**：
  | 手段 | 实测结果 |
  | --- | --- |
  | 单元测试 `pytest tests -q` | **43 passed**（0.5 s） |
  | 冒烟 `tools/smoke_phase5.py` | **PHASE5 SMOKE OK: 13 ok endpoints** |
  | 冒烟 `tools/smoke_phase6.py` | **PHASE6 SMOKE OK**（归档→purge→恢复→回链→退避→维护全链路） |
  | 压测 `tools/stress_test.py --count 5000 --archive` | **4,014 msg/s**、事件循环延迟 max 0.9 ms、`write_errors=0`、`quick_check=ok`、归档全链路 ok（1 万条时 4,244 msg/s、延迟 max 0.5 ms） |
  | 端到端集成 `tools/integration_test.py` | **exit=0，44 项断言全过，0 失败**（真 AstrBot + 假 OneBot 逆向 WS + LLM 陷阱服务） |
- **集成测试发现并修复了 1 个真实并发缺陷**（事务嵌套 `BEGIN`，见 §7.5），已补 `groupscope/storage/tx.py` 与回归测试。
- 规模：代码 **7,026 行**（`groupscope/` + `main.py`），测试与工具 **3,080 行**，WebUI 视图 **1,138 行**；数据库 **22 表 / 29 索引 / schema v1→v5**；**21 项配置**、**29 条指令**、**12 个只读扩展 API**、**5 个视图**。
- **未实现/未验证项已如实列在 §8、§9**，未按完成描述。

---

## 2. 交付文件列表

`git ls-files` 共 **64 个文件**。按目录归类：

| 分组 | 文件 |
| --- | --- |
| 入口与元数据 | `main.py`、`metadata.yaml`、`requirements.txt`、`_conf_schema.json`、`README.md`、`.gitignore`、`.astrbot-plugin/i18n/zh-CN.json` |
| 文档 | `docs/phase0-technical-proposal.md`、`docs/final-report.md` |
| 配置/路径 | `groupscope/__init__.py`、`groupscope/config.py`、`groupscope/paths.py` |
| 采集 | `groupscope/capture/{__init__,listener,models,serializer,service}.py` |
| 隐私 | `groupscope/privacy/{__init__,policy}.py` |
| 存储 | `groupscope/storage/{__init__,db,migrations,repo,tx}.py`、`schema_v1.sql` … `schema_v5.sql` |
| 分析 | `groupscope/analysis/{__init__,addressing,llm,prompts,rules,runner,tasks}.py` |
| 人物与关系 | `groupscope/social/{__init__,extract,profiles,snapshots}.py` |
| 群体记忆 | `groupscope/memory/{__init__,rules,store}.py` |
| 运维 | `groupscope/ops/{__init__,archives,backup,export}.py` |
| WebUI 后端 | `groupscope/web/{__init__,api}.py` |
| WebUI 视图 | `views/overview/index.html`、`views/relations/index.html`、`views/members/index.html`、`views/search/index.html`、`views/memory/index.html` |
| 单元测试 | `tests/conftest.py`、`tests/test_serializer.py`(11)、`tests/test_storage.py`(10)、`tests/test_capture_pipeline.py`(15)、`tests/test_phase6.py`(4)、`tests/test_tx_lock.py`(3) |
| 测试工具 | `tools/stress_test.py`、`tools/smoke_phase5.py`、`tools/smoke_phase6.py`、`tools/integration_test.py` |

行数统计：`groupscope/` + `main.py` = **7,026 行**；`tests/` + `tools/` = **3,080 行**；`views/*/index.html` = **1,138 行**。

---

## 3. Phase 0–6 功能清单

### Phase 0 — 环境与 API 调研（`docs/phase0-technical-proposal.md`，578 行）
- 实测本机 AstrBot 4.29.4 的插件骨架、消息事件/消息段结构、handler 执行机制（决定"不阻塞主流程"的实现方式）、数据目录与存储 API、WebUI 扩展方式、测试设施。
- 记录 10 条设计原则、总体架构、数据库设计、隐私设计、分阶段检查表，以及 4 项已确认决策（本机装 AstrBot 做集成测试 / QQ(aiocqhttp)为主+多平台降级 / WebUI 走插件可视化视图不建独立服务 / LLM 只复用 AstrBot 已配置 Provider + 插件侧限流）。

### Phase 1 — 消息档案（无 AI）
- 插件骨架：`metadata.yaml`、`main.py`（`Star`、`terminate`、`on_astrbot_loaded`）、`_conf_schema.json`、`requirements.txt`。
- SQLite 存储层：连接/PRAGMA（WAL 等）、版本链迁移框架（v1→v5）、schema v1。
- 采集链路：`listener` → 规范化（消息段/`@`/`Reply` 全量抽取，`Plain`/`At`/`Reply`/`Image`…）→ **有界队列**（`queue_maxsize=10000`，满则丢弃计数、绝不阻塞主流程）→ 批量事务写入 → `dedup_key` 去重。
- 通知事件（撤回/入群等，按 `notice_type` 识别）入库为 `event_kind='notice'`，只保存可观测内容、不做推断。
- 查询指令 `/gs status|stats|recent|search|pause|resume`；备份 `/gs backup|backups|restore`；导出 `/gs export <json|csv>`（流式分页，不整表进内存）；`/gs reload` 热读配置。
- 隐私：白名单为空 = **不采集任何群**；黑名单优先；`excluded_users` 排除；附件默认只存元数据、内联 base64 内容入库前洗除。
- 后台维护：保留期清理（`log_retention_days`，0=永久）、自动备份轮转（`backup_auto`/`backup_keep`）。

### Phase 2 — 人物与关系
- `interactions` 三类**确定性**互动（`@` / 回复 / 引用），有向、带时间与证据消息指针。
- `relationship_snapshots` 按日/周/月/季窗口聚合的有向互动统计：**只存计数与证据量**，`confidence` 是证据强度而非亲密度，禁止据此给人物排序。
- 成员档案 `/gs member`（别名史 `member_aliases`、`member_group` 群内昵称/发言数、有向互动、最近消息）。
- 身份不自动合并：`/gs merge <主ID> <被合并ID>` 人工合并 + `member_merges` 留痕；`/gs relations`、`/gs snapshot` 查询与即时计算。

### Phase 3 — 称呼与语义分析
- `addressing` 表区分 `source = rule / llm / manual`（人工优先级最高，`/gs alias`）。
- 规则层（**零 LLM**）：昵称/别名/`@` 上下文确定性抽取 `extract_addressing_rules`。
- LLM 消歧层：`LLMGateway` 复用 AstrBot Provider，插件侧限流（并发 `analysis_concurrency`、最小间隔 `analysis_interval_ms`、每日配额 `analysis_daily_limit`），配额耗尽当天只跑规则层。
- 任务队列 `analysis_tasks`（`UNIQUE(kind, dedup_key)`）+ 执行器 `AnalysisRunner`：`addressing_rule/addressing_llm` 等 6 种任务；**限流退回 pending 不消耗重试**。
- 提示词版本化（`addr-v1`/`topic-v1`/`event-v1`），每次调用记录 `prompt_version`；`/gs reanalyze` 按当前版本重跑历史。
- 幻觉防护：LLM 返回的证据 id 不在输入集合内 → 丢弃；身份不存在 → 丢弃；`RateLimitExceeded` → 退回 pending。
- 指令 `/gs address`、`/gs analyze <群> [rule|llm|topic|event|all]`、`/gs tasks`、`/gs reanalyze`、`/gs alias`。

### Phase 4 — 群体记忆
- `topics` / `topic_mentions` / `events` / `event_evidence` / `memory_timeline`。
- 规则层关键词抽取 + LLM 抽取（话题 0~5 个、事件 `event_kind ∈ {meme,event,dispute,routine}`）。
- 梗/事件的**首现→传播→演变**证据链（`role = origin/spread/evolve`），每条结论 1~5 条消息证据。
- 自动入队按时间桶去重：规则每小时、addressing LLM 每 6h、topic/event LLM 每 12h。
- 指令 `/gs topics`、`/gs topic 1 <ID>`、`/gs events`、`/gs event 1 <ID>`、`/gs timeline`。

### Phase 5 — WebUI
- 5 个视图 `views/<name>/index.html`（Dashboard 自动发现，iframe 加载）：`overview` 总览、`relations` 关系网络（自绘 canvas 有向力导向图，线宽=证据量）、`members` 人物档案、`search` 消息检索（含前后 5 条上下文与完整消息段）、`memory` 群体记忆（话题/事件/时间线/称呼，均带证据跳转）。
- 12 个只读 GET 扩展 API，经 `context.register_web_api` 注册，route 带插件名前缀 `/astrbot_plugin_groupscope/...`，实际挂在 `/api/v1/plugins/extensions/astrbot_plugin_groupscope/...`：`overview`、`messages`、`message`、`members`、`member`、`network`、`addressing`、`topics`、`topic`、`events`、`event`、`timeline`。
- 鉴权完全复用 Dashboard（JWT / plugin-scope API Key），不自建账号体系；视图标题与描述来自 `.astrbot-plugin/i18n/zh-CN.json` 的 `views.<name>.title`。**所有写操作只走管理员指令，视图只读。**

### Phase 6 — 长期运行
- **月度归档**：`/gs archive <YYYY-MM>` 生成独立归档库 `archives/messages-YYYY-MM.db`（`ATTACH` 主库集合拷贝，**行数对账 + sha256**），主库不动；带 `purge` 才从主库删除该月消息。
- **证据指针搬迁**：purge 前把话题/事件/称呼的消息级证据指针写入 `archived_evidence`（唯一索引含 `IFNULL(role,'')`），**分析结论行不删**，归档后证据仍可回溯。
- **恢复**：`/gs unarchive <YYYY-MM>` 按 `dedup_key` 去重回灌、可断点续跑（重复执行只跳过）、恢复后回链证据；恢复前校验归档 sha256 + `quick_check`，损坏文件拒绝恢复。
- **校验/维护**：`/gs verify [YYYY-MM]` = 主库 `quick_check` + 归档对账 + `PRAGMA optimize`/`wal_checkpoint(PASSIVE)`；分析循环每小时 `requeue_stale(600s)` 回收卡死任务 + `db.maintain()`。
- **失败退避**：30s×2ⁿ 指数退避，上限 1 小时；限流退回的任务不消耗重试次数。
- **历史范围参数**：`/gs analyze|reanalyze ... since=YYYY-MM-DD until=YYYY-MM-DD`（`until` 含当天整天，非法日期给格式提示）。
- 4 条归档指令（`archive`/`archives`/`unarchive`/`verify`）与压测工具 `tools/stress_test.py`。

---

## 4. 数据库结构

单库 `groupscope.db`（WAL），**22 张表、29 个索引**，版本链 `schema_v1 → v5`（`groupscope/storage/migrations.py`，只增不改、启动时按序升级，`LATEST_VERSION=5`）。

| 版本 | 阶段 | 新增表 |
| --- | --- | --- |
| v1 | Phase 1 | `groups` `members` `member_group` `member_aliases` `messages` `message_segments` `message_mentions` `plugin_state` |
| v2 | Phase 2 | `interactions` `relationship_snapshots` `member_merges` `member_manual_aliases` |
| v3 | Phase 3 | `addressing` `analysis_tasks` `analysis_runs` |
| v4 | Phase 4 | `topics` `topic_mentions` `events` `event_evidence` `memory_timeline` |
| v5 | Phase 6 | `archive_manifest` `archived_evidence` |

要点：

- **原始与分析分离**：`messages`/`message_segments`/`message_mentions` 只追加；AI 结论写在 `addressing`/`topics`/`events`，且必须带证据指针。
- **证据外键**：`topic_mentions`、`event_evidence`、`interactions`、`message_segments`、`message_mentions` 对 `messages` 均为 `ON DELETE CASCADE`；`addressing.evidence_message` 为 `ON DELETE SET NULL`（消息被清理后结论保留、证据标记为缺失）。
- **去重**：`messages.dedup_key` 唯一 —— 普通消息 `m:{platform_id}:{message_id}`（`message_id` 存平台原始 id），通知事件用剔除时间字段后的内容指纹。
- **成员身份**：`groups`/`members` 各有 `UNIQUE(platform_id, group_id/user_id)`；合并走 `member_merges` 留痕。
- **任务队列**：`analysis_tasks` `UNIQUE(kind, dedup_key)`，`kind ∈ {addressing_rule, addressing_llm, topic_rule, topic_llm, event_rule, event_llm}`；`prompt_version` 记录提示词版本。
- **归档**：`archive_manifest`（月份、行数、sha256、是否 purge/已恢复）+ `archived_evidence`（被清理消息的证据指针镜像）。
- 关键字段（`messages`）：`platform_id/platform_name/group_ref/group_raw_id/message_id/dedup_key/sender_ref/sender_raw_id/sender_raw_name/event_kind/notice_type/sent_at/received_at/message_type/text/reply_to_message_id/reply_to_sender_id/raw_json/inserted_at`。

---

## 5. 配置（21 项）与指令（29 条）

### 5.1 配置 `_conf_schema.json`（21 项，全部走 AstrBot 管理界面）

采集域：`capture_enabled(true)`、`group_whitelist([]，留空=不采集)`、`group_blacklist([])`、`excluded_users([])`、`record_notices(true)`、`attachment_policy(metadata_only)`、`attachment_max_bytes(2097152)`。
白名单/黑名单/排除名单的条目支持三种写法：**UMO**（AstrBot 自带白名单同款，如 `default:GroupMessage:1013637348`，可直接粘贴）、`平台:群ID`（如 `aiocqhttp:123456789`）、纯 `群ID`（匹配所有平台）。
写入域：`batch_max_rows(100)`、`batch_max_interval_ms(500)`、`queue_maxsize(10000)`
长期域：`log_retention_days(0)`、`backup_auto(false)`、`backup_keep(7)`、`log_detail(false)`
分析域：`analysis_enabled(true)`、`llm_provider_id("")`、`analysis_concurrency(1)`、`analysis_interval_ms(1500)`、`analysis_daily_limit(200)`、`analysis_window_hours(24)`、`analysis_batch_size(3)`

### 5.2 指令（29 条，除注明外均需管理员权限）

| # | 指令 | 阶段 |
| --- | --- | --- |
| 1 | `/gs status` | P1 |
| 2 | `/gs stats` | P1 |
| 3 | `/gs recent [群ID] [条数]` | P1 |
| 4 | `/gs search <关键词> [群ID]` | P1 |
| 5–6 | `/gs pause <群ID>`、`/gs resume <群ID>` | P1 |
| 7–9 | `/gs backup`、`/gs backups`、`/gs restore <文件名>` | P1 |
| 10 | `/gs export <json\|csv> [群ID]` | P1 |
| 11 | `/gs reload` | P1 |
| 12 | `/gs member <成员ID\|昵称>` | P2 |
| 13 | `/gs relations [群ID] [day\|week\|month\|quarter]` | P2 |
| 14 | `/gs merge <主ID> <被合并ID>` | P2 |
| 15 | `/gs snapshot [群ID] [窗口]` | P2 |
| 16 | `/gs address [群ID] [关键词] [rule\|llm\|manual]` | P3 |
| 17 | `/gs analyze <群ID> [rule\|llm\|topic\|event\|all] [since=…] [until=…]` | P3/4/6 |
| 18 | `/gs tasks` | P3 |
| 19 | `/gs reanalyze <群ID> [rule\|llm\|topic\|event\|all] [since=…] [until=…]` | P3/4/6 |
| 20 | `/gs alias <成员ID> <称呼> [群ID]` | P3 |
| 21–22 | `/gs topics`、`/gs topic 1 <ID>` | P4 |
| 23–24 | `/gs events`、`/gs event 1 <ID>` | P4 |
| 25 | `/gs timeline [群ID] [条数]` | P4 |
| 26–27 | `/gs archive <YYYY-MM> [purge]`、`/gs archives` | P6 |
| 28–29 | `/gs unarchive <YYYY-MM>`、`/gs verify [YYYY-MM]` | P6 |

---

## 6. 安装与配置步骤

```bash
# 1) 准备 AstrBot（本机实测 4.29.4）
uv tool install astrbot --python 3.12      # 或 pipx / 官方安装方式

# 2) 安装插件（开发期软链；正式安装走 WebUI → 插件 → 从 GitHub 安装）
ln -s /path/to/astrbot_plugin_groupscope ~/.astrbot/data/plugins/astrbot_plugin_groupscope

# 3) 依赖（aiosqlite 已随 AstrBot 主程序，requirements.txt 里显式声明）
#    测试额外需要：uv pip install --python .venv/bin/python pytest pytest-timeout

# 4) 启动
astrbot init -y     # 首次只建目录；data/cmd_config.json 由首次 astrbot run 生成
astrbot run
```

运行期配置（WebUI → 插件 → GroupScope → 配置 → 重载插件）：

1. `group_whitelist` 填 `aiocqhttp:<群ID>`（**留空不采集**），需要排除的发送者填 `excluded_users`。
2. 需要 AI 分析时保持 `analysis_enabled=true`，`llm_provider_id` 留空则自动取第一个可用 Provider（**插件不存 API Key、不新增 base_url**）。
3. 数据目录 `<ASTRBOT_ROOT>/data/plugin_data/astrbot_plugin_groupscope/`：`groupscope.db`（+ WAL）、`backups/`、`exports/`、`archives/messages-YYYY-MM.db`。
4. WebUI：WebUI → 插件 → GroupScope → 详情页 → 打开视图（鉴权复用 Dashboard 登录态）。

---

## 7. 测试方法与实测结果

### 7.1 单元测试 —— **43 passed**

```bash
cd astrbot_plugin_groupscope && .venv/bin/python -m pytest tests -q
# 43 passed, 1 warning in 0.51s
```

| 文件 | 项数 | 覆盖 |
| --- | --- | --- |
| `tests/test_serializer.py` | 11 | 消息段/`@`/`Reply` 抽取、base64 洗除、notice 指纹、异常事件不抛出 |
| `tests/test_storage.py` | 10 | 迁移幂等与重开库版本、写入去重与计数、查询、保留期清理、备份校验、JSON/CSV 导出 |
| `tests/test_capture_pipeline.py` | 15 | 隐私白/黑名单/排除（含 UMO 写法）、有界队列丢弃计数、批量写入、停机排空、异常隔离 |
| `tests/test_phase6.py` | 4 | 归档→purge→恢复→回链、退避与 `requeue_stale`、维护、范围解析 |
| `tests/test_tx_lock.py` | 3 | 事务串行化回归（见 §7.5） |

### 7.2 冒烟测试

```bash
.venv/bin/python tools/smoke_phase5.py   # → PHASE5 SMOKE OK: 13 ok endpoints
.venv/bin/python tools/smoke_phase6.py   # → PHASE6 SMOKE OK
```

- Phase 5：注册 12 个扩展 API → 按官方路由匹配规则逐一调用 → 断言 13 项 ok（含 `member_id` 非数字 → HTTP 400、未知路由 404）。
- Phase 6：造数 → `create_archive`（主库不动）→ sha256/行数对账 → `purge`（3 条证据指针入 `archived_evidence`、分析结论保留）→ `restore`（20 条回灌、3 条证据回链）→ 断点续跑 `skipped=20` → **损坏归档被拒绝恢复**（`sha256 校验失败`）→ 退避 `+30s → +60s`、`max_attempts → failed` → `requeue_stale` 回收 1 个任务 → `maintain + quick_check ok`。

两个脚本都自建全新临时 `ASTRBOT_ROOT`（`tempfile.mkdtemp`），不触碰真实数据。

### 7.3 压力与稳定性测试

```bash
.venv/bin/python tools/stress_test.py --count 5000 --archive
```

| 指标 | 5,000 条 | 10,000 条 |
| --- | --- | --- |
| 吞吐 | **4,014 msg/s** | **4,244 msg/s** |
| 事件循环延迟 | max **0.9 ms** / p99 0.9 ms | max **0.5 ms** |
| 写入 | `inserted=5000 written=5000 batches=50 write_errors=0` | 同口径 |
| 队列 | 上限 10000，注入期丢弃 **0** | 丢弃 0 |
| 库体积 | 1.6 MB（337 B/条） | — |
| 收尾 | `quick_check=ok`，归档 5000 条 → verify ok → purge → 恢复 5000 条 | — |

### 7.4 端到端集成测试 —— **44 项断言全过（exit=0）**

```bash
.venv/bin/python tools/integration_test.py          # 全流程
.venv/bin/python tools/integration_test.py --keep   # 保留实例排查
```

机制：在隔离 `ASTRBOT_ROOT` 里启动**真实 `astrbot run`**，用假 OneBot 客户端反向连接 `ws://127.0.0.1:<port>/ws`（header `X-Client-Role: universal` + `X-Self-ID`）注入事件并应答 API，再起一个 **LLM 陷阱服务**（`/v1/chat/completions`）记录命中次数并按提示词返回"合法" JSON。

**R1（`analysis_enabled=false`，零 LLM 断言）28 项**

- 入库 4 条（普通 / 引用 / `@` / 通知），重复 `message_id` 去重、notice 标记正确；
- 白名单外群、被排除成员、私聊 **0 条入库**；
- 消息段 `Plain/At/Reply` 齐全（共 5 段）、`reply_to_message_id=9000001`、`message_mentions` 记录 `at→9004`；
- `@` 消息触发 `get_group_member_info`、引用消息触发 `get_msg`（假端均被调用）；
- 4 条指令各 1 次回复（管理员 `/gs status` 有状态、非管理员收到"权限不足"、`/gs backup` 有"备份完成"），**第一条指令之前 0 次回复**，备份文件已生成；
- 规则任务入队并 `done`、规则称呼条目 2 条，**陷阱服务命中 0 次**（规则层 + 非唤醒消息零 LLM）；
- SIGINT 干净停机：`exit=0`、日志含"GroupScope 已卸载"、`PRAGMA quick_check=ok`。

**R2（`analysis_enabled=true`，真实 LLM 链路）16 项**

- 陷阱命中 **3 次（addressing/event/topic）**，3 个 LLM 任务 `done`；
- `addressing` 出现 `source='llm'` 且证据指向真实消息 id、**没有证据为空的 LLM 结论**；`topics/events source=llm` 各 1 条；
- `/gs tasks` 回复任务统计与 `LLM: 调用 3 | 错误 0 | 限流 0 | 拒绝 0 | 今日剩余 197`；
- `since=/until=` 生效（回复含 `范围: 11-05 00:00 → 11-07 00:00`），非法日期 `since=2026-13-40` 被拦截提示；
- 普通消息依旧零回复；
- **WebUI 端到端**：Dashboard 登录 `HTTP 200` 拿到 token → 扩展 API `/status` **200**、`/messages?q=爬山` **200 命中 1 条**；
- 二次 SIGINT 干净停机（`exit=0` + 卸载日志 + `quick_check=ok`）。

完整输出：`/tmp/opencode/gs_itest_run.log`（44 个 `✓`、0 个 `✗`）。

### 7.5 集成测试发现并修复的真实缺陷：事务串行化

**现象**：真实 AstrBot 并发下写入偶发
`sqlite3.OperationalError: cannot start a transaction within a transaction`（采集侧表现为 `write_errors`，分析侧表现为任务失败重试）。

**根因**：插件只用**一个** aiosqlite 连接（`isolation_level=None`），写事务靠显式 `BEGIN IMMEDIATE/COMMIT/ROLLBACK`。aiosqlite 只把**单条语句**排进队列，不把"多条语句的事务"当原子块；而事务体内有 `await`（回查成员主键、回查证据消息、更新关联行），让出控制权时另一个协程（采集批量写入 / 称呼或话题落库）就会在事务未提交时再次 `BEGIN IMMEDIATE`。

**修复**：新增 `groupscope/storage/tx.py` —— 每连接一把 `asyncio.Lock`，`tx.begin/commit/rollback` 把 `BEGIN → COMMIT/ROLLBACK` 整段串行化；`begin` 拿锁后若发现残留事务先 `ROLLBACK` 自愈；`rollback` 即使失败也释放锁（防死锁）。已替换 **6 个文件、18 处**调用（`analysis/addressing.py`、`storage/repo.py`、`social/snapshots.py`、`social/profiles.py`、`memory/store.py`、`ops/archives.py`）。
**约束**：事务体内不得再调用 `tx.begin()`（同一把锁不可重入会死锁）—— 当前代码库所有事务体内只有 SELECT/INSERT/UPDATE/DELETE。
**回归**：`tests/test_tx_lock.py` 3 项（4 协程 × 25 次事务体内 `await asyncio.sleep(0)` 诱发交错 → 不嵌套且 4 把钥匙全写入；`begin` 自愈遗留事务；`rollback` 释放锁且内容不落库）。

---

## 8. 未实现清单

以下均为**明确未实现或未做到计划描述的程度**，不计入已完成：

1. **`attachment_policy = download_if_small` 未实现**：附件内容从不下载（插件内无任何 HTTP 下载逻辑），内联 base64 在入库前统一洗成 `base64://<omitted>`。当前 `none` 与 `metadata_only` 实际行为一致——只存消息段元数据。
2. **多平台仅代码级支持、未实测**：端到端验证只在 **aiocqhttp（QQ 逆向 WS）** 上做过；`metadata.yaml` 声明的 qq_official / telegram / discord / lark / slack / kook / satori / misskey / line / mattermost 未逐一验证，跨平台降级（平台不支持 `get_msg`/`get_group_member_info` 时的容错）只有代码路径，无实测数据。
3. **`kill -9` 硬崩溃恢复演练未做**（Phase 1 检查表第 3 项）：只做了 SIGINT 干净停机 + `quick_check=ok`；WAL 与事务理论上安全，但"写入中强杀 → 重启校验"没有专项用例。
4. **WebUI 视图未在真实浏览器里人工点验**：集成测试只验证了 2 个扩展 API 经 Dashboard 登录态返回 200；5 个视图的渲染、图表交互、从分析结论跳原消息的 iframe 内跳转，均未经浏览器实测。
5. **方案中的 Cytoscape.js 关系图未采用**：`views/relations` 改为自绘 canvas 力导向图（无外部 CDN/依赖，更符合离线与隐私原则），但与 Phase 0 方案文档的描述不一致。
6. **无自动身份合并**：只有人工 `/gs merge`；同一人多账号/改号的自动识别未实现（符合"AI 推断非事实"原则，但功能上确实缺）。
7. **关键词搜索是 `LIKE %kw%`，未上 FTS5/语义检索**：无分词索引、无 embedding/向量检索，长库下搜索是全表扫描（`ORDER BY m.id DESC` + `LIMIT`）。
8. **消息撤回不回写**：`group_recall` 只作为 notice 行入库，**原消息不会被标记/删除**，档案里仍保留撤回前内容（这是"原始消息可靠保存"的取舍，但与"撤回"语义不完全一致）。
9. **迁移演练不充分**：只测了"迁移幂等 + 重开库版本 = LATEST"；没有"用真实 v1 旧库（带数据）升级到 v5 并对账"的专项用例。
10. **`analysis_runs` 有表无深度利用**：记录了运行批次，但未提供按 run 回看/回滚单次分析结果的指令或视图。
11. **无跨群/跨平台聚合分析**：所有分析与视图均按单群维度，未做跨群画像。

---

## 9. 已知限制与风险

- **单连接串行写**：所有写走一把 `tx` 锁，吞吐靠批量（4,000+ msg/s 实测）而非并行；超大规模（>10 万条/天）需要重新评估。
- **队列有界即有损**：`queue_maxsize` 满即丢弃并计数（`/gs status` 可见），这是"绝不阻塞主流程"的必然取舍。
- **LLM 结论非事实**：所有 `source='llm'` 结果带 `confidence` 与 `prompt_version`，证据 id 不在输入集合内即丢弃；仍建议人工用 `/gs alias`、`/gs merge` 校正。
- **`confidence` 是证据强度不是亲密度**：关系快照只存计数与证据量，禁止据此给人排序（设计原则 6）。
- **归档文件需自行保管**：`archives/*.db` 是独立库，`purge` 后主库不再有该月原始消息，备份（`backups/`）与归档互为独立保险，需分别留存。
- **隐私默认最小化**：白名单为空即不采集；但一旦开白名单，原始消息会长期落在本地库中，需自行遵守所在群规与法规，退出删除要靠 `/gs` 数据清理 + 手动删库。
- **依赖 AstrBot 内部 API**：`register_web_api`、视图自动发现、`AstrBotConfig` 等属 AstrBot 扩展面，跨大版本（≥5）可能需适配（`metadata.yaml` 限定 `>=4.16,<5`）。
- **无 CI / 无 release**：远端只有 `master` 分支，没有 GitHub Actions 与 tag，单测、冒烟、压测、集成测试都需在本地手动执行。

---

## 10. 提交记录

git 身份 `tlw00988 <twlfox@outlook.com>`，`commit.gpgsign=false`。
远端：**https://github.com/tlw00988/astrbot_plugin_groupscope**（public，`master`），9 个提交已全部推送、工作区干净。

```
53b3bbe  Phase 0 技术方案 + Phase 1 骨架（存储/采集/隐私模块，尚未完成）
68112bc  Phase 1：单测 33 项 + 采集服务停机/空闲信号修复
c838f61  Phase 2：人物与关系（互动表/窗口快照/成员档案/人工合并）
d7a80ed  Phase 3：称呼与语义分析（规则层/LLM 消歧/任务队列/限流/5 条新指令）
5735da4  Phase 4：群体记忆（话题/事件梗规则+LLM 抽取、证据链、时间线、5 条新指令）
b2b6cfd  Phase 5：WebUI（12 个只读扩展 API + 5 个 Dashboard 视图 + i18n 元数据）
5b1856c  Phase 6：长期运行（月度归档/恢复/校验、任务退避与滞留回收、周期维护、历史分析范围参数、压测工具）
f974369  事务串行化修复（tx 锁，杜绝嵌套 BEGIN）+ 端到端集成测试与冒烟工具入库
<本报告>  最终交付报告（docs/final-report.md）+ README 测试章节更新
```

---

## 11. 一句话结论

**Phase 0–6 功能全部完成并通过 4 类验证（单测 43 / 冒烟 2 条链路 / 压测 4,014 msg/s / 集成 44 断言）；集成测试额外揪出并修复了一个真实并发写入缺陷；未实现项（附件下载、多平台实测、硬崩溃演练、浏览器人工点验、FTS 检索、自动身份合并等 11 条）已在 §8 如实列出。**
