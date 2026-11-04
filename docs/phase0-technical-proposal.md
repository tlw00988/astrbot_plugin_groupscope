# Phase 0：环境与 API 调研 + GroupScope 技术方案

> 项目：AstrBot 群聊数字档案与社会关系图谱插件（GroupScope）
> 状态：**Phase 0 已完成（仅调研与方案，未编写业务代码）**
> 调研日期：2026-11-04
> 调研对象版本：AstrBot `master` 分支，`pyproject.toml version = 4.29.4`，`astrbot/__init__.py __version__ = "4.29.4"`

---

## 0. 本阶段交付物

| 交付物 | 位置 | 说明 |
| --- | --- | --- |
| 调研报告 + 技术方案 | `docs/phase0-technical-proposal.md`（本文件） | Phase 0 唯一产出 |
| AstrBot 源码快照（调研用） | `/tmp/opencode/astrbot-src`（临时目录，非交付物） | 用于核对 API 真实实现 |

**本阶段修改文件列表**

- 新增 `docs/phase0-technical-proposal.md`

**本阶段未实现的功能（明确声明）**

- 消息采集、SQLite 存储、人物档案、称呼识别、事件记忆、WebUI、AI 分析——**全部尚未实现**，本阶段没有任何 Python 业务代码。
- 下文所有 DDL、目录结构、流程图均为**设计**，尚未落盘为代码。

---

## 1. 运行环境实况

| 项目 | 结果 |
| --- | --- |
| 工作目录 | `/home/alhsk/projects/astrbot_plugin_groupscope`（空目录，非 git 仓库） |
| 本机是否已安装 AstrBot | 调研开始时**未安装**；已于同日通过 `uv tool install astrbot --python 3.12` 安装成功（`astrbot version 4.29.4`，`~/.local/share/uv/tools/astrbot`） |
| 本机 Python | 3.14.9（`/usr/bin/python3`） |
| AstrBot 版本要求 | `requires-python = ">=3.12"`，官方推荐 `uv tool install astrbot --python 3.12` |
| 包管理工具 | `uv 0.13.6` 可用（`/usr/bin/uv`），pip/pipx 可用 |
| Docker | 无运行中的容器 |
| 参考项目 | `~/projects/langbot_plugin`、`~/projects/AI_Message_anayzle`（历史 QQ 消息分析脚本，与本项目无关但可参考数据处理思路） |

**结论**：目标运行环境已就绪——AstrBot 4.29.4 已通过 `uv tool install astrbot --python 3.12` 安装到本机（CLI 可用）。所有 API 调研**以源码 + 安装环境冒烟实测为准**，而非仅凭记忆/二手资料。AstrBot 实例尚未 `init/run`（留给 Phase 1 集成测试）。

---

## 2. AstrBot 插件规范调研结论（源码核对）

调研方式：克隆官方仓库 → 通读 `docs/zh/dev/star/*` → 逐条 grep 源码验证。

### 2.1 插件骨架

| 项 | 结论 | 源码依据 |
| --- | --- | --- |
| 插件目录 | `data/plugins/<plugin_name>/` | `get_astrbot_plugin_path()` → `data/plugins` |
| 入口文件 | `main.py`，插件类继承 `Star` | `docs/zh/dev/star/guides/simple.md`、`astrbot/core/star/base.py` |
| 元数据 | `metadata.yaml`（**必需**）：`name/display_name/desc/version/author/repo/astrbot_version/support_platforms` | `docs/zh/dev/star/plugin-publish.md` |
| 配置 | `_conf_schema.json`（可选），加载后实例化时以 `config: AstrBotConfig` 传入 `__init__`，实体文件落在 `data/config/<plugin_name>_config.json`，`self.config.save_config()` 保存 | `docs/zh/dev/star/guides/plugin-config.md`、`star_manager.py:1167-1243` |
| 依赖 | 插件根目录 `requirements.txt`（pip 格式） | `plugin-new.md` |
| 日志 | `self.logger`（插件专属 logger，由 `Star.__init__` 注入），或 `from astrbot.api import logger` | `astrbot/core/star/base.py:27-55` |
| 生命周期 | `async def terminate()`：插件卸载/停用时调用；`@filter.on_astrbot_loaded()`：AstrBot 初始化完成钩子 | `simple.md`、`listen-message-event.md` |
| 国际化 | `.astrbot-plugin/i18n/<locale>.json` | `guides/plugin-i18n.md` |
| Logo | `logo.png`（256×256，可选） | `plugin-new.md` |
| 版本约束 | `metadata.yaml` 中 `astrbot_version: ">=4.16,<5"`（PEP 440，不带 v） | `plugin-new.md` |

配置 Schema 支持类型：`string/text/int/float/bool/object/list/dict/template_list/file`，支持 `default/hint/options/labels/slider/secret/invisible/_special`。`_special: select_provider` 可让用户直接选择 AstrBot 已配置的模型服务商——**后续 LLM 配置会用到**。

### 2.2 消息事件与消息对象（采集核心）

```python
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star

class Main(Star):
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def on_group_msg(self, event: AstrMessageEvent): ...
```

`event.message_obj`（`AstrBotMessage`，`astrbot/core/platform/astrbot_message.py`）字段：

| 字段 | 类型 | 采集用途 |
| --- | --- | --- |
| `type` | `MessageType`（GROUP_MESSAGE / FRIEND_MESSAGE / OTHER_MESSAGE） | 消息类别 |
| `self_id` | `str` | 机器人 ID |
| `session_id` | `str` | 会话 ID（受 `unique_session` 影响） |
| `message_id` | `str` | **去重主键之一** |
| `group` / `group_id` | `Group \| None` / `str`（私聊为空） | 群 ID；`Group` 还带 `group_name/group_avatar/group_owner/group_admins/members/member_count` |
| `sender` | `MessageMember(user_id, nickname)` | 发送者；`nickname` 为群名片或昵称（aiocqhttp: `card or nickname`） |
| `message` | `list[BaseMessageComponent]` | **消息段链，必须保留结构** |
| `message_str` | `str` | 纯文本（由 Plain 拼接） |
| `raw_message` | `object` | 平台适配器原始对象（可能不可 JSON 序列化） |
| `timestamp` | `int`（秒） | 发送时间（可能为接收时兜底值） |

事件对象常用方法：`get_platform_name()`（`aiocqhttp` 等）、`get_platform_id()`（同类型多实例时唯一）、`get_group_id()`、`get_sender_id()`、`get_sender_name()`、`get_message_type()`、`unified_msg_origin`、`is_private_chat()`、`get_messages()`、`get_group(group_id)`（可取群成员列表，视平台支持）。

消息段组件（`astrbot/core/message/components.py`，Pydantic v2 模型）关键字段：

| 组件 | 关键字段 | 采集注意 |
| --- | --- | --- |
| `Plain` | `text` | 文本 |
| `At` | `qq`（`str`，`"all"` 表示全体） | @ 关系 |
| `AtAll` | 继承 `At`，`qq="all"` | @全体 |
| `Reply` | `id`（被引用消息 ID）、`sender_id`、`sender_nickname`、`time`、`message_str`、`chain` | **引用关系 + 被引用消息摘要，信息量大** |
| `Image` | `file`、`url`、`path` | 附件元数据 |
| `File` | `name`、`file`、`url` | 附件元数据 |
| `Record` / `Video` | `file` 等 | 附件元数据 |
| `Face` / `Poke` / `Json` / `Node` / `Nodes` / `Unknown` 等 | 各自字段 | 原样保留 |

**序列化陷阱（已实测确认，2026-11-04 / AstrBot 4.29.4）**：

1. 组件自带的 `toDict()` / `await to_dict()` 输出的是 **OneBot 风格精简结构**（`Reply.toDict()` 只保留 `{"type":"reply","data":{"id":...}}`，`Plain` 输出 `{"type":"text"}`），会**丢失** `Reply.sender_id`、`Image.url` 等字段 → 不可用于归档。
2. 消息段实际基于 **pydantic v1 兼容层**（`BaseMessageComponent.__mro__[1] = pydantic.v1.main.BaseModel`，pydantic 2.14.2 环境），因此**没有 `model_dump()`**，只有 v1 API `.dict()` / `.json()`。`.dict()` 会残留 `ComponentType` 枚举对象，不能直接 `json.dumps`。

**结论（采集序列化标准）**：

```python
payload_json = seg.json()   # 全量字段 + Enum 自动转字符串 + 可直接存 TEXT，实测输出示例：
# Reply: {"type":"Reply","id":"123","chain":[],"sender_id":456,"sender_nickname":"张三","time":...,"message_str":"hi",...}
# Plain: {"type":"Plain","text":"hello"}      At: {"type":"At","qq":999,"name":""}
# Image:{"type":"Image","file":"http://x/y.png","url":"","path":""}
```

兜底：`.json()` 失败时降级 `json.dumps(seg.dict(), ensure_ascii=False, default=str)`，仍失败则存 `{"type": <类名>, "raw": str(seg)}`，**绝不因单个消息段失败丢弃整条消息**。

（`BaseMessageComponent.__repr_args__` 只对 repr 做截断，不影响 `.json()` 全量输出。）

**根路径解析（已实测）**：`get_astrbot_root()` = 环境变量 `ASTRBOT_ROOT` > 桌面运行时 `~/.astrbot` > **当前工作目录**。即 `data/plugin_data/` 随 AstrBot 进程 cwd 变化；插件应在初始化时解析一次并缓存，单测通过 `ASTRBOT_ROOT` 固定路径。

### 2.3 监听器执行机制（决定"不阻塞主流程"的实现方式）

源码：`astrbot/core/pipeline/stage_order.py`、`waking_check/stage.py`、`process_stage/method/star_request.py`、`context_utils.call_handler`。

1. 管道阶段顺序：`WakingCheck → WhitelistCheck → SessionStatusCheck → RateLimit → ContentSafetyCheck → PreProcess → Process(插件/LLM) → ResultDecorate → Respond`。
2. handler 可以是**协程**（`async def`，返回 `None`）或**异步生成器**（`yield`）。协程返回非 `MessageEventResult` 值时，管道继续向后执行，不打断其他插件与 LLM。→ **采集器应写成普通 `async def`，绝不 yield/不设置返回结果。**
3. **关键副作用（必须实测验证）**：`WakingCheckStage` 中，只要某插件 listener 的 filter 全部通过，就会 `is_wake = True`，事件不再被 `stop_event()`。这意味着：装了本插件后，**所有群消息都会继续走完后续阶段**（白名单/限流/内容安全/预处理/插件处理）。
   - **不会**因此触发 LLM：LLM 调用条件是 `event.is_at_or_wake_command`（被 @、命中唤醒前缀、私聊等），仅 `is_wake` 不足以触发（`process_stage/stage.py` 末尾判断）。
   - 仍需在 Phase 1 实测确认：未 @ 机器人的普通群消息不会产生额外回复/额外 LLM 调用。
4. **异常风险**：插件 handler 抛异常时，AstrBot 会记录错误并向用户回一句报错，然后 `event.stop_event()`——**会中断该消息的正常处理链**。→ 采集器内部必须整体 `try/except`，任何异常只记日志，绝不外抛。
5. handler 排序：按 `priority` 降序（`sort(key=lambda h: -priority)`），默认 `0`。采集器建议 `priority=10` 以尽早入库（先于其他插件的处理），但不依赖此顺序保证正确性。
6. 事件类型过滤器：`EventMessageType = Flag(GROUP_MESSAGE | PRIVATE_MESSAGE | OTHER_MESSAGE)`，`ALL` 为三者并集；`OTHER_MESSAGE` 覆盖通知类事件。
7. 撤回/编辑等可观测事件：
   - **无统一的"消息撤回"事件类型**。aiocqhttp 适配器把 OneBot `notice` 转成 `AstrBotMessage`：`message_str=""`、`message=[]`、`message_id=uuid4().hex`、`raw_message=原始 notice dict`、`type` 按有无 `group_id` 判定（`aiocqhttp_platform_adapter.py:_convert_handle_notice_event`）。可从 `raw_message["notice_type"]` 识别 `group_recall/friend_recall/poke` 等。
   - 消息"编辑"：OneBot v11 无编辑事件；其他平台适配器未提供统一抽象。
   - → 设计：`messages.event_kind` 字段区分 `message/notice/unknown`，通知类事件按平台原始类型存储，**只保存可观测到的内容，不做推断**。

### 2.4 数据目录与存储 API

| 项 | 结论 |
| --- | --- |
| 插件专用数据目录 | `data/plugin_data/<plugin_name>/`，通过 `Path(get_astrbot_plugin_data_path()) / plugin_name` 或 `StarTools.get_data_dir()` 获取（文档明确要求大文件放这里，**不放插件目录**，避免升级覆盖） |
| 官方 KV 存储 | `await self.put_kv_data/get_kv_data/delete_kv_data`（≥4.9.2，按插件隔离）——只适合存小状态，**档案数据仍用自建 SQLite** |
| AstrBot 本体数据库 | `context.get_db()` 返回 `BaseDatabase`（会话/用户等）——**不复用**，符合"原始档案与本体数据分离"原则 |
| 依赖 | `aiosqlite>=0.21.0` 已在 AstrBot 依赖中（`pyproject.toml`），插件可直接使用但仍应写入 `requirements.txt` |

### 2.5 WebUI 扩展方式（Phase 5 关键依据）

AstrBot 提供**插件可视化视图**（`docs/zh/dev/star/guides/plugin-pages.md`，677 行完整规范）：

- 前端：插件目录 `views/<view_name>/index.html`（兼容 `pages/`），由 Dashboard 以**受限 iframe** 加载（`allow-scripts allow-forms allow-downloads`），不能触碰父页面/cookie/localStorage。
- 桥接：`window.AstrBotPluginView`（别名 `window.AstrBotPluginPage`），API：`ready()/getContext()/getLocale()/getI18n()/t()/onContext()`、`apiGet(endpoint, params)`、`apiPost(endpoint, body)`、`upload(endpoint, file)`、`download(endpoint, params, filename)`、`subscribeSSE(endpoint, handlers, params)` / `unsubscribeSSE(id)`。SDK 自动注入（`/api/plugin/page/bridge-sdk.js`）。
- 后端：`context.register_web_api(route, handler, methods, desc)`，`route` **必须以插件名作前缀**（`/{PLUGIN_NAME}/...`）；bridge 端点写相对路径（不带插件名），Dashboard 转发到 `/api/v1/plugins/extensions/<plugin_name>/<endpoint>`，动态片段 `<name>`、`<path:name>` 作为 handler 关键字参数。
- 请求/响应：统一用 `astrbot.api.web` 的 `request`（`method/path/username/headers/query.get/getlist/json/form/files/path_params/client_host`）与 `json_response/error_response/file_response/stream_response`。**不要**混用 Quart 的 `request`。
- 主题：`<html data-theme="light|dark">`，`bridge.getContext().isDark`，推荐 CSS 变量。
- 静态资源：相对路径即可；内容路径带**与 Dashboard 会话同寿命的 path token**，不要手工拼 token、不要 `..` 逃逸。

**鉴权（Phase 5 依赖此结论）**：插件扩展 API 全部经 `Depends(require_plugin_scope)`（`ScopeDependency("plugin")`，`astrbot/dashboard/api/auth.py`），要求 Dashboard 登录会话 JWT 或带 `plugin` scope 的 API Key。即：**WebUI 访问控制由 AstrBot Dashboard 提供，插件无需自建登录体系**；插件侧仍需做输入校验与权限范围内的数据隔离（不暴露 `/api` 之外的入口）。

### 2.6 其他可用能力

| 能力 | API | 备注 |
| --- | --- | --- |
| LLM 调用（复用已配置服务商） | `context.get_provider_by_id()`、`context.get_all_providers()`、`context.get_using_provider(umo)` | 配合 `_special: select_provider` |
| 定时任务 | **无官方插件调度 API**（`context.register_task` 已标 `[DEPRECATED]`；`context.cron_manager` 属于 AstrBot 自身任务体系，任务落 AstrBot DB） | → 分析批次调度采用**插件自建 asyncio 循环**，`terminate()` 时优雅关闭 |
| 消息发送 | `event.plain_result()` / `yield` / `event.send()` | 指令回复用 |
| 平台实例 | `self.context.get_platform_inst(platform_id)` | 可调协议端 API（如 `delete_msg`、取成员列表） |
| 温馨提示 | 不要用 `requests`，用 `httpx/aiohttp`；不要阻塞事件循环 | 官方开发原则 |

### 2.7 测试设施

- AstrBot 自带 pytest 测试体系（`pyproject.toml` 含 `pytest/pytest-asyncio/pytest-cov`，`tests/` 下 100+ 用例）。
- `tests/fixtures/` 提供：`plugins/fixture_plugin.py + metadata.yaml`（插件夹具）、`messages/test_messages.json`、`helpers.py`（平台配置工厂、mock 工具）。
- → 本插件采用：**自有 `tests/` 目录 + 合成 `AstrBotMessage/AstrMessageEvent` 的单元测试**；集成测试用本地 AstrBot 实例冒烟（见第 9 节）。

---

## 3. 关键风险与设计对策

| # | 风险 | 对策 |
| --- | --- | --- |
| R1 | listener 使所有消息变为 `is_wake`，可能引发额外副作用 | handler 用普通协程、不 yield、不设置结果；Phase 1 实测"未 @ 机器人的消息不产生回复/LLM 调用"；必要时在 README 提示用户 |
| R2 | handler 异常导致 `stop_event()`，影响机器人正常回复 | 采集入口整体 `try/except`，异常仅记插件日志并计数；写库走独立队列，写失败不影响事件返回 |
| R3 | 阻塞消息主流程 | listener 只做"规范化 + `put_nowait`"；有界队列（默认 10000）+ 后台 writer 批量事务写入；队列满时丢弃并记录 `dropped_count`（绝不 await 阻塞） |
| R4 | 数据库写并发 / 崩溃损坏 | SQLite `journal_mode=WAL`、`synchronous=NORMAL`、`busy_timeout=5000`、单写连接串行化、批量事务；启动时 `PRAGMA quick_check` + 未完成批次清理 |
| R5 | 消息段信息丢失 | 不用 `toDict()`，用 v1 API `.json()` 全量保存（`model_dump` 不存在，见 2.2）；失败降级 `dict()+default=str` 再降级 `str(seg)` |
| R6 | 去重（平台重发、重启重放） | `UNIQUE(platform_id, message_id)` + `INSERT OR IGNORE`；通知类事件 `message_id` 由适配器 `uuid4` 生成 → 用 `(platform_id, notice 事件指纹)` 辅助去重 |
| R7 | 长期数据量增长 | 合理索引（3 个核心索引，避免过度索引）、WAL、`PRAGMA optimize`、按需 `VACUUM`、可选按月归档（Phase 6 评估）、FTS5 搜索表（Phase 1 可选启用） |
| R8 | 附件体积膨胀 | 默认只存元数据（`file/url/name`），**不下载**；策略可配置：`none / metadata_only / download_if_small`（含大小上限、保留天数） |
| R9 | AI 推断污染原始数据 | AI 结果只写 `analysis_*` 表，**永不 UPDATE/DELETE `messages`**；所有推断带 `confidence + evidence_message_ids + model + prompt_version` |
| R10 | 隐私 | 群白名单默认为空（不采集任何群）、退出/删除指令、成员数据删除、WebUI 继承 Dashboard 鉴权、LLM 上送前脱敏策略、默认不启用 AI |
| R11 | 跨账号/跨群身份误合并 | 主键一律为 `(platform_id, user_id, group_id)` 三元组，**不做跨平台自然人合并**；合并只能人工确认（Phase 2+ 提供显式操作） |
| R12 | 插件异常拖垮 AstrBot | `terminate()` 优雅停机（flush 队列→关库）；所有后台 task 受控；错误隔离 |
| R13 | 数据目录随 cwd 漂移（`get_astrbot_root()` 默认取 cwd） | 初始化时解析并缓存绝对路径；单测/集成测试用 `ASTRBOT_ROOT` 显式固定 |

---

## 4. 插件总体架构

```
                       AstrBot 消息事件
                              │
              ┌───────────────▼────────────────┐
              │ capture.listener（async 协程）   │  ← @filter.event_message_type(ALL)
              │  · 白名单/隐私过滤               │     priority=10
              │  · 规范化 + 消息段序列化          │     全 try/except，绝不 yield
              │  · put_nowait(有界队列)          │
              └───────────────┬────────────────┘
                              │ asyncio.Queue
              ┌───────────────▼────────────────┐
              │ capture.writer（后台 task）      │
              │  · 批量取数（条数/时间双阈值）     │
              │  · 单事务写入 + 去重              │
              │  · 更新 groups/members/aliases   │
              └───────────────┬────────────────┘
                              │ aiosqlite (WAL)
              ┌───────────────▼────────────────┐
              │ storage: data/plugin_data/      │
              │          astrbot_plugin_groupscope/
              │          ├─ groupscope.db        │
              │          ├─ backups/            │
              │          └─ exports/            │
              └───────────────┬────────────────┘
                              │ 只读查询
        ┌─────────────────────┼──────────────────────┐
        │                     │                       │
  Phase2 派生统计        Phase3/4 分析批处理        Phase5 WebUI
  (成员/别名/互动)        (任务队列→LLM→校验→落库)    (views/ + register_web_api)
```

### 4.1 目录结构（Phase 1 落地版本）

```
astrbot_plugin_groupscope/
├─ metadata.yaml                 # 插件元数据（必需）
├─ _conf_schema.json             # WebUI 可视化配置
├─ requirements.txt              # aiosqlite / httpx（按阶段引入）
├─ main.py                       # Star 入口：生命周期、listener、指令、Web API
├─ README.md                     # 安装、配置、隐私说明
├─ docs/
│  ├─ phase0-technical-proposal.md   # 本文件
│  └─ (各阶段交付说明)
├─ groupscope/                   # 业务包（main.py 之外的代码）
│  ├─ __init__.py
│  ├─ config.py                  # 配置读取与校验（含默认值兜底）
│  ├─ paths.py                   # 数据目录解析
│  ├─ storage/
│  │  ├─ __init__.py
│  │  ├─ db.py                   # 连接管理：WAL/PRAGMA/事务/单写连接
│  │  ├─ migrations.py           # schema_version + 迁移链
│  │  ├─ schema_v1.sql           # Phase 1 DDL（内嵌资源）
│  │  └─ repo_messages.py        # 消息/消息段读写、分页查询
│  ├─ capture/
│  │  ├─ __init__.py
│  │  ├─ listener.py             # 事件 → NormalizedMessage（异常隔离）
│  │  ├─ serializer.py           # 消息段 seg.json() 全量序列化 + At/Reply 抽取
│  │  ├─ queue.py                # 有界队列 + writer 批处理循环
│  │  └─ dedup.py                # 去重键生成
│  ├─ privacy/
│  │  ├─ __init__.py
│  │  └─ policy.py               # 白名单、排除名单、删除请求（Phase 1 骨架）
│  └─ ops/
│     ├─ __init__.py
│     ├─ backup.py               # VACUUM INTO 备份 / 恢复
│     └─ export.py               # JSON/CSV 导出（Phase 1 支持 messages）
├─ views/                        # Phase 5 才创建（插件可视化视图）
└─ tests/
   ├─ conftest.py                # 合成 AstrBotMessage/AstrMessageEvent 夹具
   ├─ test_serializer.py
   ├─ test_storage.py
   └─ test_capture_pipeline.py
```

### 4.2 配置项草案（`_conf_schema.json`，Phase 1 版本）

```jsonc
{
  "capture_enabled":        { "type": "bool",   "default": true,  "description": "总开关" },
  "group_whitelist":        { "type": "list",   "default": [],    "description": "采集群白名单（格式 平台名:群ID；留空=不采集）" },
  "group_blacklist":        { "type": "list",   "default": [],    "description": "黑名单优先于白名单" },
  "excluded_users":         { "type": "list",   "default": [],    "description": "排除的发送者（平台名:用户ID）" },
  "record_notices":         { "type": "bool",   "default": true,  "description": "记录撤回/戳一戳等可观测通知事件" },
  "attachment_policy":      { "type": "options", "options": ["none","metadata_only","download_if_small"],
                              "default": "metadata_only", "description": "附件策略：不记录/仅元数据/小文件下载" },
  "attachment_max_bytes":   { "type": "int",    "default": 2097152, "description": "download_if_small 的大小上限" },
  "batch_max_rows":         { "type": "int",    "default": 100,   "description": "写库批处理行数阈值" },
  "batch_max_interval_ms":  { "type": "int",    "default": 500,   "description": "写库批处理时间阈值(ms)" },
  "queue_maxsize":          { "type": "int",    "default": 10000, "description": "内存队列上限，满则丢弃并计数" },
  "backup_auto":            { "type": "bool",   "default": false, "description": "自动每日备份" },
  "backup_keep":            { "type": "int",    "default": 7,     "description": "保留备份数量" },
  "log_retention_days":     { "type": "int",    "default": 0,     "description": "原始消息保留天数，0=永久" }
}
```

（`ai_analysis`、`llm_*`、`desensitize` 等配置项**不在 Phase 1 的 schema 中**，随 Phase 3/6 引入，避免出现"未实现但已暴露"的假配置。）

---

## 5. 数据存储设计

### 5.1 存储技术选型

- **SQLite 单文件**，`data/plugin_data/astrbot_plugin_groupscope/groupscope.db`
- 驱动：`aiosqlite`（AstrBot 已依赖），插件内**单写连接 + 串行写入**；查询复用同一连接（asyncio 天然串行），Phase 6 视负载引入独立只读连接
- PRAGMA：`journal_mode=WAL`、`synchronous=NORMAL`、`busy_timeout=5000`、`foreign_keys=ON`、`temp_store=MEMORY`
- 事务：批量写入使用单事务（`BEGIN IMMEDIATE` … `COMMIT`），异常回滚
- 迁移：`schema_migrations(version, applied_at)` 表 + 内嵌迁移脚本链，启动时自动按序升级；**只增不改**，破坏性变更走新版本迁移

### 5.2 Phase 1 数据库结构（将随 Phase 1 交付实现）

```sql
-- 迁移记录
CREATE TABLE schema_migrations (
  version     INTEGER PRIMARY KEY,
  applied_at  INTEGER NOT NULL,
  note        TEXT
);

-- 群组
CREATE TABLE groups (
  id              INTEGER PRIMARY KEY,
  platform_id     TEXT NOT NULL,      -- 平台适配器实例 id（同类型多实例唯一）
  platform_name   TEXT NOT NULL,      -- aiocqhttp / telegram / ...
  group_id        TEXT NOT NULL,
  group_name      TEXT,               -- 可获取时记录（含历史值见 group_names）
  first_seen      INTEGER NOT NULL,   -- unix 秒
  last_seen       INTEGER NOT NULL,
  member_count    INTEGER,
  capture_enabled INTEGER NOT NULL DEFAULT 1,
  meta_json       TEXT,               -- 平台附加信息
  UNIQUE (platform_id, group_id)
);
CREATE INDEX idx_groups_last_seen ON groups(last_seen);

-- 成员（平台内身份，绝不跨平台合并）
CREATE TABLE members (
  id                INTEGER PRIMARY KEY,
  platform_id       TEXT NOT NULL,
  user_id           TEXT NOT NULL,
  current_nickname  TEXT,             -- 最近一次观测到的昵称/群名片
  first_seen        INTEGER NOT NULL,
  last_seen         INTEGER NOT NULL,
  UNIQUE (platform_id, user_id)
);

-- 成员在各群中的表现（同一个人在不同群是不同"档案单元"）
CREATE TABLE member_group (
  id              INTEGER PRIMARY KEY,
  member_id       INTEGER NOT NULL REFERENCES members(id) ON DELETE CASCADE,
  group_id        INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
  current_nick    TEXT,               -- 该群内最近昵称
  first_seen      INTEGER NOT NULL,
  last_seen       INTEGER NOT NULL,
  message_count   INTEGER NOT NULL DEFAULT 0,
  UNIQUE (member_id, group_id)
);

-- 历史昵称/群名片（member_aliases 可能与之合并，Phase 2 定稿）
CREATE TABLE member_aliases (
  id          INTEGER PRIMARY KEY,
  member_id   INTEGER NOT NULL REFERENCES members(id) ON DELETE CASCADE,
  alias       TEXT NOT NULL,
  source      TEXT NOT NULL,          -- platform_nickname / group_card / manual
  first_seen  INTEGER NOT NULL,
  last_seen   INTEGER NOT NULL,
  seen_count  INTEGER NOT NULL DEFAULT 1,
  UNIQUE (member_id, alias, source)
);

-- 原始消息（只追加，AI 永不修改）
CREATE TABLE messages (
  id                  INTEGER PRIMARY KEY,
  platform_id         TEXT NOT NULL,
  platform_name       TEXT NOT NULL,
  group_ref           INTEGER REFERENCES groups(id),      -- 私聊为 NULL
  group_raw_id        TEXT,
  message_id          TEXT NOT NULL,   -- 平台消息 id
  dedup_key           TEXT NOT NULL,   -- 见 5.3
  sender_ref          INTEGER REFERENCES members(id),
  sender_raw_id       TEXT,            -- 兜底，避免成员表异常时丢数据
  sender_raw_name     TEXT,
  event_kind          TEXT NOT NULL DEFAULT 'message',   -- message / notice / unknown
  notice_type         TEXT,            -- group_recall / friend_recall / poke / ...
  sent_at             INTEGER NOT NULL, -- 消息时间戳（秒）
  received_at         INTEGER NOT NULL, -- 插件接收时间（秒）
  message_type        TEXT NOT NULL,    -- group / friend / other
  text                TEXT,            -- message_str（搜索用）
  reply_to_message_id TEXT,            -- Reply 段抽取
  reply_to_sender_id  TEXT,            -- Reply 段抽取（平台提供时）
  raw_json            TEXT,            -- raw_message 可序列化部分（失败则 NULL）
  inserted_at         INTEGER NOT NULL
);
CREATE UNIQUE INDEX ux_messages_dedup ON messages(dedup_key);
CREATE INDEX idx_messages_group_time  ON messages(group_ref, sent_at);
CREATE INDEX idx_messages_sender_time ON messages(sender_ref, sent_at);
CREATE INDEX idx_messages_reply       ON messages(reply_to_message_id) WHERE reply_to_message_id IS NOT NULL;

-- 消息段（保留完整结构，避免只存纯文本）
CREATE TABLE message_segments (
  id            INTEGER PRIMARY KEY,
  message_ref   INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  seg_index     INTEGER NOT NULL,
  seg_type      TEXT NOT NULL,        -- Plain / At / Reply / Image / Face / ...
  payload_json  TEXT NOT NULL,        -- seg.json() 全量（pydantic v1 API，含 Enum 转字符串）
  text          TEXT,                 -- Plain.text 或图片 file 名等，便于搜索
  UNIQUE (message_ref, seg_index)
);
CREATE INDEX idx_segments_type ON message_segments(seg_type);

-- @ 关系（从 At 段确定性抽取，非推断）
CREATE TABLE message_mentions (
  id            INTEGER PRIMARY KEY,
  message_ref   INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  kind          TEXT NOT NULL,        -- at / at_all
  target_raw_id TEXT,                 -- At.qq（at_all 为 "all"）
  target_ref    INTEGER REFERENCES members(id),
  UNIQUE (message_ref, kind, target_raw_id)
);
CREATE INDEX idx_mentions_target ON message_mentions(target_raw_id);

-- 插件运行状态（队列丢弃计数、上次写入时间、版本等）
CREATE TABLE plugin_state (
  key        TEXT PRIMARY KEY,
  value      TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);
```

> **刻意不在 Phase 1 创建的表**（避免"建了不用"的死结构）：`addressing`、`interactions`、`topics`、`events`、`event_evidence`、`analysis_tasks`、`analysis_results`、`relationship_snapshots`。它们将在 Phase 2/3/4/6 通过 `schema_v2/v3...` 迁移创建，届时给出完整 DDL。

### 5.3 去重键设计

```
普通消息：  dedup_key = "m:{platform_id}:{message_id}"
通知事件：  dedup_key = "n:{platform_id}:{group_raw_id}:{notice_type}:{raw指纹}"
            raw指纹 = sha1(sorted_json(raw_message 中除时间外的关键字段)) 兜底
```

`INSERT OR IGNORE INTO messages ...` 保证幂等；重启/重放不会产生重复行。通知事件的 `message_id` 是适配器随机 UUID，**不能**作为去重键。

### 5.4 备份 / 恢复 / 导出

| 能力 | 方案 | 阶段 |
| --- | --- | --- |
| 在线备份 | `VACUUM INTO '<backups>/groupscope-YYYYMMDD-HHMMSS.db'`（原子、不阻塞读者） | Phase 1 |
| 自动备份 | 配置开关 + 每日一次（后台循环），按 `backup_keep` 轮转 | Phase 1 |
| 恢复 | 停止 writer → 校验 `PRAGMA quick_check` → 替换主库 → 重启加载 | Phase 1 |
| 导出 | 流式导出 `messages/message_segments` 为 **JSON / CSV**（迭代器分页，避免整表进内存） | Phase 1 |
| 一致性自检 | 启动时 `PRAGMA quick_check`、孤儿段清理（`ON DELETE CASCADE` + 补扫） | Phase 1 |
| 数据库迁移 | `schema_migrations` 版本链，启动自动升级 | Phase 1（框架）/ Phase 6（演练） |

---

## 6. 隐私与安全设计（贯穿各阶段）

1. **默认不采集**：`group_whitelist` 默认为空 → 插件装上后不记录任何群，需显式添加。
2. **可告知**：提供 `/gs status` 指令返回当前采集范围；README 附"告知群成员"文案模板。
3. **可停止**：`/gs pause <群>`（对应 `groups.capture_enabled=0`）、`/gs resume`、总开关 `capture_enabled`。
4. **可删除**：
   - `/gs forget_user <平台>:<用户>`：删除该成员在库中的消息、段、@ 记录并写入 tombstone（记录"已删除"，不保留内容）。
   - `/gs purge_group <群>`：清空该群全部档案。
   - WebUI（Phase 5）提供同样能力的入口，均走 Dashboard 鉴权。
5. **访问控制**：WebUI 复用 AstrBot Dashboard 登录与 `plugin` scope；插件**不开放**任何绕过 Dashboard 的端口/路由；导出文件带随机文件名 + 过期清理。
6. **LLM 边界**（Phase 3 起生效）：AI 分析默认关闭；启用前需明确配置；上送前执行脱敏（可配置：隐藏用户 ID、昵称掩码、自定义正则）；原始消息永不因 AI 失败而丢失；AI 结论一律带置信度与证据，展示时标注"AI 推断，非事实"。
7. **不做隐蔽监控**：不提供跨群自动采集、不提供对未启用群的记录、不向第三方发送数据。

---

## 7. 分阶段实施计划（各阶段交付检查表）

> 每阶段结束按交付要求输出：修改文件列表 / 功能说明 / 数据库结构 / 安装配置步骤 / 测试方法 / 实测结果 / 未实现清单。

### Phase 1 — 消息档案（无 AI）

- 任务：
  1. 插件骨架：`metadata.yaml`、`main.py`（`Star`、`terminate`、`on_astrbot_loaded`）、`_conf_schema.json`、`requirements.txt`、`README.md`
  2. SQLite 存储层：连接/PRAGMA/事务/迁移框架/schema v1
  3. 采集链路：listener → 规范化 → 有界队列 → 批量 writer → 去重落库（含消息段、@、Reply 抽取、通知事件）
  4. 基本查询指令：`/gs stats`、`/gs recent [群] [条数]`、`/gs search <关键词>`、`/gs pause|resume`、`/gs status`
  5. 备份/恢复/导出：`/gs backup`、`/gs export <json|csv>`、恢复说明
- 数据库：`schema_v1`（第 5.2 节全部表）
- 测试方法：
  1. pytest 单测：合成事件 → 断言 `messages/message_segments/message_mentions` 行数与字段；重复事件 → 断言去重；异常事件（`raw_message` 不可序列化、`sender=None`）→ 断言不抛出
  2. 集成冒烟：本地 AstrBot 实例注入群消息 → 校验入库、未 @ 机器人时不产生回复
  3. 崩溃恢复：写入过程中 `kill -9` → 重启后 `quick_check` 通过、无半截批次
  4. 性能：脚本注入 1 万条消息 → 事件循环延迟、写入耗时、DB 体积
- 未实现（届时会明确列出）：人物档案、称呼识别、互动统计、话题/事件、WebUI、AI 分析

### Phase 2 — 人物与关系

成员档案查询、历史昵称、@/回复/引用三类确定性互动表（`interactions`）、按时间窗口（日/周/月/季）的互动统计、`relationship_snapshots` 快照、身份不合并原则与人工合并入口。

### Phase 3 — 称呼与语义分析

`addressing` 表、称呼候选的规则层抽取（昵称/别名/@上下文）+ LLM 消歧层、置信度与证据链、异步批处理任务队列（`analysis_tasks`）、**复用 AstrBot 已配置 Provider（`_special: select_provider`）+ 插件侧并发/频率限流**、提示词版本化、重新分析历史数据。

### Phase 4 — 群体记忆

`topics` / `events` / `event_evidence`、梗的首现-传播-演变追踪（依赖 Phase 3 的称呼与主题能力）、事件时间线与证据回溯。

### Phase 5 — WebUI

`views/<name>/index.html` + `context.register_web_api` 全套接口：总览、关系网络（Cytoscape.js）、人物档案、聊天档案检索、群体记忆；全部走 Dashboard 鉴权；从分析结论跳转原消息。

### Phase 6 — 长期运行优化

历史重新分析、调度与并发/限流控制、月度归档与性能优化、迁移演练、完整备份恢复演练、压力与稳定性测试。

---

## 8. 决策记录（用户已确认，2026-11-04）

| 决策项 | 结论 | 对设计的影响 |
| --- | --- | --- |
| 本机安装 AstrBot 做集成测试 | **允许**（`uv tool install astrbot --python 3.12` 后台执行中） | Phase 1 必须完成真实实例冒烟测试与唤醒副作用验证 |
| 目标平台 | **QQ（aiocqhttp/NapCat）为主 + 多平台通用降级** | 采集层按平台能力降级：QQ 下采集 notice 类撤回/戳一戳、`sender.card` 群名片、`At/Reply/Face` 段；其他平台字段缺失即留空，不做推断 |
| WebUI 形态 | **AstrBot 插件可视化视图**（`views/` + `register_web_api`） | 不开独立端口、不自建登录，全部走 Dashboard 鉴权；独立 Web 服务**不实现** |
| LLM 接入（Phase 3 起） | **只复用 AstrBot 已配置 Provider** | 通过 `_special: select_provider` 选择服务商；分析并发/频率控制在插件侧自行限流；**不实现**独立 base_url/api_key 配置项 |

---

## 9. 安装与配置步骤（Phase 1 完成后生效；当前尚未实现）

```bash
# 1) 准备 AstrBot（本机实测路径）
uv tool install astrbot --python 3.12
astrbot init    # 首次
astrbot run

# 2) 安装插件（开发期软链）
mkdir -p <astrbot>/data/plugins
ln -s /home/alhsk/projects/astrbot_plugin_groupscope <astrbot>/data/plugins/astrbot_plugin_groupscope
# WebUI → 插件 → 重载插件

# 3) 配置
# WebUI → 插件 → GroupScope → 填写 group_whitelist（格式 aiocqhttp:123456789）
# 或直接编辑 data/config/astrbot_plugin_groupscope_config.json 后重载

# 4) 验证
# 群内发消息 → /gs stats
```

---

## 10. Phase 0 实测记录（本阶段实际执行的验证）

| # | 操作 | 结果 |
| --- | --- | --- |
| 1 | 搜索本机 AstrBot 安装（pip/pipx/find/docker） | 未发现，环境为零 |
| 2 | `git clone --depth 1 https://github.com/AstrBotDevs/AstrBot` | 成功，`version = 4.29.4`（`pyproject.toml`、`__version__` 双重确认） |
| 3 | 通读 `docs/zh/dev/star/*`（plugin-new / listen-message-event / plugin-config / plugin-pages / storage / simple / other / plugin-publish，合计约 2000 行） | 成功，规范已提取到第 2 节 |
| 4 | 源码核对：`AstrBotMessage/MessageMember/Group` 字段 | 与文档一致，补充 `Group` 含 `members/member_count` |
| 5 | 源码核对：消息段组件字段与 `toDict()` 语义 | **发现序列化降级问题**（见 2.2），改用 `model_dump` |
| 6 | 源码核对：管道阶段顺序与 `WakingCheck` 唤醒判定 | **发现 listener 会置 `is_wake=True`**（风险 R1），LLM 触发条件为 `is_at_or_wake_command` |
| 7 | 源码核对：handler 返回值/异常处理（`call_handler`、`star_request`） | 确认协程 + 全捕获策略（风险 R2） |
| 8 | 源码核对：aiocqhttp notice → 撤回事件可达性 | 确认可通过 `raw_message` 观测撤回/戳一戳（无统一事件类型） |
| 9 | 源码核对：WebUI 插件视图与鉴权链路 | 确认 `register_web_api` + `require_plugin_scope`（Dashboard JWT / API Key） |
| 10 | 源码核对：插件数据目录、配置落盘、KV 存储、生命周期 | 确认 `data/plugin_data/<name>/`、`data/config/<name>_config.json`、`terminate()` |
| 11 | `uv tool install astrbot --python 3.12` | **成功**：`astrbot version 4.29.4`，CLI 可用（`init/run/conf/password/plug`），安装于 `~/.local/share/uv/tools/astrbot` |
| 12 | 安装环境冒烟：导入 `astrbot.api.event/star/web`、消息组件、`AstrBotMessage/MessageMember/Group` | **成功**（pydantic 2.14.2 环境下的 `pydantic.v1` 兼容层） |
| 13 | 消息段序列化实测 | `Reply/Plain/At/Image` 的 `.json()` 全量输出正常；**确认 `model_dump` 不存在、`toDict()` 丢字段** → 2.2 节序列化标准据此修正 |
| 14 | 路径解析实测（`ASTRBOT_ROOT` 注入） | `get_astrbot_root()` = `ASTRBOT_ROOT` > 桌面运行时 `~/.astrbot` > **cwd**；`data/plugin_data`、`data/config` 跟随根路径 |

**Phase 0 尚未进行的验证**（留待 Phase 1）：插件 listener 实际唤醒副作用的运行时验证、真实平台消息采集、SQLite 长期写入性能。

---

## 11. 本阶段明确未实现的功能

- 无任何插件业务代码（未创建 `main.py` / `groupscope/` / 测试代码）。
- 数据库未创建（DDL 仅为设计）。
- WebUI、AI 分析、人物/称呼/事件功能全部未实现。
- 本机 AstrBot 实例已安装（4.29.4），但**尚未 `astrbot init` / `astrbot run`**，未进行任何运行时消息采集验证。
