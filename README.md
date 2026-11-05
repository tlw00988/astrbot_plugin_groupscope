# GroupScope — 群聊观察与数字档案（AstrBot 插件）

长期、可靠地记录群聊原始消息与群体互动，为人物关系、称呼体系、群体文化、共同记忆的回溯分析打基础。

> 当前版本 **0.1.0**，功能开发按 Phase 1→6 推进。本 README 随阶段推进持续更新；
> 各阶段的完整交付报告（含实测结果）见 `docs/`。

## 设计原则

1. **原始消息可靠保存**：去重、批量事务、WAL、在线备份、导出。
2. **原始与 AI 分析分离**：原始消息表只追加，AI 结论写入独立表，且必须带消息证据。
3. **分析保留证据**：每条推断都可回跳到原消息上下文。
4. **AI 推断非事实**：所有分析结果带置信度，与观察事实严格区分。
5. **可重分析**：提示词版本化，历史数据可按新版本重新分析。
6. **关系有向、时间性、不确定**：`interactions` 记录方向与时间窗口，不把消息数当亲密度。
7. **本地轻量存储**：SQLite（WAL），不引入 Neo4j / PostgreSQL / Redis。
8. **长期运行**：有界队列、保留期清理、自动备份轮转、可恢复停机。
9. **隐私**：白名单默认为空（装上即不采集）、可暂停/删除、WebUI 复用 Dashboard 鉴权、附件默认不落盘、不默认上送第三方。
10. **不改 AstrBot 本体**：数据全部在插件 data 目录，配置走 AstrBot 管理界面。

## 安装

```bash
# 1) 准备 AstrBot（本机实测路径）
uv tool install astrbot --python 3.12   # 或按官方文档安装

# 2) 安装插件（开发期用软链）
ln -s /path/to/astrbot_plugin_groupscope ~/.astrbot/data/plugins/astrbot_plugin_groupscope
# 正式安装：WebUI → 插件 → 从 GitHub 安装

# 3) WebUI → 插件 → GroupScope → 配置 → 填写 group_whitelist → 重载插件
```

数据目录：`<ASTRBOT_ROOT>/data/plugin_data/astrbot_plugin_groupscope/`

- `groupscope.db` — 主数据库（WAL）
- `backups/` — 备份（`VACUUM INTO` 一致性副本，自动轮转）
- `exports/` — 导出产物

## 配置（`_conf_schema.json`）

| 配置项 | 默认 | 说明 |
| --- | --- | --- |
| `capture_enabled` | `true` | 总开关，关闭后保留历史、停止记录 |
| `group_whitelist` | `[]` | **留空 = 不采集任何群**。格式 `平台:群ID` 或仅 `群ID` |
| `group_blacklist` | `[]` | 黑名单优先于白名单 |
| `excluded_users` | `[]` | 排除的发送者 |
| `record_notices` | `true` | 记录撤回/戳一戳等平台可观测通知（不做推断） |
| `attachment_policy` | `metadata_only` | `none` / `metadata_only` / `download_if_small` |
| `batch_max_rows` / `batch_max_interval_ms` | `100` / `500` | 写库批量阈值 |
| `queue_maxsize` | `10000` | 有界队列，满则丢弃并计数（绝不阻塞消息主流程） |
| `log_retention_days` | `0` | 原始消息保留天数，`0` = 永久 |
| `backup_auto` / `backup_keep` | `false` / `7` | 每日自动备份与轮转 |
| `log_detail` | `false` | 采集详细日志（排障用） |
| `analysis_enabled` | `true` | AI 分析总开关（称呼/话题/事件；规则层始终零成本运行） |
| `llm_provider_id` | 空 | 分析使用的对话模型，留空 = 自动取第一个可用 Provider（**不存 API Key**） |
| `analysis_concurrency` | `1` | LLM 并发上限 |
| `analysis_interval_ms` | `1500` | LLM 最小调用间隔（毫秒） |
| `analysis_daily_limit` | `200` | 每日 LLM 调用上限（耗尽后当天只跑规则层） |
| `analysis_window_hours` | `24` | 每次分析的回溯窗口（小时） |
| `analysis_batch_size` | `3` | 分析循环单次处理任务数 |

## 指令（均需管理员权限）

| 指令 | 说明 |
| --- | --- |
| `/gs status` | 采集状态与运行指标（队列/丢弃/写入/库体积） |
| `/gs stats` | 消息、成员、群、近 7 日消息统计 |
| `/gs recent [群ID] [条数]` | 查看最近消息 |
| `/gs search <关键词> [群ID]` | 关键词搜索 |
| `/gs pause <群ID>` / `/gs resume <群ID>` | 暂停/恢复某群采集 |
| `/gs backup` / `/gs backups` / `/gs restore <文件名>` | 备份、列出、恢复 |
| `/gs export <json\|csv> [群ID]` | 导出消息档案（含消息段） |
| `/gs reload` | 重新读取配置（无需重载插件） |
| `/gs member <成员ID\|昵称>` | 成员档案（别名史/互动/最近消息）（Phase 2） |
| `/gs relations [群ID] [day\|week\|month\|quarter]` | 有向关系快照（Phase 2） |
| `/gs merge <主ID> <被合并ID>` | 人工合并重复成员（Phase 2） |
| `/gs snapshot [群ID] [窗口]` | 立即计算关系快照（Phase 2） |
| `/gs address [群ID] [关键词]` | 称呼体系查询，标注来源与置信度（Phase 3） |
| `/gs analyze <群ID> [rule\|llm\|topic\|event\|all]` | 立即入队分析（Phase 3/4） |
| `/gs tasks` | 分析任务队列与 LLM 限流状态（Phase 3） |
| `/gs reanalyze <群ID> [rule\|llm\|topic\|event\|all]` | 按当前提示词版本重新分析（Phase 3/4） |
| `/gs alias <成员ID> <称呼> [群ID]` | 人工指定称呼（`manual`，优先级最高）（Phase 3） |
| `/gs topics` / `/gs topic 1 <ID>` | 话题列表 / 话题证据消息（Phase 4） |
| `/gs events` / `/gs event 1 <ID>` | 事件与梗 / 首现→传播证据链（Phase 4） |
| `/gs timeline [群ID] [条数]` | 群体记忆时间线（Phase 4） |

## WebUI（Phase 5，复用 AstrBot Dashboard）

视图放在插件 `views/` 目录，由 Dashboard 以 iframe 加载；入口：**WebUI → 插件 → GroupScope → 详情页 → 打开视图**。
鉴权完全复用 Dashboard（JWT / plugin-scope API Key），不自建账号体系。

| 视图 | 内容 |
| --- | --- |
| `overview` 总览 | 采集/AI 开关、消息量与 7 日柱状图、群列表、分析管线与限流状态 |
| `relations` 关系网络 | 有向互动力导向图（@/回复/引用为边，线宽=证据量），**不含亲密度评价** |
| `members` 人物档案 | 身份、别名史、被称呼记录、有向互动、最近消息，可点开原消息与完整消息段 |
| `search` 消息检索 | 检索原始存档，查看前后 5 条上下文与完整消息段 |
| `memory` 群体记忆 | 话题 / 事件与梗 / 时间线 / 称呼体系，全部带证据并可跳回原消息 |

后端接口经 `context.register_web_api` 注册在 `/api/v1/plugins/extensions/astrbot_plugin_groupscope/...`，
只读（删除类操作只走管理员指令）；视图元数据在 `.astrbot-plugin/i18n/zh-CN.json`。

## 隐私

- **默认不采集**：白名单为空时插件不记录任何群消息。
- 采集范围、排除名单全部在本地配置，不上传。
- 附件默认只存元数据，不下载内容；内联 base64 内容在入库前被洗除。
- WebUI 复用 AstrBot Dashboard 的 JWT / plugin-scope API Key 鉴权，不自建账号体系。
- LLM 分析（Phase 3 起）只复用 AstrBot 已配置的 Provider，不新增 base_url/api_key 配置。

## 开发与测试

```bash
uv venv .venv --python 3.12
uv pip install --python .venv/bin/python astrbot pytest pytest-timeout
.venv/bin/python -m pytest tests -q        # 单测
```

集成测试与压测方法见各阶段交付报告（`docs/`）。

## 许可

MIT
