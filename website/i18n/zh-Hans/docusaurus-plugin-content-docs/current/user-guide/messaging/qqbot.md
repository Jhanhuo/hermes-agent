# QQ Bot

本页的 Python 依赖命令使用 [PM 准备的源码环境](../../reference/package-management.md#developer-workflow)。
依赖变更后，请重新激活该 checkout 并重启 Hermes。

通过**官方 QQ Bot API（v2）**将 Hermes 接入 QQ——支持私聊（C2C）、群消息（含 @提及）、频道及直接消息，并具备语音转写功能。

## 概述

QQ Bot 适配器使用[官方 QQ Bot API](https://bot.q.qq.com/wiki/develop/api-v2/) 实现以下功能：

- 通过持久 **WebSocket** 连接至 QQ Gateway（网关）接收消息
- 通过 **REST API** 发送文本和 Markdown 回复
- 下载并处理图片、语音消息及文件附件
- 使用腾讯内置 ASR 或可配置的 STT（语音转文字）提供商转写语音消息

## 前提条件

1. **QQ Bot 应用** — 在 [q.qq.com](https://q.qq.com) 注册：
   - 创建新应用并记录您的 **App ID** 和 **App Secret**
   - 启用所需 intent（意图）：C2C 消息、群组 @-消息、频道消息
   - 在沙盒模式下配置机器人以进行测试，或发布至生产环境

2. **依赖项** — 适配器需要 `aiohttp` 和 `httpx`：
   ```bash
   python -c "import pm; pm.sync_venv(['messaging'], explicit=True)"
   ```

## 配置

### 交互式设置

```bash
hermes gateway setup
```

从平台列表中选择 **QQ Bot** 并按提示操作。

### 手动配置

在 `~/.hermes/.env` 中设置所需环境变量：

```bash
QQ_APP_ID=your-app-id
QQ_CLIENT_SECRET=your-app-secret
```

## 环境变量

| 变量 | 描述 | 默认值 |
|---|---|---|
| `QQ_APP_ID` | QQ Bot App ID（必填） | — |
| `QQ_CLIENT_SECRET` | QQ Bot App Secret（必填） | — |
| `QQBOT_HOME_CHANNEL` | 用于 cron/通知投递的 OpenID | — |
| `QQBOT_HOME_CHANNEL_NAME` | 主频道显示名称 | `Home` |
| `QQ_ALLOWED_USERS` | 允许私聊访问的用户 OpenID 列表（逗号分隔） | 开放（所有用户） |
| `QQ_GROUP_ALLOWED_USERS` | 允许群组访问的群组 OpenID 列表（逗号分隔） | — |
| `QQ_ALLOW_ALL_USERS` | 设为 `true` 以允许所有私聊 | `false` |
| `QQ_PORTAL_HOST` | 覆盖 QQ portal 主机（沙盒路由设为 `sandbox.q.qq.com`） | `q.qq.com` |
| `QQ_STT_API_KEY` | 语音转文字提供商的 API 密钥 | — |
| `QQ_STT_BASE_URL` | （不直接读取——请在 `config.yaml` 中设置 `platforms.qqbot.extra.stt.baseUrl`） | n/a |
| `QQ_STT_MODEL` | STT 模型名称 | `glm-asr` |

## 高级配置

如需精细控制，可在 `~/.hermes/config.yaml` 中添加平台设置：

```yaml
platforms:
  qqbot:
    enabled: true
    extra:
      app_id: "your-app-id"
      client_secret: "your-secret"
      markdown_support: true       # enable QQ markdown (msg_type 2). Config-only; no env-var equivalent.
      dm_policy: "open"          # open | allowlist | disabled
      allow_from:
        - "user_openid_1"
      group_policy: "open"       # open | allowlist | disabled
      group_allow_from:
        - "group_openid_1"
      stt:
        provider: "zai"          # zai (GLM-ASR), openai (Whisper), etc.
        baseUrl: "https://open.bigmodel.cn/api/coding/paas/v4"
        apiKey: "your-stt-key"
        model: "glm-asr"
```

## 可选：群文本旁听观察

保持 `platforms.qqbot.require_mention: true`，另显式设置
`platforms.qqbot.extra.observe_group_messages: true`，即可通过既有
`gateway_platform_event` 发布 `event_type: group_message`、
`payload.schema_version: qq.group_message.v1` 的纯文本观察事件。默认关闭，只接受布尔值。
这不让普通消息进入模型回合，也不下载其附件；没有网关回调则不观察。

适配器先执行群 ACL，网关按接收端策略授权，再在路由后的目标 profile 查找与调用插件，
包括 secondary Bot 共享给其他 profile 的路由。载荷只含精确群/成员/消息 ID、最多 2000 字符的正文、
`is_bot`、`is_mention` 等固定字段，不含原始扩展、附件字段或 SDK 对象。
观察者仍须单独显式同意保存，并检查自己的群白名单。

观察发生在正式回合去重与仅 @ 丢弃之前，所以 full/AT 副本都可能被观察；消费者应按
`(chat_id, message_id)` 去重。旁听 full 不占用后到 @ 的正式回合槽。
观察者返回值不会开启回合、发送消息或授予权限。QQ 群文本观察者在事件循环之外由单个 FIFO worker 执行，
每个 runner 最多排队 32 条，满额丢弃最新观察，不影响正式消息准入或网关心跳；每条工作单独保留授权后的目标 profile 上下文。
其他平台观察事件保持原有执行语义。没有伪装成能强杀线程的超时：卡死回调占据唯一 worker，后续溢出会丢弃。
停机丢弃待执行观察，已运行线程可能完成本次写入。旁听是 best-effort，可能晚于正式回复、在过载或重启时缺失；
异常记录但不改变正式回合的 @ 门控。持久化群正文前须取得群同意；开关不代替 QQ 平台权限。

## 全量群消息

适配器接收 QQ 实际投递的 `GROUP_MESSAGE_CREATE`（普通群消息）和
`GROUP_AT_MESSAGE_CREATE`（群内 @机器人消息）。普通消息投递需要 QQ 为应用授予并启用相应权限；
Hermes 不能代为开通。本次不改变现有 WebSocket intent 位；若 QQ 仅允许应用接收 @消息，
增加事件处理器并不会解除平台限制。

两类事件共用 `group_policy` / `group_allow_from` 群 ACL、消息 ID 去重、附件/语音转写及引用消息管线。
普通消息不会删除开头对其他用户的 @提及。已确认寻址且已知 `READY.user.id` 时，两类事件都会
精确清理开头属于本机器人的 `<@id>` / `<@!id>`，使其后的 slash 命令（例如 `/qqsilent off`）
不因 full/AT 先后顺序而失去识别；不会删除其他用户的 ID 提及，也不单凭正文推断身份。
@事件另外保留原有的 `@name` 前缀清理。
群和成员 OpenID 保持字符串；如有昵称则保留至 `source.user_name`；原始消息、引用及扩展字段
仍在 `event.raw_message` 中。`author.bot` 经 `source.is_bot` 交由现有网关机器人准入及循环保护判断，
不会在适配器中无差别丢弃所有其他机器人的消息。

| 字段 | `GROUP_AT_MESSAGE_CREATE` | `GROUP_MESSAGE_CREATE` |
|---|---|---|
| `metadata.qq_event_type` | 原始事件名称 | 原始事件名称 |
| `metadata.is_mention` | `true` | `mentions[].id` 精确匹配本机器人 `READY.user.id` 时为 `true` |
| `reply_expected` | `true` | 与 `metadata.is_mention` 相同 |

提及他人或其他机器人不表示提及本机器人；适配器使用 READY 提供的身份，不猜测 App ID 等于成员 OpenID，也不通过昵称推断。尚未收到自身身份时，不把全量事件推断为提及本机器人。
`reply_expected` 是寻址信号，**本身不会跳过模型回合**。原生的仅 @群消息策略请配置：

```yaml
platforms:
  qqbot:
    require_mention: true
```

也可放在 `platforms.qqbot.extra` 内。该值必须是 YAML 布尔值，字符串 `"false"`、`null` 等无效类型会使适配器配置报错。默认 `false` 保留全量群消息能力。设置 `true` 后，`QQAdapter.handle_message` 在 Base 适配器的空闲/忙碌分流、排队、steer 及控制命令旁路之前丢弃所有 `reply_expected is not True` 的 QQ 群消息，且不更新用于 typing 的最后消息 ID 缓存。此策略只管 QQ 群，不扩展到同样使用 `source.chat_type="group"` 的 guild 频道：既有频道 full/@接收和私聊行为不变。获准进入的消息仍需通过正常认证及机器人准入。

### 同 ID 的 full/@双投递及限制

- `require_mention: true`：未寻址或无法证实寻址的 full 事件，在附件/STT 处理和占用去重 ID
  **之前**丢弃，后续同 ID 的 @事件仍能执行。已寻址 full 或 @事件在附件异步等待前同步占用
  同一去重槽；两种到达顺序都只接收第一个已寻址副本，保留该副本的规范化事件和原始 payload。
- 默认 `false`：立即交付，**同 ID 首个副本胜出**，不按事件类型拆分去重 key，不补发第二个模型回合，
  也不事后合并正文或 metadata。若无法证实寻址的 full 先到，其 `metadata.is_mention` 和
  `reply_expected` 保持 `false`，稍后到达的 @副本不会升级它们。上表描述各副本独立处理时的值；
  被去重的副本不会到达 hooks。full 缺少寻址证据时，其中的 self-mention 命令也可能仍作为正文。
- 这是“全量即时交付且避免重复回合”的明确取舍，不保证任意晚到 @信息的无损升级。去重仅在当前进程内，
  TTL 为 300 秒，最多保留 1,000 个 ID；过期、容量淘汰、重启、处理失败或后续 ACL 拒绝时，
  不提供全局 exactly-once 保证。本次不引入等待 timer、跨事件业务历史或重放。

需要认证后处理的插件可使用 `post_gateway_admission`，它只接收 `reply_expected` 标量快照，不接收 QQ 原始消息或凭据。该 hook 仅覆盖新回合，忙碌路径可能绕过网关 hooks，因此它不能单独保证仅 @策略。不要把需要权限保障的消费逻辑放在认证前的 `pre_gateway_dispatch`。

## 语音消息（STT）

语音转写分两个阶段进行：

1. **QQ 内置 ASR**（免费，始终优先尝试）——QQ 在语音消息附件中提供 `asr_refer_text`，使用腾讯自有语音识别
2. **已配置的 STT 提供商**（备用）——若 QQ 的 ASR 未返回文本，适配器将调用兼容 OpenAI 的 STT API：

   - **智谱/GLM（zai）**：默认提供商，使用 `glm-asr` 模型
   - **OpenAI Whisper**：设置 `QQ_STT_BASE_URL` 和 `QQ_STT_MODEL`
   - 任何兼容 OpenAI 的 STT 端点

## 故障排查

### 机器人立即断开连接（快速断连）

通常原因如下：
- **App ID / Secret 无效** — 在 q.qq.com 仔细核对您的凭据
- **缺少权限** — 确保机器人已启用所需 intent
- **仅限沙盒的机器人** — 若机器人处于沙盒模式，只能接收来自 QQ 沙盒测试频道的消息

### 语音消息未被转写

1. 检查附件数据中是否存在 QQ 内置的 `asr_refer_text`
2. 若使用自定义 STT 提供商，验证 `QQ_STT_API_KEY` 是否正确设置
3. 查看 gateway 日志中的 STT 错误信息

### 消息未送达

- 在 q.qq.com 验证机器人的 **intent** 是否已启用
- 若私聊访问受限，检查 `QQ_ALLOWED_USERS`
- 普通群消息需确认 QQ 已授予应用全量群事件投递权限，否则需 **@提及**机器人；两类消息均受群白名单/策略约束。
- 检查 `QQBOT_HOME_CHANNEL` 以确认 cron/通知投递配置

### 连接错误

- 确保已安装 `aiohttp` 和 `httpx`：`python -c "import pm; pm.sync_venv(['messaging'], explicit=True)"`
- 检查与 `api.sgroup.qq.com` 及 WebSocket gateway 的网络连通性
- 查看 gateway 日志以获取详细错误信息和重连行为