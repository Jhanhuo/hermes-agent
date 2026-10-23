# QQ Bot

Python dependency commands on this page use a
[PM-prepared source checkout](../../reference/package-management.md#developer-workflow).
After a dependency change, reactivate the checkout and restart Hermes.

Connect Hermes to QQ via the **Official QQ Bot API (v2)** — supporting private (C2C), group messages (including @-mentions), guild, and direct messages with voice transcription.

## Overview

The QQ Bot adapter uses the [Official QQ Bot API](https://bot.q.qq.com/wiki/develop/api-v2/) to:

- Receive messages via a persistent **WebSocket** connection to the QQ Gateway
- Send text and markdown replies via the **REST API**
- Download and process images, voice messages, and file attachments
- Transcribe voice messages using Tencent's built-in ASR or a configurable STT provider

## Prerequisites

1. **QQ Bot Application** — Register at [q.qq.com](https://q.qq.com):
   - Create a new application and note your **App ID** and **App Secret**
   - Enable the required intents: C2C messages, Group @-messages, Guild messages
   - Configure your bot in sandbox mode for testing, or publish for production

2. **Dependencies** — The adapter requires `aiohttp` and `httpx`:
   ```bash
   python -c "import pm; pm.sync_venv(['messaging'], explicit=True)"
   ```

## Configuration

### Interactive setup

```bash
hermes gateway setup
```

Select **QQ Bot** from the platform list and follow the prompts.

### Manual configuration

Set the required environment variables in `~/.hermes/.env`:

```bash
QQ_APP_ID=your-app-id
QQ_CLIENT_SECRET=your-app-secret
```

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| `QQ_APP_ID` | QQ Bot App ID (required) | — |
| `QQ_CLIENT_SECRET` | QQ Bot App Secret (required) | — |
| `QQBOT_HOME_CHANNEL` | OpenID for cron/notification delivery | — |
| `QQBOT_HOME_CHANNEL_NAME` | Display name for home channel | `Home` |
| `QQ_ALLOWED_USERS` | Comma-separated user OpenIDs for DM access | open (all users) |
| `QQ_GROUP_ALLOWED_USERS` | Comma-separated group OpenIDs for group access | — |
| `QQ_ALLOW_ALL_USERS` | Set to `true` to allow all DMs | `false` |
| `QQ_PORTAL_HOST` | Override the QQ portal host (set to `sandbox.q.qq.com` for sandbox routing) | `q.qq.com` |
| `QQ_STT_API_KEY` | API key for voice-to-text provider | — |
| `QQ_STT_BASE_URL` | (Not read directly — set `platforms.qqbot.extra.stt.baseUrl` in `config.yaml` instead) | n/a |
| `QQ_STT_MODEL` | STT model name | `glm-asr` |

## Advanced Configuration

For fine-grained control, add platform settings to `~/.hermes/config.yaml`:

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
        timeout: 60              # seconds per transcription request (default 60)
```

## Optional passive group text observation

Keep `platforms.qqbot.require_mention: true` and explicitly set
`platforms.qqbot.extra.observe_group_messages: true` to expose a bounded,
text-only `gateway_platform_event` envelope (`event_type: group_message`,
`payload.schema_version: qq.group_message.v1`). Default is `false`; only
literal booleans are accepted. It does not enable model turns for ordinary
messages or download their attachments. No gateway callback means no observation.

The adapter first enforces its group ACL; the runner then authorizes the source
under the receiving transport's policy before looking up hooks in the routed
runtime profile (including a secondary bot serving a shared-profile route).
The payload includes exact group/member/message IDs, `text` capped at 2000
characters, `is_bot`, and `is_mention`, not raw extensions, attachment fields,
or SDK objects. Observers must opt in separately to storage and apply their
own allowed-group policy. Bot-authored input still carries its source identity.

Observation precedes turn deduplication and mention-only rejection, so full/AT
duplicates may both be observed. Consumers deduplicate by `(chat_id, message_id)`;
observing a full copy never reserves the later addressed copy's normal-turn slot.
The event is read-only: callback results do not start turns, send replies or grant
permissions. QQ group-message callbacks run off the event loop in a single FIFO
worker per runner, with 32 pending observations and drop-newest overflow. Each
queued item captures its authorized runtime context; this queue never drives
message turns. Other platform-event types keep their existing dispatch behavior.
Slow/locked observers do not hold normal QQ message admission or gateway heartbeats.
There is no fake thread-kill timeout: a hung callback occupies the sole worker;
subsequent overflow is discarded. Shutdown drops pending work; an already running
thread may complete its current write. Storage is best-effort and may lag a reply
or miss observations during overload/restart. Observer errors are logged without
changing the normal-turn mention gate. Obtain consent before persisting group
text; these options do not grant QQ permissions.

## Full group messages

The adapter accepts both `GROUP_MESSAGE_CREATE` (ordinary group messages) and
`GROUP_AT_MESSAGE_CREATE` (group messages addressed to the bot) when QQ delivers
them. Receiving ordinary messages requires the corresponding permissions/event
delivery to be enabled for your application at QQ; Hermes cannot grant them.
The existing WebSocket intent selection is unchanged. If QQ only delivers
@-messages for your application, adding this handler does not unlock full-group access.

Both events use the same group ACL (`group_policy` / `group_allow_from`), message-ID
deduplication, attachment/STT and quoted-message pipeline. Full events retain
leading mentions of other users. When addressing is proven and `READY.user.id` is
known, both event types remove only an exact leading `<@id>` / `<@!id>` token for
this bot, so a following slash command (for example `/qqsilent off`) is recognized
regardless of which copy arrives first. Other users' ID tokens are retained; no
identity is inferred from text alone. The @-event also retains its legacy `@name`
prefix cleanup. Group and member OpenIDs remain strings. Supplied display names are kept
in `source.user_name`; the original payload, references and extension fields remain
in `event.raw_message`. `author.bot` is passed as `source.is_bot` for the gateway's
existing bot-admission/loop controls, not used to blanket-drop other bots.

Normalized group events expose:

| Field | `GROUP_AT_MESSAGE_CREATE` | `GROUP_MESSAGE_CREATE` |
|---|---|---|
| `metadata.qq_event_type` | Original event name | Original event name |
| `metadata.is_mention` | `true` | `true` only when `mentions[].id` matches this bot's `READY.user.id` |
| `reply_expected` | `true` | Same addressing result as `metadata.is_mention` |

Mentions of other users/bots do not imply a mention of this bot. Hermes learns its
user ID from `READY`; it does not equate the application's App ID with a member
OpenID or infer identity from a nickname. Without a known identity, a full event
is not assumed to address this bot.
`reply_expected` alone does not suppress agent turns. For native mention-only group intake, set:

```yaml
platforms:
  qqbot:
    require_mention: true
```

The same key is accepted under `platforms.qqbot.extra`. It must be a YAML boolean; invalid types (including quoted `"false"` and `null`) reject adapter configuration. The default is `false`, preserving full-group intake. With `true`, `QQAdapter.handle_message` drops QQ group events unless `reply_expected is True`, **before** Base adapter idle/busy dispatch, queuing, steering, or control-command bypass. Dropped events do not replace the cached last inbound message ID used for typing. This policy applies to QQ groups, not guild channels (which also use `source.chat_type="group"` internally): existing guild full/@ intake and DMs are unchanged. Admitted events still pass normal authorization and bot-admission controls.

### Same-ID full/@ delivery and limits

- With `require_mention: true`, an unaddressed or unproven full event is dropped
  **before** media/STT work and ID reservation. A later same-ID @-event can run.
  An addressed full event or @-event reserves the shared ID synchronously before
  attachment awaits; the other copy is suppressed in either order. The first
  addressed copy supplies the normalized event and its original raw payload.
- With the default `false`, delivery is immediate and **first copy wins** for a
  shared ID, regardless of event type. No second agent turn is emitted and there
  is no later metadata/body merge. If an unproven full copy wins, its
  `metadata.is_mention` and `reply_expected` stay `false` even if an @-copy arrives
  later. The table above describes each copy in isolation; a suppressed copy
  never reaches hooks or upgrades a previously delivered event. A self-mention
  command whose full copy lacks proof of addressing may likewise remain text.
- This deliberately favors immediate full intake without duplicate turns, not
  globally lossless addressing upgrades. Deduplication is process-local, expires
  after 300 seconds, and retains at most 1,000 IDs. Expiry, eviction, restart,
  failed processing, or downstream ACL rejection are not exactly-once delivery
  guarantees. No waiting timer, cross-event history, or replay is added.

Plugins needing post-authorization processing can use [`post_gateway_admission`](../features/hooks.md#post_gateway_admission), which receives a scalar `reply_expected` snapshot, not the raw QQ payload. That hook runs only on the new-turn lane and cannot enforce mention-only intake on its own: busy-session messages can bypass gateway hooks entirely. Do not implement permission-sensitive consumption in `pre_gateway_dispatch`, which runs before authorization.

## Voice Messages (STT)

Voice transcription works in two stages:

1. **QQ built-in ASR** (free, always tried first) — QQ provides `asr_refer_text` in voice message attachments, which uses Tencent's own speech recognition
2. **Configured STT provider** (fallback) — If QQ's ASR doesn't return text, the adapter calls an OpenAI-compatible STT API:

   - **Zhipu/GLM (zai)**: Default provider, uses `glm-asr` model
   - **OpenAI Whisper**: Set `QQ_STT_BASE_URL` and `QQ_STT_MODEL`
   - Any OpenAI-compatible STT endpoint

## Troubleshooting

### Bot disconnects immediately (quick disconnect)

This usually means:
- **Invalid App ID / Secret** — Double-check your credentials at q.qq.com
- **Missing permissions** — Ensure the bot has the required intents enabled
- **Sandbox-only bot** — If the bot is in sandbox mode, it can only receive messages from QQ's sandbox test channel

### Voice messages not transcribed

1. Check if QQ's built-in `asr_refer_text` is present in the attachment data
2. If using a custom STT provider, verify `QQ_STT_API_KEY` is set correctly
3. Check gateway logs for STT error messages

### Messages not delivered

- Verify the bot's **intents** are enabled at q.qq.com
- Check `QQ_ALLOWED_USERS` if DM access is restricted
- For ordinary group messages, check that QQ grants your application full-group event delivery; otherwise **@mention** the bot. Both event types still obey the group allowlist/policy.
- Check `QQBOT_HOME_CHANNEL` for cron/notification delivery

### Connection errors

- Ensure `aiohttp` and `httpx` are installed: `python -c "import pm; pm.sync_venv(['messaging'], explicit=True)"`
- Check network connectivity to `api.sgroup.qq.com` and the WebSocket gateway
- Review gateway logs for detailed error messages and reconnect behavior
