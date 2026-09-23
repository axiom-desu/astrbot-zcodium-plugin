# astrbot_plugin_zcodium

把 AstrBot 收到的聊天消息转发给 **ZCodium** 的 bots bridge，并把 agent 输出流式回发。
AstrBot 只做平台传输层，不走自带 LLM。

## 连接

ZCodium 桌面端启动时会在本机 loopback 起 WebSocket 服务，并写一个运行时文件：

```
<ZCodium 数据目录>/bots-bridge.runtime.v2.json
{ "url": "ws://127.0.0.1:<port>/bots/bridge/v2", "port": 12345, "token": "..." }
```

把 `url` 和 `token` 填到本插件的 `bridge_url` / `bridge_token`。

## 工作方式

```
平台消息 → 本插件 → bridge{command/prompt} → ZCodium 解析并驱动 agent
ZCodium → bridge{delivery/status} → 本插件 → event.send_streaming(...)
```

- 用户文本原样透传，ZCodium 集中解析 `/new`、`/stop`、`/permission`、`/elicitation` 等命令。
- `text` / `tool` / `changes` / `notice` / `selection` 都渲染成平台文本。
- 权限/提问用**文本命令**回答（协议里 selection 自带 canonical 文本与对应命令）。
- 平台名填到 `channels` 里才生效；留空表示所有平台。

## 依赖

只依赖 AstrBot 自带的 `aiohttp`。