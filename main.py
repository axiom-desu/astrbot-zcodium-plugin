"""ZCodium bridge plugin for AstrBot.

把聊天消息转发给 ZCodium 的 bots bridge（loopback WebSocket，协议 v2），
把 agent 输出流式回发。AstrBot 只做传输层，不走自带 LLM。

协议见 ZCodium 仓库 `.agents/specs/bots-astrbot-bridge.md`。
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import AsyncGenerator, Optional

import aiohttp

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Plain
from astrbot.api.star import Context, Star, register

PLUGIN_NAME = "astrbot_plugin_zcodium"
PROTOCOL_VERSION = 2
RECONNECT_DELAY_SECONDS = 3.0


def _frame(kind: str, **fields) -> dict:
    return {"v": PROTOCOL_VERSION, "kind": kind, "id": uuid.uuid4().hex, **fields}


def _payload_to_text(payload: dict) -> str:
    """把 ZCodium delivery payload 渲染成平台文本。"""
    kind = payload.get("type")
    if kind == "text":
        return str(payload.get("text") or "")
    if kind == "tool":
        title = payload.get("title") or payload.get("toolId") or "tool"
        return f"🔧 {title} [{payload.get('status')}]"
    if kind == "changes":
        files = payload.get("files") or []
        lines = [f"变更：{payload.get('fileCount', 0)} 个文件"]
        for item in files[:10]:
            lines.append(
                f"- {item.get('path')} (+{item.get('additions')}/-{item.get('deletions')})"
            )
        return "\n".join(lines)
    if kind == "notice":
        return str(payload.get("message") or "")
    if kind == "selection":
        # selection 自带 canonical 文本（选项 + 对应命令），直接打印。
        return str(payload.get("text") or payload.get("title") or "")
    return ""


@register(
    PLUGIN_NAME,
    "moyamryia",
    "ZCodium bridge：把消息交给 ZCodium agent 并流式回复；AstrBot 仅作传输层。",
    "0.1.0",
)
class ZCodiumBridgePlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.bridge_url = str(config.get("bridge_url") or "").strip()
        self.bridge_token = str(config.get("bridge_token") or "").strip()
        self.channels = [
            str(channel).strip() for channel in (config.get("channels") or []) if str(channel).strip()
        ]
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._connect_task: Optional[asyncio.Task] = None
        self._send_lock = asyncio.Lock()
        self._inflight: dict[str, asyncio.Queue] = {}
        self._streams: dict[str, asyncio.Queue] = {}

    # ---------- 生命周期 ----------

    async def initialize(self) -> None:
        if not self.bridge_url:
            logger.warning("[zcodium] bridge_url 未配置，插件不会连接。")
            return
        self._connect_task = asyncio.create_task(self._connect_loop())
        logger.info("[zcodium] bridge 客户端已启动 url=%s", self.bridge_url)

    async def terminate(self) -> None:
        if self._connect_task:
            self._connect_task.cancel()
            self._connect_task = None
        self._flush_all("插件已停止。")

    # ---------- 连接 ----------

    async def _connect_loop(self) -> None:
        while True:
            try:
                headers = {}
                if self.bridge_token:
                    headers["Authorization"] = f"Bearer {self.bridge_token}"
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(
                        self.bridge_url, headers=headers, heartbeat=30
                    ) as ws:
                        self._ws = ws
                        await self._send_frame(
                            _frame(
                                "hello",
                                clientId=PLUGIN_NAME,
                                clientVersion="0.1.0",
                                channels=self.channels,
                            )
                        )
                        logger.info("[zcodium] bridge 已连接")
                        async for message in ws:
                            if message.type == aiohttp.WSMsgType.TEXT:
                                await self._handle_frame(json.loads(message.data))
                            elif message.type in (
                                aiohttp.WSMsgType.CLOSED,
                                aiohttp.WSMsgType.ERROR,
                            ):
                                break
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001 - 连接异常需要重连
                logger.warning("[zcodium] bridge 连接异常: %s", error)
            finally:
                self._ws = None
                self._flush_all("bridge 连接已断开，请稍后重试。")
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)

    async def _send_frame(self, frame: dict) -> None:
        ws = self._ws
        if ws is None or ws.closed:
            raise RuntimeError("bridge 未连接")
        async with self._send_lock:
            await ws.send_json(frame)

    async def _handle_frame(self, frame: dict) -> None:
        kind = frame.get("kind")
        if kind == "welcome":
            logger.info("[zcodium] bridge 握手完成 enabled=%s", frame.get("enabled"))
            return
        if kind == "accepted":
            stream_id = frame.get("streamId")
            queue = self._inflight.pop(frame.get("inReplyTo") or "", None)
            if stream_id and queue is not None:
                self._streams[stream_id] = queue
            return
        if kind == "delivery":
            queue = self._streams.get(frame.get("streamId"))
            if queue is not None:
                text = _payload_to_text(frame.get("payload") or {})
                if text:
                    await queue.put(MessageChain([Plain(text)]))
            await self._ack(frame.get("id"))
            return
        if kind == "status":
            queue = self._streams.pop(frame.get("streamId"), None)
            if queue is not None:
                message = frame.get("message")
                if message:
                    await queue.put(MessageChain([Plain(message)]))
                await queue.put(None)
            return
        if kind == "error":
            queue = self._inflight.pop(frame.get("inReplyTo") or "", None)
            if queue is None and len(self._streams) == 1:
                queue = next(iter(self._streams.values()))
            if queue is not None:
                await queue.put(MessageChain([Plain(f"ZCodium 错误：{frame.get('message')}")]))
                await queue.put(None)
            return

    async def _ack(self, delivery_id: Optional[str]) -> None:
        if not delivery_id:
            return
        try:
            await self._send_frame(_frame("ack", inReplyTo=delivery_id, ok=True))
        except Exception:  # noqa: BLE001 - ack 失败不影响渲染
            pass

    def _flush_all(self, reason: str) -> None:
        for queue in list(self._inflight.values()) + list(self._streams.values()):
            queue.put_nowait(MessageChain([Plain(reason)]))
            queue.put_nowait(None)
        self._inflight.clear()
        self._streams.clear()

    # ---------- 事件 ----------

    def _is_allowed(self, event: AstrMessageEvent) -> bool:
        return not self.channels or event.get_platform_name() in self.channels

    def _actor(self, event: AstrMessageEvent) -> dict:
        platform = event.get_platform_name()
        chat_type = "private" if event.is_private_chat() else "group"
        # ZCodium 侧 channel 统一记为 astrbot；真实平台由插件用 id 前缀隔离，保证跨平台唯一。
        actor = {
            "channel": "astrbot",
            "externalUserId": f"{platform}:{event.get_sender_id() or event.get_session_id()}",
            "chatType": chat_type,
        }
        if chat_type == "group" and event.get_group_id():
            actor["chatId"] = f"{platform}:{event.get_group_id()}"
        return actor

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_message(self, event: AstrMessageEvent):
        if not self._is_allowed(event):
            return
        text = (event.message_str or "").strip()
        if not text:
            return
        # 命中：不要让 AstrBot 自带 LLM 也参与。
        event.should_call_llm(False)
        if self._ws is None or self._ws.closed:
            await event.send(MessageChain([Plain("ZCodium bridge 未连接。")]))
            return

        queue: asyncio.Queue = asyncio.Queue()
        command_id = uuid.uuid4().hex
        request = _frame(
            "command",
            commandId=command_id,
            actor=self._actor(event),
            command={"type": "prompt", "text": text},
        )
        # 服务端 accepted.inReplyTo 用的是帧 id（而非 commandId），必须以帧 id 登记 inflight。
        self._inflight[request["id"]] = queue
        try:
            await self._send_frame(request)
        except Exception as error:  # noqa: BLE001 - 发送失败需要回执
            self._inflight.pop(request["id"], None)
            await event.send(MessageChain([Plain(f"发送到 ZCodium 失败：{error}")]))
            return

        supports_streaming = bool(
            getattr(getattr(event, "platform_meta", None), "support_streaming_message", True)
        )
        if supports_streaming:
            await event.send_streaming(self._stream(queue))
            return
        chain = MessageChain()
        async for item in self._stream(queue):
            chain.chain.extend(item.chain)
        if chain.chain:
            await event.send(chain)

    async def _stream(self, queue: asyncio.Queue) -> AsyncGenerator[MessageChain, None]:
        while True:
            item = await queue.get()
            if item is None:
                return
            yield item