"""
Max Messenger platform adapter for Hermes Gateway.

Uses maxapi library (https://github.com/love-apples/maxapi) for API calls.
maxapi handles SSL with Russian Trusted CA bundle (Минцифры), retry logic,
file uploads, and typed responses out of the box.

Features:
- Webhook mode (full message payloads including voice/audio)
- Forwarded message support (link.type=forward)
- Burst-merge for rapid follow-up messages
- Inline keyboard for approval prompts (dangerous commands)
- Inline keyboard for clarify prompts (multiple-choice questions)
- Message editing (remove buttons after callback)
- File/image/video/voice upload and sending via maxapi
- Media download for STT/vision pipeline (media_urls + media_types)
- Per-platform busy_text_mode and debounce configuration

API docs: https://dev.max.ru/docs-api
Base URL: https://platform-api2.max.ru (handled by maxapi internally)
"""

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

from aiohttp import web

from maxapi import Bot
from maxapi.enums.parse_mode import TextFormat
from maxapi.enums.upload_type import UploadType
from maxapi.exceptions.max import InvalidToken, MaxApiError, MaxConnection
from maxapi.types.attachments.attachment import Attachment, AttachmentType, ButtonsPayload
from maxapi.types.attachments.buttons.attachment_button import AttachmentButton
from maxapi.types.attachments.buttons.callback_button import CallbackButton
from maxapi.types.attachments.upload import AttachmentUpload
from maxapi.types.input_media import InputMedia

from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.config import Platform, PlatformConfig

logger = logging.getLogger(__name__)

DEFAULT_WEBHOOK_HOST = "0.0.0.0"
DEFAULT_WEBHOOK_PORT = 8088
DEFAULT_WEBHOOK_PATH = "/max/webhook"


class MaxAdapter(BasePlatformAdapter):
    """Webhook-based adapter for Max Messenger using maxapi library."""

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform("max"))
        extra = config.extra or {}
        self.token = os.getenv("MAX_BOT_TOKEN") or getattr(config, "token", None) or extra.get("token", "")
        self._bot: Optional[Bot] = None
        self._webhook_runner: Optional[web.AppRunner] = None
        self._known_message_ids: set = set()  # Dedup
        # Burst-merge: when Max sends forwarded content as a separate update
        # right after a text message, glue them together before dispatching.
        self._burst_window: float = extra.get("burst_merge_seconds", 2.0)
        self._last_msg: Optional[dict] = None  # {chat_id, text, ts, task}
        # Approval state: approval_id → session_key
        self._approval_state: dict = {}
        self._approval_counter = 0
        # Clarify state: clarify_id → session_key
        self._clarify_state: dict = {}
        # Webhook config
        self._webhook_host = str(extra.get("webhook_host", DEFAULT_WEBHOOK_HOST))
        self._webhook_port = int(extra.get("webhook_port", DEFAULT_WEBHOOK_PORT))
        self._webhook_path = extra.get("webhook_path", DEFAULT_WEBHOOK_PATH)
        # Public URL for Max API subscription — can be set via env or extra
        self._webhook_url = os.getenv("MAX_WEBHOOK_URL") or extra.get("webhook_url", "")
        self._webhook_secret = os.getenv("MAX_WEBHOOK_SECRET") or extra.get("webhook_secret", "")

    # ── connection ──────────────────────────────────────────────

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not self.token:
            logger.error("Max: MAX_BOT_TOKEN not set")
            return False

        self._bot = Bot(token=self.token, auto_requests=True)

        # Verify token
        try:
            data = await self._bot.get_updates(limit=1, timeout=5)
            marker = data.get("marker", "?")
            logger.info(f"Max: token OK (marker={marker})")
        except InvalidToken:
            logger.error("Max: invalid token")
            await self._bot.close_session()
            self._bot = None
            return False
        except (MaxApiError, MaxConnection, Exception) as e:
            logger.error(f"Max: token check failed: {e}")
            await self._bot.close_session()
            self._bot = None
            return False

        # Delete any existing webhook subscriptions so long polling is clean
        try:
            await self._bot.delete_webhook()
        except Exception:
            pass

        # Start webhook HTTP server
        app = web.Application()
        app.router.add_post(self._webhook_path, self._handle_webhook)
        app.router.add_get(self._webhook_path, self._handle_health)
        self._webhook_runner = web.AppRunner(app)
        await self._webhook_runner.setup()
        site = web.TCPSite(self._webhook_runner, self._webhook_host, self._webhook_port)
        await site.start()
        logger.info(f"Max: webhook server on {self._webhook_host}:{self._webhook_port}{self._webhook_path}")

        # Subscribe webhook with Max API
        if self._webhook_url:
            try:
                await self._bot.subscribe_webhook(
                    url=self._webhook_url,
                    secret=self._webhook_secret or None,
                )
                logger.info(f"Max: subscribed webhook → {self._webhook_url}")
            except Exception as e:
                logger.error(f"Max: failed to subscribe webhook: {e}")
        else:
            logger.warning("Max: MAX_WEBHOOK_URL not set — webhook server running but not subscribed with Max API")

        self._mark_connected()
        return True

    async def disconnect(self) -> None:
        # Unsubscribe webhook
        if self._bot and self._webhook_url:
            try:
                await self._bot.delete_webhook()
            except Exception:
                pass

        # Stop webhook server
        if self._webhook_runner:
            try:
                await self._webhook_runner.cleanup()
            except Exception:
                pass
            self._webhook_runner = None

        if self._bot:
            await self._bot.close_session()
            self._bot = None
        self._mark_disconnected()

    # ── webhook handler ─────────────────────────────────────────

    async def _handle_health(self, request: "web.Request") -> "web.Response":
        """GET handler — simple health check."""
        return web.Response(text="ok")

    async def _handle_webhook(self, request: "web.Request") -> "web.Response":
        """POST handler — receive webhook payload from Max API."""
        try:
            payload = await request.json()
            logger.info(f"Max: webhook payload: {json.dumps(payload, ensure_ascii=False)[:500]}")
            # Process asynchronously so we return 200 immediately
            asyncio.create_task(self._handle_update(payload))
            return web.Response(status=200)
        except Exception as e:
            logger.error(f"Max: webhook handler error: {e}")
            return web.Response(status=200)  # Always 200 so Max doesn't retry

    # ── message sending ─────────────────────────────────────────

    async def send(self, chat_id, content, reply_to=None, metadata=None) -> SendResult:
        if not self._bot:
            return SendResult(success=False, error="Not connected")

        if not content:
            return SendResult(success=False, error="Empty content")

        text = str(content)[:4000]

        try:
            result = await self._bot.send_message(
                chat_id=int(chat_id),
                text=text,
                format=TextFormat.MARKDOWN,
            )
            if result and result.message and result.message.body:
                msg_id = str(result.message.body.mid)
                return SendResult(success=True, message_id=msg_id)
        except (MaxApiError, MaxConnection) as e:
            logger.warning(f"Max send error: {e}")
        except Exception as e:
            logger.warning(f"Max send unexpected error: {e}")

        return SendResult(success=False, error="API returned empty response")

    async def send_exec_approval(
        self, chat_id: str, command: str, session_key: str,
        description: str = "dangerous command",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send an inline-keyboard approval prompt with interactive buttons.

        Buttons use Max callback type with payload format ``ea:<choice>:<id>``.
        The callback handler in _handle_update resolves via
        resolve_gateway_approval().
        """
        if not self._bot:
            return SendResult(success=False, error="Not connected")

        self._approval_counter += 1
        approval_id = self._approval_counter

        cmd_preview = command[:3800] + "..." if len(command) > 3800 else command
        text = (
            f"⚠️ **Требуется подтверждение команды**\n\n"
            f"```\n{cmd_preview}\n```\n"
            f"Причина: {description}"
        )

        buttons_row1 = [
            CallbackButton(text="✅ Один раз", payload=f"ea:once:{approval_id}"),
            CallbackButton(text="✅ Сессия", payload=f"ea:session:{approval_id}"),
        ]
        buttons_row2 = [
            CallbackButton(text="✅ Всегда", payload=f"ea:always:{approval_id}"),
            CallbackButton(text="❌ Отклонить", payload=f"ea:deny:{approval_id}"),
        ]

        keyboard = AttachmentButton(
            type=AttachmentType.INLINE_KEYBOARD,
            payload=ButtonsPayload(buttons=[buttons_row1, buttons_row2]),
        )

        try:
            result = await self._bot.send_message(
                chat_id=int(chat_id),
                text=text,
                format=TextFormat.MARKDOWN,
                attachments=[keyboard],
            )
            if result and result.message and result.message.body:
                msg_id = str(result.message.body.mid)
                self._approval_state[approval_id] = {
                    "session_key": session_key,
                    "message_id": msg_id,
                }
                logger.info(f"Max: sent approval prompt id={approval_id} session={session_key}")
                return SendResult(success=True, message_id=msg_id)
        except (MaxApiError, MaxConnection) as e:
            logger.warning(f"Max send approval error: {e}")
        except Exception as e:
            logger.warning(f"Max send approval unexpected error: {e}")

        return SendResult(success=False, error="API returned empty response")

    async def send_clarify(
        self,
        chat_id: str,
        question: str,
        choices: Optional[list],
        clarify_id: str,
        session_key: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Render a clarify prompt with inline buttons (one per choice)."""
        if not self._bot:
            return SendResult(success=False, error="Not connected")

        if not choices:
            # Open-ended: plain text, gateway intercepts next message
            from tools.clarify_gateway import mark_awaiting_text
            mark_awaiting_text(clarify_id)
            return await self.send(chat_id=chat_id, content=f"❓ {question}")

        # Build message text with numbered options
        option_lines = "\n".join(
            f"{i + 1}. {c}" for i, c in enumerate(choices)
        )
        text = f"❓ {question}\n\n{option_lines}"

        # Build inline keyboard: one button per choice + "Other"
        button_rows = []
        for idx, choice in enumerate(choices):
            label = str(choice)[:50]  # Max button text limit
            button_rows.append([
                CallbackButton(text=label, payload=f"cl:{clarify_id}:{idx}")
            ])
        button_rows.append([
            CallbackButton(text="✏️ Свой ответ", payload=f"cl:{clarify_id}:other")
        ])

        keyboard = AttachmentButton(
            type=AttachmentType.INLINE_KEYBOARD,
            payload=ButtonsPayload(buttons=button_rows),
        )

        try:
            result = await self._bot.send_message(
                chat_id=int(chat_id),
                text=text,
                format=TextFormat.MARKDOWN,
                attachments=[keyboard],
            )
            if result and result.message and result.message.body:
                msg_id = str(result.message.body.mid)
                self._clarify_state[clarify_id] = {
                    "session_key": session_key,
                    "message_id": msg_id,
                }
                logger.info(f"Max: sent clarify prompt id={clarify_id} choices={len(choices)}")
                return SendResult(success=True, message_id=msg_id)
        except (MaxApiError, MaxConnection) as e:
            logger.warning(f"Max send clarify error: {e}")
        except Exception as e:
            logger.warning(f"Max send clarify unexpected error: {e}")

        return SendResult(success=False, error="API returned empty response")

    async def send_typing(self, chat_id) -> None:
        """Typing indicator — Max API only supports it in group chats, not dialogs."""
        pass

    async def _edit_message(self, message_id: str, text: str, attachments: list = None) -> bool:
        """Edit a bot message via bot.edit_message. Empty attachments removes keyboard."""
        if not self._bot:
            return False
        try:
            await self._bot.edit_message(
                message_id=message_id,
                text=text,
                format=TextFormat.MARKDOWN,
                attachments=attachments or [],
            )
            return True
        except (MaxApiError, MaxConnection) as e:
            logger.warning(f"Max edit message error: {e}")
        except Exception as e:
            logger.warning(f"Max edit message unexpected error: {e}")
        return False

    async def _send_media_file(self, chat_id, file_path: str, upload_type: UploadType,
                                attachment_type: str, caption: str = None) -> SendResult:
        """Upload and send a media file via maxapi."""
        if not self._bot:
            return SendResult(success=False)

        path = Path(file_path)
        if not path.exists():
            logger.warning(f"Max upload: file not found: {file_path}")
            return SendResult(success=False)

        try:
            # Upload via maxapi
            media = InputMedia(path=str(path), type=upload_type)
            upload_result = await self._bot.upload_media(media)

            # Build attachment
            att = Attachment(
                type=attachment_type,
                payload=upload_result.payload,
            )

            result = await self._bot.send_message(
                chat_id=int(chat_id),
                text=caption or "",
                format=TextFormat.MARKDOWN,
                attachments=[att],
            )
            if result and result.message and result.message.body:
                msg_id = str(result.message.body.mid)
                logger.info(f"Max: sent {attachment_type} file={path.name} → msg_id={msg_id}")
                return SendResult(success=True, message_id=msg_id)
        except (MaxApiError, MaxConnection) as e:
            logger.warning(f"Max upload/send {attachment_type} error: {e}")
        except Exception as e:
            logger.warning(f"Max upload/send {attachment_type} unexpected error: {e}")

        return SendResult(success=False)

    async def send_image(self, chat_id, image_url, caption=None) -> SendResult:
        """Send image by URL — Max may not support URL-based images directly."""
        # Fall back: try sending as text with URL
        text = caption or ""
        return await self.send(chat_id, f"{text}\n{image_url}" if image_url else text)

    async def send_image_file(self, chat_id, path, caption=None) -> SendResult:
        """Send image from local file path."""
        return await self._send_media_file(chat_id, path, UploadType.IMAGE, "image", caption)

    async def send_document(self, chat_id, path, caption=None) -> SendResult:
        """Send document from local file path."""
        return await self._send_media_file(chat_id, path, UploadType.FILE, "file", caption)

    async def send_voice(self, chat_id, path) -> SendResult:
        """Send voice message from local file path."""
        return await self._send_media_file(chat_id, path, UploadType.AUDIO, "audio")

    async def send_video(self, chat_id, path, caption=None) -> SendResult:
        """Send video from local file path."""
        return await self._send_media_file(chat_id, path, UploadType.VIDEO, "video", caption)

    # ── chat info ───────────────────────────────────────────────

    async def get_chat_info(self, chat_id) -> dict:
        if not self._bot:
            return {"name": str(chat_id), "type": "dm"}
        try:
            chat = await self._bot.get_chat_by_id(int(chat_id))
            return {
                "name": chat.title or f"chat_{chat_id}",
                "type": "group" if getattr(chat, "is_group", False) else "dm",
                "chat_id": chat_id,
            }
        except Exception:
            pass
        return {"name": f"chat_{chat_id}", "type": "dm"}

    # ── update handler (called from webhook) ────────────────────

    async def _handle_update(self, u: dict):
        """Parse a Max update (from /updates) and forward as MessageEvent.

        Handles forwarded messages: when body.text is empty, checks msg.link
        (LinkedMessage with type='forward') and extracts text from
        link.message (a MessageBody).
        """
        update_type = u.get("update_type", "")
        logger.info(f"Max: _handle_update raw={u}")

        # Bot started event
        if update_type == "bot_started":
            chat_id = u.get("chat_id")
            if chat_id:
                logger.info(f"Max: bot_started in chat {chat_id}")
            return

        # Callback (inline button press)
        if update_type == "message_callback":
            callback = u.get("callback", {}) or {}
            payload = callback.get("payload", "")
            msg = u.get("message", {}) or {}
            chat_id = str(msg.get("chat_id") or u.get("chat_id", ""))
            author = msg.get("author", msg.get("from", {}))
            author_id = str(author.get("user_id", ""))

            # Approval button callback: payload format "ea:<choice>:<id>"
            if payload.startswith("ea:"):
                parts = payload.split(":", 2)
                if len(parts) == 3:
                    choice = parts[1]
                    approval_id = parts[2]
                    state = self._approval_state.pop(int(approval_id), None)
                    if state:
                        session_key = state.get("session_key", "")
                        msg_id = state.get("message_id", "")
                        from tools.approval import resolve_gateway_approval
                        count = resolve_gateway_approval(session_key, choice)
                        logger.info(f"Max: approval callback choice={choice} id={approval_id} resolved={count}")
                        labels = {
                            "once": "✅ Выполнено (один раз)",
                            "session": "✅ Разрешено для сессии",
                            "always": "✅ Разрешено навсегда",
                            "deny": "❌ Отклонено",
                        }
                        result_text = labels.get(choice, choice)
                        # Edit original message: remove keyboard, show choice
                        if msg_id:
                            await self._edit_message(msg_id, f"⚠️ Подтверждение команды\n\n{result_text}", attachments=[])
                        else:
                            await self.send(chat_id, result_text)
                        return
                    else:
                        logger.warning(f"Max: approval callback — no session for id={approval_id}")
                        return

            # Clarify button callback: payload format "cl:<clarify_id>:<idx|other>"
            if payload.startswith("cl:"):
                parts = payload.split(":", 2)
                if len(parts) == 3:
                    clarify_id = parts[1]
                    selection = parts[2]
                    state = self._clarify_state.pop(clarify_id, None)
                    msg_id = state.get("message_id", "") if state else ""

                    if selection == "other":
                        from tools.clarify_gateway import mark_awaiting_text
                        mark_awaiting_text(clarify_id)
                        if msg_id:
                            await self._edit_message(msg_id, "❓ Вопрос\n\n✏️ Напишите свой ответ:", attachments=[])
                        else:
                            await self.send(chat_id, "✏️ Напишите свой ответ:")
                        return

                    # Resolve by index
                    idx = int(selection)
                    from tools.clarify_gateway import _entries, _lock
                    with _lock:
                        entry = _entries.get(clarify_id)
                    resolved_text = None
                    if entry and entry.choices and 0 <= idx < len(entry.choices):
                        resolved_text = entry.choices[idx]

                    if resolved_text is None:
                        resolved_text = f"choice {idx + 1}"

                    from tools.clarify_gateway import resolve_gateway_clarify
                    resolved = resolve_gateway_clarify(clarify_id, resolved_text)
                    if resolved:
                        if msg_id:
                            await self._edit_message(msg_id, f"❓ Вопрос\n\n✓ {resolved_text[:80]}", attachments=[])
                        else:
                            await self.send(chat_id, f"✓ {resolved_text[:80]}")
                    else:
                        if msg_id:
                            await self._edit_message(msg_id, "❓ Вопрос\n\n⏰ Время ответа истекло", attachments=[])
                        else:
                            await self.send(chat_id, "⏰ Время ответа истекло")
                    return

            # Generic callback: forward as text
            source = self.build_source(
                chat_id=chat_id,
                user_id=author_id,
            )

            event = MessageEvent(
                message_id=str(u.get("update_id", u.get("event_id", ""))),
                text=f"/callback {payload}",
                source=source,
                message_type=MessageType.TEXT,
            )
            await self.handle_message(event)
            return

        # Regular message
        msg = u.get("message", {}) or {}
        if not isinstance(msg, dict):
            logger.warning(f"Max: msg is not dict, type={type(msg)}")
            return
        logger.info(f"Max: raw msg keys={list(msg.keys())} body={msg.get('body', {})}")
        recipient = msg.get("recipient", {}) or {}
        body = msg.get("body", {}) or {}
        chat_id = str(recipient.get("chat_id", ""))
        text = (body.get("text") or "").strip()

        # ── Forwarded message handling ──────────────────────────
        # Max API: body can be null when message contains only a forwarded
        # message. The forwarded content is in msg.link.message (MessageBody).
        link = msg.get("link") or {}
        forwarded_text = ""
        forwarded_from = ""
        if isinstance(link, dict) and link.get("type") == "forward":
            link_msg = link.get("message", {}) or {}
            forwarded_text = (link_msg.get("text") or "").strip()
            link_sender = link.get("sender") or {}
            forwarded_from = (
                link_sender.get("name")
                or link_sender.get("first_name")
                or ""
            )
            # Also collect attachments from the forwarded message
            link_attachments = link_msg.get("attachments") or []
            if not body.get("attachments") and link_attachments:
                body["attachments"] = link_attachments

            # Append attachment URLs to forwarded text so the agent can
            # access them (download images, files, etc.)
            if link_attachments:
                att_parts = []
                for att in link_attachments:
                    att_type = att.get("type", "file")
                    fname = att.get("filename") or att.get("title") or ""
                    fsize = att.get("size", 0)
                    furl = (att.get("payload") or {}).get("url", "")
                    size_str = ""
                    if fsize:
                        if fsize >= 1_000_000:
                            size_str = f" ({fsize / 1_000_000:.1f} МБ)"
                        elif fsize >= 1_000:
                            size_str = f" ({fsize / 1_000:.0f} КБ)"
                    icon = {"file": "📎", "image": "🖼", "video": "🎬",
                            "audio": "🎵", "voice": "🎤"}.get(att_type, "📄")
                    if not furl and att_type == "image":
                        # Images may have token-based URL; construct from photo_id
                        photo_id = (att.get("payload") or {}).get("photo_id", "")
                        if photo_id:
                            furl = f"[photo_id={photo_id}]"
                    part = f"{icon} {fname}{size_str}"
                    if furl:
                        part += f"\n{furl}"
                    att_parts.append(part)
                att_text = "\n".join(att_parts)
                if forwarded_text:
                    forwarded_text = f"{forwarded_text}\n\n{att_text}"
                else:
                    forwarded_text = att_text

        # Build final text: use own text, or fall back to forwarded content
        if not text and forwarded_text:
            if forwarded_from:
                text = f"[Переслано от {forwarded_from}]\n{forwarded_text}"
            else:
                text = f"[Пересланное сообщение]\n{forwarded_text}"
            logger.info(f"Max: extracted forwarded content ({len(text)} chars) from link")
        elif text and forwarded_text:
            # User added their own text + forwarded message
            text = f"{text}\n\n[Переслано от {forwarded_from or '—'}]\n{forwarded_text}"

        # ── Regular attachment handling ───────────────────────────
        # If the message has no text but has attachments (voice, image, file,
        # video, audio), extract attachment info so the message isn't skipped.
        if not text:
            own_attachments = body.get("attachments") or []
            if own_attachments:
                att_parts = []
                for att in own_attachments:
                    att_type = att.get("type", "file")
                    fname = att.get("filename") or att.get("title") or ""
                    fsize = att.get("size", 0)
                    furl = (att.get("payload") or {}).get("url", "")
                    size_str = ""
                    if fsize:
                        if fsize >= 1_000_000:
                            size_str = f" ({fsize / 1_000_000:.1f} МБ)"
                        elif fsize >= 1_000:
                            size_str = f" ({fsize / 1_000:.0f} КБ)"
                    icon = {"file": "📎", "image": "🖼", "video": "🎬",
                            "audio": "🎵", "voice": "🎤"}.get(att_type, "📄")
                    if not furl and att_type == "image":
                        photo_id = (att.get("payload") or {}).get("photo_id", "")
                        if photo_id:
                            furl = f"[photo_id={photo_id}]"
                    part = f"{icon} {fname}{size_str}"
                    if furl:
                        part += f"\n{furl}"
                    att_parts.append(part)
                text = "\n".join(att_parts)
                logger.info(f"Max: extracted attachment content ({len(text)} chars) from body")

        logger.debug(f"Max: parsed chat_id={chat_id} text='{text[:50]}'")

        if not chat_id or not text:
            logger.debug(f"Max: skipping — no chat_id or text")
            return

        message_id = str(body.get("mid") or msg.get("message_id") or u.get("update_id", ""))
        if message_id in self._known_message_ids:
            logger.debug(f"Max: duplicate message_id={message_id}, skipping")
            return
        self._known_message_ids.add(message_id)

        # Housekeeping
        if len(self._known_message_ids) > 10000:
            self._known_message_ids.clear()

        sender = msg.get("sender") or msg.get("author") or msg.get("from") or {}
        author_id = str(sender.get("user_id", ""))
        author_name = sender.get("first_name") or sender.get("name") or f"user_{author_id}"

        # Skip own messages (bot's user_id from config or env)
        bot_user_id = os.getenv("MAX_BOT_USER_ID", "")
        if bot_user_id and author_id == bot_user_id:
            return

        logger.debug(f"Max: building event for user={author_name} chat={chat_id} text='{text[:50]}'")

        source = self.build_source(
            chat_id=chat_id,
            user_id=author_id,
            user_name=author_name,
        )

        attachments_raw = body.get("attachments") or []
        has_attachments = len(attachments_raw) > 0

        # For forwarded messages we already encoded attachment info (filename,
        # size, URL) into the text. Use TEXT type so the message qualifies for
        # debounce/merge.
        is_forwarded = bool(link and link.get("type") == "forward")

        # ── Download media for STT / vision pipeline ───────────────
        # Gateway uses event.media_urls (local file paths) + event.media_types
        # (MIME strings) to feed STT (voice/audio) and vision (images).
        # Download attachments via maxapi.Bot.download_file() so the gateway
        # pipeline can process them.
        media_urls: list[str] = []
        media_types: list[str] = []
        if has_attachments and not is_forwarded:
            media_cache = Path.home() / ".hermes" / "cache" / "max_media"
            media_cache.mkdir(parents=True, exist_ok=True)
            for att in attachments_raw:
                if not isinstance(att, dict):
                    continue
                att_type = att.get("type", "file")
                payload = att.get("payload") or {}
                furl = payload.get("url", "")
                if not furl and att_type == "image":
                    # Images may have token-based URL; skip download, keep photo_id in text
                    continue
                if not furl:
                    continue
                # Only download types the gateway pipeline handles
                if att_type not in ("audio", "voice", "image", "video", "file"):
                    continue
                mime_map = {
                    "audio": "audio/ogg",
                    "voice": "audio/ogg",
                    "image": "image/png",
                    "video": "video/mp4",
                    "file": "application/octet-stream",
                }
                try:
                    local_path = await self._bot.download_file(
                        url=furl,
                        destination=str(media_cache),
                    )
                    media_urls.append(str(local_path))
                    media_types.append(mime_map.get(att_type, "application/octet-stream"))
                    logger.info(f"Max: downloaded {att_type} → {local_path}")
                except Exception as e:
                    logger.warning(f"Max: failed to download {att_type} from {furl[:80]}: {e}")

        if not has_attachments or is_forwarded:
            msg_type = MessageType.TEXT
        else:
            first_att = attachments_raw[0] if isinstance(attachments_raw[0], dict) else {}
            att_type = first_att.get("type", "")
            msg_type = {
                "image": MessageType.PHOTO,
                "photo": MessageType.PHOTO,
                "video": MessageType.VIDEO,
                "audio": MessageType.AUDIO,
                "voice": MessageType.VOICE,
                "file": MessageType.DOCUMENT,
                "sticker": MessageType.STICKER,
                "location": MessageType.LOCATION,
            }.get(att_type, MessageType.TEXT)

        event = MessageEvent(
            message_id=message_id,
            text=text,
            source=source,
            message_type=msg_type,
            media_urls=media_urls,
            media_types=media_types,
        )

        # ── Burst-merge ──────────────────────────────────────────
        # Max sends forwarded content as a separate update 1–2s after the
        # user's text. If we dispatch immediately, the agent processes them
        # as two independent turns. Instead, wait briefly and merge if a
        # follow-up arrives from the same user in the same chat.
        now = time.monotonic()
        prev = self._last_msg
        if (
            prev
            and prev["chat_id"] == chat_id
            and prev["user_id"] == author_id
            and (now - prev["ts"]) < self._burst_window
            and not prev.get("dispatched")
        ):
            # Cancel pending dispatch and merge
            prev["dispatched"] = True
            merged_text = f"{prev['text']}\n\n{text}"
            event = MessageEvent(
                message_id=message_id,
                text=merged_text,
                source=source,
                message_type=MessageType.TEXT,
            )
            logger.info(f"Max: burst-merged with previous message ({len(merged_text)} chars)")
        else:
            # Schedule a deferred dispatch so a follow-up can merge in
            self._last_msg = {
                "chat_id": chat_id,
                "user_id": author_id,
                "text": text,
                "ts": now,
                "dispatched": False,
            }
            await asyncio.sleep(self._burst_window)
            # If a newer message merged into us, skip dispatch
            if self._last_msg and self._last_msg.get("dispatched"):
                logger.info(f"Max: skipping dispatch — burst-merged into newer message")
                return

        logger.debug(f"Max: calling handle_message with event")
        await self.handle_message(event)


# ── plugin entry points ─────────────────────────────────────────

def check_requirements() -> bool:
    """Check if Max adapter dependencies are available."""
    try:
        import maxapi  # noqa: F401
        return True
    except ImportError:
        return False


def validate_config(config: PlatformConfig) -> bool:
    """Validate that we have a token."""
    extra = getattr(config, "extra", {}) or {}
    token = os.getenv("MAX_BOT_TOKEN") or getattr(config, "token", None) or extra.get("token", "")
    return bool(token)


def register(ctx):
    """Register Max Messenger platform adapter."""
    ctx.register_platform(
        name="max",
        label="Max Messenger",
        adapter_factory=lambda cfg: MaxAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        required_env=["MAX_BOT_TOKEN"],
        install_hint="pip install maxapi",
        allowed_users_env="MAX_ALLOWED_USERS",
        allow_all_env="MAX_ALLOW_ALL_USERS",
        max_message_length=4000,
        emoji="💎",
        platform_hint=(
            "You are on Max Messenger (max.ru) — российский мессенджер. "
            "Поддерживает markdown: **bold**, *italic*, ~~strikethrough~~, `code`, "
            "[links](url), ## headers. Таблиц нет — используй списки. "
            "Можно отправлять изображения, файлы, голосовые сообщения. "
            "Лимит сообщения: 4000 символов. Общайся на русском."
        ),
    )
