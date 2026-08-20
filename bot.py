"""Telegram voice-chat music bot.

The bot account handles commands and uploads the downloaded audio visibly as a
bot. A Telethon user session is used only for reading the source chat and
joining the target voice chat, which Telegram does not allow bot accounts to do.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import re
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from pyrogram import Client as PyrogramClient
from pyrogram import filters
from pytgcalls import PyTgCalls
from pytgcalls.types import AudioQuality, MediaStream
from telethon import TelegramClient, events
from telethon.sessions import StringSession

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("voice-bot")


def required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


API_ID = int(required("API_ID"))
API_HASH = required("API_HASH")
BOT_TOKEN = required("BOT_TOKEN")
SESSION_STRING = required("SESSION_STRING")
SOURCE_CHAT_ID = int(required("SOURCE_CHAT_ID"))
TARGET_CHAT_ID = int(required("TARGET_CHAT_ID"))
SOURCE_REQUEST_PREFIX = os.getenv("SOURCE_REQUEST_PREFIX", "يوت").strip()
SOURCE_DOWNLOADER_USERNAME = "w60ybot"
SOURCE_WAIT_SECONDS = int(os.getenv("SOURCE_WAIT_SECONDS", "120"))
MAX_AUDIO_MB = int(os.getenv("MAX_AUDIO_MB", "100"))
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "downloads"))
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)


def is_audio_message(message: Any) -> bool:
    if getattr(message, "voice", None) or getattr(message, "audio", None):
        return True
    document = getattr(message, "document", None)
    mime_type = getattr(document, "mime_type", "") or ""
    return mime_type.startswith("audio/")


def audio_size(message: Any) -> int:
    media = getattr(message, "audio", None) or getattr(message, "voice", None)
    media = media or getattr(message, "document", None)
    return int(getattr(media, "file_size", 0) or 0)


def safe_name(message: Any) -> str:
    media = getattr(message, "audio", None) or getattr(message, "voice", None)
    media = media or getattr(message, "document", None)
    name = getattr(media, "file_name", None) or f"track_{message.id}.bin"
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", name)[:120]


class VoiceMusicBot:
    def __init__(self) -> None:
        self.bot = PyrogramClient(
            "bot",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            workdir="sessions",
        )
        self.user = TelegramClient(
            StringSession(SESSION_STRING),
            API_ID,
            API_HASH,
            flood_sleep_threshold=60,
            connection_retries=10,
            retry_delay=5,
        )
        self.calls = PyTgCalls(self.user)
        self.pending_source: asyncio.Queue[Any] = asyncio.Queue()
        self.current: dict[str, Any] | None = None
        self.started_at: float | None = None
        self._stop_event = asyncio.Event()

    async def send_status(self, text: str) -> None:
        await self.bot.send_message(TARGET_CHAT_ID, text)

    async def on_source_audio(self, event: Any) -> None:
        message = event.message
        sender = await event.get_sender()
        sender_username = (getattr(sender, "username", "") or "").lower().lstrip("@")
        if (
            sender_username == SOURCE_DOWNLOADER_USERNAME
            and is_audio_message(message)
        ):
            await self.pending_source.put(message)

    async def wait_for_source_audio(self) -> Any:
        deadline = time.monotonic() + SOURCE_WAIT_SECONDS
        while time.monotonic() < deadline:
            remaining = max(0.1, deadline - time.monotonic())
            try:
                return await asyncio.wait_for(
                    self.pending_source.get(), timeout=remaining
                )
            except asyncio.TimeoutError:
                break
        raise TimeoutError("لم يصل ملف صوتي من مجموعة المصدر خلال المهلة المحددة.")

    async def download_source_audio(self, message: Any) -> Path:
        size = audio_size(message)
        if size and size > MAX_AUDIO_MB * 1024 * 1024:
            raise ValueError(f"حجم الملف أكبر من الحد المسموح ({MAX_AUDIO_MB}MB).")
        path = DOWNLOAD_DIR / f"{message.id}_{safe_name(message)}"
        await self.user.download_media(message, file=str(path))
        return path

    async def play(self, path: Path) -> None:
        # MediaStream is configured with audio only. The user account is required
        # because Telegram voice chats cannot be joined by bot accounts.
        await self.calls.play(
            TARGET_CHAT_ID,
            MediaStream(str(path), audio_parameters=AudioQuality.STUDIO),
        )
        self.current = {"path": path, "title": path.name}
        self.started_at = time.monotonic()

    async def stop(self) -> bool:
        if not self.current:
            return False
        with contextlib.suppress(Exception):
            await self.calls.leave_call(TARGET_CHAT_ID)
        path = self.current.get("path")
        if isinstance(path, Path):
            path.unlink(missing_ok=True)
        self.current = None
        self.started_at = None
        return True

    async def request_and_play(self, query: str) -> None:
        # Empty old results before asking the source downloader.
        while not self.pending_source.empty():
            with contextlib.suppress(asyncio.QueueEmpty):
                self.pending_source.get_nowait()

        # The user session sends the request because @W60yBot must receive it
        # from a normal Telegram account, not from another bot.
        await self.user.send_message(
            SOURCE_CHAT_ID,
            f"{SOURCE_REQUEST_PREFIX} {query}",
        )
        source_message = await self.wait_for_source_audio()
        path = await self.download_source_audio(source_message)
        try:
            await self.bot.send_audio(
                TARGET_CHAT_ID,
                audio=str(path),
                caption=f"تم تجهيز الصوت: {query}",
            )
            await self.play(path)
        except Exception:
            path.unlink(missing_ok=True)
            raise

    async def is_group_admin(self, user_id: int) -> bool:
        try:
            member = await self.bot.get_chat_member(TARGET_CHAT_ID, user_id)
            status = str(member.status).lower()
            return status.endswith("owner") or status.endswith("administrator")
        except Exception:
            log.exception("Could not verify group administrator status")
            return False

    def register_handlers(self) -> None:
        @self.bot.on_message(filters.chat(TARGET_CHAT_ID) & filters.text)
        async def command_handler(_client: Any, message: Any) -> None:
            user = message.from_user
            if not user or not await self.is_group_admin(user.id):
                return
            text = (message.text or message.caption or "").strip()
            parts = text.split(maxsplit=1)
            command = parts[0].lstrip("/").lower() if parts else ""
            if command not in {"شغل", "play", "وقف", "stop", "حالة", "status"}:
                return

            if command in {"وقف", "stop"}:
                stopped = await self.stop()
                await message.reply_text(
                    "تم إيقاف التشغيل الصوتي." if stopped else "لا يوجد تشغيل حاليًا."
                )
                return

            if command in {"حالة", "status"}:
                if not self.current:
                    await message.reply_text("لا يوجد صوت يعمل حاليًا.")
                    return
                elapsed = int(time.monotonic() - (self.started_at or time.monotonic()))
                await message.reply_text(
                    f"التشغيل الحالي: {self.current['title']}\n"
                    f"المدة منذ البدء: {elapsed // 60}د {elapsed % 60}ث"
                )
                return

            if len(parts) < 2 or not parts[1].strip():
                await message.reply_text("الاستخدام: شغل اسم الأغنية أو رابطها")
                return

            query = parts[1].strip()
            status = await message.reply_text("جاري طلب الصوت من مجموعة المصدر...")
            try:
                await self.stop()
                await self.request_and_play(query)
                await status.edit_text("تم تشغيل الصوت في المحادثة الصوتية.")
            except TimeoutError as exc:
                await status.edit_text(str(exc))
            except Exception:
                log.exception("Playback failed")
                await status.edit_text(
                    "تعذر التشغيل. تأكد من صلاحيات الحساب، وأن المحادثة الصوتية مفتوحة."
                )

    async def run(self) -> None:
        self.register_handlers()
        await self.user.connect()
        if not await self.user.is_user_authorized():
            raise RuntimeError(
                "SESSION_STRING غير مصادق عليه. أنشئ StringSession بالحساب المطلوب أولًا."
            )
        await self.bot.start()
        self.user.add_event_handler(
            self.on_source_audio,
            events.NewMessage(chats=SOURCE_CHAT_ID),
        )
        await self.calls.start()
        log.info("Bot is running. Target chat: %s", TARGET_CHAT_ID)
        try:
            await self._stop_event.wait()
        finally:
            await self.stop()
            with contextlib.suppress(Exception):
                await self.calls.stop()
            await self.bot.stop()
            await self.user.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(VoiceMusicBot().run())
    except KeyboardInterrupt:
        log.info("Stopped by user.")
