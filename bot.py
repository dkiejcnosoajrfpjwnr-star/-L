"""Group-only Telegram music bot.

Admins reply to a video/audio with "شغل". The bot downloads the media, queues
it, and the Telethon user session plays its audio in the target voice chat.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import re
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from pytgcalls import PyTgCalls
from pytgcalls.types import AudioQuality, MediaStream
from telethon import Button, TelegramClient, events
from telethon.sessions import StringSession

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("music-bot")


def required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required secret: {name}")
    return value


API_ID = int(required("API_ID"))
API_HASH = required("API_HASH")
BOT_TOKEN = required("BOT_TOKEN")
SESSION_STRING = required("SESSION_STRING")
TARGET_CHAT_ID = int(required("TARGET_CHAT_ID"))
DEVELOPER_URL = os.getenv("DEVELOPER_URL", "https://t.me/c3cccc3c")
MAX_MEDIA_MB = int(os.getenv("MAX_MEDIA_MB", "200"))

DOWNLOAD_DIR = Path("downloads")
SESSION_DIR = Path("sessions")
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
SESSION_DIR.mkdir(parents=True, exist_ok=True)

PLAY_WORDS = {"شغل", "تشغيل", "play", "/شغل", "/تشغيل", "/play"}
STOP_WORDS = {"ايقاف", "إيقاف", "وقف", "stop", "/ايقاف", "/إيقاف", "/وقف", "/stop"}
STATUS_WORDS = {"حالة", "status", "/حالة", "/status"}


def clean_command(text: str) -> str:
    return text.strip().split(maxsplit=1)[0].lower()


def media_from_message(message: Any) -> Any | None:
    if getattr(message, "video", None):
        return message.video
    if getattr(message, "audio", None):
        return message.audio
    if getattr(message, "voice", None):
        return message.voice
    document = getattr(message, "document", None)
    mime = (getattr(document, "mime_type", "") or "").lower()
    if mime.startswith("video/") or mime.startswith("audio/"):
        return document
    return None


def media_filename(message: Any) -> str:
    media = media_from_message(message)
    message_file = getattr(message, "file", None)
    name = (
        getattr(media, "file_name", None)
        or getattr(message_file, "name", None)
        or f"media_{message.id}.mp4"
    )
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", name)[:120]


class GroupMusicBot:
    def __init__(self) -> None:
        self.bot = TelegramClient(
            str(SESSION_DIR / "bot"),
            api_id=API_ID,
            api_hash=API_HASH,
        )
        self.user = TelegramClient(
            StringSession(SESSION_STRING),
            API_ID,
            API_HASH,
            flood_sleep_threshold=60,
            connection_retries=10,
            retry_delay=5,
        )
        self.calls: PyTgCalls | None = None
        self.queue: asyncio.Queue[Path] = asyncio.Queue()
        self.player_task: asyncio.Task[None] | None = None
        self.current: Path | None = None
        self.stop_requested = False

    async def is_admin(self, user_id: int) -> bool:
        try:
            permissions = await self.bot.get_permissions(TARGET_CHAT_ID, user_id)
            return bool(permissions.is_admin or permissions.is_creator)
        except Exception:
            log.exception("Could not check group admin status")
            return False

    async def get_reply_media(self, message: Any) -> Any | None:
        reply = await message.get_reply_message()
        return reply if reply and media_from_message(reply) else None

    async def download_media(self, message: Any) -> Path:
        media = media_from_message(message)
        size = int(
            getattr(media, "file_size", None)
            or getattr(media, "size", 0)
            or 0
        )
        if size > MAX_MEDIA_MB * 1024 * 1024:
            raise ValueError(f"حجم الملف أكبر من {MAX_MEDIA_MB} ميغابايت.")

        path = DOWNLOAD_DIR / f"{message.id}_{media_filename(message)}"
        downloaded = await self.bot.download_media(message, file=str(path))
        if not downloaded or not path.exists():
            raise RuntimeError("فشل تنزيل الملف من تيليجرام.")
        return path

    async def media_duration(self, path: Path) -> float:
        process = await asyncio.create_subprocess_exec(
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        output, _ = await process.communicate()
        try:
            return max(1.0, float(output.decode().strip()))
        except (ValueError, UnicodeDecodeError):
            return 3600.0

    async def player_loop(self) -> None:
        while True:
            path = await self.queue.get()
            self.current = path
            try:
                if self.calls is None:
                    raise RuntimeError("Voice call client is not started.")
                await self.calls.play(
                    TARGET_CHAT_ID,
                    MediaStream(str(path), audio_parameters=AudioQuality.STUDIO),
                )
                duration = await self.media_duration(path)
                log.info("Playing %s for %.1f seconds", path.name, duration)
                await asyncio.sleep(duration + 1)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Could not play %s", path)
                with contextlib.suppress(Exception):
                    await self.bot.send_message(
                        TARGET_CHAT_ID,
                        "تعذر تشغيل هذا الملف. تأكد من فتح المحادثة الصوتية.",
                    )
            finally:
                path.unlink(missing_ok=True)
                self.current = None
                self.queue.task_done()

    async def stop_playback(self) -> bool:
        had_media = self.current is not None or not self.queue.empty()
        self.stop_requested = True
        while not self.queue.empty():
            with contextlib.suppress(asyncio.QueueEmpty):
                path = self.queue.get_nowait()
                path.unlink(missing_ok=True)
                self.queue.task_done()

        if self.player_task and not self.player_task.done():
            self.player_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.player_task
        self.player_task = None

        if self.calls is not None:
            with contextlib.suppress(Exception):
                await self.calls.leave_call(TARGET_CHAT_ID)
        if self.current:
            self.current.unlink(missing_ok=True)
            self.current = None
        return had_media

    async def handle_message(self, event: Any) -> None:
        message = event.message
        if not event.sender_id or not await self.is_admin(event.sender_id):
            return

        text = (event.raw_text or "").strip()
        command = clean_command(text)

        if command in STOP_WORDS:
            stopped = await self.stop_playback()
            await event.reply(
                "تم إيقاف التشغيل وتفريغ القائمة."
                if stopped
                else "لا يوجد تشغيل حاليًا."
            )
            return

        if command in STATUS_WORDS:
            if self.current:
                await event.reply(f"يعمل الآن: {self.current.name}")
            elif not self.queue.empty():
                await event.reply("لا يوجد ملف يعمل حاليًا، توجد ملفات في القائمة.")
            else:
                await event.reply("القائمة فارغة.")
            return

        if command not in PLAY_WORDS:
            if command in {"/start", "start"}:
                await event.reply(
                    "أهلًا بك في بوت ميوزك\n"
                    "لل تشغيل: رد على فيديو أو صوت بكلمة شغل\n"
                    "للإيقاف: اكتب ايقاف",
                    buttons=[[Button.url("مطور البوت", DEVELOPER_URL)]],
                )
            return

        media_message = await self.get_reply_media(message)
        if not media_message:
            await event.reply("يجب كتابة شغل كرد على فيديو أو ملف صوتي.")
            return

        status = await event.reply("جاري تحميل الملف وإضافته إلى القائمة...")
        try:
            path = await self.download_media(media_message)
            self.stop_requested = False
            await self.queue.put(path)
            if self.player_task is None or self.player_task.done():
                self.player_task = asyncio.create_task(self.player_loop())
            await status.edit("تمت إضافة الملف، وسيعمل تلقائيًا بالترتيب.")
        except Exception:
            log.exception("Download failed")
            await status.edit("فشل تحميل الملف. تأكد من صلاحيات البوت وحجم الملف.")

    async def run(self) -> None:
        self.bot.add_event_handler(
            self.handle_message,
            events.NewMessage(chats=TARGET_CHAT_ID),
        )
        await self.user.connect()
        if not await self.user.is_user_authorized():
            raise RuntimeError("SESSION_STRING غير مصادق عليه.")
        await self.bot.start(bot_token=BOT_TOKEN)
        self.calls = PyTgCalls(self.user)
        await self.calls.start()
        log.info("Music bot is running in target group %s", TARGET_CHAT_ID)
        try:
            await asyncio.Event().wait()
        finally:
            await self.stop_playback()
            if self.calls:
                with contextlib.suppress(Exception):
                    await self.calls.stop()
            await self.bot.disconnect()
            await self.user.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(GroupMusicBot().run())
    except KeyboardInterrupt:
        log.info("Stopped by user.")
