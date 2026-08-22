"""Telethon-only group music bot.

Reply to a video or audio with "شغل". Videos are converted to audio before
they are sent to the voice chat. New items wait in a queue and play in order.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import re
from dataclasses import dataclass
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
SKIP_WORDS = {"تخطي", "التالي", "skip", "/تخطي", "/التالي", "/skip"}
STATUS_WORDS = {"حالة", "status", "/حالة", "/status"}


@dataclass
class Track:
    path: Path
    status_message: Any
    title: str


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
    message_file = getattr(message, "file", None)
    name = getattr(message_file, "name", None) or f"media_{message.id}.bin"
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", name)[:120]


def control_buttons() -> list[list[Any]]:
    return [
        [
            Button.inline("إيقاف", b"music_stop"),
            Button.inline("تخطي", b"music_skip"),
            Button.inline("حالة", b"music_status"),
        ]
    ]


class GroupMusicBot:
    def __init__(self) -> None:
        self.bot = TelegramClient(
            str(SESSION_DIR / "bot"),
            API_ID,
            API_HASH,
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
        self.queue: asyncio.Queue[Track] = asyncio.Queue()
        self.player_task: asyncio.Task[None] | None = None
        self.current: Track | None = None
        self.advance_event = asyncio.Event()
        self.leave_when_empty = False

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
            getattr(media, "size", 0)
            or getattr(media, "file_size", 0)
            or 0
        )
        if size > MAX_MEDIA_MB * 1024 * 1024:
            raise ValueError(f"حجم الملف أكبر من {MAX_MEDIA_MB} ميغابايت.")

        original = DOWNLOAD_DIR / f"{message.id}_{media_filename(message)}"
        downloaded = await self.bot.download_media(message, file=str(original))
        if not downloaded or not original.exists():
            raise RuntimeError("فشل تنزيل الملف من تيليجرام.")

        # Always produce an audio-only file. This prevents a video stream from
        # ever reaching the voice chat, even when the replied media is a video.
        audio_path = original.with_suffix(".ogg")
        process = await asyncio.create_subprocess_exec(
            "ffmpeg",
            "-y",
            "-i",
            str(original),
            "-vn",
            "-map",
            "0:a:0",
            "-c:a",
            "libopus",
            "-b:a",
            "128k",
            str(audio_path),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, error = await process.communicate()
        original.unlink(missing_ok=True)
        if process.returncode != 0 or not audio_path.exists():
            detail = error.decode(errors="ignore")[-300:].strip()
            raise RuntimeError(f"تعذر استخراج الصوت من الملف. {detail}")
        return audio_path

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

    async def edit_status(self, track: Track, text: str) -> None:
        with contextlib.suppress(Exception):
            await track.status_message.edit(text, buttons=control_buttons())

    async def player_loop(self) -> None:
        while True:
            track = await self.queue.get()
            self.current = track
            try:
                if self.calls is None:
                    raise RuntimeError("Voice call client is not started.")

                await self.edit_status(track, f"• تم التشغيل\n{track.title}")
                await self.calls.play(
                    TARGET_CHAT_ID,
                    MediaStream(
                        str(track.path),
                        audio_parameters=AudioQuality.STUDIO,
                    ),
                )
                duration = await self.media_duration(track.path)
                self.advance_event.clear()
                try:
                    await asyncio.wait_for(
                        self.advance_event.wait(),
                        timeout=duration + 1,
                    )
                except asyncio.TimeoutError:
                    pass
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Could not play %s", track.path)
                with contextlib.suppress(Exception):
                    await self.bot.send_message(
                        TARGET_CHAT_ID,
                        "تعذر تشغيل الصوت. تأكد من فتح المحادثة الصوتية.",
                    )
            finally:
                track.path.unlink(missing_ok=True)
                self.current = None
                self.queue.task_done()

                if self.leave_when_empty and self.queue.empty():
                    self.leave_when_empty = False
                    if self.calls is not None:
                        with contextlib.suppress(Exception):
                            await self.calls.leave_call(TARGET_CHAT_ID)

    async def advance(self) -> bool:
        if self.current is None:
            return False
        self.leave_when_empty = self.queue.empty()
        self.advance_event.set()
        return True

    async def status_text(self) -> str:
        waiting = self.queue.qsize()
        if self.current:
            return (
                f"• التشغيل الآن:\n{self.current.title}\n"
                f"• طابور الانتظار: {waiting} ملف"
            )
        return f"• لا يوجد تشغيل\n• طابور الانتظار: {waiting} ملف"

    async def callback_handler(self, event: Any) -> None:
        if event.chat_id != TARGET_CHAT_ID:
            return
        if not event.sender_id or not await self.is_admin(event.sender_id):
            await event.answer("هذا الزر للمشرفين فقط.", alert=True)
            return

        action = event.data
        if action in {b"music_stop", b"music_skip"}:
            advanced = await self.advance()
            await event.answer(
                "تم الانتقال للملف التالي."
                if advanced and not self.leave_when_empty
                else "تم إيقاف التشغيل ومغادرة الاستيج."
                if advanced
                else "لا يوجد ملف يعمل حاليًا.",
                alert=False,
            )
        elif action == b"music_status":
            await event.answer(await self.status_text(), alert=True)

    async def handle_message(self, event: Any) -> None:
        if not event.sender_id or not await self.is_admin(event.sender_id):
            return

        text = (event.raw_text or "").strip()
        command = clean_command(text)

        if command in STOP_WORDS or command in SKIP_WORDS:
            advanced = await self.advance()
            await event.reply(
                "• تم الانتقال للملف التالي."
                if advanced and not self.leave_when_empty
                else "• تم إيقاف التشغيل ومغادرة الاستيج."
                if advanced
                else "• لا يوجد ملف يعمل حاليًا."
            )
            return

        if command in STATUS_WORDS:
            await event.reply(await self.status_text(), buttons=control_buttons())
            return

        if command in {"/start", "start"}:
            await event.reply(
                "أهلًا بك في بوت ميوزك\n"
                "رد على فيديو أو صوت بكلمة شغل للتشغيل.",
                buttons=[[Button.url("مطور البوت", DEVELOPER_URL)]],
            )
            return

        if command not in PLAY_WORDS:
            return

        media_message = await self.get_reply_media(event.message)
        if not media_message:
            await event.reply("• يجب أن ترد بكلمة شغل على فيديو أو ملف صوتي.")
            return

        waiting_before = self.queue.qsize() + (1 if self.current else 0)
        status = await event.reply(
            "• جار التشغيل" if waiting_before == 0 else "• جار التحميل ووضعه في الطابور"
        )
        try:
            path = await self.download_media(media_message)
            track = Track(path=path, status_message=status, title=media_filename(media_message))
            await self.queue.put(track)
            if self.player_task is None or self.player_task.done():
                self.player_task = asyncio.create_task(self.player_loop())
        except Exception:
            log.exception("Download failed")
            await status.edit("• فشل تحميل الملف. تأكد من الصلاحيات وحجم الفيديو.")

    async def run(self) -> None:
        self.bot.add_event_handler(
            self.handle_message,
            events.NewMessage(chats=TARGET_CHAT_ID),
        )
        self.bot.add_event_handler(
            self.callback_handler,
            events.CallbackQuery(),
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
            if self.player_task and not self.player_task.done():
                self.player_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self.player_task
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
