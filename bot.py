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
import time
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
YOUTUBE_CHAT_ID = int(required("YOUTUBE_CHAT_ID"))
DEVELOPER_URL = os.getenv("DEVELOPER_URL", "https://t.me/c3cccc3c")
YOUTUBE_BOT_USERNAME = os.getenv("YOUTUBE_BOT_USERNAME", "C11BOT")
MAX_MEDIA_MB = int(os.getenv("MAX_MEDIA_MB", "200"))

DOWNLOAD_DIR = Path("downloads")
SESSION_DIR = Path("sessions")
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
SESSION_DIR.mkdir(parents=True, exist_ok=True)

# Faster Telegram downloads and persistent audio cache.
DOWNLOAD_CHUNK_SIZE = 1024 * 1024
PARALLEL_DOWNLOAD_WORKERS = 4
CACHE_DIR = DOWNLOAD_DIR / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

PLAY_WORDS = {"شغل", "تشغيل", "play", "/شغل", "/تشغيل", "/play"}
STOP_WORDS = {"ايقاف", "إيقاف", "وقف", "stop", "/ايقاف", "/إيقاف", "/وقف", "/stop"}
SKIP_WORDS = {"تخطي", "التالي", "skip", "/تخطي", "/التالي", "/skip"}
STATUS_WORDS = {
    "حالة",
    "حاله",
    "الحالة",
    "الحاله",
    "status",
    "/حالة",
    "/حاله",
    "/الحالة",
    "/الحاله",
    "/status",
}
CLEAR_QUEUE_WORDS = {
    "مسح حالة",
    "مسح حاله",
    "مسح الحالة",
    "مسح الحاله",
    "/مسح حالة",
    "/مسح حاله",
    "/مسح الحالة",
    "/مسح الحاله",
}


@dataclass
class Track:
    path: Path
    status_message: Any
    title: str


def clean_command(text: str) -> str:
    return text.strip().split(maxsplit=1)[0].lower()


def normalized_text(text: str) -> str:
    return " ".join(text.strip().lower().split())


def format_duration(seconds: float) -> str:
    total = max(0, int(seconds))
    minutes, remaining = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{remaining:02d}"
    return f"{minutes:02d}:{remaining:02d}"


def progress_bar(elapsed: float, duration: float, width: int = 24) -> str:
    ratio = min(max(elapsed / max(duration, 1), 0.0), 1.0)
    position = min(int(ratio * (width - 1)), width - 1)
    return "━" * position + "●" + "━" * (width - position - 1)


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
        self.download_locks: dict[str, asyncio.Lock] = {}

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

    async def download_media(self, message: Any, status_message: Any) -> Path:
        media = media_from_message(message)
        if media is None:
            raise ValueError("الرسالة لا تحتوي على ملف صوتي أو فيديو.")

        size = int(
            getattr(media, "size", 0)
            or getattr(media, "file_size", 0)
            or 0
        )
        if size > MAX_MEDIA_MB * 1024 * 1024:
            raise ValueError(f"حجم الملف أكبر من {MAX_MEDIA_MB} ميغابايت.")

        # Telegram's media id is stable when the same file is sent again.
        # The converted OGG is cached, so repeat plays skip both download and
        # ffmpeg conversion.
        cache_key = str(getattr(media, "id", None) or message.id)
        cached_path = CACHE_DIR / f"{cache_key}.ogg"
        lock = self.download_locks.setdefault(cache_key, asyncio.Lock())

        async with lock:
            if cached_path.exists() and cached_path.stat().st_size > 0:
                log.info("Using cached media %s", cached_path)
                return cached_path

            started = time.monotonic()
            temporary_source = DOWNLOAD_DIR / (
                f".{cache_key}_{message.id}_{media_filename(message)}.part"
            )
            temporary_source.unlink(missing_ok=True)

            downloaded_bytes = 0
            progress_lock = asyncio.Lock()
            last_progress = 0.0

            async def show_progress() -> None:
                nonlocal last_progress
                now = time.monotonic()
                if now - last_progress < 5:
                    return
                async with progress_lock:
                    now = time.monotonic()
                    if now - last_progress < 5:
                        return
                    last_progress = now
                    elapsed = max(now - started, 0.001)
                    speed = downloaded_bytes / elapsed
                    if size:
                        percent = min(downloaded_bytes * 100 / size, 100)
                        text = (
                            "• جار التحميل بسرعة\n"
                            f"{percent:.1f}% | {speed / 1024 / 1024:.2f} MB/s"
                        )
                    else:
                        text = (
                            "• جار التحميل بسرعة\n"
                            f"{downloaded_bytes / 1024 / 1024:.1f} MB"
                        )
                    with contextlib.suppress(Exception):
                        await status_message.edit(text)

            # Split large files into aligned ranges and download the ranges
            # concurrently, matching the fast strategy in the supplied file.
            parts: list[tuple[int, int]] = []
            if size >= DOWNLOAD_CHUNK_SIZE * PARALLEL_DOWNLOAD_WORKERS:
                aligned = (
                    size // PARALLEL_DOWNLOAD_WORKERS // DOWNLOAD_CHUNK_SIZE
                ) * DOWNLOAD_CHUNK_SIZE
                aligned = max(aligned, DOWNLOAD_CHUNK_SIZE)
                for index in range(PARALLEL_DOWNLOAD_WORKERS):
                    offset = index * aligned
                    if offset >= size:
                        break
                    limit = (
                        aligned
                        if index < PARALLEL_DOWNLOAD_WORKERS - 1
                        else size - offset
                    )
                    parts.append((offset, limit))
            else:
                parts = [(0, size)] if size else [(0, 0)]

            temporary_parts = [
                Path(f"{temporary_source}.{index}") for index in range(len(parts))
            ]

            async def download_part(
                index: int, offset: int, limit: int
            ) -> None:
                nonlocal downloaded_bytes
                written = 0
                with temporary_parts[index].open("wb") as output:
                    iterator = self.bot.iter_download(
                        message.media,
                        offset=offset,
                        limit=limit or None,
                        request_size=DOWNLOAD_CHUNK_SIZE,
                    )
                    async for chunk in iterator:
                        remaining = limit - written if limit else len(chunk)
                        if remaining <= 0:
                            break
                        data = chunk[:remaining]
                        output.write(data)
                        written += len(data)
                        downloaded_bytes += len(data)
                        await show_progress()

                if limit and written != limit:
                    raise RuntimeError(
                        f"اكتمل تنزيل جزء غير كامل ({written}/{limit} بايت)."
                    )

            try:
                await asyncio.gather(
                    *(
                        download_part(index, offset, limit)
                        for index, (offset, limit) in enumerate(parts)
                    )
                )
                with temporary_source.open("wb") as output:
                    for part in temporary_parts:
                        with part.open("rb") as input_file:
                            while data := input_file.read(DOWNLOAD_CHUNK_SIZE):
                                output.write(data)
                        part.unlink(missing_ok=True)

                process = await asyncio.create_subprocess_exec(
                    "ffmpeg",
                    "-y",
                    "-i",
                    str(temporary_source),
                    "-vn",
                    "-map",
                    "0:a:0",
                    "-c:a",
                    "libopus",
                    "-b:a",
                    "128k",
                    str(cached_path),
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.PIPE,
                )
                _, error = await process.communicate()
                if process.returncode != 0 or not cached_path.exists():
                    detail = error.decode(errors="ignore")[-300:].strip()
                    raise RuntimeError(f"تعذر استخراج الصوت من الملف. {detail}")

                log.info(
                    "Downloaded and cached media %s in %.2fs",
                    cache_key,
                    time.monotonic() - started,
                )
                return cached_path
            except Exception:
                cached_path.unlink(missing_ok=True)
                raise
            finally:
                temporary_source.unlink(missing_ok=True)
                for part in temporary_parts:
                    part.unlink(missing_ok=True)

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

    async def download_youtube_audio(
        self, response: Any, query: str
    ) -> tuple[Path, str] | None:
        media = media_from_message(response)
        if media is None:
            return None

        cache_key = str(getattr(media, "id", None) or response.id)
        cached_path = CACHE_DIR / f"{cache_key}.ogg"
        lock = self.download_locks.setdefault(cache_key, asyncio.Lock())
        async with lock:
            if cached_path.exists() and cached_path.stat().st_size > 0:
                return cached_path, query

            source_path = DOWNLOAD_DIR / f".youtube_{cache_key}_{response.id}.source"
            source_path.unlink(missing_ok=True)
            cached_path.unlink(missing_ok=True)
            try:
                downloaded = await self.user.download_media(
                    response, file=str(source_path)
                )
                if not downloaded or not source_path.exists():
                    return None

                process = await asyncio.create_subprocess_exec(
                    "ffmpeg",
                    "-y",
                    "-i",
                    str(source_path),
                    "-vn",
                    "-map",
                    "0:a:0",
                    "-c:a",
                    "libopus",
                    "-b:a",
                    "128k",
                    str(cached_path),
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.PIPE,
                )
                _, error = await process.communicate()
                if process.returncode != 0 or not cached_path.exists():
                    log.error(
                        "YouTube audio conversion failed: %s",
                        error.decode(errors="ignore")[-300:].strip(),
                    )
                    cached_path.unlink(missing_ok=True)
                    return None
                return cached_path, query
            finally:
                source_path.unlink(missing_ok=True)

    async def wait_for_youtube_audio(self, request: Any) -> Any | None:
        try:
            youtube_bot = await self.user.get_entity(YOUTUBE_BOT_USERNAME)
            youtube_bot_id = getattr(youtube_bot, "id", None)
        except Exception:
            log.exception("Could not resolve the YouTube helper bot")
            return None

        # The helper can send a text status before the final voice message.
        # Polling keeps those private messages inside the source group.
        for _ in range(60):
            async for response in self.user.iter_messages(
                YOUTUBE_CHAT_ID, min_id=request.id, limit=20
            ):
                sender = getattr(response, "sender_id", None)
                if youtube_bot_id and sender != youtube_bot_id:
                    continue
                if media_from_message(response) is not None:
                    return response
                response_text = (response.raw_text or "").lower()
                if any(
                    phrase in response_text
                    for phrase in (
                        "لا توجد نتائج",
                        "لا يوجد نتائج",
                        "لم يتم العثور",
                        "not found",
                        "no results",
                    )
                ):
                    return None
            await asyncio.sleep(1)
        return None

    async def request_youtube_audio(self, query: str) -> tuple[Path, str] | None:
        try:
            request = await self.user.send_message(
                YOUTUBE_CHAT_ID, f"يوت {query}"
            )
            response = await self.wait_for_youtube_audio(request)
            if response is None:
                return None
            return await self.download_youtube_audio(response, query)
        except Exception:
            log.exception("Private YouTube helper request failed")
            return None

    async def update_playback_status(
        self, track: Track, started: float, duration: float
    ) -> None:
        while True:
            elapsed = min(max(time.monotonic() - started, 0.0), duration)
            await self.edit_status(
                track,
                f"• التشغيل الآن\n"
                f"{track.title}\n"
                f"⏱ {format_duration(elapsed)} / {format_duration(duration)}\n"
                f"{progress_bar(elapsed, duration)}",
            )
            await asyncio.sleep(2)

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
                progress_task = asyncio.create_task(
                    self.update_playback_status(track, time.monotonic(), duration)
                )
                self.advance_event.clear()
                try:
                    await asyncio.wait_for(
                        self.advance_event.wait(),
                        timeout=duration + 1,
                    )
                except asyncio.TimeoutError:
                    pass
                finally:
                    progress_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await progress_task
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
                f"• قائمة الانتظار: {waiting} ملف"
            )
        return f"• لا يوجد تشغيل\n• قائمة الانتظار: {waiting} ملف"

    async def clear_waiting(self) -> int:
        removed = 0
        while True:
            try:
                track = self.queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            self.queue.task_done()
            removed += 1
            with contextlib.suppress(Exception):
                await track.status_message.edit(
                    "↢ تم مسح الملف من قائمة الانتظار."
                )
        return removed

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
        full_command = normalized_text(text)

        if full_command in CLEAR_QUEUE_WORDS:
            removed = await self.clear_waiting()
            await event.reply(
                f"↢ تم مسح قائمة الانتظار.\n"
                f"↢ عدد الأصوات التي تم مسحها: {removed}"
            )
            return

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

        if full_command in {
            "حالة",
            "حاله",
            "الحالة",
            "الحاله",
            "/حالة",
            "/حاله",
            "/الحالة",
            "/الحاله",
            "status",
            "/status",
        }:
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

        # A play command with text after it is a private YouTube-helper
        # request. The helper conversation never gets forwarded to the public
        # group; only the resulting audio enters the normal queue.
        command_parts = text.split(maxsplit=1)
        youtube_query = command_parts[1].strip() if len(command_parts) > 1 else ""
        if youtube_query:
            status = await event.reply("• جار البحث عن المقطع وتشغيله ...")
            result = await self.request_youtube_audio(youtube_query)
            if result is None:
                await status.edit("• لا توجد نتائج.")
                return

            path, title = result
            waiting_before = self.queue.qsize() + (1 if self.current else 0)
            track = Track(path=path, status_message=status, title=title)
            await self.queue.put(track)
            if self.player_task is None or self.player_task.done():
                self.player_task = asyncio.create_task(self.player_loop())
            elif self.current is not None:
                await status.edit(
                    f'↢ تمت إضافة "ملف صوتي" إلى قائمة التشغيل.\n'
                    f"↢ عدد الأصوات في الانتظار: {self.queue.qsize()}",
                    buttons=control_buttons(),
                )
            return

        media_message = await self.get_reply_media(event.message)
        if not media_message:
            await event.reply("• يجب أن ترد بكلمة شغل على فيديو أو ملف صوتي.")
            return

        waiting_before = self.queue.qsize() + (1 if self.current else 0)
        status = await event.reply(
            "• جار التشغيل"
            if waiting_before == 0
            else "• جار التحميل ووضعه في قائمة الانتظار"
        )
        try:
            path = await self.download_media(media_message, status)
            track = Track(path=path, status_message=status, title=media_filename(media_message))
            await self.queue.put(track)
            if self.player_task is None or self.player_task.done():
                self.player_task = asyncio.create_task(self.player_loop())
            elif self.current is not None:
                await status.edit(
                    f'↢ تمت إضافة "ملف صوتي" إلى قائمة التشغيل.\n'
                    f"↢ عدد الأصوات في الانتظار: {self.queue.qsize()}",
                    buttons=control_buttons(),
                )
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
