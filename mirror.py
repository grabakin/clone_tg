#!/usr/bin/env python3
import asyncio
import getpass
import html as std_html
import logging
import os
import re
import sqlite3
import sys
import tempfile
import time
import json
import urllib.request

from telethon import TelegramClient, events
from telethon.errors import (ChatForwardsRestrictedError, FloodWaitError, MessageNotModifiedError,
                             SessionPasswordNeededError, UserAlreadyParticipantError)
from telethon.extensions import html as tg_html
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto

INVITE_RE = re.compile(r"(?:t\.me/\+|t\.me/joinchat/|tg://join\?invite=)([A-Za-z0-9_-]+)")
TME_C_RE = re.compile(r"(https?://)?t\.me/c/(\d+)/(\d+)")
TME_USER_RE = re.compile(r"(https?://)?t\.me/([A-Za-z0-9_]+)/(\d+)")


class MirrorDB:
    def __init__(self, path):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS message_map (
                src_chat_id INTEGER NOT NULL,
                src_msg_id INTEGER NOT NULL,
                dst_chat_id INTEGER NOT NULL,
                dst_msg_id INTEGER NOT NULL,
                grouped_id INTEGER,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (src_chat_id, src_msg_id)
            )
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_map_grouped ON message_map (src_chat_id, grouped_id)"
        )
        self.conn.commit()
        self.lock = asyncio.Lock()

    async def save_map(self, src_chat_id, src_msg_id, dst_chat_id, dst_msg_id, grouped_id=None):
        async with self.lock:
            self.conn.execute(
                """
                INSERT OR IGNORE INTO message_map
                (src_chat_id, src_msg_id, dst_chat_id, dst_msg_id, grouped_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (src_chat_id, src_msg_id, dst_chat_id, dst_msg_id, grouped_id, int(time.time())),
            )
            self.conn.commit()

    async def get_dest_id(self, src_chat_id, src_msg_id):
        async with self.lock:
            row = self.conn.execute(
                "SELECT dst_msg_id FROM message_map WHERE src_chat_id=? AND src_msg_id=?",
                (src_chat_id, src_msg_id),
            ).fetchone()
        return row[0] if row else None

    async def get_dest_ids(self, src_chat_id, src_ids):
        if not src_ids:
            return {}
        placeholders = ",".join("?" for _ in src_ids)
        query = (
            "SELECT src_msg_id, dst_msg_id FROM message_map "
            f"WHERE src_chat_id=? AND src_msg_id IN ({placeholders})"
        )
        params = [src_chat_id, *src_ids]
        async with self.lock:
            rows = self.conn.execute(query, params).fetchall()
        return {row[0]: row[1] for row in rows}

    async def delete_by_src_ids(self, src_chat_id, src_ids):
        if not src_ids:
            return
        placeholders = ",".join("?" for _ in src_ids)
        query = f"DELETE FROM message_map WHERE src_chat_id=? AND src_msg_id IN ({placeholders})"
        params = [src_chat_id, *src_ids]
        async with self.lock:
            self.conn.execute(query, params)
            self.conn.commit()

    async def get_last_src_id(self, src_chat_id):
        async with self.lock:
            row = self.conn.execute(
                "SELECT MAX(src_msg_id) FROM message_map WHERE src_chat_id=?",
                (src_chat_id,),
            ).fetchone()
        return row[0] if row and row[0] is not None else None

    async def get_map_batch(self, src_chat_id, last_id, limit):
        async with self.lock:
            rows = self.conn.execute(
                """
                SELECT src_msg_id, dst_msg_id
                FROM message_map
                WHERE src_chat_id=? AND src_msg_id>?
                ORDER BY src_msg_id
                LIMIT ?
                """,
                (src_chat_id, last_id, limit),
            ).fetchall()
        return rows


class MirrorConfig:
    def __init__(self):
        data_dir = _default_data_dir()
        self.data_dir = data_dir
        self.api_id = _prompt_int("API_ID", _env_int("API_ID"), required=True)
        self.api_hash = _prompt_value("API_HASH", _env_value("API_HASH"), required=True)
        self.session_name = os.getenv("SESSION_NAME", os.path.join(data_dir, "mirror"))
        self.db_path = os.getenv("DB_PATH", os.path.join(data_dir, "mirror.db"))
        self.run_mode = _prompt_run_mode(_env_value("RUN_MODE"))
        if self.run_mode == "continuous":
            self.mirror_edits = _prompt_bool(
                "Mirror edits",
                _env_bool("MIRROR_EDITS", False),
            )
            self.mirror_deletes = _prompt_bool(
                "Mirror deletes",
                _env_bool("MIRROR_DELETES", False),
            )
        else:
            self.mirror_edits = False
            self.mirror_deletes = False
        self.preflight = _prompt_bool("Run network check", True)
        self.log_level = os.getenv("LOG_LEVEL", "INFO")


def _env_value(name):
    value = os.getenv(name)
    if value:
        return value
    return None


def _env_int(name):
    value = _env_value(name)
    if not value:
        return None
    try:
        return int(value)
    except ValueError as exc:
        raise SystemExit(f"Env var {name} must be an integer") from exc


def _env_bool(name, default=None):
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _default_data_dir():
    env_dir = os.getenv("DATA_DIR")
    if env_dir:
        os.makedirs(env_dir, exist_ok=True)
        return env_dir
    if os.path.isdir("/data"):
        return "/data"
    base_dir = os.path.dirname(
        sys.executable if getattr(sys, "frozen", False) else os.path.abspath(__file__)
    )
    data_dir = os.path.join(base_dir, "data")
    try:
        os.makedirs(data_dir, exist_ok=True)
        return data_dir
    except OSError:
        return os.getcwd()


def _prompt_value(label, default=None, secret=False, required=False):
    while True:
        prompt = f"{label}"
        if default is not None:
            prompt += f" [{default}]"
        prompt += ": "
        if secret:
            value = getpass.getpass(prompt)
        else:
            value = input(prompt)
        value = value.strip()
        if not value:
            if default is not None:
                return str(default)
            if required:
                print("Value is required.")
                continue
        return value


def _prompt_int(label, default=None, required=False):
    while True:
        raw = _prompt_value(label, default, required=required)
        if not raw:
            if required:
                continue
            return default
        try:
            return int(raw)
        except ValueError:
            print("Please enter a number.")


def _prompt_bool(label, default=False):
    suffix = "Y/n" if default else "y/N"
    while True:
        raw = _prompt_value(f"{label} [{suffix}]").lower()
        if not raw:
            return default
        if raw in {"y", "yes"}:
            return True
        if raw in {"n", "no"}:
            return False
        print("Please answer y or n.")


def _prompt_run_mode(raw_default=None):
    default = _parse_run_mode(raw_default) or "oneshot"
    while True:
        raw = _prompt_value("Run mode (oneshot/continuous/links)", default).lower()
        mode = _parse_run_mode(raw)
        if mode:
            return mode
        print("Please enter 'oneshot', 'continuous', or 'links'.")


def _ensure_tmp_dir(data_dir):
    path = os.path.join(data_dir, "tmp")
    os.makedirs(path, exist_ok=True)
    return path


def _cleanup_file(path):
    if not path:
        return
    try:
        os.unlink(path)
    except OSError:
        pass


def _guess_media_suffix(message):
    file_obj = getattr(message, "file", None)
    ext = getattr(file_obj, "ext", None)
    if not ext:
        return ".bin"
    if ext.startswith("."):
        return ext
    return f".{ext}"


def _safe_filename(name, default_suffix):
    if not name:
        return None
    name = os.path.basename(name)
    name = name.replace("\x00", "")
    name = name.replace("/", "_").replace("\\", "_").strip()
    if not name:
        return None
    root, ext = os.path.splitext(name)
    if not ext and default_suffix:
        ext = default_suffix if default_suffix.startswith(".") else f".{default_suffix}"
        name = f"{name}{ext}"
    max_len = 180
    if len(name) > max_len:
        root, ext = os.path.splitext(name)
        keep = max_len - len(ext)
        name = f"{root[:keep]}{ext}"
    return name


def _unique_path(directory, filename, message_id=None):
    path = os.path.join(directory, filename)
    if not os.path.exists(path):
        return path
    base, ext = os.path.splitext(filename)
    suffix = str(message_id or int(time.time() * 1000))
    candidate = os.path.join(directory, f"{base}_{suffix}{ext}")
    if not os.path.exists(candidate):
        return candidate
    for index in range(1, 1000):
        candidate = os.path.join(directory, f"{base}_{suffix}_{index}{ext}")
        if not os.path.exists(candidate):
            return candidate
    return os.path.join(directory, f"{base}_{suffix}_{int(time.time() * 1000)}{ext}")


def _format_bytes(value):
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(value)
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            return f"{size:.1f}{unit}"
        size /= 1024.0
    return f"{size:.1f}TB"


class ProgressBar:
    def __init__(self, label, total=None, enabled=True):
        self.label = label
        self.total = total or 0
        self.enabled = enabled and sys.stderr.isatty()
        self.start = time.monotonic()
        self.last_print = 0.0
        self.last_current = 0
        self.done = False

    def update(self, current, total):
        if total:
            self.total = total
        if not self.enabled:
            return
        now = time.monotonic()
        if now - self.last_print < 0.2:
            return
        self.last_print = now
        self.last_current = current

        elapsed = max(now - self.start, 0.001)
        speed = current / elapsed
        eta = None
        if self.total and speed > 0:
            eta = int((self.total - current) / speed)

        if self.total:
            pct = min(current / self.total, 1.0)
            bar_len = 24
            filled = int(bar_len * pct)
            bar = "#" * filled + "-" * (bar_len - filled)
            eta_str = f" eta {eta}s" if eta is not None else ""
            line = (
                f"\r{self.label} [{bar}] {pct*100:5.1f}% "
                f"{_format_bytes(current)}/{_format_bytes(self.total)} "
                f"{_format_bytes(speed)}/s{eta_str}"
            )
        else:
            line = (
                f"\r{self.label} {_format_bytes(current)} "
                f"{_format_bytes(speed)}/s"
            )
        sys.stderr.write(line)
        sys.stderr.flush()
        if self.total and current >= self.total:
            self.finish()

    def finish(self):
        if self.done or not self.enabled or self.last_print == 0.0:
            return
        self.done = True
        sys.stderr.write("\n")
        sys.stderr.flush()


class AlbumProgress:
    def __init__(self, label, count, enabled=True):
        self.label = label
        self.count = count
        self.index = 1
        self.enabled = enabled
        self.bar = ProgressBar(f"{self.label} {self.index}/{self.count}", enabled=enabled)
        self.last_current = 0
        self.last_total = None

    def update(self, current, total):
        if self.last_total is not None and current < self.last_current:
            self.bar.finish()
            self.index = min(self.index + 1, self.count)
            self.bar = ProgressBar(
                f"{self.label} {self.index}/{self.count}",
                total=total,
                enabled=self.enabled,
            )
        self.bar.update(current, total)
        self.last_current = current
        self.last_total = total

    def finish(self):
        self.bar.finish()


async def _download_media_to_file(message, temp_dir):
    suffix = _guess_media_suffix(message)
    original_name = getattr(getattr(message, "file", None), "name", None)
    safe_name = _safe_filename(original_name, suffix)
    if safe_name:
        path = _unique_path(temp_dir, safe_name, message.id)
    else:
        fd, path = tempfile.mkstemp(prefix="tg_media_", suffix=suffix, dir=temp_dir)
        os.close(fd)
    try:
        size = getattr(getattr(message, "file", None), "size", None)
        progress = ProgressBar(f"Download {message.id}", total=size)
        result = await message.download_media(file=path, progress_callback=progress.update)
        progress.finish()
        if not result:
            raise RuntimeError("Download failed")
        return path
    except Exception:
        _cleanup_file(path)
        raise


def _parse_run_mode(value):
    if not value:
        return None
    raw = value.strip().lower()
    if raw in {"oneshot", "one-shot", "once", "one", "1"}:
        return "oneshot"
    if raw in {"continuous", "cont", "live", "follow", "2"}:
        return "continuous"
    if raw in {"links", "link", "relink", "update-links", "update_links", "3"}:
        return "links"
    return None


def _public_ip_check(timeout=5):
    try:
        with urllib.request.urlopen("https://api.ipify.org?format=json", timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            return payload.get("ip")
    except Exception as exc:
        logging.warning("Public IP check failed: %s", exc)
        return None


def _network_preflight():
    logging.info("Network check: starting")
    proxies = {}
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"):
        value = os.getenv(key) or os.getenv(key.lower())
        if value:
            proxies[key] = value
    if proxies:
        logging.info("Proxy env detected: %s", ", ".join(f"{k}={v}" for k, v in proxies.items()))
    ip = _public_ip_check()
    if ip:
        logging.info("Public IP: %s", ip)
        logging.info("If VPN is enabled, verify this IP matches your VPN exit.")
    else:
        logging.info("Public IP: unavailable")


def _load_dotenv(path=".env"):
    if not os.path.isfile(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip("\"'")
                if key and key not in os.environ:
                    os.environ[key] = value
    except OSError as exc:
        logging.warning("Failed to read %s: %s", path, exc)


def _extract_invite_hash(spec):
    match = INVITE_RE.search(spec)
    return match.group(1) if match else None


def _render_text(message):
    text = message.message or ""
    entities = message.entities or []
    if not text:
        return "", None
    if entities:
        return tg_html.unparse(text, entities), "html"
    return std_html.escape(text), "html"


def _supported_media(message):
    return isinstance(message.media, (MessageMediaPhoto, MessageMediaDocument))


def _normalize_sent_messages(sent):
    if isinstance(sent, list):
        return sent
    return [sent]


def _channel_internal_id(chat_id):
    raw = str(chat_id)
    if raw.startswith("-100"):
        return raw[4:]
    if raw.startswith("-"):
        return raw[1:]
    return raw


def _build_dest_link(dest_username, dest_internal_id, dest_msg_id, prefix):
    base = (
        f"t.me/{dest_username}/{dest_msg_id}"
        if dest_username
        else f"t.me/c/{dest_internal_id}/{dest_msg_id}"
    )
    if prefix:
        return f"{prefix}{base}"
    return base


async def _rewrite_links(html_text, source, dest, db):
    if not html_text:
        return html_text
    source_internal_id = _channel_internal_id(source.id)
    dest_internal_id = _channel_internal_id(dest.id)
    source_username = getattr(source, "username", None)
    dest_username = getattr(dest, "username", None)

    msg_ids = set()
    for match in TME_C_RE.finditer(html_text):
        if match.group(2) == source_internal_id:
            msg_ids.add(int(match.group(3)))
    if source_username:
        src_user_lower = source_username.lower()
        for match in TME_USER_RE.finditer(html_text):
            if match.group(2).lower() == src_user_lower:
                msg_ids.add(int(match.group(3)))
    if not msg_ids:
        return html_text

    mapping = await db.get_dest_ids(source.id, list(msg_ids))
    if not mapping:
        return html_text

    def replace_c(match):
        if match.group(2) != source_internal_id:
            return match.group(0)
        src_msg_id = int(match.group(3))
        dest_msg_id = mapping.get(src_msg_id)
        if not dest_msg_id:
            return match.group(0)
        return _build_dest_link(dest_username, dest_internal_id, dest_msg_id, match.group(1))

    def replace_user(match):
        if not source_username or match.group(2).lower() != source_username.lower():
            return match.group(0)
        src_msg_id = int(match.group(3))
        dest_msg_id = mapping.get(src_msg_id)
        if not dest_msg_id:
            return match.group(0)
        return _build_dest_link(dest_username, dest_internal_id, dest_msg_id, match.group(1))

    updated = TME_C_RE.sub(replace_c, html_text)
    updated = TME_USER_RE.sub(replace_user, updated)
    return updated


async def _run_with_flood_wait(func, *args, **kwargs):
    while True:
        try:
            return await func(*args, **kwargs)
        except FloodWaitError as exc:
            wait_for = exc.seconds + 1
            logging.warning("FloodWait %ss; sleeping", wait_for)
            await asyncio.sleep(wait_for)


async def _ensure_login(client):
    if await client.is_user_authorized():
        return
    method = _prompt_value("Login method (qr/phone)", "qr").lower()
    if method.startswith("p"):
        await _login_phone(client)
    else:
        await _login_qr(client)


async def _login_qr(client):
    logging.info("Not authorized; starting QR login")
    qr_login = await client.qr_login()
    qr_url = qr_login.url
    print("Scan this QR code with Telegram (Settings -> Devices -> Scan QR)")
    print(qr_url)
    try:
        import qrcode

        qr = qrcode.QRCode()
        qr.add_data(qr_url)
        qr.print_ascii(invert=True)
    except Exception as exc:  # qrcode optional
        logging.warning("QR rendering unavailable: %s", exc)
    try:
        await qr_login.wait()
    except SessionPasswordNeededError:
        password = _prompt_value("Enter 2FA password", secret=True)
        await client.sign_in(password=password)


async def _login_phone(client):
    logging.info("Not authorized; starting phone login")
    phone = _prompt_value("Phone number (international format)")
    await client.send_code_request(phone)
    code = _prompt_value("Enter the code from Telegram")
    try:
        await client.sign_in(phone=phone, code=code)
    except SessionPasswordNeededError:
        password = _prompt_value("Enter 2FA password", secret=True)
        await client.sign_in(password=password)


async def _resolve_channel(client, spec):
    if isinstance(spec, int):
        return await client.get_entity(spec)
    if spec.lstrip("-").isdigit():
        return await client.get_entity(int(spec))
    invite_hash = _extract_invite_hash(spec)
    if invite_hash:
        try:
            result = await client(ImportChatInviteRequest(invite_hash))
            if result.chats:
                return result.chats[0]
        except UserAlreadyParticipantError:
            return await client.get_entity(spec)
    return await client.get_entity(spec)


async def _choose_channel(client, label, spec=None):
    if spec:
        return await _resolve_channel(client, spec)
    while True:
        raw = _prompt_value(
            f"{label} (enter @username, -100... id, invite link, or 'list')",
            "list",
        ).strip()
        if raw.lower() in {"list", "l"}:
            chosen = await _select_from_dialogs(client, label)
            if chosen is not None:
                return chosen
            continue
        try:
            return await _resolve_channel(client, raw)
        except Exception as exc:
            logging.warning("Failed to resolve channel: %s", exc)


async def _select_from_dialogs(client, label):
    dialogs = []
    async for dialog in client.iter_dialogs():
        if dialog.is_channel or dialog.is_group or dialog.is_user:
            dialogs.append(dialog)
    if not dialogs:
        print("No chats available in dialogs.")
        return None
    print(f"{label} selection:")
    for idx, dialog in enumerate(dialogs[:50], 1):
        entity = dialog.entity
        username = getattr(entity, "username", None)
        name = dialog.name or "Untitled"
        if dialog.is_group:
            kind = "group"
        elif dialog.is_channel:
            kind = "channel"
        elif dialog.is_user:
            kind = "user"
        else:
            kind = "chat"
        hint = f"@{username}" if username else "private"
        print(f"{idx}. {name} ({kind}, {hint}) id={entity.id}")
    raw = _prompt_value("Choose by number").strip()
    if not raw.isdigit():
        print("Invalid selection.")
        return None
    index = int(raw)
    if index < 1 or index > min(50, len(dialogs)):
        print("Selection out of range.")
        return None
    return dialogs[index - 1].entity


async def main():
    _load_dotenv()
    config = MirrorConfig()
    logging.basicConfig(
        level=getattr(logging, config.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    temp_dir = _ensure_tmp_dir(config.data_dir)

    client = TelegramClient(config.session_name, config.api_id, config.api_hash)
    await client.connect()
    await _ensure_login(client)

    if config.preflight:
        _network_preflight()

    source = await _choose_channel(client, "Source chat", _env_value("CHANNEL_A"))
    dest = await _choose_channel(client, "Destination chat", _env_value("CHANNEL_B"))

    if source.id == dest.id:
        logging.error("Source and destination channels are the same")
        raise SystemExit(1)

    db = MirrorDB(config.db_path)

    source_id = source.id
    dest_id = dest.id

    logging.info("Mirroring from %s (%s) to %s (%s)", getattr(source, "title", "?"), source_id,
                 getattr(dest, "title", "?"), dest_id)

    async def send_media_with_fallback(message, text, parse_mode):
        try:
            size = getattr(getattr(message, "file", None), "size", None)
            progress = ProgressBar(f"Upload {message.id}", total=size)
            try:
                result = await _run_with_flood_wait(
                    client.send_file,
                    dest,
                    message.media,
                    caption=text,
                    parse_mode=parse_mode,
                    progress_callback=progress.update,
                )
            finally:
                progress.finish()
            return result
        except ChatForwardsRestrictedError:
            logging.info("Protected content; re-uploading media for message %s", message.id)
            path = await _download_media_to_file(message, temp_dir)
            try:
                size = os.path.getsize(path)
                progress = ProgressBar(f"Upload {message.id}", total=size)
                try:
                    result = await _run_with_flood_wait(
                        client.send_file,
                        dest,
                        path,
                        caption=text,
                        parse_mode=parse_mode,
                        progress_callback=progress.update,
                    )
                finally:
                    progress.finish()
                return result
            finally:
                _cleanup_file(path)

    async def send_album_with_fallback(messages, captions, parse_mode):
        try:
            progress = AlbumProgress("Upload album", len(messages))
            try:
                result = await _run_with_flood_wait(
                    client.send_file,
                    dest,
                    [m.media for m in messages],
                    caption=captions,
                    parse_mode=parse_mode,
                    progress_callback=progress.update,
                )
            finally:
                progress.finish()
            return result
        except ChatForwardsRestrictedError:
            logging.info("Protected content; re-uploading album %s", messages[0].grouped_id)
            files = []
            try:
                progress = AlbumProgress("Upload album", len(messages))
                for msg in messages:
                    files.append(await _download_media_to_file(msg, temp_dir))
                try:
                    result = await _run_with_flood_wait(
                        client.send_file,
                        dest,
                        files,
                        caption=captions,
                        parse_mode=parse_mode,
                        progress_callback=progress.update,
                    )
                finally:
                    progress.finish()
                return result
            finally:
                for path in files:
                    _cleanup_file(path)

    async def mirror_single(message):
        if await db.get_dest_id(source_id, message.id):
            logging.info("Skipping already mirrored message %s", message.id)
            return
        text, parse_mode = _render_text(message)
        text = await _rewrite_links(text, source, dest, db)
        sent = None
        if message.media and _supported_media(message):
            sent = await send_media_with_fallback(message, text, parse_mode)
        elif text:
            sent = await _run_with_flood_wait(
                client.send_message,
                dest,
                text,
                parse_mode=parse_mode,
            )
        else:
            logging.info("Skipping unsupported or empty message %s", message.id)
            return
        await db.save_map(source_id, message.id, dest_id, sent.id, message.grouped_id)
        logging.info("Mirrored message %s -> %s", message.id, sent.id)

    async def mirror_album(event):
        await mirror_album_messages(event.messages)

    async def mirror_album_messages(messages):
        messages_sorted = sorted(messages, key=lambda m: m.id)
        supported = [m for m in messages_sorted if m.media and _supported_media(m)]
        if not supported:
            logging.info("Skipping album with no supported media")
            return
        already = await db.get_dest_id(source_id, supported[0].id)
        if already:
            logging.info("Skipping already mirrored album %s", supported[0].grouped_id)
            return
        captions = []
        for msg in supported:
            text, _ = _render_text(msg)
            text = await _rewrite_links(text, source, dest, db)
            captions.append(text)
        parse_mode = "html" if any(captions) else None
        sent = await send_album_with_fallback(supported, captions, parse_mode)
        sent_messages = _normalize_sent_messages(sent)
        if len(sent_messages) != len(supported):
            logging.warning("Album size mismatch: sent %s, expected %s", len(sent_messages), len(supported))
        for src_msg, dst_msg in zip(supported, sent_messages):
            await db.save_map(source_id, src_msg.id, dest_id, dst_msg.id, src_msg.grouped_id)
        logging.info("Mirrored album %s (%s items)", supported[0].grouped_id, len(supported))

    async def on_new_message(event):
        if event.message.grouped_id:
            return
        try:
            await mirror_single(event.message)
        except Exception:
            logging.exception("Failed to mirror message %s", event.message.id)

    async def on_album(event):
        try:
            await mirror_album(event)
        except Exception:
            logging.exception("Failed to mirror album %s", event.messages[0].grouped_id)

    async def on_edit(event):
        message = event.message
        try:
            dest_msg_id = await db.get_dest_id(source_id, message.id)
            if not dest_msg_id:
                return
            text, parse_mode = _render_text(message)
            if not text:
                return
            await _run_with_flood_wait(
                client.edit_message,
                dest,
                dest_msg_id,
                text,
                parse_mode=parse_mode,
            )
            logging.info("Edited message %s -> %s", message.id, dest_msg_id)
        except MessageNotModifiedError:
            return
        except Exception:
            logging.exception("Failed to mirror edit %s", message.id)

    async def on_delete(event):
        try:
            dest_map = await db.get_dest_ids(source_id, event.deleted_ids)
            if not dest_map:
                return
            dest_ids = list(dest_map.values())
            await _run_with_flood_wait(client.delete_messages, dest, dest_ids)
            await db.delete_by_src_ids(source_id, list(dest_map.keys()))
            logging.info("Deleted %s mirrored messages", len(dest_ids))
        except Exception:
            logging.exception("Failed to mirror delete for %s", event.deleted_ids)

    async def backfill_history():
        last_id = await db.get_last_src_id(source_id)
        if last_id:
            logging.info("Backfill: starting from id > %s", last_id)
        else:
            logging.info("Backfill: starting from beginning")
        current_group_id = None
        current_group = []
        processed = 0
        async for message in client.iter_messages(source, reverse=True, min_id=last_id or 0):
            if message.grouped_id:
                if current_group_id is None:
                    current_group_id = message.grouped_id
                if message.grouped_id != current_group_id:
                    try:
                        await mirror_album_messages(current_group)
                    except Exception:
                        logging.exception("Failed to mirror album %s", current_group_id)
                    processed += len(current_group)
                    current_group = []
                    current_group_id = message.grouped_id
                current_group.append(message)
                continue
            if current_group:
                try:
                    await mirror_album_messages(current_group)
                except Exception:
                    logging.exception("Failed to mirror album %s", current_group_id)
                processed += len(current_group)
                current_group = []
                current_group_id = None
            try:
                await mirror_single(message)
            except Exception:
                logging.exception("Failed to mirror message %s", message.id)
            processed += 1
            if processed and processed % 100 == 0:
                logging.info("Backfill progress: %s messages processed", processed)
        if current_group:
            try:
                await mirror_album_messages(current_group)
            except Exception:
                logging.exception("Failed to mirror album %s", current_group_id)
            processed += len(current_group)
        logging.info("Backfill done: processed %s messages", processed)

    async def update_links():
        logging.info("Updating links in mirrored messages...")
        last_id = 0
        processed = 0
        updated = 0
        batch_size = 200
        while True:
            rows = await db.get_map_batch(source_id, last_id, batch_size)
            if not rows:
                break
            dest_ids = [row[1] for row in rows]
            dest_messages = await _run_with_flood_wait(client.get_messages, dest, ids=dest_ids)
            if not isinstance(dest_messages, list):
                dest_messages = [dest_messages]
            dest_by_id = {msg.id: msg for msg in dest_messages if msg}

            for src_id, dst_id in rows:
                message = dest_by_id.get(dst_id)
                if not message:
                    continue
                text, parse_mode = _render_text(message)
                if not text:
                    continue
                new_text = await _rewrite_links(text, source, dest, db)
                if new_text == text:
                    continue
                try:
                    await _run_with_flood_wait(
                        client.edit_message,
                        dest,
                        dst_id,
                        new_text,
                        parse_mode=parse_mode,
                    )
                    updated += 1
                except MessageNotModifiedError:
                    continue
                except Exception:
                    logging.exception("Failed to update links for dest message %s", dst_id)
                processed += 1
                if processed and processed % 200 == 0:
                    logging.info("Link update progress: %s processed, %s updated", processed, updated)
            last_id = rows[-1][0]

        logging.info("Link update done: %s updated", updated)

    if config.run_mode == "oneshot":
        await backfill_history()
        await client.disconnect()
        return
    if config.run_mode == "links":
        await update_links()
        await client.disconnect()
        return

    client.add_event_handler(on_album, events.Album(chats=source))
    client.add_event_handler(on_new_message, events.NewMessage(chats=source))
    if config.mirror_edits:
        client.add_event_handler(on_edit, events.MessageEdited(chats=source))
    if config.mirror_deletes:
        client.add_event_handler(on_delete, events.MessageDeleted(chats=source))

    logging.info("Listening for new messages...")
    await client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
