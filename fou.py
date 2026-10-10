# fou.py — /info and /stats
import asyncio
import html as html_lib
import logging
import time
from datetime import datetime, timezone, timedelta

from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════
#  INJECTED FROM bot.py  (DO NOT modify here)
# ══════════════════════════════════════════════════════════
user_collection             = None
chat_collection             = None
admin_collection            = None
warn_collection             = None
global_filter_collection    = None
chat_member_collection      = None
welcome_settings_collection = None

OWNER_IDS      = frozenset()
BOT_START_TIME = 0.0

# Injected helper functions
_is_owner                 = None   # sync  (user_id) -> bool
_is_bot_admin             = None   # async (user_id) -> bool
_is_group_admin           = None   # async (update, user_id) -> bool
_get_cached_group_admins  = None   # sync  (chat_id) -> frozenset | None

# ══════════════════════════════════════════════════════════
#  MESSAGE COUNT BUFFER
# ══════════════════════════════════════════════════════════
_msg_buffer    = {}
_flush_lock    = None
FLUSH_INTERVAL = 60


def init_flush_lock():
    global _flush_lock
    if _flush_lock is None:
        _flush_lock = asyncio.Lock()


def bump_message_count(user_id: int) -> None:
    """Sync — increment in-memory counter. Flushed by flush_loop()."""
    if not user_id:
        return
    _msg_buffer[user_id] = _msg_buffer.get(user_id, 0) + 1


async def flush_message_counts() -> int:
    """Write buffered counts to DB. Returns number of users flushed."""
    global _msg_buffer
    if not _msg_buffer:
        return 0
    if _flush_lock is None:
        init_flush_lock()
    async with _flush_lock:
        batch = _msg_buffer
        _msg_buffer = {}
    flushed = 0
    for uid, cnt in batch.items():
        try:
            await user_collection.update_one(
                {"user_id": uid},
                {"$inc": {"message_count": cnt}},
            )
            flushed += 1
        except Exception as e:
            logger.error(f"flush_message_counts uid={uid} failed: {e}")
    if flushed:
        logger.debug(f"Flushed message counts for {flushed} users.")
    return flushed


async def flush_loop():
    """Background task — flush every FLUSH_INTERVAL seconds."""
    while True:
        try:
            await asyncio.sleep(FLUSH_INTERVAL)
            await flush_message_counts()
        except asyncio.CancelledError:
            try:
                await flush_message_counts()
            except Exception as e:
                logger.error(f"Final flush on cancel failed: {e}")
            raise
        except Exception as e:
            logger.error(f"flush_loop error: {e}")


# ══════════════════════════════════════════════════════════
#  SAFE API WRAPPERS  — never raise
# ══════════════════════════════════════════════════════════
async def _safe_get_member(chat, user_id):
    try:
        return await chat.get_member(user_id)
    except Exception as e:
        logger.debug(f"get_member({user_id}) in {chat.id} failed: {e}")
        return None


async def _safe_get_member_count(chat):
    try:
        return await chat.get_member_count()
    except Exception as e:
        logger.debug(f"get_member_count({chat.id}) failed: {e}")
        return None


async def _safe_resolve_chat(bot, identifier):
    """identifier = int | str(username). Returns Chat/User or None."""
    try:
        if isinstance(identifier, int):
            return await bot.get_chat(identifier)
        s = str(identifier).lstrip("@")
        if s.lstrip("-").isdigit():
            return await bot.get_chat(int(s))
        return await bot.get_chat(f"@{s}")
    except Exception as e:
        logger.debug(f"resolve_chat({identifier}) failed: {e}")
        return None


# ══════════════════════════════════════════════════════════
#  FORMATTERS
# ══════════════════════════════════════════════════════════
def _ensure_utc(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _fmt_ago(dt) -> str:
    dt = _ensure_utc(dt)
    if not dt:
        return "—"
    secs = int((datetime.now(timezone.utc) - dt).total_seconds())
    if secs < 0:
        secs = 0
    if secs < 60:
        return f"{secs}s ago"
    if secs < 3600:
        return f"{secs // 60}m ago"
    if secs < 86400:
        return f"{secs // 3600}h ago"
    return f"{secs // 86400}d ago"


def _fmt_joined(dt) -> str:
    dt = _ensure_utc(dt)
    if not dt:
        return "—"
    days = max(0, (datetime.now(timezone.utc) - dt).days)
    return f"{dt.strftime('%Y-%m-%d')} ({days}d ago)"


def _fmt_uptime(secs: float) -> str:
    secs = max(0, int(secs))
    d, secs = divmod(secs, 86400)
    h, secs = divmod(secs, 3600)
    m, s    = divmod(secs, 60)
    parts = []
    if d: parts.append(f"{d}d")
    if h: parts.append(f"{h}h")
    if m: parts.append(f"{m}m")
    if not parts: parts.append(f"{s}s")
    return " ".join(parts)


def _esc(v) -> str:
    return "" if v is None else html_lib.escape(str(v))


def _profile_link(user_id: int, display: str) -> str:
    return f'<a href="tg://user?id={user_id}">{_esc(display)}</a>'


class _UserStub:
    """Minimal User-like object for DB-only records."""
    def __init__(self, row):
        self.id            = row.get("user_id")
        self.first_name    = row.get("first_name")
        self.last_name     = None
        self.username      = row.get("username")
        self.language_code = None
        self.is_bot        = False
        self.is_premium    = None


# ══════════════════════════════════════════════════════════
#  /info
# ══════════════════════════════════════════════════════════
async def info_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    msg    = update.message
    chat   = update.effective_chat
    caller = update.effective_user

    try:
        # Caller's permission level
        caller_is_admin = False
        try:
            caller_is_admin = (
                _is_owner(caller.id)
                or await _is_bot_admin(caller.id)
                or (chat.type != "private" and await _is_group_admin(update, caller.id))
            )
        except Exception as e:
            logger.debug(f"caller permission check failed: {e}")

        reply = msg.reply_to_message

        # Case 1: reply → channel message
        if reply and reply.sender_chat and not reply.from_user:
            return await _show_channel_info(msg, reply.sender_chat)

        # Case 2: reply → user message
        if reply and reply.from_user:
            return await _show_user_info(
                msg, reply.from_user, chat, caller, caller_is_admin
            )

        # Case 3: /info @username  |  /info 12345
        args = context.args or []
        if args:
            ident    = args[0]
            resolved = await _safe_resolve_chat(context.bot, ident)
            if resolved:
                if getattr(resolved, "type", None) == "channel":
                    return await _show_channel_info(msg, resolved)
                return await _show_user_info(
                    msg, resolved, chat, caller, caller_is_admin
                )
            # Fallback: DB lookup
            row = None
            try:
                if ident.lstrip("-").isdigit():
                    row = await user_collection.find_one({"user_id": int(ident)})
                else:
                    row = await user_collection.find_one({"username": ident.lstrip("@")})
            except Exception as e:
                logger.error(f"info db lookup error: {e}")
            if row:
                return await _show_user_info(
                    msg, _UserStub(row), chat, caller, caller_is_admin
                )
            return await msg.reply_text("❌ User ရှာမတွေ့ပါ။")

        # Case 4: private chat, no arg → self
        if chat.type == "private":
            return await _show_user_info(msg, caller, chat, caller, caller_is_admin)

        # Case 5: group chat, no arg → chat info
        return await _show_chat_info(msg, chat)

    except Exception as e:
        logger.error(f"info_command fatal error: {e}")
        try:
            await msg.reply_text("❌ /info လုပ်ဆောင်ရာတွင် error ဖြစ်ပါသည်။")
        except Exception:
            pass


async def _collect_user_info(user, chat) -> dict:
    """Gather fields. Never raises."""
    info = {
        "id":              getattr(user, "id", None),
        "first_name":      getattr(user, "first_name", None),
        "last_name":       getattr(user, "last_name", None),
        "username":        getattr(user, "username", None),
        "language":        getattr(user, "language_code", None),
        "is_bot":          getattr(user, "is_bot", False),
        "is_premium":      getattr(user, "is_premium", None),
        "warn_count":      0,
        "is_bot_admin":    False,
        "is_owner":        False,
        "is_group_admin":  False,
        "joined_at":       None,
        "message_count":   0,
        "last_seen":       None,
    }

    try:
        info["is_owner"] = _is_owner(user.id)
    except Exception:
        pass

    try:
        info["is_bot_admin"] = await _is_bot_admin(user.id)
    except Exception:
        pass

    # Group admin (live check, target's actual status)
    if chat and chat.type != "private":
        member = await _safe_get_member(chat, user.id)
        if member and member.status in ("administrator", "creator"):
            info["is_group_admin"] = True

    # Warn count (per current chat)
    if warn_collection is not None and chat and chat.type != "private":
        try:
            wdoc = await warn_collection.find_one(
                {"chat_id": chat.id, "user_id": user.id}
            )
            if wdoc:
                info["warn_count"] = wdoc.get("count", 0)
        except Exception as e:
            logger.debug(f"warn lookup failed: {e}")

    # DB fields
    try:
        urow = await user_collection.find_one(
            {"user_id": user.id},
            {"message_count": 1, "last_seen": 1}
        )
        if urow:
            info["message_count"] = urow.get("message_count", 0) or 0
            info["last_seen"]     = urow.get("last_seen")
    except Exception as e:
        logger.debug(f"user db lookup failed: {e}")

    # Joined at (per chat)
    if chat and chat.type != "private" and chat_member_collection is not None:
        try:
            crow = await chat_member_collection.find_one(
                {"chat_id": chat.id, "user_id": user.id}
            )
            if crow:
                info["joined_at"] = crow.get("joined_at")
        except Exception as e:
            logger.debug(f"chat_member lookup failed: {e}")

    return info


def _render_user_info(info: dict, is_self: bool, viewer_is_admin: bool,
                      chat_type: str) -> str:
    show_private = is_self or viewer_is_admin
    user_id      = info["id"]

    name = (info.get("first_name") or "").strip()
    if info.get("last_name"):
        name = f"{name} {info['last_name']}".strip()
    name = name or "—"

    lines = []
    lines.append("╔══════════════════════╗")
    lines.append("   👤  <b>User Info</b>")
    lines.append("╚══════════════════════╝\n")
    lines.append(f"🆔 ID: <code>{user_id}</code>")
    lines.append(f"📛 Name: {_profile_link(user_id, name)}")
    if info.get("username"):
        lines.append(f"🔗 Username: @{_esc(info['username'])}")
    if info.get("language"):
        lines.append(f"🌐 Language: <code>{_esc(info['language'])}</code>")
    if info.get("is_bot"):
        lines.append("🤖 Bot: <b>Yes</b>")
    if info.get("is_premium") is not None:
        lines.append(f"💎 Premium: {'Yes' if info['is_premium'] else 'No'}")

    if chat_type != "private":
        lines.append(f"👮 Group Admin: {'✅' if info.get('is_group_admin') else '❌'}")

    if show_private:
        lines.append("")
        lines.append("━━━━━━━━━━━━━━━")
        if chat_type != "private":
            lines.append(f"⚠️ Warns (this chat): <b>{info.get('warn_count', 0)}/3</b>")
        lines.append(f"🛡 Bot Admin: {'✅' if info.get('is_bot_admin') else '❌'}")
        lines.append(f"👑 Owner: {'✅' if info.get('is_owner') else '❌'}")
        if info.get("joined_at"):
            lines.append(f"📅 Joined: {_fmt_joined(info['joined_at'])}")
        if info.get("message_count") is not None:
            lines.append(f"💬 Messages: <b>{info['message_count']:,}</b>")
        if info.get("last_seen"):
            lines.append(f"🕐 Last seen: {_fmt_ago(info['last_seen'])}")

    return "\n".join(lines)


async def _show_user_info(msg, user, chat, caller, viewer_is_admin: bool):
    try:
        info     = await _collect_user_info(user, chat)
        is_self  = (info.get("id") == getattr(caller, "id", None))
        text     = _render_user_info(info, is_self, viewer_is_admin, chat.type)
        await msg.reply_text(
            text, parse_mode="HTML", disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"_show_user_info error: {e}")
        try:
            await msg.reply_text("❌ User info ရယူရာတွင် error ဖြစ်ပါသည်။")
        except Exception:
            pass


async def _show_channel_info(msg, channel):
    try:
        lines = []
        lines.append("╔══════════════════════╗")
        lines.append("   📢  <b>Channel Info</b>")
        lines.append("╚══════════════════════╝\n")
        lines.append(f"🆔 ID: <code>{channel.id}</code>")
        if getattr(channel, "title", None):
            lines.append(f"📛 Title: {_esc(channel.title)}")
        if getattr(channel, "username", None):
            lines.append(f"🔗 Username: @{_esc(channel.username)}")
        if getattr(channel, "type", None):
            lines.append(f"📁 Type: <code>{_esc(channel.type)}</code>")
        if getattr(channel, "linked_chat_id", None):
            lines.append(f"🔗 Linked chat: <code>{channel.linked_chat_id}</code>")
        await msg.reply_text("\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logger.error(f"_show_channel_info error: {e}")


async def _show_chat_info(msg, chat):
    try:
        lines = []
        lines.append("╔══════════════════════╗")
        lines.append("   💬  <b>Chat Info</b>")
        lines.append("╚══════════════════════╝\n")
        lines.append(f"🆔 ID: <code>{chat.id}</code>")
        if chat.title:
            lines.append(f"📛 Title: {_esc(chat.title)}")
        lines.append(f"📁 Type: <code>{_esc(chat.type)}</code>")
        if getattr(chat, "username", None):
            lines.append(f"🔗 Username: @{_esc(chat.username)}")

        # Member count (1 API call)
        mc = await _safe_get_member_count(chat)
        if mc is not None:
            lines.append(f"👥 Members: <b>{mc:,}</b>")
        else:
            lines.append("👥 Members: <i>N/A</i>")

        # Admin count — prefer cache
        admin_count = None
        cached = None
        if _get_cached_group_admins is not None:
            try:
                cached = _get_cached_group_admins(chat.id)
            except Exception:
                cached = None
        if cached is not None:
            admin_count = len(cached)
        else:
            try:
                admins      = await chat.get_administrators()
                admin_count = len(admins)
            except Exception as e:
                logger.debug(f"get_administrators failed: {e}")
        if admin_count is not None:
            lines.append(f"👮 Admins: <b>{admin_count}</b>")

        # Filter count
        if global_filter_collection is not None:
            try:
                fc = await global_filter_collection.count_documents({"chat_id": chat.id})
                lines.append(f"🔎 Filters: <b>{fc}</b>")
            except Exception:
                pass

        # Greetings set?
        if welcome_settings_collection is not None:
            try:
                w = await welcome_settings_collection.find_one({"chat_id": chat.id})
                flags = []
                if w and "welcome_text" in w: flags.append("Welcome")
                if w and "goodbye_text" in w: flags.append("Goodbye")
                lines.append(f"👋 Greetings: {' + '.join(flags) if flags else '❌'}")
            except Exception:
                pass

        await msg.reply_text("\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logger.error(f"_show_chat_info error: {e}")
        try:
            await msg.reply_text("❌ Chat info ရယူရာတွင် error ဖြစ်ပါသည်။")
        except Exception:
            pass


# ══════════════════════════════════════════════════════════
#  /stats
# ══════════════════════════════════════════════════════════
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    caller = update.effective_user
    try:
        if not (_is_owner(caller.id) or await _is_bot_admin(caller.id)):
            return await update.message.reply_text(
                "❌ Bot owner / admin သာ ကြည့်နိုင်သည်။"
            )
    except Exception as e:
        logger.error(f"stats permission check failed: {e}")
        return

    try:
        cutoff_7d = datetime.now(timezone.utc) - timedelta(days=7)

        tasks = [
            user_collection.count_documents({}),
            chat_collection.count_documents({"type": {"$ne": "private"}}),
            user_collection.count_documents({"last_seen": {"$gte": cutoff_7d}}),
            chat_collection.count_documents({
                "last_seen": {"$gte": cutoff_7d},
                "type": {"$ne": "private"},
            }),
            global_filter_collection.count_documents({}),
            global_filter_collection.count_documents({"chat_id": "global"}),
            admin_collection.count_documents({}),
            warn_collection.count_documents({}),
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        def _val(idx):
            r = results[idx]
            if isinstance(r, Exception):
                logger.error(f"stats count[{idx}] failed: {r}")
                return 0
            return r or 0

        total_users    = _val(0)
        total_groups   = _val(1)
        active_users   = _val(2)
        active_groups  = _val(3)
        total_filters  = _val(4)
        global_filters = _val(5)
        total_admins   = _val(6)
        total_warns    = _val(7)

        uptime = time.time() - (BOT_START_TIME or time.time())

        lines = []
        lines.append("╔══════════════════════╗")
        lines.append("   📊  <b>Bot Statistics</b>")
        lines.append("╚══════════════════════╝\n")
        lines.append(f"⏱ Uptime: <b>{_fmt_uptime(uptime)}</b>\n")

        lines.append("<b>👥 Users</b>")
        lines.append("┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄")
        lines.append(f"   Total:        <b>{total_users:,}</b>")
        lines.append(f"   Active (7d):  <b>{active_users:,}</b>\n")

        lines.append("<b>👥 Groups</b>")
        lines.append("┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄")
        lines.append(f"   Total:        <b>{total_groups:,}</b>")
        lines.append(f"   Active (7d):  <b>{active_groups:,}</b>\n")

        lines.append("<b>🔎 Filters</b>")
        lines.append("┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄")
        lines.append(f"   Total:        <b>{total_filters:,}</b>")
        lines.append(f"   Global:       <b>{global_filters:,}</b>\n")

        lines.append("<b>🛡 Moderation</b>")
        lines.append("┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄")
        lines.append(f"   Bot Admins:   <b>{total_admins:,}</b>")
        lines.append(f"   Total Warns:  <b>{total_warns:,}</b>")

        await update.message.reply_text(
            "\n".join(lines),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception as e:
        logger.error(f"stats_command error: {e}")
        try:
            await update.message.reply_text("❌ Stats ရယူရာတွင် error ဖြစ်ပါသည်။")
        except Exception:
            pass
