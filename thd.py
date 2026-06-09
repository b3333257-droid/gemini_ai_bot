# thd.py (fully async – Motor compatible, all original features preserved)
import html as html_lib
import logging
import os
from datetime import datetime, timezone
from pymongo.errors import PyMongoError
from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)

# ── DB (injected by bot.py) ────────────────────────────────
welcome_settings_collection = None
admin_collection            = None   # ← bot.py မှ inject လုပ်မည်

# ── Owner IDs (injected by bot.py) ─────────────────────────
OWNER_IDS: frozenset = frozenset()

# ══════════════════════════════════════════════════════════
#  INIT (now async)
# ══════════════════════════════════════════════════════════

async def init_db(db) -> None:
    """Initialize welcome_settings collection and create index asynchronously."""
    global welcome_settings_collection
    try:
        welcome_settings_collection = db["welcome_settings"]
        # Motor: create_index returns a coroutine, must be awaited
        await welcome_settings_collection.create_index("chat_id", unique=True)
        logger.info("thd.py: DB initialized.")
    except PyMongoError as e:
        logger.critical(f"thd.py init_db DB error: {e}")
        raise
    except Exception as e:
        logger.critical(f"thd.py init_db unexpected error: {e}")
        raise

# ══════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════

def get_owner_ids() -> frozenset:
    """Return injected OWNER_IDS; fall back to env vars."""
    if OWNER_IDS:
        return OWNER_IDS
    ids = set()
    for key in ("OWNER_ID", "OWNER_ID_2"):
        try:
            v = int(os.environ.get(key, 0))
            if v:
                ids.add(v)
        except (ValueError, TypeError):
            pass
    return frozenset(ids)

def is_owner(user_id: int) -> bool:
    return user_id in get_owner_ids()

async def is_bot_admin(user_id: int) -> bool:
    """Bot admin collection မှာ ရှိမရှိ စစ်ဆေး။"""
    if admin_collection is None:
        return False
    try:
        doc = await admin_collection.find_one({"user_id": user_id})
        return doc is not None
    except Exception as e:
        logger.error(f"is_bot_admin check error: {e}")
        return False

async def get_welcome_settings(chat_id) -> dict | None:
    """
    Return settings dict with field-wise fallback to global.
    Motor requires await for find_one operations.
    """
    if welcome_settings_collection is None:
        logger.error("welcome_settings_collection not initialized.")
        return None
    try:
        local = await welcome_settings_collection.find_one({"chat_id": chat_id})
        global_doc = await welcome_settings_collection.find_one({"chat_id": "global"})

        # If no local settings, use global (might be None)
        if not local:
            return global_doc

        # If we have local, fill missing fields from global
        if global_doc:
            for key in ("welcome_text", "goodbye_text"):
                if key not in local and key in global_doc:
                    local[key] = global_doc[key]
        return local

    except PyMongoError as e:
        logger.error(f"DB error fetching welcome settings ({chat_id}): {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching welcome settings: {e}")
        return None

def format_message_with_placeholders(text: str, user, chat) -> str:
    """
    Replace placeholders in welcome/goodbye text:
      {name}  → clickable mention (HTML link)
      {id}    → user ID
      {group} → group title (escaped)
      {gp}    → group @username (if exists), else group title
      (@)     → clickable @username or first name (HTML link)
      {(@)}   → same as (@)
      {@}     → same as (@)
    """
    if not text:
        return ""
    try:
        # ① Escape the entire template first to prevent HTML injection
        escaped = html_lib.escape(text)

        # Prepare replacements
        # user mention (already HTML)
        user_mention = (user.mention_html()
                        if hasattr(user, "mention_html")
                        else html_lib.escape(getattr(user, "full_name", "") or ""))
        user_id_str = str(user.id)

        # Group title (escape to be safe)
        group_title = html_lib.escape(str(chat.title)) if chat.title else ""

        # ② Username replacement as clickable HTML anchor (fixes space issue)
        username = getattr(user, "username", None)
        if username:
            at_html = f'<a href="tg://user?id={user.id}">@{username}</a>'
        else:
            first_name = getattr(user, "first_name", "") or ""
            at_html = f'<a href="tg://user?id={user.id}">{html_lib.escape(first_name)}</a>'

        # ③ {gp} placeholder: group username or fallback to title
        if getattr(chat, "username", None):
            gp_text = f"@{chat.username}"
        else:
            gp_text = group_title  # already escaped

        # Replace all placeholders (order does not matter after escaping)
        escaped = escaped.replace("{name}", user_mention)
        escaped = escaped.replace("{id}", user_id_str)
        escaped = escaped.replace("{group}", group_title)
        escaped = escaped.replace("{gp}", gp_text)
        escaped = escaped.replace("(@)", at_html)
        escaped = escaped.replace("{(@)}", at_html)
        escaped = escaped.replace("{@}", at_html)
        return escaped
    except Exception as e:
        logger.error(f"format_message_with_placeholders error: {e}")
        return text

# ══════════════════════════════════════════════════════════
#  PERMISSION GUARD (shared for welcome/goodbye commands)
# ══════════════════════════════════════════════════════════

async def _resolve_scope(update: Update) -> tuple[str | int | None, str]:
    """
    Returns (target_chat_id, scope_label) or (None, "") on permission failure.
    Sends the error reply itself if permission is denied.

    Scope logic (filter system နဲ့ တူညီ):
      - Owner / Bot admin  → global scope (private chat မှာဖြစ်စေ group မှာဖြစ်စေ)
      - Group admin/creator (group ထဲမှာ command လုပ်ရင်) → local scope (that group only)
      - ကျန်သူများ → ပငျဆငျး
    """
    user_id = update.effective_user.id
    chat    = update.effective_chat

    # Private chat မှာ command လုပ်ရင် → global scope (owner / bot admin သာ)
    if chat.type == "private":
        if is_owner(user_id) or await is_bot_admin(user_id):
            return "global", "🌐 Global"
        await update.message.reply_text("❌ Bot admin / owner သာ global setting ပြောင်းနိုင်သည်။")
        return None, ""

    # Group မှာ command လုပ်ရင် → scope ခွဲ
    else:
        # Owner / Bot admin → global scope
        if is_owner(user_id) or await is_bot_admin(user_id):
            return "global", "🌐 Global"

        # Group admin / creator → local scope (that group only)
        try:
            member = await chat.get_member(user_id)
            if member.status in ("administrator", "creator"):
                return chat.id, f"💬 {chat.title}"
        except Exception as e:
            logger.error(f"_resolve_scope get_member error: {e}")

        await update.message.reply_text("❌ Group admin သာ ပြောင်းနိုင်သည်။")
        return None, ""

def _get_text_from_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> str | None:
    """Extract text from reply_to_message or command args. Returns None if empty."""
    if update.message.reply_to_message:
        return (update.message.reply_to_message.text
                or update.message.reply_to_message.caption
                or None)
    joined = " ".join(context.args) if context.args else ""
    return joined if joined.strip() else None

# ══════════════════════════════════════════════════════════
#  SETWELCOME / SETGOODBYE
# ══════════════════════════════════════════════════════════

async def set_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if welcome_settings_collection is None:
        return await update.message.reply_text("❌ Database ချိတ်ဆက်မှု မရှိပါ။")
    try:
        target_chat_id, scope_text = await _resolve_scope(update)
        if target_chat_id is None:
            return

        text = _get_text_from_message(update, context)
        if not text:
            return await update.message.reply_text(
                "❌ Welcome စာသားထည့်ပေးပါ။\n"
                "<b>Placeholders:</b> <code>{name}</code>  <code>{id}</code>  "
                "<code>{group}</code>  <code>(@)</code>",
                parse_mode="HTML"
            )

        # Motor: update_one must be awaited
        await welcome_settings_collection.update_one(
            {"chat_id": target_chat_id},
            {"$set": {"welcome_text": text, "updated_at": datetime.now(timezone.utc)}},
            upsert=True
        )
        await update.message.reply_text(
            f"✅ <b>Welcome message သတ်မှတ်ပြီးပါပြီ</b>\n"
            f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
            f"📍 Scope: {scope_text}\n\n"
            f"<b>Preview:</b>\n"
            f"<i>{html_lib.escape(text)}</i>",
            parse_mode="HTML"
        )
    except PyMongoError as e:
        logger.error(f"set_welcome DB error: {e}")
        await update.message.reply_text("❌ Database error ဖြစ်ပါသည်။")
    except Exception as e:
        logger.error(f"set_welcome error: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

async def set_goodbye(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if welcome_settings_collection is None:
        return await update.message.reply_text("❌ Database ချိတ်ဆက်မှု မရှိပါ။")
    try:
        target_chat_id, scope_text = await _resolve_scope(update)
        if target_chat_id is None:
            return

        text = _get_text_from_message(update, context)
        if not text:
            return await update.message.reply_text(
                "❌ Goodbye စာသားထည့်ပေးပါ။\n"
                "<b>Placeholders:</b> <code>{name}</code>  <code>{id}</code>  "
                "<code>{group}</code>  <code>(@)</code>",
                parse_mode="HTML"
            )

        await welcome_settings_collection.update_one(
            {"chat_id": target_chat_id},
            {"$set": {"goodbye_text": text, "updated_at": datetime.now(timezone.utc)}},
            upsert=True
        )
        await update.message.reply_text(
            f"✅ <b>Goodbye message သတ်မှတ်ပြီးပါပြီ</b>\n"
            f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
            f"📍 Scope: {scope_text}\n\n"
            f"<b>Preview:</b>\n"
            f"<i>{html_lib.escape(text)}</i>",
            parse_mode="HTML"
        )
    except PyMongoError as e:
        logger.error(f"set_goodbye DB error: {e}")
        await update.message.reply_text("❌ Database error ဖြစ်ပါသည်။")
    except Exception as e:
        logger.error(f"set_goodbye error: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

# ══════════════════════════════════════════════════════════
#  DELWELCOME / DELGOODBYE
# ══════════════════════════════════════════════════════════

async def del_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if welcome_settings_collection is None:
        return await update.message.reply_text("❌ Database ချိတ်ဆက်မှု မရှိပါ။")
    try:
        target_chat_id, scope_text = await _resolve_scope(update)
        if target_chat_id is None:
            return

        result = await welcome_settings_collection.update_one(
            {"chat_id": target_chat_id},
            {"$unset": {"welcome_text": ""}}
        )
        # Clean up document if it has no useful fields left
        if target_chat_id != "global":
            doc = await welcome_settings_collection.find_one({"chat_id": target_chat_id})
            if doc and "welcome_text" not in doc and "goodbye_text" not in doc:
                await welcome_settings_collection.delete_one({"chat_id": target_chat_id})

        if result.modified_count > 0:
            await update.message.reply_text(
                f"✅ <b>Welcome message ဖျက်ပြီးပါပြီ</b>\n"
                f"📍 Scope: {scope_text}",
                parse_mode="HTML"
            )
        else:
            await update.message.reply_text("❌ Welcome message မသတ်မှတ်ရသေးပါ။")
    except PyMongoError as e:
        logger.error(f"del_welcome DB error: {e}")
        await update.message.reply_text("❌ Database error ဖြစ်ပါသည်။")
    except Exception as e:
        logger.error(f"del_welcome error: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

async def del_goodbye(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if welcome_settings_collection is None:
        return await update.message.reply_text("❌ Database ချိတ်ဆက်မှု မရှိပါ။")
    try:
        target_chat_id, scope_text = await _resolve_scope(update)
        if target_chat_id is None:
            return

        result = await welcome_settings_collection.update_one(
            {"chat_id": target_chat_id},
            {"$unset": {"goodbye_text": ""}}
        )
        # Clean up document if no fields remain
        if target_chat_id != "global":
            doc = await welcome_settings_collection.find_one({"chat_id": target_chat_id})
            if doc and "welcome_text" not in doc and "goodbye_text" not in doc:
                await welcome_settings_collection.delete_one({"chat_id": target_chat_id})

        if result.modified_count > 0:
            await update.message.reply_text(
                f"✅ <b>Goodbye message ဖျက်ပြီးပါပြီ</b>\n"
                f"📍 Scope: {scope_text}",
                parse_mode="HTML"
            )
        else:
            await update.message.reply_text("❌ Goodbye message မသတ်မှတ်ရသေးပါ။")
    except PyMongoError as e:
        logger.error(f"del_goodbye DB error: {e}")
        await update.message.reply_text("❌ Database error ဖြစ်ပါသည်။")
    except Exception as e:
        logger.error(f"del_goodbye error: {e}")
        await update.message.reply_text(f"❌ Error: {e}")

# ══════════════════════════════════════════════════════════
#  HELP
# ══════════════════════════════════════════════════════════

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    help_text = (
        "╔══════════════════╗\n"
        "  🤖  <b>Bot Commands</b>\n"
        "╚══════════════════╝\n\n"

        "👤 <b>General</b>\n"
        "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
        "/start   —  Bot စတင်ရန်\n"
        "/help    —  Command စာရင်းကြည့်ရန်\n\n"

        "🔨 <b>Moderation</b>\n"
        "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
        "/ban     —  User ကို ban လုပ်ရန်\n"
        "/unban   —  User ကို ban ဖြုတ်ရန်\n"
        "/mute    —  User ကို mute လုပ်ရန်\n"
        "/unmute  —  User ကို mute ဖြုတ်ရန်\n"
        "/warn    —  User ကို သတိပေးရန် <i>(၃ ကြိမ် = ban)</i>\n"
        "/unwarn  —  သတိပေးချက် ဖျက်ရန်\n"
        "/warns   —  User ၏ warn အခြေအနေ ကြည့်ရန်\n\n"

        "🔎 <b>Filters</b>\n"
        "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
        "/filter      —  Filter ထည့်ရန် <i>(text / sticker)</i>\n"
        "/delfilter   —  Filter ဖျက်ရန်\n"
        "/filterlist  —  Dashboard ကြည့်ရန် <i>(private chat only)</i>\n"
        "/post        —  Broadcast လုပ်ရန် <i>(owner only)</i>\n\n"

        "⚙️ <b>Bot Admin</b>\n"
        "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
        "/admin      —  Bot admin ထည့်ရန် <i>(owner only)</i>\n"
        "/unadmin    —  Bot admin ဖယ်ရှားရန် <i>(owner only)</i>\n"
        "/adminlist  —  Bot admin စာရင်းကြည့်ရန်\n\n"

        "👋 <b>Greetings</b>\n"
        "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
        "/setwelcome  —  Welcome message သတ်မှတ်ရန်\n"
        "/setgoodbye  —  Goodbye message သတ်မှတ်ရန်\n"
        "/delwelcome  —  Welcome message ဖျက်ရန်\n"
        "/delgoodbye  —  Goodbye message ဖျက်ရန်\n\n"

        "💡 <b>Placeholders:</b> <code>{name}</code>  <code>{id}</code>  "
        "<code>{group}</code>  <code>(@)</code>"
    )
    try:
        await update.message.reply_text(help_text, parse_mode="HTML")
    except Exception as e:
        logger.error(f"help_cmd error: {e}")

# ══════════════════════════════════════════════════════════
#  MEMBER STATUS HANDLER  (Welcome + Goodbye)
# ══════════════════════════════════════════════════════════

async def member_status_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.chat_member:
        return
    new = update.chat_member.new_chat_member
    old = update.chat_member.old_chat_member
    if new is None or old is None:
        return

    chat = update.effective_chat

    # ── Welcome: user joined ────────────────────────────────
    if new.status == "member" and old.status in ("left", "kicked"):
        user = new.user
        # Bug fix: skip bots (including this bot joining a group)
        if getattr(user, "is_bot", False):
            return
        # get_welcome_settings is now async, must await it
        settings = await get_welcome_settings(chat.id)
        if settings and "welcome_text" in settings:
            try:
                msg = format_message_with_placeholders(settings["welcome_text"], user, chat)
                await context.bot.send_message(chat_id=chat.id, text=msg, parse_mode="HTML")
            except Exception as e:
                logger.error(f"Welcome message error: {e}")

    # ── Goodbye: user left / was kicked ────────────────────
    elif old.status in ("member", "administrator") and new.status in ("left", "kicked"):
        user = old.user
        # Skip bots
        if getattr(user, "is_bot", False):
            return
        settings = await get_welcome_settings(chat.id)
        if settings and "goodbye_text" in settings:
            try:
                msg = format_message_with_placeholders(settings["goodbye_text"], user, chat)
                await context.bot.send_message(chat_id=chat.id, text=msg, parse_mode="HTML")
            except Exception as e:
                logger.error(f"Goodbye message error: {e}")
