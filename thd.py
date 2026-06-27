# thd.py (fixed for Steps 1, 5, 6: welcome/goodbye entity support)
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
#  INIT (unchanged)
# ══════════════════════════════════════════════════════════

async def init_db(db) -> None:
    global welcome_settings_collection
    try:
        welcome_settings_collection = db["welcome_settings"]
        await welcome_settings_collection.create_index("chat_id", unique=True)
        logger.info("thd.py: DB initialized.")
    except PyMongoError as e:
        logger.critical(f"thd.py init_db DB error: {e}")
        raise
    except Exception as e:
        logger.critical(f"thd.py init_db unexpected error: {e}")
        raise

# ══════════════════════════════════════════════════════════
#  ENTITY → HTML HELPER (NEW – duplicated from bot.py logic)
# ══════════════════════════════════════════════════════════

def _entities_to_html(text: str, entities: list) -> str:
    """Convert plain text with Telegram entities to an HTML string,
    including <tg-emoji> for custom emoji entities."""
    if not entities:
        return html_lib.escape(text)

    entities = sorted(entities, key=lambda e: e.offset)
    result = []
    last_idx = 0
    for entity in entities:
        if entity.offset > last_idx:
            result.append(html_lib.escape(text[last_idx:entity.offset]))
        entity_text = text[entity.offset:entity.offset + entity.length]

        if entity.type == "custom_emoji":
            emoji_id = getattr(entity, "custom_emoji_id", None)
            if emoji_id:
                result.append(
                    f'<tg-emoji emoji-id="{emoji_id}">'
                    f'{html_lib.escape(entity_text)}</tg-emoji>'
                )
            else:
                result.append(html_lib.escape(entity_text))
        elif entity.type == "bold":
            result.append(f"<b>{html_lib.escape(entity_text)}</b>")
        elif entity.type == "italic":
            result.append(f"<i>{html_lib.escape(entity_text)}</i>")
        elif entity.type == "underline":
            result.append(f"<u>{html_lib.escape(entity_text)}</u>")
        elif entity.type == "strikethrough":
            result.append(f"<s>{html_lib.escape(entity_text)}</s>")
        elif entity.type == "code":
            result.append(f"<code>{html_lib.escape(entity_text)}</code>")
        elif entity.type == "pre":
            result.append(f"<pre>{html_lib.escape(entity_text)}</pre>")
        elif entity.type == "text_link":
            url = getattr(entity, "url", "")
            result.append(
                f'<a href="{html_lib.escape(url)}">'
                f'{html_lib.escape(entity_text)}</a>'
            )
        elif entity.type == "text_mention":
            user = getattr(entity, "user", None)
            if user:
                result.append(
                    f'<a href="tg://user?id={user.id}">'
                    f'{html_lib.escape(entity_text)}</a>'
                )
            else:
                result.append(html_lib.escape(entity_text))
        else:
            result.append(html_lib.escape(entity_text))

        last_idx = entity.offset + entity.length

    if last_idx < len(text):
        result.append(html_lib.escape(text[last_idx:]))
    return "".join(result)


# ══════════════════════════════════════════════════════════
#  HELPERS (modified)
# ══════════════════════════════════════════════════════════

def get_owner_ids() -> frozenset:
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
    Return settings dict (including entities fields) with field‑wise
    fallback to global.  Motor requires await for find_one operations.
    """
    if welcome_settings_collection is None:
        logger.error("welcome_settings_collection not initialized.")
        return None
    try:
        local = await welcome_settings_collection.find_one({"chat_id": chat_id})
        global_doc = await welcome_settings_collection.find_one({"chat_id": "global"})

        if not local:
            return global_doc

        # Fallback fields: text and entities
        if global_doc:
            for key in ("welcome_text", "goodbye_text",
                        "welcome_entities", "goodbye_entities"):
                if key not in local and key in global_doc:
                    local[key] = global_doc[key]
        return local

    except PyMongoError as e:
        logger.error(f"DB error fetching welcome settings ({chat_id}): {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching welcome settings: {e}")
        return None


def format_message_with_placeholders(
    text: str, user, chat, entities: list | None = None
) -> str:
    """
    Replace placeholders in welcome/goodbye text.
    If `entities` is provided (list of Telegram MessageEntity objects),
    the message is first converted to HTML using those entities, then
    placeholders are replaced.  Otherwise the legacy escaping logic is used.
    """
    if not text:
        return ""

    try:
        # ── 1. Build HTML from text + entities (if any) ─────
        if entities:
            # entities exist → build full HTML string
            html_message = _entities_to_html(text, entities)
        else:
            # No entities → legacy: escape the whole template
            html_message = html_lib.escape(text)

        # ── 2. Prepare replacement values (always safe HTML) ─
        user_mention = (
            user.mention_html()
            if hasattr(user, "mention_html")
            else html_lib.escape(getattr(user, "full_name", "") or "")
        )
        user_id_str = str(user.id)

        group_title = html_lib.escape(str(chat.title)) if chat.title else ""

        username = getattr(user, "username", None)
        if username:
            at_html = f'<a href="tg://user?id={user.id}">@{username}</a>'
        else:
            first_name = getattr(user, "first_name", "") or ""
            at_html = (
                f'<a href="tg://user?id={user.id}">'
                f'{html_lib.escape(first_name)}</a>'
            )

        if getattr(chat, "username", None):
            gp_text = f"@{chat.username}"
        else:
            gp_text = group_title

        # ── 3. Replace placeholders ──────────────────────────
        html_message = html_message.replace("{name}", user_mention)
        html_message = html_message.replace("{id}", user_id_str)
        html_message = html_message.replace("{group}", group_title)
        html_message = html_message.replace("{gp}", gp_text)
        html_message = html_message.replace("(@)", at_html)
        html_message = html_message.replace("{(@)}", at_html)
        html_message = html_message.replace("{@}", at_html)

        return html_message

    except Exception as e:
        logger.error(f"format_message_with_placeholders error: {e}")
        return text


# ══════════════════════════════════════════════════════════
#  PERMISSION GUARD (unchanged)
# ══════════════════════════════════════════════════════════

async def _resolve_scope(update: Update) -> tuple[str | int | None, str]:
    user_id = update.effective_user.id
    chat    = update.effective_chat

    if chat.type == "private":
        if is_owner(user_id) or await is_bot_admin(user_id):
            return "global", "🌐 Global"
        await update.message.reply_text(
            "❌ Bot admin / owner သာ global setting ပြောင်းနိုင်သည်။"
        )
        return None, ""

    else:
        if is_owner(user_id) or await is_bot_admin(user_id):
            return "global", "🌐 Global"

        try:
            member = await chat.get_member(user_id)
            if member.status in ("administrator", "creator"):
                return chat.id, f"💬 {chat.title}"
        except Exception as e:
            logger.error(f"_resolve_scope get_member error: {e}")

        await update.message.reply_text("❌ Group admin သာ ပြောင်းနိုင်သည်။")
        return None, ""


# ── Updated text extractor ──────────────────────────────────

def _get_text_and_entities(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Extract text AND entities from reply or command args.
    Returns (text, entities_list_or_None).  Returns (None, None) if empty.
    """
    if update.message.reply_to_message:
        reply = update.message.reply_to_message
        if reply.text:
            return reply.text, getattr(reply, "entities", None)
        if reply.caption:
            return reply.caption, getattr(reply, "caption_entities", None)
        return None, None
    joined = " ".join(context.args) if context.args else ""
    return (joined, None) if joined.strip() else (None, None)


# ══════════════════════════════════════════════════════════
#  SETWELCOME / SETGOODBYE (updated to store entities)
# ══════════════════════════════════════════════════════════

async def set_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if welcome_settings_collection is None:
        return await update.message.reply_text("❌ Database ချိတ်ဆက်မှု မရှိပါ။")
    try:
        target_chat_id, scope_text = await _resolve_scope(update)
        if target_chat_id is None:
            return

        text, entities = _get_text_and_entities(update, context)
        if not text:
            return await update.message.reply_text(
                "❌ Welcome စာသားထည့်ပေးပါ။\n"
                "<b>Placeholders:</b> <code>{name}</code>  <code>{id}</code>  "
                "<code>{group}</code>  <code>(@)</code>",
                parse_mode="HTML"
            )

        # Build the $set data – entities may be None
        set_data = {
            "welcome_text": text,
            "updated_at": datetime.now(timezone.utc),
        }
        if entities is not None:
            # Convert entities to serialisable dict list (MongoDB will store as array of objects)
            set_data["welcome_entities"] = [e.to_dict() for e in entities]
        else:
            # If no entities, ensure the field is removed (if it existed before)
            await welcome_settings_collection.update_one(
                {"chat_id": target_chat_id},
                {"$unset": {"welcome_entities": ""}}
            )

        await welcome_settings_collection.update_one(
            {"chat_id": target_chat_id},
            {"$set": set_data},
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

        text, entities = _get_text_and_entities(update, context)
        if not text:
            return await update.message.reply_text(
                "❌ Goodbye စာသားထည့်ပေးပါ။\n"
                "<b>Placeholders:</b> <code>{name}</code>  <code>{id}</code>  "
                "<code>{group}</code>  <code>(@)</code>",
                parse_mode="HTML"
            )

        set_data = {
            "goodbye_text": text,
            "updated_at": datetime.now(timezone.utc),
        }
        if entities is not None:
            set_data["goodbye_entities"] = [e.to_dict() for e in entities]
        else:
            await welcome_settings_collection.update_one(
                {"chat_id": target_chat_id},
                {"$unset": {"goodbye_entities": ""}}
            )

        await welcome_settings_collection.update_one(
            {"chat_id": target_chat_id},
            {"$set": set_data},
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
#  DELWELCOME / DELGOODBYE (also clear entities)
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
            {"$unset": {"welcome_text": "", "welcome_entities": ""}}
        )
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
            {"$unset": {"goodbye_text": "", "goodbye_entities": ""}}
        )
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
#  HELP (unchanged)
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
#  MEMBER STATUS HANDLER (updated to pass entities)
# ══════════════════════════════════════════════════════════

async def member_status_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.chat_member:
        return
    new = update.chat_member.new_chat_member
    old = update.chat_member.old_chat_member
    if new is None or old is None:
        return

    chat = update.effective_chat

    # ── Welcome ─────────────────────────────────────────────
    if new.status == "member" and old.status in ("left", "kicked"):
        user = new.user
        if getattr(user, "is_bot", False):
            return
        settings = await get_welcome_settings(chat.id)
        if settings and "welcome_text" in settings:
            try:
                entities = settings.get("welcome_entities")  # list of dicts or None
                # Convert stored dicts back to entity objects if needed? We'll
                # keep them as plain dicts – `_entities_to_html` expects objects
                # with .type, .offset, .length etc.  We'll need to convert them.
                if entities:
                    # Reconstruct MessageEntity-like objects
                    from telegram import MessageEntity
                    entities = [
                        MessageEntity(
                            type=e["type"],
                            offset=e["offset"],
                            length=e["length"],
                            **{k: v for k, v in e.items() if k not in ("type", "offset", "length")}
                        )
                        for e in entities
                    ]
                msg = format_message_with_placeholders(
                    settings["welcome_text"], user, chat, entities=entities
                )
                await context.bot.send_message(
                    chat_id=chat.id, text=msg, parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Welcome message error: {e}")

    # ── Goodbye ─────────────────────────────────────────────
    elif old.status in ("member", "administrator") and new.status in ("left", "kicked"):
        user = old.user
        if getattr(user, "is_bot", False):
            return
        settings = await get_welcome_settings(chat.id)
        if settings and "goodbye_text" in settings:
            try:
                entities = settings.get("goodbye_entities")
                if entities:
                    from telegram import MessageEntity
                    entities = [
                        MessageEntity(
                            type=e["type"],
                            offset=e["offset"],
                            length=e["length"],
                            **{k: v for k, v in e.items() if k not in ("type", "offset", "length")}
                        )
                        for e in entities
                    ]
                msg = format_message_with_placeholders(
                    settings["goodbye_text"], user, chat, entities=entities
                )
                await context.bot.send_message(
                    chat_id=chat.id, text=msg, parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Goodbye message error: {e}")
