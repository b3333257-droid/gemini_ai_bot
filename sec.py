# sec.py (fully async – all verified issues fixed)
import asyncio
import html as html_lib
import logging
import os
import re
import time
from datetime import datetime, timezone
from bson import ObjectId
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram.error import RetryAfter
from pymongo.errors import PyMongoError

logger = logging.getLogger(__name__)

# ── DB collections (injected by bot.py) ────────────────────
global_filter_collection = None
user_collection          = None
chat_collection          = None

# ── Owner IDs (injected by bot.py) ─────────────────────────
OWNER_IDS: frozenset = frozenset()

# ── Filter cache ────────────────────────────────────────────
_filter_cache      = {}
_filter_cache_time = {}
FILTER_CACHE_TTL   = 1200  # 20 minutes

# ── Cooldown state ──────────────────────────────────────────
chat_cooldowns = {}

# ── FILTER LIMIT (prevent abuse) ──────────────────────────
MAX_FILTERS_PER_ADMIN = 500

# ── Owner helper ────────────────────────────────────────────
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

# ══════════════════════════════════════════════════════════
#  COOLDOWN
# ══════════════════════════════════════════════════════════

def cleanup_chat_cooldowns():
    now      = time.time()
    expired  = [cid for cid, s in chat_cooldowns.items()
                if now - s.get("last_active", 0) > 900]
    for cid in expired:
        del chat_cooldowns[cid]
    if expired:
        logger.debug(f"Cooldown: cleaned {len(expired)} entries.")

# ══════════════════════════════════════════════════════════
#  FILTER CACHE (now async)
# ══════════════════════════════════════════════════════════

async def _load_filter_cache(chat_id):
    now = time.time()
    if chat_id in _filter_cache_time and (now - _filter_cache_time[chat_id] < FILTER_CACHE_TTL):
        return _filter_cache.get(chat_id, [])
    try:
        cursor = global_filter_collection.find(
            {"chat_id": {"$in": [chat_id, "global"]}},
            {"keyword": 1, "reply": 1, "is_sticker": 1, "_id": 0}
        )
        rows = await cursor.to_list(length=None)
        _filter_cache[chat_id]      = rows
        _filter_cache_time[chat_id] = now
        logger.debug(f"Filter cache loaded: {len(rows)} entries for chat {chat_id}.")
        return rows
    except PyMongoError as e:
        logger.error(f"DB error loading filters for {chat_id}: {e}")
        return _filter_cache.get(chat_id, [])
    except Exception as e:
        logger.error(f"Unexpected error loading filters for {chat_id}: {e}")
        return _filter_cache.get(chat_id, [])

def _invalidate_filter_cache(chat_id=None):
    if chat_id is None or chat_id == "global":
        _filter_cache.clear()
        _filter_cache_time.clear()
        logger.debug("Filter cache: fully cleared (global scope).")
    else:
        _filter_cache.pop(chat_id, None)
        _filter_cache_time.pop(chat_id, None)
        logger.debug(f"Filter cache: cleared for chat {chat_id}.")

# ══════════════════════════════════════════════════════════
#  FILTER CRUD (async)
# ══════════════════════════════════════════════════════════

async def add_global_filter(keyword: str, reply: str, creator_id: int,
                             creator_name: str, chat_id="global",
                             is_sticker: bool = False) -> bool:
    if global_filter_collection is None:
        logger.error("global_filter_collection not initialized.")
        return False
    try:
        cleaned = keyword.strip().lower()
        if not cleaned or len(cleaned) > 100:
            logger.warning(f"Filter keyword invalid length: '{cleaned}'")
            return False
        if not is_sticker and len(reply) > 4000:
            logger.warning(f"Filter reply too long ({len(reply)} chars), truncating.")
            reply = reply[:4000]

        if chat_id == "global":
            current_count = await global_filter_collection.count_documents(
                {"creator_id": creator_id, "chat_id": "global"}
            )
            if current_count >= MAX_FILTERS_PER_ADMIN:
                logger.warning(
                    f"Filter limit reached for creator {creator_id} ({current_count}/{MAX_FILTERS_PER_ADMIN})"
                )
                return False

        data = {
            "keyword":      cleaned,
            "reply":        reply,
            "creator_id":   creator_id,
            "creator_name": creator_name,
            "chat_id":      chat_id,
            "is_sticker":   is_sticker,
            "created_at":   datetime.now(timezone.utc),
        }
        await global_filter_collection.update_one(
            {"keyword": cleaned, "chat_id": chat_id},
            {"$set": data},
            upsert=True
        )
        _invalidate_filter_cache(chat_id)
        logger.info(f"Filter '{cleaned}' saved by {creator_id} in '{chat_id}'.")
        return True
    except PyMongoError as e:
        logger.error(f"DB error saving filter '{keyword}': {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error saving filter '{keyword}': {e}")
        return False

async def delete_filter_by_keyword(keyword: str, chat_id) -> bool:
    if global_filter_collection is None:
        logger.error("global_filter_collection not initialized.")
        return False
    try:
        result = await global_filter_collection.delete_one({
            "keyword": keyword.strip().lower(),
            "chat_id": chat_id,
        })
        if result.deleted_count > 0:
            _invalidate_filter_cache(chat_id)
            logger.info(f"Filter '{keyword}' deleted from '{chat_id}'.")
            return True
        return False
    except PyMongoError as e:
        logger.error(f"DB error deleting filter '{keyword}': {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error deleting filter '{keyword}': {e}")
        return False

# ══════════════════════════════════════════════════════════
#  AUTO REPLY (now async filter loading, safe HTML)
# ══════════════════════════════════════════════════════════

async def auto_reply(update: Update, is_direct: bool = False):
    if not update.message:
        return
    text = update.message.text or update.message.caption
    if not text or global_filter_collection is None:
        return

    try:
        message_text = text.strip()
        if not message_text or len(message_text) > 500:
            return

        chat            = update.effective_chat
        current_chat_id = chat.id if chat.type != "private" else update.effective_user.id

        if not is_direct:
            now   = time.time()
            state = chat_cooldowns.get(
                current_chat_id,
                {"count": 0, "cooldown_until": 0.0, "last_active": now}
            )
            state["last_active"] = now
            if now < state["cooldown_until"]:
                chat_cooldowns[current_chat_id] = state
                return
            state["count"] += 1
            if state["count"] >= 5:
                state["cooldown_until"] = now + 5.0
                state["count"]          = 0
            chat_cooldowns[current_chat_id] = state

        filters_list = await _load_filter_cache(current_chat_id)
        if not filters_list:
            return

        msg_lower   = message_text.lower()
        latin_words = set(re.findall(r'\b\w+\b', msg_lower))

        for f in filters_list:
            keyword = f["keyword"].lower()
            matched = False

            if any(ord(c) > 127 for c in keyword):
                matched = keyword in msg_lower
            elif keyword.startswith("@"):
                matched = keyword in msg_lower
            else:
                matched = keyword in latin_words

            if matched:
                if f.get("is_sticker"):
                    await update.message.reply_sticker(sticker=f["reply"])
                else:
                    # HTML injection prevention: escape the stored reply
                    safe_reply = html_lib.escape(f["reply"])
                    await update.message.reply_text(safe_reply, parse_mode="HTML")
                logger.info(f"Auto-reply: '{keyword}' matched in chat {current_chat_id}.")
                return

    except PyMongoError as e:
        logger.error(f"DB error in auto_reply: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in auto_reply: {e}")

# ══════════════════════════════════════════════════════════
#  DASHBOARD KEYBOARDS (async)
# ══════════════════════════════════════════════════════════

async def _generate_admin_list_keyboard(bot, page: int, admins_per_page: int = 10):
    if global_filter_collection is None:
        return InlineKeyboardMarkup([])
    pipeline = [
        {"$match": {"chat_id": "global"}},
        {"$group": {
            "_id":          "$creator_id",
            "name":         {"$first": "$creator_name"},
            "filter_count": {"$sum": 1},
        }},
        {"$sort": {"name": 1}},
    ]
    try:
        cursor = global_filter_collection.aggregate(pipeline)
        all_admins = await cursor.to_list(length=None)
        total      = len(all_admins)
        total_filters = sum(a.get("filter_count", 0) for a in all_admins)
        skip       = (page - 1) * admins_per_page
        page_admins = all_admins[skip: skip + admins_per_page]

        keyboard = []
        keyboard.append([InlineKeyboardButton(
            f"📊  {total} admins  ·  {total_filters} filters total",
            callback_data="noop"
        )])
        for admin in page_admins:
            keyboard.append([InlineKeyboardButton(
                f"🛡  {admin['name']}   ·   {admin['filter_count']} filters",
                callback_data=f"admin|{admin['_id']}"
            )])

        nav_row     = []
        total_pages = max(1, (total + admins_per_page - 1) // admins_per_page)
        if page > 1:
            nav_row.append(InlineKeyboardButton("◀️ Prev", callback_data=f"page|{page - 1}|0"))
        if total_pages > 1:
            nav_row.append(InlineKeyboardButton(f"· {page}/{total_pages} ·", callback_data="noop"))
        if page < total_pages:
            nav_row.append(InlineKeyboardButton("Next ▶️", callback_data=f"page|{page + 1}|0"))
        if nav_row:
            keyboard.append(nav_row)
        return InlineKeyboardMarkup(keyboard)

    except PyMongoError as e:
        logger.error(f"DB error generating admin list: {e}")
        return InlineKeyboardMarkup([])
    except Exception as e:
        logger.error(f"Unexpected error generating admin list: {e}")
        return InlineKeyboardMarkup([])


async def _generate_filter_list_keyboard(bot, admin_id, page: int,
                                          filters_per_page: int = 15):
    if global_filter_collection is None:
        return InlineKeyboardMarkup([])
    try:
        query = {"creator_id": admin_id, "chat_id": "global"}
        total = await global_filter_collection.count_documents(query)

        cursor = global_filter_collection.find(query, {"keyword": 1}) \
                                          .sort("keyword", 1) \
                                          .skip((page - 1) * filters_per_page) \
                                          .limit(filters_per_page)
        rows = await cursor.to_list(length=filters_per_page)

        keyboard = []
        keyboard.append([InlineKeyboardButton(
            f"🔎  {total} filters found",
            callback_data="noop"
        )])

        row = []
        for i, f in enumerate(rows, 1):
            row.append(InlineKeyboardButton(
                f"🌐 {f['keyword']}",
                callback_data=f"filter_id|{f['_id']}"
            ))
            if i % 2 == 0:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)

        pag_row     = []
        total_pages = max(1, (total + filters_per_page - 1) // filters_per_page)
        chunk_size  = 5
        start       = ((page - 1) // chunk_size) * chunk_size + 1
        end         = min(start + chunk_size - 1, total_pages)
        if page > 1:
            pag_row.append(InlineKeyboardButton("◀️", callback_data=f"page|{page - 1}|{admin_id}"))
        for p in range(start, end + 1):
            label = f"[{p}]" if p == page else str(p)
            pag_row.append(InlineKeyboardButton(label, callback_data=f"page|{p}|{admin_id}"))
        if page < total_pages:
            pag_row.append(InlineKeyboardButton("▶️", callback_data=f"page|{page + 1}|{admin_id}"))
        if pag_row:
            keyboard.append(pag_row)

        keyboard.append([InlineKeyboardButton("🏠  Back to Admins", callback_data="home")])
        return InlineKeyboardMarkup(keyboard)

    except PyMongoError as e:
        logger.error(f"DB error generating filter list: {e}")
        return InlineKeyboardMarkup([])
    except Exception as e:
        logger.error(f"Unexpected error generating filter list: {e}")
        return InlineKeyboardMarkup([])


async def show_dashboard_admin(bot, user_id, page: int = 1, admin_id=None):
    try:
        if not admin_id or admin_id == 0:
            return await _generate_admin_list_keyboard(bot, page)
        else:
            return await _generate_filter_list_keyboard(bot, admin_id, page)
    except Exception as e:
        logger.error(f"show_dashboard_admin error: {e}")
        return None

# ══════════════════════════════════════════════════════════
#  CALLBACK HANDLER (with abuse prevention)
# ══════════════════════════════════════════════════════════

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    if query.message.chat.type != "private":
        await query.answer("This feature is only available in private chat.", show_alert=True)
        return

    await query.answer()
    data      = query.data or ""
    user_id   = query.from_user.id
    owner_ids = get_owner_ids()

    try:
        if data == "noop":
            return

        elif data.startswith("admin|"):
            try:
                target_admin_id = int(data.split("|", 1)[1])
            except (IndexError, ValueError):
                return
            markup = await show_dashboard_admin(context.bot, user_id, page=1,
                                                admin_id=target_admin_id)
            if markup:
                await query.message.edit_reply_markup(reply_markup=markup)
            else:
                await query.message.reply_text("❌ Filter များ ဖွင့်၍မရပါ။")

        elif data.startswith("page|"):
            parts = data.split("|")
            try:
                page            = int(parts[1])
                admin_id_part   = parts[2] if len(parts) > 2 else "0"
                target_admin_id = None if admin_id_part == "0" else int(admin_id_part)
            except (IndexError, ValueError):
                return
            markup = await show_dashboard_admin(context.bot, user_id, page=page,
                                                admin_id=target_admin_id)
            if markup:
                await query.message.edit_reply_markup(reply_markup=markup)
            else:
                await query.message.reply_text("❌ Page ဖွင့်၍မရပါ။")

        elif data == "home":
            markup = await show_dashboard_admin(context.bot, user_id, page=1)
            if markup:
                await query.message.edit_reply_markup(reply_markup=markup)
            else:
                await query.message.reply_text("❌ Dashboard ပြန်ဖွင့်၍မရပါ။")

        elif data.startswith("filter_id|"):
            obj_id_str = data.split("filter_id|", 1)[1]
            try:
                obj_id = ObjectId(obj_id_str)
            except Exception:
                await query.message.reply_text("❌ Filter ID မမှန်ပါ။")
                return
            try:
                fd = await global_filter_collection.find_one({"_id": obj_id})
                if not fd:
                    await query.message.reply_text("❌ Filter မတွေ့ပါ။")
                    return
                keyword    = fd["keyword"]
                reply_txt  = fd["reply"]
                creator    = fd.get("creator_name", "Unknown")
                scope      = "🌐 Global" if fd.get("chat_id") == "global" else "💬 Local"
                f_type     = "🎭 Sticker" if fd.get("is_sticker") else "💬 Text"
                created_at = fd.get("created_at")
                date_str   = created_at.strftime("%Y-%m-%d") if created_at else "—"

                if fd.get("is_sticker"):
                    reply_preview = "🎭 <i>[Sticker]</i>"
                else:
                    reply_preview = f"<i>{html_lib.escape(str(reply_txt))}</i>"

                text = (
                    f"🔍 <b>Filter Details</b>\n"
                    f"━━━━━━━━━━━━━━━\n"
                    f"🔑 <b>Keyword:</b> <code>{html_lib.escape(keyword)}</code>\n\n"
                    f"💬 <b>Reply:</b>\n"
                    f"{reply_preview}\n\n"
                    f"📍 Scope: {scope}\n"
                    f"📁 Type: {f_type}\n"
                    f"👤 Creator: <b>{html_lib.escape(creator)}</b>\n"
                    f"📅 Added: {date_str}"
                )
                buttons = []
                if user_id == fd.get("creator_id") or user_id in owner_ids:
                    buttons.append(InlineKeyboardButton(
                        "🗑  Delete This Filter",
                        callback_data=f"del_filter|{obj_id_str}"
                    ))
                markup = InlineKeyboardMarkup([buttons]) if buttons else None
                await query.message.reply_text(text, parse_mode="HTML", reply_markup=markup)
            except Exception as e:
                logger.error(f"Filter detail error: {e}")
                await query.message.reply_text("❌ Filter အသေးစိတ် ဖွင့်ရာတွင် error ဖြစ်ပါသည်။")

        elif data.startswith("del_filter|"):
            obj_id_str = data.split("del_filter|", 1)[1]
            try:
                obj_id = ObjectId(obj_id_str)
            except Exception:
                await query.message.reply_text("❌ Filter ID မမှန်ပါ။")
                return
            try:
                fd = await global_filter_collection.find_one({"_id": obj_id})
                if not fd:
                    await query.message.reply_text("❌ Filter မတွေ့ပါ။")
                    return
                if user_id != fd.get("creator_id") and user_id not in owner_ids:
                    await query.message.reply_text("❌ ခွင့်ပြုချက် မရှိပါ။")
                    return
                await global_filter_collection.delete_one({"_id": obj_id})
                _invalidate_filter_cache(fd.get("chat_id"))
                await query.message.reply_text(
                    f"✅ Filter <code>{html_lib.escape(fd['keyword'])}</code> ဖျက်ပြီးပါပြီ",
                    parse_mode="HTML"
                )
                markup = await show_dashboard_admin(context.bot, user_id, page=1)
                if markup:
                    await query.message.edit_reply_markup(reply_markup=markup)
            except Exception as e:
                logger.error(f"Delete filter error: {e}")
                await query.message.reply_text("❌ Filter ဖျက်ရာတွင် error ဖြစ်ပါသည်။")

    except Exception as e:
        logger.error(f"handle_callback unhandled error: {e}")
        try:
            await query.message.reply_text("❌ တစ်ခုခု error ဖြစ်သွားပါသည်။")
        except Exception:
            pass

# ══════════════════════════════════════════════════════════
#  BROADCAST (memory‑optimized, async)
# ══════════════════════════════════════════════════════════

async def smart_post(message_id: int, bot, from_chat_id: int, content_type: str = "any"):
    if user_collection is None or chat_collection is None:
        logger.error("Collections not initialized for broadcast.")
        return 0, 0

    try:
        total_users = await user_collection.count_documents({})
        total_chats = await chat_collection.count_documents({})
        total = total_users + total_chats
        if not total:
            logger.warning("Broadcast: no recipients found.")
            return 0, 0

        semaphore = asyncio.Semaphore(10)
        sent_count = 0
        user_success = 0
        group_success = 0

        progress_msgs = []
        for oid in get_owner_ids():
            try:
                pm = await bot.send_message(
                    oid,
                    f"📢 <b>Broadcast စတင်ပါပြီ</b>\n"
                    f"━━━━━━━━━━━━━━━\n"
                    f"📋 ပေးပို့မည့် အရေအတွက်: <b>{total}</b>",
                    parse_mode="HTML"
                )
                progress_msgs.append(pm)
            except Exception as e:
                logger.error(f"Broadcast: progress msg to owner {oid} failed: {e}")

        async def _send(tid, is_group=False):
            nonlocal sent_count, user_success, group_success
            async with semaphore:
                for attempt in range(3):
                    try:
                        await bot.copy_message(
                            chat_id=tid, from_chat_id=from_chat_id,
                            message_id=message_id
                        )
                        if is_group:
                            group_success += 1
                        else:
                            user_success += 1
                        break
                    except RetryAfter as e:
                        await asyncio.sleep(e.retry_after)
                    except Exception as e:
                        logger.debug(f"Broadcast: failed for {tid}: {e}")
                        break
            sent_count += 1
            if sent_count % 50 == 0 or sent_count == total:
                pct  = int((sent_count / total) * 100)
                fill = int(pct / 10)
                bar  = "█" * fill + "░" * (10 - fill)
                txt  = (
                    f"📢 <b>Broadcast လုပ်နေပါသည်...</b>\n"
                    f"━━━━━━━━━━━━━━━\n"
                    f"[{bar}] {pct}%\n"
                    f"📨 {sent_count} / {total}"
                )
                for pm in progress_msgs:
                    try:
                        await pm.edit_text(txt, parse_mode="HTML")
                    except Exception:
                        pass
            await asyncio.sleep(0.05)

        # Send to users (batch of 100) – streaming directly from cursor
        batch = []
        async for doc in user_collection.find({}, {"user_id": 1}):
            uid = doc["user_id"]
            batch.append(uid)
            if len(batch) >= 100:
                await asyncio.gather(*(_send(uid, False) for uid in batch))
                batch.clear()
        if batch:
            await asyncio.gather(*(_send(uid, False) for uid in batch))

        # Send to chats (batch of 100)
        batch = []
        async for doc in chat_collection.find({}, {"chat_id": 1}):
            cid = doc["chat_id"]
            batch.append(cid)
            if len(batch) >= 100:
                await asyncio.gather(*(_send(cid, True) for cid in batch))
                batch.clear()
        if batch:
            await asyncio.gather(*(_send(cid, True) for cid in batch))

        finish = (
            f"✅ <b>Broadcast ပြီးပါပြီ</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📨 စုစုပေါင်း: <b>{user_success + group_success}</b>\n\n"
            f"👤 Users: <b>{user_success}</b>\n"
            f"👥 Groups: <b>{group_success}</b>"
        )
        for pm in progress_msgs:
            try:
                await pm.edit_text(finish, parse_mode="HTML")
            except Exception:
                pass

        logger.info(f"Broadcast done: {user_success} users, {group_success} groups.")
        return user_success, group_success

    except PyMongoError as e:
        logger.error(f"Broadcast DB error: {e}")
        return 0, 0
    except Exception as e:
        logger.error(f"Broadcast unexpected error: {e}")
        return 0, 0
