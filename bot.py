# bot.py
import asyncio
import os
import threading
import time
import logging
from datetime import datetime, timezone, timedelta
from flask import Flask
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ReturnDocument
from telegram import Update, ChatPermissions
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
    ChatMemberHandler
)

import sec
import thd
import cacu          # <-- Calculator module

# ── Logging ────────────────────────────────────────────────
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────
class Config:
    TOKEN      = os.environ.get("BOT_TOKEN")
    MONGO_URI  = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
    DB_NAME    = os.environ.get("DB_NAME", "telegram_bot_db")
    SELF_URL   = os.environ.get("SELF_URL")
    FLASK_PORT = int(os.environ.get("PORT", 8080))
    OWNER_ID   = int(os.environ.get("OWNER_ID",   0))
    OWNER_ID_2 = int(os.environ.get("OWNER_ID_2", 0))
    OWNER_IDS: frozenset = frozenset()

    @classmethod
    def validate(cls):
        if not cls.TOKEN:
            raise ValueError("BOT_TOKEN မထည့်ရသေးပါ။")
        if not cls.OWNER_ID:
            logger.warning("OWNER_ID မသတ်မှတ်ရသေးပါ။")
        cls.OWNER_IDS = frozenset(i for i in (cls.OWNER_ID, cls.OWNER_ID_2) if i)
        if cls.OWNER_ID_2:
            logger.info(f"Dual-owner mode: {cls.OWNER_ID} + {cls.OWNER_ID_2}")

Config.validate()

# ── Database ───────────────────────────────────────────────
try:
    mongo_client             = AsyncIOMotorClient(Config.MONGO_URI, serverSelectionTimeoutMS=5000)
    db                       = mongo_client[Config.DB_NAME]
    user_collection          = db["users"]
    admin_collection         = db["admins"]
    warn_collection          = db["warns"]
    chat_collection          = db["chats"]
    global_filter_collection = db["global_filters"]
    logger.info("Database ချိတ်ဆက်မှု အောင်မြင်ပါသည်။")
except Exception as e:
    logger.critical(f"MongoDB ချိတ်ဆက်မှု မအောင်မြင်ပါ: {e}")
    raise

# Inject → sec.py
sec.global_filter_collection = global_filter_collection
sec.user_collection          = user_collection
sec.chat_collection          = chat_collection
sec.filters_col              = global_filter_collection
sec.OWNER_IDS                = Config.OWNER_IDS

# Inject → thd.py
# thd.init_db will be called later in main()

# ── Permission caches ──────────────────────────────────────
_admin_cache = {}
_group_admin_cache = {}

def is_owner(user_id: int) -> bool:
    return user_id in Config.OWNER_IDS

async def is_admin(user_id: int) -> bool:
    now = time.time()
    if user_id in _admin_cache and now - _admin_cache[user_id] < 300:
        return True
    if await admin_collection.find_one({"user_id": user_id}):
        _admin_cache[user_id] = now
        return True
    return False

async def is_group_admin(update: Update, user_id: int) -> bool:
    chat = update.effective_chat
    if not chat or chat.type == "private":
        return False
    now = time.time()
    entry = _group_admin_cache.get(chat.id)
    if entry and now < entry["expires"]:
        return user_id in entry["admins"]
    try:
        admins   = await chat.get_administrators()
        admin_ids = frozenset(a.user.id for a in admins)
        _group_admin_cache[chat.id] = {"admins": admin_ids, "expires": now + 300}
        return user_id in admin_ids
    except Exception as e:
        logger.error(f"Group admin စစ်ဆေးမှု error ({chat.id}): {e}")
        return False

async def can_moderate(update: Update, user_id: int) -> bool:
    return is_owner(user_id) or await is_admin(user_id) or await is_group_admin(update, user_id)

def is_strict_command(text: str, command: str, bot_username: str) -> bool:
    if not text:
        return False
    first = text.split()[0]
    return first in (f"/{command}", f"/{command}@{bot_username or ''}")

async def check_hierarchy(chat, issuer_id: int, target_id: int):
    if chat.type == "private":
        return None
    try:
        issuer = await chat.get_member(issuer_id)
        target = await chat.get_member(target_id)
        if issuer.status == "creator":
            return None
        if target.status in ("administrator", "creator"):
            return "❌ Admin အချင်းချင်း အရေးယူ၍မရပါ။"
    except Exception as e:
        logger.error(f"Hierarchy check error: {e}")
    return None

class UserData:
    def __init__(self, user_id: int, username=None, first_name="Unknown"):
        self.id         = user_id
        self.username   = username
        self.first_name = first_name

async def get_user_mention(bot, user_id: int) -> str:
    try:
        chat = await bot.get_chat(user_id)
        return f"@{chat.username}" if chat.username else chat.mention_html()
    except Exception:
        row = await user_collection.find_one({"user_id": user_id})
        if row:
            if row.get("username"):
                return f"@{row['username']}"
            name = row.get("first_name", "Unknown")
            return f'<a href="tg://user?id={user_id}">{name}</a>'
        return str(user_id)

async def get_target_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.reply_to_message:
        return update.message.reply_to_message.from_user
    if not context.args:
        return None
    arg = context.args[0]
    try:
        if arg.lstrip("-").isdigit():
            uid = int(arg)
            try:
                return await context.bot.get_chat(uid)
            except Exception:
                row = await user_collection.find_one({"user_id": uid})
                if row:
                    return UserData(uid, row.get("username"), row.get("first_name"))
        else:
            uname = arg.lstrip("@")
            try:
                return await context.bot.get_chat(f"@{uname}")
            except Exception:
                row = await user_collection.find_one({"username": uname})
                if row:
                    return UserData(row["user_id"], row.get("username"), row.get("first_name"))
    except Exception as e:
        logger.error(f"Target user parse error '{arg}': {e}")
    return None

# ── Sync helpers ───────────────────────────────────────────
user_sync_cache = {}
chat_sync_cache = {}

async def sync_user_data(update: Update):
    user = update.effective_user
    if not user:
        return
    now = time.time()
    if user.id in user_sync_cache and now - user_sync_cache[user.id] < 1800:
        return
    try:
        full_name = getattr(user, "full_name", None) or \
                    f"{user.first_name or ''} {user.last_name or ''}".strip()
        await user_collection.update_one(
            {"user_id": user.id},
            {"$set": {
                "last_seen":  datetime.now(timezone.utc),
                "username":   user.username,
                "first_name": user.first_name,
                "full_name":  full_name
            }},
            upsert=True
        )
        user_sync_cache[user.id] = now
    except Exception as e:
        logger.error(f"User sync error {user.id}: {e}")

async def sync_chat_data(update: Update):
    chat = update.effective_chat
    if not chat or chat.type == "private":
        return
    now = time.time()
    if chat.id in chat_sync_cache and now - chat_sync_cache[chat.id] < 3600:
        return
    try:
        await chat_collection.update_one(
            {"chat_id": chat.id},
            {"$set": {
                "chat_id":   chat.id,
                "title":     chat.title,
                "type":      chat.type,
                "last_seen": datetime.now(timezone.utc)
            }},
            upsert=True
        )
        chat_sync_cache[chat.id] = now
    except Exception as e:
        logger.error(f"Chat sync error {chat.id}: {e}")

def cleanup_caches():
    now = time.time()
    for cache in (user_sync_cache, chat_sync_cache):
        for k in [k for k, v in cache.items() if now - v > 7200]:
            del cache[k]
    for k in [k for k, v in _admin_cache.items() if now - v > 600]:
        del _admin_cache[k]
    for cid in [c for c, e in _group_admin_cache.items() if now > e["expires"]]:
        del _group_admin_cache[cid]
    try:
        sec.cleanup_chat_cooldowns()
    except Exception as e:
        logger.error(f"Cooldown cleanup error: {e}")

def cache_cleaner_task():
    while True:
        time.sleep(1800)
        try:
            cleanup_caches()
        except Exception as e:
            logger.error(f"Cache cleaner error: {e}")

MUTE_PERMISSIONS = ChatPermissions(can_send_messages=False)

# ══════════════════════════════════════════════════════════
#  MODERATION
# ══════════════════════════════════════════════════════════

async def ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_strict_command(update.message.text, "ban", context.bot.username or ""):
        return
    if not await can_moderate(update, update.effective_user.id):
        return await update.message.reply_text("❌ ခွင့်ပြုချက် မရှိပါ။")
    target = await get_target_user(update, context)
    if not target:
        return await update.message.reply_text("❌ User ရှာမတွေ့ပါ။")
    err = await check_hierarchy(update.effective_chat, update.effective_user.id, target.id)
    if err:
        return await update.message.reply_text(err)
    try:
        await update.effective_chat.ban_member(target.id)
        mention = await get_user_mention(context.bot, target.id)
        issuer  = update.effective_user
        await update.message.reply_text(
            f"🔨 <b>Ban လုပ်ပြီးပါပြီ</b>\n"
            f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
            f"👤 {mention}\n"
            f"🆔 <code>{target.id}</code>\n\n"
            f"👮 By: <b>{issuer.first_name}</b>",
            parse_mode="HTML"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ <b>Error:</b> <code>{e}</code>", parse_mode="HTML")

async def unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_strict_command(update.message.text, "unban", context.bot.username or ""):
        return
    if not await can_moderate(update, update.effective_user.id):
        return await update.message.reply_text("❌ ခွင့်ပြုချက် မရှိပါ။")
    target = await get_target_user(update, context)
    if not target:
        return await update.message.reply_text("❌ User ရှာမတွေ့ပါ။")
    err = await check_hierarchy(update.effective_chat, update.effective_user.id, target.id)
    if err:
        return await update.message.reply_text(err)
    try:
        await update.effective_chat.unban_member(target.id)
        mention = await get_user_mention(context.bot, target.id)
        issuer  = update.effective_user
        await update.message.reply_text(
            f"✅ <b>Unban လုပ်ပြီးပါပြီ</b>\n"
            f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
            f"👤 {mention}\n"
            f"🆔 <code>{target.id}</code>\n\n"
            f"👮 By: <b>{issuer.first_name}</b>",
            parse_mode="HTML"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ <b>Error:</b> <code>{e}</code>", parse_mode="HTML")

async def mute_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_strict_command(update.message.text, "mute", context.bot.username or ""):
        return
    if not await can_moderate(update, update.effective_user.id):
        return await update.message.reply_text("❌ ခွင့်ပြုချက် မရှိပါ။")
    target = await get_target_user(update, context)
    if not target:
        return await update.message.reply_text("❌ User ရှာမတွေ့ပါ။")
    err = await check_hierarchy(update.effective_chat, update.effective_user.id, target.id)
    if err:
        return await update.message.reply_text(err)
    try:
        await update.effective_chat.restrict_member(target.id, permissions=MUTE_PERMISSIONS)
        mention = await get_user_mention(context.bot, target.id)
        issuer  = update.effective_user
        await update.message.reply_text(
            f"🔇 <b>Mute လုပ်ပြီးပါပြီ</b>\n"
            f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
            f"👤 {mention}\n"
            f"🆔 <code>{target.id}</code>\n\n"
            f"👮 By: <b>{issuer.first_name}</b>",
            parse_mode="HTML"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ <b>Error:</b> <code>{e}</code>", parse_mode="HTML")

async def unmute_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_strict_command(update.message.text, "unmute", context.bot.username or ""):
        return
    if not await can_moderate(update, update.effective_user.id):
        return await update.message.reply_text("❌ ခွင့်ပြုချက် မရှိပါ။")
    target = await get_target_user(update, context)
    if not target:
        return await update.message.reply_text("❌ User ရှာမတွေ့ပါ။")
    err = await check_hierarchy(update.effective_chat, update.effective_user.id, target.id)
    if err:
        return await update.message.reply_text(err)
    try:
        chat         = update.effective_chat
        default_perms = chat.permissions or ChatPermissions(
            can_send_messages=True,
            can_send_polls=True,
            can_send_other_messages=True,
            can_add_web_page_previews=True,
        )
        await chat.restrict_member(target.id, permissions=default_perms)
        mention = await get_user_mention(context.bot, target.id)
        issuer  = update.effective_user
        await update.message.reply_text(
            f"🔊 <b>Unmute လုပ်ပြီးပါပြီ</b>\n"
            f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
            f"👤 {mention}\n"
            f"🆔 <code>{target.id}</code>\n\n"
            f"👮 By: <b>{issuer.first_name}</b>",
            parse_mode="HTML"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ <b>Error:</b> <code>{e}</code>", parse_mode="HTML")

async def warn_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_strict_command(update.message.text, "warn", context.bot.username or ""):
        return
    if not await can_moderate(update, update.effective_user.id):
        return await update.message.reply_text("❌ ခွင့်ပြုချက် မရှိပါ။")
    target = await get_target_user(update, context)
    if not target:
        return await update.message.reply_text("❌ User ရှာမတွေ့ပါ။")
    err = await check_hierarchy(update.effective_chat, update.effective_user.id, target.id)
    if err:
        return await update.message.reply_text(err)
    try:
        warn_data = await warn_collection.find_one_and_update(
            {"chat_id": update.effective_chat.id, "user_id": target.id},
            {
                "$inc": {"count": 1},
                "$setOnInsert": {"chat_id": update.effective_chat.id, "user_id": target.id}
            },
            upsert=True,
            return_document=ReturnDocument.AFTER
        )
        current = warn_data.get("count", 1) if warn_data else 1
        mention = await get_user_mention(context.bot, target.id)
        issuer  = update.effective_user
        if current >= 3:
            await update.effective_chat.ban_member(target.id)
            await warn_collection.delete_one({"chat_id": update.effective_chat.id, "user_id": target.id})
            await update.message.reply_text(
                f"🚫 <b>Ban ခံရပြီ</b>  <i>(warn ၃ ကြိမ် ပြည့်)</i>\n"
                f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
                f"👤 {mention}\n"
                f"🆔 <code>{target.id}</code>\n"
                f"📊 Warns: 🟥🟥🟥  3/3\n\n"
                f"👮 By: <b>{issuer.first_name}</b>",
                parse_mode="HTML"
            )
        else:
            bar = "🟥" * current + "⬜" * (3 - current)
            await update.message.reply_text(
                f"⚠️ <b>သတိပေးချက်</b>\n"
                f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
                f"👤 {mention}\n"
                f"🆔 <code>{target.id}</code>\n"
                f"📊 Warns: {bar}  {current}/3\n\n"
                f"👮 By: <b>{issuer.first_name}</b>\n"
                f"💡 <i>warn ၃ ကြိမ် = ban</i>",
                parse_mode="HTML"
            )
    except Exception as e:
        await update.message.reply_text(f"❌ <b>Error:</b> <code>{e}</code>", parse_mode="HTML")

async def unwarn_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_strict_command(update.message.text, "unwarn", context.bot.username or ""):
        return
    if not await can_moderate(update, update.effective_user.id):
        return await update.message.reply_text("❌ ခွင့်ပြုချက် မရှိပါ။")
    target = await get_target_user(update, context)
    if not target:
        return await update.message.reply_text("❌ User ရှာမတွေ့ပါ။")
    err = await check_hierarchy(update.effective_chat, update.effective_user.id, target.id)
    if err:
        return await update.message.reply_text(err)
    try:
        updated = await warn_collection.find_one_and_update(
            {"chat_id": update.effective_chat.id, "user_id": target.id, "count": {"$gt": 0}},
            {"$inc": {"count": -1}},
            return_document=ReturnDocument.AFTER
        )
        if updated:
            new_count = updated.get("count", 0)
            mention   = await get_user_mention(context.bot, target.id)
            bar       = "🟥" * new_count + "⬜" * (3 - new_count)
            issuer    = update.effective_user
            await update.message.reply_text(
                f"✅ <b>Warn ဖျက်ပြီးပါပြီ</b>\n"
                f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
                f"👤 {mention}\n"
                f"📊 Warns: {bar}  {new_count}/3\n\n"
                f"👮 By: <b>{issuer.first_name}</b>",
                parse_mode="HTML"
            )
            if new_count <= 0:
                await warn_collection.delete_one({"chat_id": update.effective_chat.id, "user_id": target.id})
        else:
            await update.message.reply_text("❌ ဤ user တွင် warn မရှိပါ။")
    except Exception as e:
        await update.message.reply_text(f"❌ <b>Error:</b> <code>{e}</code>", parse_mode="HTML")

async def warns_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_strict_command(update.message.text, "warns", context.bot.username or ""):
        return
    if not await can_moderate(update, update.effective_user.id):
        return await update.message.reply_text("❌ ခွင့်ပြုချက် မရှိပါ။")
    target = await get_target_user(update, context)
    if not target:
        return await update.message.reply_text("❌ User ရှာမတွေ့ပါ။")
    try:
        row   = await warn_collection.find_one({"chat_id": update.effective_chat.id, "user_id": target.id})
        count = row.get("count", 0) if row else 0
        mention = await get_user_mention(context.bot, target.id)
        bar   = "🟥" * count + "⬜" * (3 - count)
        await update.message.reply_text(
            f"📊 <b>Warn အခြေအနေ</b>\n"
            f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
            f"👤 {mention}\n"
            f"📊 Warns: {bar}  {count}/3",
            parse_mode="HTML"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ <b>Error:</b> <code>{e}</code>", parse_mode="HTML")

# ══════════════════════════════════════════════════════════
#  BOT ADMIN COMMANDS
# ══════════════════════════════════════════════════════════

async def add_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return await update.message.reply_text("❌ Bot owner သာ admin ထည့်နိုင်သည်။")
    target = await get_target_user(update, context)
    if not target:
        return await update.message.reply_text("❌ User ရှာမတွေ့ပါ။")
    try:
        await admin_collection.update_one(
            {"user_id": target.id},
            {"$set": {"user_id": target.id, "added_by": update.effective_user.id,
                      "added_at": datetime.now(timezone.utc)}},
            upsert=True
        )
        _admin_cache[target.id] = time.time()
        mention = await get_user_mention(context.bot, target.id)
        await update.message.reply_text(
            f"🛡 <b>Bot Admin ထည့်ပြီးပါပြီ</b>\n"
            f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
            f"👤 {mention}\n"
            f"🆔 <code>{target.id}</code>",
            parse_mode="HTML"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ <b>Error:</b> <code>{e}</code>", parse_mode="HTML")

async def remove_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        return await update.message.reply_text("❌ Bot owner သာ admin ဖယ်ရှားနိုင်သည်။")
    target = await get_target_user(update, context)
    if not target:
        return await update.message.reply_text("❌ User ရှာမတွေ့ပါ။")
    if target.id in Config.OWNER_IDS:
        return await update.message.reply_text("❌ Owner ကို ဖယ်ရှား၍မရပါ။")
    try:
        await admin_collection.delete_one({"user_id": target.id})
        _admin_cache.pop(target.id, None)
        mention = await get_user_mention(context.bot, target.id)
        await update.message.reply_text(
            f"🗑 <b>Bot Admin ဖယ်ရှားပြီးပါပြီ</b>\n"
            f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
            f"👤 {mention}\n"
            f"🆔 <code>{target.id}</code>",
            parse_mode="HTML"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ <b>Error:</b> <code>{e}</code>", parse_mode="HTML")

async def list_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not (is_owner(user_id) or await is_admin(user_id)):
        return await update.message.reply_text("❌ Bot admin / owner သာ ကြည့်နိုင်သည်။")
    try:
        text = "🛡 <b>Bot Admin Panel</b>\n━━━━━━━━━━━━━━━\n\n👑 <b>Owners</b>\n"
        owner_list = [oid for oid in (Config.OWNER_ID, Config.OWNER_ID_2) if oid]
        for i, oid in enumerate(owner_list):
            prefix  = "└" if i == len(owner_list) - 1 else "├"
            mention = await get_user_mention(context.bot, oid)
            text   += f"{prefix} {mention} · <code>{oid}</code>\n"

        admins = await admin_collection.find({"user_id": {"$nin": list(Config.OWNER_IDS)}}).to_list(length=None)
        text  += f"\n🔰 <b>Admins</b> ({len(admins)})\n"
        if admins:
            for i, doc in enumerate(admins):
                prefix  = "└" if i == len(admins) - 1 else "├"
                mention = await get_user_mention(context.bot, doc["user_id"])
                text   += f"{prefix} {mention} · <code>{doc['user_id']}</code>\n"
        else:
            text += "└ <i>Admin မရှိသေးပါ။</i>"
        await update.message.reply_text(text, parse_mode="HTML")
    except Exception as e:
        await update.message.reply_text(f"❌ <b>Error:</b> <code>{e}</code>", parse_mode="HTML")

# ══════════════════════════════════════════════════════════
#  BASIC
# ══════════════════════════════════════════════════════════

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await sync_user_data(update)
    await sync_chat_data(update)
    user = update.effective_user
    await update.message.reply_text(
        f"👋 <b>မင်္ဂလာပါ, {user.first_name}!</b>\n\n"
        f"ကျွန်တော်သည် group စီမံခန့်ခွဲမှု bot ဖြစ်ပါသည်။\n\n"
        f"🔨 Moderation  •  🔎 Filters  •  👋 Greetings\n\n"
        f"📌 /help — command အားလုံး ကြည့်ရန်",
        parse_mode="HTML"
    )

# ══════════════════════════════════════════════════════════
#  FILTER COMMANDS
# ══════════════════════════════════════════════════════════

async def add_filter_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await sync_user_data(update)
    await sync_chat_data(update)
    reply_message        = update.message.reply_to_message
    is_sticker_filter    = False
    filter_reply_content = None
    filter_keyword       = None
    user_id              = update.effective_user.id
    chat_id              = update.effective_chat.id

    try:
        if reply_message:
            if not context.args:
                return await update.message.reply_text(
                    "❌ Keyword ထည့်ပေးပါ။\nဥပမာ — <code>/filter မင်္ဂလာပါ</code>",
                    parse_mode="HTML"
                )
            filter_keyword = " ".join(context.args).lower().strip()
            if reply_message.sticker:
                filter_reply_content = reply_message.sticker.file_id
                is_sticker_filter    = True
            elif reply_message.text:
                filter_reply_content = reply_message.text
            else:
                return await update.message.reply_text("❌ Text သို့မဟုတ် Sticker reply သာ ရပါသည်။")

        elif context.args and " - " in " ".join(context.args):
            full_text = " ".join(context.args)
            parts     = full_text.split(" - ", 1)
            if len(parts) != 2:
                return await update.message.reply_text(
                    "❌ Format မမှန်ပါ။\nဥပမာ — <code>/filter keyword - reply</code>",
                    parse_mode="HTML"
                )
            filter_keyword, filter_reply_content = parts[0].strip().lower(), parts[1].strip()

        else:
            return await update.message.reply_text(
                "❌ <b>သုံးနည်း:</b>\n"
                "① Text filter: <code>/filter keyword - reply</code>\n"
                "② Sticker filter: Sticker ကို reply + <code>/filter keyword</code>",
                parse_mode="HTML"
            )

        if not filter_keyword or not filter_reply_content:
            return await update.message.reply_text("❌ Keyword သို့မဟုတ် reply ဗလာဖြစ်နေပါသည်။")

        final_chat_id = None
        scope_message = ""
        if is_owner(user_id) or await is_admin(user_id):
            final_chat_id = "global"
            scope_message = "🌐 Global"
        elif update.effective_chat.type in ["group", "supergroup"]:
            member = await update.effective_chat.get_member(user_id)
            if member.status in ["administrator", "creator"]:
                final_chat_id = chat_id
                scope_message = "💬 Local"
            else:
                return await update.message.reply_text("❌ Group admin သာ local filter ထည့်နိုင်သည်။")
        else:
            return await update.message.reply_text("❌ Group ထဲမှသာ ထည့်နိုင်သည်။")

        await sec.add_global_filter(
            keyword=filter_keyword, reply=filter_reply_content,
            creator_id=user_id, creator_name=update.effective_user.first_name,
            chat_id=final_chat_id, is_sticker=is_sticker_filter
        )
        msg_type = "🎭 Sticker" if is_sticker_filter else "💬 Text"
        await update.message.reply_text(
            f"✅ <b>Filter ထည့်ပြီးပါပြီ</b>\n"
            f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
            f"🔑 Keyword: <code>{filter_keyword}</code>\n"
            f"📍 Scope: {scope_message}\n"
            f"📁 Type: {msg_type}",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"add_filter error: {e}")
        await update.message.reply_text(f"❌ Filter ထည့်ရာတွင် error ဖြစ်ပါသည်: {e}")

async def del_filter_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_strict_command(update.message.text, "delfilter", context.bot.username or ""):
        return
    if not context.args:
        return await update.message.reply_text(
            "❌ Keyword ထည့်ပေးပါ။\nဥပမာ — <code>/delfilter မင်္ဂလာပါ</code>",
            parse_mode="HTML"
        )
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    keyword = " ".join(context.args).lower().strip()

    deleted = False
    if is_owner(user_id) or await is_admin(user_id):
        deleted = await sec.delete_filter_by_keyword(keyword, "global")
        if not deleted and update.effective_chat.type in ["group", "supergroup"]:
            deleted = await sec.delete_filter_by_keyword(keyword, chat_id)
    elif update.effective_chat.type in ["group", "supergroup"]:
        member = await update.effective_chat.get_member(user_id)
        if member.status in ["administrator", "creator"]:
            deleted = await sec.delete_filter_by_keyword(keyword, chat_id)
        else:
            return await update.message.reply_text("❌ ခွင့်ပြုချက် မရှိပါ။")
    else:
        return await update.message.reply_text("❌ ခွင့်ပြုချက် မရှိပါ။")

    if deleted:
        await update.message.reply_text(
            f"✅ Filter <code>{keyword}</code> ဖျက်ပြီးပါပြီ", parse_mode="HTML"
        )
    else:
        await update.message.reply_text(
            f"❌ <code>{keyword}</code> Filter မတွေ့ပါ။", parse_mode="HTML"
        )

async def filter_list_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await sync_user_data(update)
    if update.effective_chat.type != "private":
        return await update.message.reply_text("❌ Filter dashboard ကို private chat ထဲမှသာ ကြည့်နိုင်သည်။")
    user_id = update.effective_user.id
    if not (is_owner(user_id) or await is_admin(user_id)):
        return await update.message.reply_text("❌ Bot admin / owner သာ ကြည့်နိုင်သည်။")
    try:
        markup = await sec.show_dashboard_admin(context.bot, user_id, page=1)
        if markup:
            await update.message.reply_text(
                "🛡 <b>Bot Admin Dashboard</b>\n"
                "━━━━━━━━━━━━━━━\n"
                "Admin တစ်ယောက်ကို နှိပ်၍ filter များကြည့်ပါ။",
                reply_markup=markup, parse_mode="HTML"
            )
        else:
            await update.message.reply_text("❌ Dashboard ဖွင့်၍မရပါ။")
    except Exception as e:
        await update.message.reply_text(f"❌ <b>Error:</b> <code>{e}</code>", parse_mode="HTML")

async def broadcast_post_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await sync_user_data(update)
    await sync_chat_data(update)
    if not is_owner(update.effective_user.id):
        return await update.message.reply_text("❌ Bot owner သာ broadcast လုပ်နိုင်သည်။")
    if not update.message.reply_to_message:
        return await update.message.reply_text("❌ Broadcast လုပ်မည့် message ကို reply လုပ်ပါ။")
    try:
        msg = update.message.reply_to_message
        user_success, group_success = await sec.smart_post(
            msg.message_id, content_type="any",
            bot=context.bot, from_chat_id=update.effective_chat.id
        )
        total = user_success + group_success
        await update.message.reply_text(
            f"📢 <b>Broadcast ပြီးပါပြီ</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"✅ စုစုပေါင်း: <b>{total}</b>\n\n"
            f"👤 Users: <b>{user_success}</b>\n"
            f"👥 Groups: <b>{group_success}</b>",
            parse_mode="HTML"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ <b>Error:</b> <code>{e}</code>", parse_mode="HTML")

# ══════════════════════════════════════════════════════════
#  AUTO REPLY
# ══════════════════════════════════════════════════════════

async def auto_reply_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or (not update.message.text and not update.message.caption):
        return
    await sync_user_data(update)
    await sync_chat_data(update)
    bot       = context.bot
    is_direct = False
    reply_msg = update.message.reply_to_message
    if reply_msg and reply_msg.from_user and reply_msg.from_user.id == bot.id:
        is_direct = True
    elif bot.username and f"@{bot.username}" in (update.message.text or update.message.caption or ""):
        is_direct = True

    text = update.message.text or update.message.caption or ""
    if cacu.is_math_expression(text):
        result = cacu.safe_calculate(text)
        if result is not None:
            reply = cacu.format_result(text, result)
            await update.message.reply_text(reply, parse_mode="HTML")
            return

    try:
        await sec.auto_reply(update, is_direct=is_direct)
    except Exception as e:
        logger.error(f"Auto-reply error: {e}")

# ══════════════════════════════════════════════════════════
#  HANDLER REGISTRATION
# ══════════════════════════════════════════════════════════

def register_all_handlers(application):
    commands = [
        ("ban",        ban_user),
        ("unban",      unban_user),
        ("mute",       mute_user),
        ("unmute",     unmute_user),
        ("warn",       warn_user),
        ("unwarn",     unwarn_user),
        ("warns",      warns_user),
        ("admin",      add_admin),
        ("unadmin",    remove_admin),
        ("adminlist",  list_admins),
        ("start",      start_command),
        ("help",       thd.help_cmd),
        ("filter",     add_filter_command),
        ("delfilter",  del_filter_command),
        ("filterlist", filter_list_command),
        ("post",       broadcast_post_command),
        ("setwelcome", thd.set_welcome),
        ("setgoodbye", thd.set_goodbye),
        ("delwelcome", thd.del_welcome),
        ("delgoodbye", thd.del_goodbye),
    ]
    for cmd, handler in commands:
        application.add_handler(CommandHandler(cmd, handler))
    application.add_handler(ChatMemberHandler(thd.member_status_handler, ChatMemberHandler.CHAT_MEMBER))
    application.add_handler(MessageHandler(
        (filters.TEXT | filters.CAPTION) & ~filters.COMMAND,
        auto_reply_handler
    ))
    application.add_handler(CallbackQueryHandler(sec.handle_callback))
    logger.info("Handler အားလုံး မှတ်ပုံတင်ပြီးပါပြီ။")

# ══════════════════════════════════════════════════════════
#  FLASK & SELF-PING
# ══════════════════════════════════════════════════════════

app_flask = Flask(__name__)

@app_flask.route("/")
def home_route():
    return "Bot is running."

def run_flask_app():
    try:
        app_flask.run(host="0.0.0.0", port=Config.FLASK_PORT)
    except Exception as e:
        logger.critical(f"Flask error: {e}")

def self_ping_task():
    if not Config.SELF_URL:
        logger.warning("SELF_URL မသတ်မှတ်ရသေးပါ။")
        return
    import requests
    while True:
        try:
            requests.get(Config.SELF_URL, timeout=10)
        except Exception as e:
            logger.error(f"Self-ping error: {e}")
        time.sleep(300)

# ══════════════════════════════════════════════════════════
#  DATABASE MAINTENANCE
# ══════════════════════════════════════════════════════════

async def prune_inactive_users():
    cutoff = datetime.now(timezone.utc) - timedelta(days=90)
    result = await user_collection.delete_many({
        "last_seen": {"$lt": cutoff},
        "user_id":   {"$nin": list(Config.OWNER_IDS)}
    })
    logger.info(f"Pruned {result.deleted_count} inactive users.")

async def prune_inactive_chats():
    cutoff = datetime.now(timezone.utc) - timedelta(days=180)
    result = await chat_collection.delete_many({"last_seen": {"$lt": cutoff}})
    logger.info(f"Pruned {result.deleted_count} inactive chats.")

async def auto_prune_scheduler():
    while True:
        try:
            await prune_inactive_users()
            await prune_inactive_chats()
        except Exception as e:
            logger.error(f"Auto-prune error: {e}")
        await asyncio.sleep(86400)

async def setup_mongodb_indexes():
    try:
        await user_collection.create_index("user_id", unique=True)
        await admin_collection.create_index("user_id", unique=True)
        await chat_collection.create_index("chat_id", unique=True)
        await global_filter_collection.create_index([("keyword", 1), ("chat_id", 1)], unique=True)
        await warn_collection.create_index([("chat_id", 1), ("user_id", 1)], unique=True)
        logger.info("MongoDB indexes ပြင်ဆင်ပြီးပါပြီ။")
    except Exception as e:
        logger.error(f"Index setup error: {e}")

# ══════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════

async def main():
    logger.info("Bot စတင်နေပါသည်...")
    await setup_mongodb_indexes()
    # thd.init_db must be called after DB is ready; it uses welcome_settings collection
    await thd.init_db(db)
    # v22.7: Application.builder() is the recommended way
    application = Application.builder().token(Config.TOKEN).build()
    register_all_handlers(application)

    threading.Thread(target=run_flask_app,       daemon=True).start()
    threading.Thread(target=self_ping_task,       daemon=True).start()
    threading.Thread(target=cache_cleaner_task,   daemon=True).start()
    asyncio.create_task(auto_prune_scheduler())   # run in the same event loop

    logger.info("Bot polling စတင်ပါပြီ...")
    await application.run_polling(allowed_updates=["message", "callback_query", "chat_member"])

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except ValueError as ve:
        logger.critical(f"Config error: {ve}")
    except Exception as e:
        logger.critical(f"Unhandled error: {e}")
