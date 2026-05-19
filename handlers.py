# handlers.py - Render‑optimized final version (broadcast uses Users collection, safe context cleanup, compiled regex, minimal typing)
import re
import asyncio
import uuid
import logging
import html
import hashlib
import time as _time
from urllib.parse import quote
from datetime import datetime, date, timedelta, timezone
from enum import Enum
from functools import wraps

import aiohttp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatAction, ParseMode
from telegram.error import BadRequest, TelegramError, RetryAfter, TimedOut, NetworkError
from telegram.ext import ContextTypes, ConversationHandler

import master
from database import UTC_TZ, _ensure_utc

logger = logging.getLogger(__name__)

WAIT_GAME_ID, WAIT_CONFIRMATION, WAIT_PAYMENT = range(3)
ADMIN_PARSE_MODE = ParseMode.HTML

# Compiled regex patterns (CPU light)
DIA_ID_RE = re.compile(r"^(\d{5,20})[\s\-()]+(\d{3,6})\)?$")
UC_ID_RE = re.compile(r"^(\d{5,20})$")
SET_PRICE_RE = re.compile(r"/set(dia|uc)\s+(.+)", re.IGNORECASE)

class OrderStatus(str, Enum):
    PENDING_ID = "pending_id"
    CONFIRMING_ID = "confirming_id"
    WAITING_PAYMENT = "waiting_payment"
    VERIFYING = "verifying"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"

# ── Helpers ──
def get_db(context: ContextTypes.DEFAULT_TYPE):
    db = context.bot_data.get('db')
    if not db:
        logger.critical("DatabaseManager missing in bot_data!")
    return db

def get_admin_id(context: ContextTypes.DEFAULT_TYPE) -> int:
    db = get_db(context)
    return db.primary_admin_id if db else 0

async def is_owner_or_master(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    return user_id == get_admin_id(context) or master.is_master(user_id)

async def send_typing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)

async def safe_delete_message(message, context: ContextTypes.DEFAULT_TYPE = None):
    if not message:
        return
    try:
        await message.delete()
    except BadRequest as e:
        if "message to delete not found" not in str(e).lower():
            logger.warning(f"Delete failed: {e}")
    except TelegramError:
        pass
    except Exception as e:
        logger.warning(f"Unexpected error during delete: {e}")

def escape_html(text: str) -> str:
    if text is None:
        return ""
    return html.escape(str(text))

def retry_on_telegram_error(max_retries=3, initial_delay=1.0):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            delay = initial_delay
            for attempt in range(1, max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except (TimedOut, NetworkError) as e:
                    logger.warning(f"Telegram network error (attempt {attempt}/{max_retries}): {e}")
                    last_exception = e
                    await asyncio.sleep(delay)
                    delay *= 2
                except RetryAfter as e:
                    wait = e.retry_after
                    logger.warning(f"Telegram RetryAfter {wait}s (attempt {attempt}/{max_retries})")
                    await asyncio.sleep(wait)
                    last_exception = e
                except TelegramError as e:
                    raise
            raise last_exception
        return wrapper
    return decorator

def handle_errors(handler_func):
    @wraps(handler_func)
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            return await handler_func(update, context)
        except Exception as e:
            logger.exception(f"Unhandled error in {handler_func.__name__}: {e}")
            try:
                admin_id = get_admin_id(context)
                user_info = ""
                if update and update.effective_user:
                    user_info = f"User: {update.effective_user.id} | "
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=f"⚠️ Error in {handler_func.__name__}\n{user_info}{str(e)[:300]}"
                )
            except Exception as notify_err:
                logger.error(f"Failed to send error notification: {notify_err}")
            error_msg = "⚠️ စနစ်တွင် အမှားအယွင်းတစ်ခု ဖြစ်ပွားသွားပါသည်။ ကျေးဇူးပြု၍ ခဏနေမှ ထပ်မံကြိုးစားပါ။"
            if update and update.effective_chat:
                try:
                    await context.bot.send_message(chat_id=update.effective_chat.id, text=error_msg)
                except Exception:
                    pass
            return ConversationHandler.END
    return wrapped

async def check_license_logic(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not update.effective_user:
        return False
    user_id = update.effective_user.id
    if master.is_master(user_id):
        return True
    db = get_db(context)
    if not db:
        return False
    return await db.license_repo.is_license_valid(get_admin_id(context))

def wrap_with_license(handler_func):
    @wraps(handler_func)
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE):
        db = get_db(context)
        if not db:
            if update.callback_query:
                await update.callback_query.answer("⏳ စနစ်ချိတ်ဆက်မှု ပြတ်တောက်နေပါသည်။ ခဏစောင့်ပါ။", show_alert=True)
            elif update.message:
                await update.message.reply_text("⏳ စနစ်ချိတ်ဆက်မှု ပြတ်တောက်နေပါသည်။ ခဏစောင့်ပါ။")
            return ConversationHandler.END
        if not await check_license_logic(update, context):
            deny_msg = "⛔ ဆိုင်၏ လိုင်စင်သက်တမ်းကုန်သွားပါပြီ။\nကျေးဇူးပြု၍ Master ထံ ဆက်သွယ်ပါ။"
            if update.message:
                await update.message.reply_text(deny_msg)
            elif update.callback_query:
                await update.callback_query.answer(deny_msg, show_alert=True)
            return ConversationHandler.END
        return await handler_func(update, context)
    return wrapped

# ── Price hash helpers (user‑scoped, sha256/12) ──
def _price_hash(diamond_str: str) -> str:
    return hashlib.sha256(diamond_str.encode()).hexdigest()[:12]

def _build_price_hash_map(prices: list) -> dict:
    mapping = {}
    for p in prices:
        amount = p.get('amount') or p.get('diamond', '')
        amount_str = str(amount).strip()
        if amount_str:
            mapping[_price_hash(amount_str)] = amount_str
    return mapping

def validate_game_id(item_type: str, text: str) -> tuple:
    text = text.strip()
    if item_type == "dia":
        match = DIA_ID_RE.match(text)
        if match:
            return match.groups()
        return None
    else:
        match = UC_ID_RE.match(text)
        if match:
            return (match.group(1), "")
        return None

def safe_user_mention(user_id: int, first_name: str = None, last_name: str = None, fallback: str = "User") -> str:
    uid = int(user_id)
    if first_name or last_name:
        name = f"{first_name or ''} {last_name or ''}".strip()
        name = escape_html(name[:64])
    else:
        name = escape_html(fallback[:64])
    return f'<a href="tg://user?id={uid}">{name}</a>'

def build_caption_from_order(order: dict, status_text: str, user_first_name: str = None, user_last_name: str = None) -> str:
    order_id = escape_html(order.get("order_id", "N/A"))
    user_id = order.get("user_id", 0)
    first = user_first_name or order.get("profile_name", str(user_id))
    last = user_last_name
    qty = order.get("order_info", {}).get("quantity", "N/A")
    item = order.get("order_info", {}).get("item_type", "dia")
    game_id = order.get("order_info", {}).get("game_id", "N/A")
    zone_id = order.get("order_info", {}).get("zone_id", "")
    nickname = order.get("order_info", {}).get("nickname", "")
    # Pre‑escape for repeated use
    safe_qty = escape_html(qty)
    safe_game_id = escape_html(game_id)
    safe_zone_id = escape_html(zone_id) if zone_id else ""
    safe_nick = escape_html(nickname) if nickname and nickname != "N/A" else None

    emoji = "💎" if item == "dia" else "💵"
    game_str = safe_game_id
    if safe_zone_id:
        game_str += f" ({safe_zone_id})"

    name_line = f"👤 ဝယ်သူ: {safe_user_mention(user_id, first, last)}"
    id_line = f'🆔 User ID: <a href="tg://user?id={user_id}">{user_id}</a>'
    ig_line = f"👤 IG Name: <b>{safe_nick}</b>\n" if safe_nick else "👤 IG Name: <i>(Check Failed)</i>\n"

    return (
        f"📦 Order ID: <code>{order_id}</code>\n"
        f"{name_line}\n"
        f"{id_line}\n"
        f"{ig_line}"
        f"🆔 Game ID: <code>{game_str}</code>\n"
        f"{emoji} ပစ္စည်း: {safe_qty} {item.upper()}\n"
        f"💰 အခြေအနေ: {escape_html(status_text)}"
    )

def sort_price_items(prices: list) -> list:
    def sort_key(item):
        raw = item.get('amount') or item.get('diamond')
        val = str(raw).strip()
        try:
            num = float(val)
            return (0, num, '')
        except ValueError:
            return (1, val.lower(), val)
    return sorted(prices, key=sort_key)

async def edit_admin_message(query, new_caption: str, reply_markup=None):
    try:
        if query.message.photo:
            await query.message.edit_caption(
                caption=new_caption,
                parse_mode=ADMIN_PARSE_MODE,
                reply_markup=reply_markup
            )
        else:
            await query.message.edit_text(
                text=new_caption,
                parse_mode=ADMIN_PARSE_MODE,
                reply_markup=reply_markup
            )
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            pass
        else:
            try:
                await query.message.edit_text(
                    text=new_caption,
                    parse_mode=ADMIN_PARSE_MODE,
                    reply_markup=reply_markup
                )
            except Exception as e2:
                logger.warning(f"edit_admin_message fallback failed: {e2}")
    except Exception as e:
        logger.error(f"Error editing admin message: {e}")

async def _send_db_error(update: Update):
    msg = "⚠️ ဒေတာဘေ့စ် ချိတ်ဆက်မှု မရှိပါ။ ကျေးဇူးပြု၍ ခဏနေမှ ထပ်စမ်းကြည့်ပါ။"
    if update.message:
        await update.message.reply_text(msg)
    elif update.callback_query:
        await update.callback_query.answer(msg, show_alert=True)

# ── Order context cleanup (safe key removal) ──
ORDER_KEYS = (
    "current_order_id", "item_type", "quantity",
    "temp_name", "temp_region", "last_payment_time",
    "price_hash_map"
)

def clear_order_context(context):
    for key in ORDER_KEYS:
        context.user_data.pop(key, None)

# ── Timeout Handler ──
TIMEOUT_SECONDS = 300

async def timeout_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    db = get_db(context)
    order_id = context.user_data.get('current_order_id')
    if db and order_id:
        try:
            await db.order_repo.delete_order(order_id)
            logger.info(f"Timeout: Order {order_id} deleted.")
        except Exception as e:
            logger.error(f"Timeout deletion failed: {e}")
    if update and update.effective_chat:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="⏰ အချိန်ကျော်သွားပါပြီ။ ၅ မိနစ်အတွင်း Game ID ထည့်သွင်းခြင်း မရှိသောကြောင့် အော်ဒါကို ပယ်ဖျက်လိုက်ပါသည်။\nကျေးဇူးပြု၍ /start မှ ပြန်လည်စတင်ပေးပါ။"
        )
    clear_order_context(context)
    return ConversationHandler.END

# ── Name Check API & Cache ──
NICKNAME_CACHE_HOURS = 24

async def get_cached_nickname(db, game_id: str, zone_id: str):
    col = db.nickname_cache
    doc = await col.find_one({"game_id": game_id, "zone_id": zone_id})
    if doc:
        ts = doc.get("timestamp")
        if ts:
            ts = _ensure_utc(ts)
            if ts and (datetime.now(UTC_TZ) - ts) < timedelta(hours=NICKNAME_CACHE_HOURS):
                return doc.get("nickname"), doc.get("region", "Unknown"), ts
    return None

async def set_cached_nickname(db, game_id: str, zone_id: str, nickname: str, region: str = "Unknown"):
    col = db.nickname_cache
    try:
        await col.update_one(
            {"game_id": game_id, "zone_id": zone_id},
            {"$set": {"nickname": nickname, "region": region, "timestamp": datetime.now(UTC_TZ)}},
            upsert=True
        )
        return True
    except Exception as e:
        logger.error(f"Failed to cache nickname: {e}")
        return False

async def fetch_game_nickname(context: ContextTypes.DEFAULT_TYPE, api_url: str, game_id: str, zone_id: str = "") -> tuple:
    session = context.bot_data.get('http_session')
    own_session = False
    if not session:
        session = aiohttp.ClientSession()
        own_session = True
    try:
        url = api_url
        safe_game_id = quote(game_id, safe='')
        safe_zone_id = quote(zone_id, safe='')
        if "{id}" in url and "{zone}" in url:
            url = api_url.format(id=safe_game_id, zone=safe_zone_id)
        else:
            url = url.replace("USER_ID", safe_game_id).replace("ZONE_ID", safe_zone_id)
            url = url.replace("{id}", safe_game_id).replace("{zone}", safe_zone_id)
        timeout = aiohttp.ClientTimeout(total=5)
        async with session.get(url, timeout=timeout) as resp:
            if resp.status == 200:
                try:
                    data = await resp.json()
                except (aiohttp.ContentTypeError, ValueError) as json_err:
                    logger.warning(f"Name check API returned invalid JSON for {game_id}: {json_err}")
                    return "N/A", "Unknown"
                return data.get("name", "Unknown"), data.get("country", "Unknown")
            else:
                logger.warning(f"Name check API returned {resp.status}")
    except asyncio.TimeoutError:
        logger.warning(f"Name check API timed out for {game_id}")
    except Exception as e:
        logger.error(f"Name check API call failed: {e}")
    finally:
        if own_session:
            await session.close()
    return "N/A", "Unknown"

# ──────────────────────────────────────
# User‑Facing Handlers
# ──────────────────────────────────────
@handle_errors
@wrap_with_license
async def send_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_typing(update, context)
    db = get_db(context)
    admin_id = db.primary_admin_id
    user_id = update.effective_user.id

    # ⚡️ Register user in lightweight Users collection for broadcast
    try:
        await db.users_repo.upsert_user(user_id)
    except Exception:
        pass

    if not await db.settings_repo.get_service_status() and user_id != admin_id:
        text = "⚠️ Service ခေတ္တရပ်ထားပါသည်"
        if update.message:
            await update.message.reply_text(text)
        elif update.callback_query:
            await update.callback_query.answer(text, show_alert=True)
        return ConversationHandler.END

    keyboard = [[
        InlineKeyboardButton("💎 စိန်ဝယ်ယူရန်", callback_data="show_dia"),
        InlineKeyboardButton("💵 UCဝယ်ယူရန်", callback_data="show_uc")
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    welcome_msg_id = await db.settings_repo.get_config("welcome_msg_id")
    if welcome_msg_id:
        try:
            chat_id = update.message.chat_id if update.message else update.callback_query.message.chat_id
            await context.bot.copy_message(chat_id=chat_id, from_chat_id=admin_id, message_id=welcome_msg_id, reply_markup=reply_markup)
            if update.callback_query:
                await safe_delete_message(update.callback_query.message, context)
            return ConversationHandler.END
        except Exception as e:
            logger.error(f"Welcome copy failed: {e}")

    text = "💎 Diamond Bot မှ ကြိုဆိုပါတယ်။\nရွေးချယ်ပါ👇"
    if update.message:
        await update.message.reply_text(text, reply_markup=reply_markup)
    else:
        await update.callback_query.message.reply_text(text, reply_markup=reply_markup)
    return ConversationHandler.END

@handle_errors
@wrap_with_license
async def show_items(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_typing(update, context)
    db = get_db(context)
    admin_id = db.primary_admin_id
    query = update.callback_query
    await query.answer()
    item_type = "dia" if query.data == "show_dia" else "uc"
    prices = await db.price_repo.get_active_prices(item_type)
    if not prices:
        await query.message.reply_text("လောလောဆယ် ဈေးနှုန်း မရှိသေးပါ။")
        return ConversationHandler.END

    sorted_prices = sort_price_items(prices)
    hash_map = _build_price_hash_map(sorted_prices)
    context.user_data['price_hash_map'] = hash_map   # per user

    keyboard = []
    row = []
    for p in sorted_prices:
        amount = p.get('amount') or p.get('diamond')
        btn_text = str(amount).strip()
        cb_data = f"price_{item_type}_{_price_hash(btn_text)}"
        if len(btn_text) > 18:
            if row:
                keyboard.append(row)
                row = []
            keyboard.append([InlineKeyboardButton(btn_text, callback_data=cb_data)])
        else:
            row.append(InlineKeyboardButton(btn_text, callback_data=cb_data))
            if len(row) == 3:
                keyboard.append(row)
                row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("🔙 နောက်သို့", callback_data="back_to_main")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    msg_id_key = "dia_msg_id" if item_type == "dia" else "uc_msg_id"
    saved_msg_id = await db.settings_repo.get_config(msg_id_key)
    if saved_msg_id:
        try:
            await context.bot.copy_message(chat_id=query.message.chat_id, from_chat_id=admin_id, message_id=saved_msg_id, reply_markup=reply_markup)
            await safe_delete_message(query.message, context)
            return ConversationHandler.END
        except Exception as e:
            logger.error(f"Item menu copy failed: {e}")

    await query.message.edit_text("ဈေးနှုန်းများ ရွေးချယ်ပါ👇", reply_markup=reply_markup)
    return ConversationHandler.END

@handle_errors
@wrap_with_license
async def step1_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_typing(update, context)
    db = get_db(context)
    query = update.callback_query
    await query.answer()
    parts = query.data.split('_', 2)
    if len(parts) < 3:
        await query.message.reply_text("⚠️ Invalid selection")
        return ConversationHandler.END
    item_type = parts[1]
    hash_val = parts[2]
    hash_map = context.user_data.get('price_hash_map', {})
    amount_str = hash_map.get(hash_val)
    # 🧹 Remove hash map from user data after use
    context.user_data.pop('price_hash_map', None)

    if not amount_str:
        await query.message.reply_text("⚠️ ဈေးနှုန်းမတွေ့ပါ။ ပြန်စပါ။")
        return ConversationHandler.END

    price_data = await db.price_repo.get_price_by_amount(item_type, amount_str)
    if not price_data:
        await query.message.reply_text("⚠️ မတွေ့ပါ")
        return ConversationHandler.END

    order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
    context.user_data['current_order_id'] = order_id
    context.user_data['item_type'] = item_type
    context.user_data['quantity'] = amount_str

    result = await db.order_repo.create_order(order_id, query.from_user.id,
                                              query.from_user.first_name,
                                              amount_str, 0, item_type)
    if not result:
        await query.message.reply_text("⚠️ Order ဖန်တီးရာတွင် အမှားအယွင်းရှိနေပါသည်။ ပြန်လည်ကြိုးစားပါ။")
        return ConversationHandler.END

    prompt = ("🆔 Game ID + Zone ID ရိုက်ထည့်ပါ\nFormat: 123456789 1234"
              if item_type == "dia" else
              "🆔 Game ID ရိုက်ထည့်ပါ (ဥပမာ - 123456789)")
    await query.message.reply_text(prompt)
    return WAIT_GAME_ID

@handle_errors
@wrap_with_license
async def step2_id_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_typing(update, context)
    db = get_db(context)
    text = update.message.text.strip()
    order_id = context.user_data.get('current_order_id')
    item_type = context.user_data.get('item_type')

    if not order_id:
        await update.message.reply_text("⚠️ အော်ဒါအချက်အလက် ပျောက်ဆုံးသွားပါသဖြင့် ကျေးဇူးပြု၍ အော်ဒါအသစ် ပြန်လုပ်ပေးပါ။")
        return ConversationHandler.END

    order = await db.order_repo.get_order(order_id)
    if not order:
        await update.message.reply_text("⏰ အော်ဒါသက်တမ်းကုန်သွားပါပြီ။ ကျေးဇူးပြု၍ ပြန်လည်စတင်ပေးပါ။")
        return ConversationHandler.END

    val = validate_game_id(item_type, text)
    if not val:
        if item_type == "dia":
            await update.message.reply_text("❌ Diamond အတွက် Format: `123456789 1234` (သို့) `123456789-1234`")
        else:
            await update.message.reply_text("❌ UC အတွက် Format: `123456789` (ID တစ်ခုတည်း)")
        return WAIT_GAME_ID

    game_id, zone_id = val
    await db.order_repo.update_order_game_id(order_id, game_id, zone_id)

    name_check_api = context.bot_data.get('name_check_api')
    nickname, region = "N/A", "Unknown"
    wait_msg = await update.message.reply_text("⏳ Game ID စစ်ဆေးနေပါသည်...")
    try:
        if name_check_api:
            cached = await get_cached_nickname(db, game_id, zone_id)
            if cached:
                nickname, region, ts = cached
            else:
                await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)
                nickname, region = await fetch_game_nickname(context, name_check_api, game_id, zone_id)
                if nickname != "N/A":
                    await set_cached_nickname(db, game_id, zone_id, nickname, region)
    finally:
        await safe_delete_message(wait_msg, context)

    try:
        await db.orders.update_one(
            {"order_id": order_id},
            {"$set": {"order_info.nickname": nickname, "order_info.region": region}}
        )
    except Exception as e:
        logger.error(f"Failed to save nickname: {e}")

    context.user_data['temp_name'] = nickname
    context.user_data['temp_region'] = region

    safe_gid = escape_html(game_id)
    safe_zid = escape_html(zone_id) if zone_id else ""
    confirm_text = (
        f"IG Name: <b>{escape_html(nickname)}</b>\n"
        f"ID     : <b>{safe_gid}{' (' + safe_zid + ')' if zone_id else ''}</b>\n"
        f"Region : <b>{escape_html(region)}</b>\n\n"
        "အချက်အလက် မှန်ကန်ပါက Confirm နှိပ်ပါ"
    )
    await update.message.reply_text(
        confirm_text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ အတည်ပြုမယ်", callback_data="confirm_id"),
            InlineKeyboardButton("✏️ ID ပြန်ပြင်မယ်", callback_data="back_id")
        ]])
    )
    return WAIT_CONFIRMATION

@handle_errors
@wrap_with_license
async def step3_validation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_typing(update, context)
    db = get_db(context)
    admin_id = db.primary_admin_id
    query = update.callback_query
    await query.answer()
    order_id = context.user_data.get('current_order_id')
    item_type = context.user_data.get('item_type')
    quantity = context.user_data.get('quantity')

    order = await db.order_repo.get_order(order_id)
    if not order:
        await query.message.reply_text("⏰ အော်ဒါသက်တမ်းကုန်သွားပါပြီ။ ပြန်လည်စတင်ပါ။")
        return ConversationHandler.END

    if query.data == "back_id":
        await query.message.reply_text("🆔 Game ID ပြန်ရိုက်ထည့်ပါ")
        return WAIT_GAME_ID
    elif query.data == "confirm_id":
        await db.order_repo.update_order_status(order_id, OrderStatus.WAITING_PAYMENT)
        region = context.user_data.get('temp_region', 'Myanmar')
        await db.orders.update_one(
            {"order_id": order_id},
            {"$set": {"order_info.region": region}}
        )
        payment_msg_id = await db.price_repo.get_price_msg_id(item_type, quantity)
        reply_markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ မဝယ်တော့ပါ", callback_data=f"cancel_user_{order_id}")
        ]])
        if payment_msg_id:
            try:
                await context.bot.copy_message(chat_id=query.message.chat_id,
                                               from_chat_id=admin_id,
                                               message_id=payment_msg_id,
                                               reply_markup=reply_markup)
                await safe_delete_message(query.message, context)
            except Exception as e:
                logger.error(f"Payment image copy failed: {e}")
                await query.message.edit_text("📸 ငွေလွှဲ Screenshot ပို့ပေးပါ", reply_markup=reply_markup)
        else:
            await query.message.edit_text("📸 ငွေလွှဲ Screenshot ပို့ပေးပါ", reply_markup=reply_markup)
        return WAIT_PAYMENT
    return ConversationHandler.END

@handle_errors
@wrap_with_license
async def step4_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    admin_id = db.primary_admin_id

    if not update.message.photo:
        await update.message.reply_text("📸 ငွေလွှဲ Screenshot (ပုံ) သာ ပို့ပေးရပါမည်။")
        return WAIT_PAYMENT

    # Anti-spam cooldown
    user_id = update.effective_user.id
    last_time = context.user_data.get('last_payment_time', 0)
    now_ts = _time.time()
    if now_ts - last_time < 10:
        await update.message.reply_text("⏳ ကျေးဇူးပြု၍ ၁၀ စက္ကန့်ခန့်စောင့်ပြီးမှ ထပ်ပို့ပါ။")
        return WAIT_PAYMENT
    context.user_data['last_payment_time'] = now_ts

    order_id = context.user_data.get('current_order_id')
    if not order_id:
        await update.message.reply_text("⚠️ အော်ဒါအချက်အလက် ပျောက်ဆုံးသွားပါသဖြင့် ကျေးဇူးပြု၍ /start မှ ပြန်လည်စတင်ပါ။")
        return ConversationHandler.END

    order = await db.order_repo.get_order(order_id)
    if not order:
        await update.message.reply_text("⏰ အော်ဒါသက်တမ်းကုန်သွားပါပြီ။ /start မှ ပြန်စတင်ပါ။")
        return ConversationHandler.END

    # Atomic transition to VERIFYING
    result = await db.orders.update_one(
        {"order_id": order_id, "status": OrderStatus.WAITING_PAYMENT},
        {"$set": {"status": OrderStatus.VERIFYING, "timestamps.updated_at": datetime.now(UTC_TZ)}}
    )
    if result.modified_count == 0:
        await update.message.reply_text("⚠️ ဤအော်ဒါကို လက်ခံပြီးသား သို့မဟုတ် အခြေအနေပြောင်းသွားပါပြီ။")
        return ConversationHandler.END

    user = update.effective_user
    qty = context.user_data.get('quantity', 'N/A')
    item = context.user_data.get('item_type', 'dia')

    caption = build_caption_from_order(order, "စစ်ဆေးရန်...", user.first_name, user.last_name)
    admin_markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ လက်ခံမယ်", callback_data=f"admin_approve_{order_id}"),
        InlineKeyboardButton("❌ ငြင်းပယ်မယ်", callback_data=f"admin_reject_{order_id}")
    ]])

    @retry_on_telegram_error(max_retries=3)
    async def send_admin_photo(chat_id, photo, caption, reply_markup):
        return await context.bot.send_photo(
            chat_id=chat_id,
            photo=photo,
            caption=caption,
            parse_mode=ADMIN_PARSE_MODE,
            reply_markup=reply_markup
        )

    try:
        sent = await send_admin_photo(
            chat_id=admin_id,
            photo=update.message.photo[-1].file_id,
            caption=caption,
            reply_markup=admin_markup
        )
        await db.order_repo.set_order_admin_msg_id(order_id, sent.message_id)
    except Exception as e:
        logger.error(f"Failed to forward to admin after retries: {e}")
        # Revert to WAITING_PAYMENT
        await db.orders.update_one(
            {"order_id": order_id, "status": OrderStatus.VERIFYING},
            {"$set": {"status": OrderStatus.WAITING_PAYMENT, "timestamps.updated_at": datetime.now(UTC_TZ)}}
        )
        await update.message.reply_text("⚠️ Admin ထံ အချက်အလက်ပို့ရာတွင် ချို့ယွင်းသွားပါသည်။ ကျေးဇူးပြု၍ ငွေလွှဲပုံကို ထပ်မံပို့ပေးပါ။")
        return WAIT_PAYMENT

    game_id = order.get("order_info", {}).get("game_id", "N/A")
    zone_id = order.get("order_info", {}).get("zone_id", "")
    nickname = order.get("order_info", {}).get("nickname", "N/A")
    game_display = f"{escape_html(game_id)} ({escape_html(zone_id)})" if zone_id else escape_html(game_id)
    summary = (
        f"📦 Order ID: <code>{escape_html(order_id)}</code>\n"
        f"👤 IG Name: <b>{escape_html(nickname) if nickname != 'N/A' else 'N/A'}</b>\n"
        f"🆔 Game ID: <b>{game_display}</b>\n"
        f"{'💎' if item == 'dia' else '💵'} ပစ္စည်း: <b>{escape_html(qty)} {item.upper()}</b>\n\n"
        "✅ <b>လူကြီးမင်း၏ Order တင်မှု အောင်မြင်ပါသည်။</b>"
    )
    await update.message.reply_text(summary, parse_mode="HTML")
    clear_order_context(context)
    return ConversationHandler.END

# ──────────────────────────────────────
# Admin Callback Handlers
# ──────────────────────────────────────
@handle_errors
async def admin_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        return
    admin_id = db.primary_admin_id
    query = update.callback_query
    user_id = query.from_user.id
    if not await db.license_repo.is_license_valid(admin_id):
        await query.answer("⛔ ဆိုင်၏ လိုင်စင်သက်တမ်းကုန်သွားပါပြီ။", show_alert=True)
        return
    if not await is_owner_or_master(user_id, context):
        await query.answer("⛔ သင်သည် ဤလုပ်ဆောင်ချက်ကို လုပ်ပိုင်ခွင့်မရှိပါ။", show_alert=True)
        return
    await query.answer()

    data = query.data
    try:
        prefix, order_id = data.rsplit("_", 1)
    except ValueError:
        await query.answer("Invalid data", show_alert=True)
        return

    order = await db.order_repo.get_order(order_id)
    if not order:
        await query.answer("အော်ဒါ မတွေ့ပါ။", show_alert=True)
        return

    user_customer = order.get("user_id")
    first_name = order.get("profile_name", str(user_customer))
    last_name = None
    qty = order.get("order_info", {}).get("quantity", "N/A")
    item = order.get("order_info", {}).get("item_type", "dia")

    if data.startswith("admin_approve_"):
        result = await db.orders.update_one(
            {"order_id": order_id, "status": OrderStatus.VERIFYING},
            {"$set": {"status": OrderStatus.PROCESSING, "timestamps.updated_at": datetime.now(UTC_TZ)}}
        )
        if result.modified_count == 0:
            await query.answer("⚠️ အော်ဒါကို လက်ခံပြီးသား သို့မဟုတ် အခြေအနေပြောင်းသွားပါပြီ။", show_alert=True)
            return

        await db.price_repo.increment_monthly_count(qty, item)
        await context.bot.send_message(chat_id=user_customer,
                                       text="✅ Admin မှ ငွေလွှဲမှုကို အတည်ပြုလိုက်ပါပြီ။ ခေတ္တစောင့်ပါ။")
        caption = build_caption_from_order(order, "⏳ Approved - Waiting for completion",
                                           first_name, last_name)
        markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("💎 ထည့်ပြီးပြီ", callback_data=f"admin_complete_{order_id}")
        ]])
        await edit_admin_message(query, caption, markup)

    elif data.startswith("admin_complete_"):
        result = await db.orders.update_one(
            {"order_id": order_id, "status": OrderStatus.PROCESSING},
            {"$set": {
                "status": OrderStatus.COMPLETED,
                "timestamps.updated_at": datetime.now(UTC_TZ)
            }}
        )
        if result.modified_count == 0:
            await query.answer("⚠️ အော်ဒါကို ပြီးဆုံးကြောင်း မှတ်သားပြီးဖြစ်နိုင်ပါသည်။", show_alert=True)
            return

        await context.bot.send_message(chat_id=user_customer,
                                       text="✅ သင်ဝယ်ထားသော Item ကို အောင်မြင်စွာ ထည့်ပေးပြီးပါပြီ။",
                                       reply_markup=InlineKeyboardMarkup([[
                                           InlineKeyboardButton("🔄 နောက်ထပ် ဝယ်ယူရန်", callback_data="new_order")
                                       ]]))
        caption = build_caption_from_order(order, "✅ Completed", first_name, last_name)
        await edit_admin_message(query, caption, reply_markup=None)
        await db.orders.update_one({"order_id": order_id}, {"$unset": {"admin_msg_id": ""}})

    elif data.startswith("admin_reject_"):
        result = await db.orders.update_one(
            {"order_id": order_id, "status": OrderStatus.VERIFYING},
            {"$set": {
                "status": OrderStatus.FAILED,
                "timestamps.updated_at": datetime.now(UTC_TZ)
            }}
        )
        if result.modified_count == 0:
            await query.answer("⚠️ အော်ဒါကို ငြင်းပယ်ပြီးဖြစ်နိုင်ပါသည်။", show_alert=True)
            return

        support_btn = InlineKeyboardMarkup([[
            InlineKeyboardButton("ownerထံ ဆက်သွယ်ရန်", url=f"tg://user?id={admin_id}")
        ]])
        try:
            await context.bot.send_message(chat_id=user_customer,
                                           text="❌ လူကြီးမင်း၏ အော်ဒါ ငြင်းပယ်ခံရပါသည်။",
                                           reply_markup=support_btn)
        except Exception as e:
            logger.error(f"Rejection msg fail: {e}")
        caption = build_caption_from_order(order, "❌ Rejected", first_name, last_name)
        await edit_admin_message(query, caption, reply_markup=None)
        await db.orders.update_one({"order_id": order_id}, {"$unset": {"admin_msg_id": ""}})

    elif data.startswith("admin_manual_"):
        result = await db.orders.update_one(
            {"order_id": order_id, "status": {"$ne": OrderStatus.COMPLETED}},
            {"$set": {
                "status": OrderStatus.COMPLETED,
                "timestamps.updated_at": datetime.now(UTC_TZ)
            }}
        )
        if result.modified_count == 0:
            await query.answer("⚠️ အော်ဒါသည် ပြီးဆုံးနေပြီး သို့မဟုတ် မတွေ့ပါ။", show_alert=True)
            return

        if order.get("status") != OrderStatus.PROCESSING:
            await db.price_repo.increment_monthly_count(qty, item)

        caption = build_caption_from_order(order, "✅ Manual Completed", first_name, last_name)
        await edit_admin_message(query, caption, reply_markup=None)
        await db.orders.update_one({"order_id": order_id}, {"$unset": {"admin_msg_id": ""}})

@handle_errors
@wrap_with_license
async def user_cancel_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    query = update.callback_query
    await query.answer()
    data = query.data
    try:
        _, order_id = data.rsplit("_", 1)
    except ValueError:
        await query.message.reply_text("Invalid cancel data")
        return
    order = await db.order_repo.get_order(order_id)
    if not order:
        await query.message.edit_text("⏰ အော်ဒါမရှိတော့ပါ။")
        return

    result = await db.orders.delete_one({
        "order_id": order_id,
        "status": OrderStatus.WAITING_PAYMENT
    })
    if result.deleted_count == 0:
        await query.message.reply_text("⚠️ အော်ဒါကို လက်ခံပြီးသားဖြစ်၍ Cancel မရတော့ပါ။")
        return

    admin_msg_id = order.get("admin_msg_id")
    if admin_msg_id:
        admin_id = db.primary_admin_id
        caption = build_caption_from_order(
            order, "❌ User မှ Cancel လုပ်သွားပါသည်",
            order.get("profile_name", str(order["user_id"]))
        )
        try:
            await context.bot.edit_message_text(chat_id=admin_id,
                                                message_id=admin_msg_id,
                                                text=caption,
                                                parse_mode=ADMIN_PARSE_MODE,
                                                reply_markup=None)
        except BadRequest as e:
            if "message is not modified" not in str(e).lower():
                try:
                    await context.bot.edit_message_caption(chat_id=admin_id,
                                                           message_id=admin_msg_id,
                                                           caption=caption,
                                                           parse_mode=ADMIN_PARSE_MODE,
                                                           reply_markup=None)
                except Exception as e2:
                    logger.warning(f"Could not update admin cancel message: {e2}")
        except Exception as e:
            logger.warning(f"Admin cancel update error: {e}")
    await query.message.edit_text("✅ Cancel ပြီး")

@handle_errors
@wrap_with_license
async def new_order_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    return await send_welcome(update, context)

# ──────────────────────────────────────
# Admin Commands (typing removed for speed)
# ──────────────────────────────────────
@handle_errors
async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        return await _send_db_error(update)
    if await is_owner_or_master(update.effective_user.id, context):
        await db.settings_repo.set_service_status(False)
        await update.message.reply_text("🛑 Service ခေတ္တရပ်ထားပါသည်။")
    else:
        await update.message.reply_text("⛔ အခွင့်အရေးမရှိပါ။")

@handle_errors
async def open_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        return await _send_db_error(update)
    if await is_owner_or_master(update.effective_user.id, context):
        await db.settings_repo.set_service_status(True)
        await update.message.reply_text("✅ Service ပြန်လည်ဖွင့်လှစ်လိုက်ပါပြီ။")
    else:
        await update.message.reply_text("⛔ အခွင့်အရေးမရှိပါ။")

@handle_errors
async def post_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        return await _send_db_error(update)
    if not (await is_owner_or_master(update.effective_user.id, context)):
        return
    if not update.message.reply_to_message:
        await update.message.reply_text("❌ ပို့ချင်သော Message ကို Reply လုပ်ပြီး /post ကိုသုံးပါ။")
        return

    await update.message.reply_text("⏳ သုံးစွဲသူများထံ စတင်ပို့နေပါသည်...")

    try:
        user_ids = await db.users_repo.get_all_user_ids()
    except Exception as e:
        logger.error(f"Failed to fetch user list: {e}")
        await update.message.reply_text("⚠️ User list မရယူနိုင်ပါ။")
        return

    batch_size = 10
    success = 0
    total = len(user_ids)
    failed = 0

    for i in range(0, total, batch_size):
        batch = user_ids[i:i+batch_size]
        for uid in batch:
            try:
                await context.bot.copy_message(
                    chat_id=uid,
                    from_chat_id=update.message.chat_id,
                    message_id=update.message.reply_to_message.message_id
                )
                success += 1
            except Exception:
                failed += 1
        await asyncio.sleep(1.5)

    await update.message.reply_text(
        f"✅ ပြီးဆုံးပါသည်။ အောင်မြင်မှု - {success}/{total} (Failed: {failed})"
    )

@handle_errors
async def active_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        return await _send_db_error(update)
    if not (await is_owner_or_master(update.effective_user.id, context)):
        return
    count = await db.order_repo.get_total_users_count()
    await update.message.reply_text(f"📊 လက်ရှိသုံးနေသူ {count} ယောက် ရှိပါသည်။")

@handle_errors
async def set_welcome_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        return await _send_db_error(update)
    if not (await is_owner_or_master(update.effective_user.id, context)):
        return
    if update.message.reply_to_message:
        await db.settings_repo.set_config("welcome_msg_id", update.message.reply_to_message.message_id)
        await update.message.reply_text("✅ Welcome Message ID သိမ်းဆည်းပြီးပါပြီ။")
    else:
        await update.message.reply_text("❌ Reply လုပ်ပြီးမှ /setwelcome ကိုသုံးပါ။")

@handle_errors
async def set_dia_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        return await _send_db_error(update)
    if not (await is_owner_or_master(update.effective_user.id, context)):
        return
    if update.message.reply_to_message:
        await db.settings_repo.set_config("dia_msg_id", update.message.reply_to_message.message_id)
        await update.message.reply_text("✅ Dia ဈေးနှုန်းပုံ Message ID သိမ်းဆည်းပြီးပါပြီ။")
        return
    match = SET_PRICE_RE.match(update.message.text)
    if match:
        amount = match.group(2).strip()
        await db.price_repo.add_or_update_price(amount, "dia")
        await update.message.reply_text(f"✅ Dia {amount} ထည့်သွင်းပြီးပါပြီ။")
    else:
        await update.message.reply_text("❌ Format: /setdia [item_name]")

@handle_errors
async def set_uc_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        return await _send_db_error(update)
    if not (await is_owner_or_master(update.effective_user.id, context)):
        return
    if update.message.reply_to_message:
        await db.settings_repo.set_config("uc_msg_id", update.message.reply_to_message.message_id)
        await update.message.reply_text("✅ UC ဈေးနှုန်းပုံ Message ID သိမ်းဆည်းပြီးပါပြီ။")
        return
    match = SET_PRICE_RE.match(update.message.text)
    if match:
        amount = match.group(2).strip()
        await db.price_repo.add_or_update_price(amount, "uc")
        await update.message.reply_text(f"✅ UC {amount} ထည့်သွင်းပြီးပါပြီ။")
    else:
        await update.message.reply_text("❌ Format: /setuc [item_name]")

@handle_errors
async def delete_dia_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        return await _send_db_error(update)
    if not (await is_owner_or_master(update.effective_user.id, context)):
        return
    amount = ' '.join(context.args)
    if not amount:
        return await update.message.reply_text("❌ ဖျက်ရန် ပစ္စည်းအမည် ထည့်ပါ။ e.g. /deletedia Weekly Pass")
    await db.price_repo.set_price_active("dia", amount, False)
    await update.message.reply_text(f"✅ Dia {amount} ကိုဖျက်ပြီးပါပြီ။")

@handle_errors
async def delete_uc_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        return await _send_db_error(update)
    if not (await is_owner_or_master(update.effective_user.id, context)):
        return
    amount = ' '.join(context.args)
    if not amount:
        return await update.message.reply_text("❌ ဖျက်ရန် ပစ္စည်းအမည် ထည့်ပါ။ e.g. /deleteuc Weekly Pass")
    await db.price_repo.set_price_active("uc", amount, False)
    await update.message.reply_text(f"✅ UC {amount} ကိုဖျက်ပြီးပါပြီ။")

@handle_errors
async def check_price_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        return await _send_db_error(update)
    if not (await is_owner_or_master(update.effective_user.id, context)):
        return
    dia = sort_price_items(await db.price_repo.get_active_prices("dia"))
    uc = sort_price_items(await db.price_repo.get_active_prices("uc"))
    text = "📋 Active Items:\n\n💎 Diamond:\n"
    text += "\n".join(p.get('amount') or p.get('diamond') for p in dia)
    text += "\n\n💵 UC:\n"
    text += "\n".join(p.get('amount') or p.get('diamond') for p in uc)
    await update.message.reply_text(text)

# ──────────────────────────────────────
# License / Paid Command
# ──────────────────────────────────────
def get_remaining_time_str(expiry_date) -> str:
    if not expiry_date:
        return "N/A"
    expiry_date = _ensure_utc(expiry_date)
    if expiry_date is None:
        return "N/A"
    diff = expiry_date - datetime.now(timezone.utc)
    if diff.days < 0:
        return "Expired"
    months = diff.days // 30
    days = diff.days % 30
    return f"({months}လ {days}ရက်ကျန်)"

@handle_errors
async def paid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        return await _send_db_error(update)
    if not master.is_master(update.effective_user.id):
        return
    admin_id = db.primary_admin_id
    args = context.args
    if len(args) == 2:
        try:
            target = int(args[0])
            months = int(args[1])
        except ValueError:
            await update.message.reply_text("❌ ID နှင့် လအရေအတွက်သည် ဂဏန်းဖြစ်ရပါမည်။")
            return
        if months <= 0:
            await update.message.reply_text("❌ လအရေအတွက် 0 ထက်ကြီးရပါမည်။")
            return
        await db.license_repo.add_or_update(target, months)
        await update.message.reply_text(f"✅ User `{target}` အား {months} လ လိုင်စင်တိုးပေးပြီးပါပြီ။",
                                        parse_mode="Markdown")
        try:
            await context.bot.send_message(chat_id=admin_id,
                                           text=f"/license_added {int(target)} {int(months)}")
        except Exception as e:
            logger.error(f"License signal failed: {e}")
        return

    licenses = await db.license_repo.get_all_licenses()
    if not licenses:
        await update.message.reply_text("📭 လိုင်စင်ရှိသူ မရှိသေးပါ။")
        return
    keyboard = []
    for lic in licenses:
        uid = lic["user_id"]
        expiry = lic.get("expiry_date")
        time_left = get_remaining_time_str(expiry)
        keyboard.append([InlineKeyboardButton(f"👤 ID: {uid} {time_left}",
                                              callback_data=f"license_view_{uid}")])
    await update.message.reply_text("📋 <b>လိုင်စင်စာရင်း</b>",
                                    parse_mode=ADMIN_PARSE_MODE,
                                    reply_markup=InlineKeyboardMarkup(keyboard))

async def _handle_license_view(query, db, target):
    await query.answer()
    lic = await db.licenses.find_one({"user_id": target})
    if not lic:
        await query.answer("မတွေ့တော့ပါ။", show_alert=True)
        return
    expiry = lic.get("expiry_date")
    text = (
        f"👤 <b>User အချက်အလက်</b>\n\n"
        f"🆔 ID: <code>{target}</code>\n"
        f"⏳ သက်တမ်းကုန်မည့်ရက်: {expiry.strftime('%Y-%m-%d') if expiry else 'N/A'}\n"
        f"📊 အခြေအနေ: {get_remaining_time_str(expiry)}"
    )
    keyb = [
        [InlineKeyboardButton("➕ 1 လတိုး", callback_data=f"license_add_1_{target}"),
         InlineKeyboardButton("➕ 3 လတိုး", callback_data=f"license_add_3_{target}")],
        [InlineKeyboardButton("🚫 လိုင်စင်ပိတ်မယ်", callback_data=f"license_revoke_{target}")],
        [InlineKeyboardButton("🔙 စာရင်းသို့", callback_data="license_main_list")]
    ]
    await query.edit_message_text(text,
                                  parse_mode=ADMIN_PARSE_MODE,
                                  reply_markup=InlineKeyboardMarkup(keyb))

async def _handle_license_main_list(query, db):
    await query.answer()
    licenses = await db.license_repo.get_all_licenses()
    keyb = []
    for lic in licenses:
        uid = lic["user_id"]
        expiry = lic.get("expiry_date")
        time_left = get_remaining_time_str(expiry)
        keyb.append([InlineKeyboardButton(f"👤 ID: {uid} {time_left}",
                                          callback_data=f"license_view_{uid}")])
    await query.edit_message_text("📋 <b>လိုင်စင်စာရင်း</b>",
                                  parse_mode=ADMIN_PARSE_MODE,
                                  reply_markup=InlineKeyboardMarkup(keyb))

async def _handle_license_add(query, db, target, months):
    await db.license_repo.add_or_update(target, months)
    confirm_msg = f"✅ User <code>{target}</code> ကို <b>{months}</b> လ လိုင်စင်တိုးပေးပြီးပါပြီ။"
    await query.message.reply_text(confirm_msg, parse_mode=ADMIN_PARSE_MODE)
    await _handle_license_view(query, db, target)

async def _handle_license_revoke(query, db, target):
    success = await db.license_repo.revoke_license(target)
    if success:
        confirm_msg = f"❌ User <code>{target}</code> ၏ လိုင်စင်ကို ပိတ်လိုက်ပါပြီ။"
    else:
        confirm_msg = "⚠️ လိုင်စင်ပိတ်ရာတွင် အမှားအယွင်းရှိပါသည်။"
    await query.message.reply_text(confirm_msg, parse_mode=ADMIN_PARSE_MODE)
    await _handle_license_main_list(query, db)

@handle_errors
async def license_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        return
    query = update.callback_query
    if not master.is_master(query.from_user.id):
        await query.answer("⛔ ခွင့်ပြုချက်မရှိပါ။", show_alert=True)
        return

    data = query.data

    if data.startswith("license_view_"):
        try:
            target = int(data.rsplit("_", 1)[-1])
        except (ValueError, IndexError):
            await query.answer("Invalid data", show_alert=True)
            return
        await _handle_license_view(query, db, target)

    elif data == "license_main_list":
        await _handle_license_main_list(query, db)

    elif data.startswith("license_add_"):
        try:
            parts = data.split("_")
            if len(parts) != 4:
                raise ValueError
            months = int(parts[2])
            target = int(parts[3])
        except (ValueError, IndexError):
            await query.answer("⚠️ ဒေတာ မှားယွင်းနေပါသည်။", show_alert=True)
            return
        await _handle_license_add(query, db, target, months)

    elif data.startswith("license_revoke_"):
        try:
            target = int(data.rsplit("_", 1)[-1])
        except (ValueError, IndexError):
            await query.answer("⚠️ ဒေတာ မှားယွင်းနေပါသည်။", show_alert=True)
            return
        await _handle_license_revoke(query, db, target)

    else:
        await query.answer("⚠️ မသိသော command", show_alert=True)

# ──────────────────────────────────────
# Refresh Command
# ──────────────────────────────────────
_LAST_CLEANUP_DATE = None

def check_refresh_limit(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    global _LAST_CLEANUP_DATE
    today = date.today().isoformat()
    limits = context.bot_data.setdefault("refresh_limits", {})

    if _LAST_CLEANUP_DATE != today:
        limits = {uid: rec for uid, rec in limits.items() if rec["date"] == today}
        context.bot_data["refresh_limits"] = limits
        _LAST_CLEANUP_DATE = today

    # aggressive cleanup if dict grows too large
    if len(limits) > 500:
        limits.clear()
        context.bot_data["refresh_limits"] = {}

    rec = limits.get(user_id, {"date": today, "count": 0})
    if rec["date"] != today:
        rec = {"date": today, "count": 0}

    if rec["count"] >= 3:
        return False
    rec["count"] += 1
    limits[user_id] = rec
    return True

@handle_errors
async def refresh_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        return await _send_db_error(update)
    user_id = update.effective_user.id
    if not await is_owner_or_master(user_id, context):
        await update.message.reply_text("⛔ ဤ Command ကို အသုံးပြုခွင့် မရှိပါ။")
        return
    if not check_refresh_limit(user_id, context):
        await update.message.reply_text("⚠️ တစ်ရက်ကို ၃ ကြိမ်သာ Refresh လုပ်ခွင့်ရှိပါသည်။")
        return

    await send_typing(update, context)   # keep only for this longer operation
    admin_id = db.primary_admin_id

    status_msg = await update.message.reply_text("⏳ လိုင်စင်စစ်ဆေးနေပါသည်...")

    valid = await db.license_repo.force_refresh(admin_id)
    if valid:
        _, expiry = await db.license_repo._check_local(admin_id)
        time_str = get_remaining_time_str(expiry)
        msg = f"✅ လိုင်စင်ရှိနေပါသည်။\n⏳ သက်တမ်းကျန်: {time_str}"
    else:
        msg = "❌ လိုင်စင် မရှိပါ သို့မဟုတ် သက်တမ်းကုန်နေပါသည်။"

    await safe_delete_message(status_msg, context)
    await update.message.reply_text(msg)

# ──────────────────────────────────────
# Background Jobs
# ──────────────────────────────────────
async def check_timeouts(context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        return
    admin_id = get_admin_id(context)
    try:
        orders = await db.order_repo.get_timeout_orders()
        for o in orders:
            order_id = o['order_id']
            admin_msg_id = o.get('admin_msg_id')
            if admin_msg_id:
                try:
                    await context.bot.delete_message(chat_id=admin_id, message_id=admin_msg_id)
                except Exception as e:
                    logger.debug(f"Could not delete admin message for {order_id}: {e}")
            await db.order_repo.delete_order(order_id)
    except Exception as e:
        logger.error(f"Timeout check error: {e}")

# ──────────────────────────────────────
# Database Migration Command
# ──────────────────────────────────────
@handle_errors
async def fix_database_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        return await _send_db_error(update)
    if not master.is_master(update.effective_user.id):
        await update.message.reply_text("⛔ ဒီခလုတ်ကို သုံးပိုင်ခွင့်မရှိပါ။")
        return
    count = await db.price_repo.migrate_legacy_diamond_types()
    if count == -1:
        await update.message.reply_text("❌ ဒေတာပြင်ဆင်မှု မအောင်မြင်ပါ။")
    elif count == 0:
        await update.message.reply_text("✅ ပြင်စရာ ဒေတာမရှိတော့ပါ။")
    else:
        await update.message.reply_text(f"✅ ဒေတာဟောင်း {count} ခုကို Version အသစ်ဖြစ်အောင် ပြုပြင်ပြီးပါပြီ။")
