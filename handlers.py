# handlers.py - Final version (Burmese button text, clickable name, fixed admin buttons, migration command)
import re
import asyncio
import uuid
import logging
import html
from datetime import datetime, date
from enum import Enum
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatAction, ParseMode
from telegram.error import BadRequest, RetryAfter, TelegramError, Forbidden
from telegram.ext import ContextTypes, ConversationHandler

import master

logger = logging.getLogger(__name__)

WAIT_GAME_ID, WAIT_CONFIRMATION, WAIT_PAYMENT = range(3)
ADMIN_PARSE_MODE = ParseMode.HTML

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

# ================== Helpers ==================
def get_db(context: ContextTypes.DEFAULT_TYPE):
    db = context.bot_data.get('db')
    if not db:
        logger.error("Database connection missing in bot_data!")
    return db

def get_admin_id(context: ContextTypes.DEFAULT_TYPE) -> int:
    return context.bot_data.get('admin_id', 0)

async def is_owner_or_master(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    admin_id = get_admin_id(context)
    return user_id == admin_id or master.is_master(user_id)

async def send_typing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)

async def safe_delete_message(message, context: ContextTypes.DEFAULT_TYPE = None):
    try:
        await message.delete()
    except BadRequest as e:
        if "message can't be deleted" in str(e).lower():
            logger.warning(f"Message {message.message_id} too old to delete, skipping.")
        else:
            logger.error(f"Unexpected BadRequest during delete: {e}")
    except TelegramError as e:
        logger.error(f"Telegram error during delete: {e}")
    except Exception as e:
        logger.error(f"Unexpected error during delete: {e}")

def escape_html(text: str) -> str:
    if not text:
        return text
    return html.escape(str(text))

def handle_errors(handler_func):
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            return await handler_func(update, context)
        except Exception as e:
            logger.exception(f"Unhandled error in {handler_func.__name__}: {e}")
            try:
                admin_id = get_admin_id(context)
                user_info = f"User: {update.effective_user.id}" if update.effective_user else "No user"
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=f"⚠️ Error in {handler_func.__name__}\n{user_info}\nError: {str(e)[:300]}"
                )
            except:
                pass
            error_msg = "⚠️ စနစ်တွင် အမှားအယွင်းတစ်ခု ဖြစ်ပွားသွားပါသည်။ ကျေးဇူးပြု၍ ခဏနေမှ ထပ်မံကြိုးစားပါ။"
            if update.effective_chat:
                await context.bot.send_message(chat_id=update.effective_chat.id, text=error_msg)
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
    admin_id = get_admin_id(context)
    return await db.is_license_valid(admin_id)

def wrap_with_license(handler_func):
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE):
        is_ok = await check_license_logic(update, context)
        if not is_ok:
            deny_msg = "⛔ ဆိုင်၏ လိုင်စင်သက်တမ်းကုန်သွားပါပြီ။\nကျေးဇူးပြု၍ Master ထံ ဆက်သွယ်ပါ။"
            if update.message:
                await update.message.reply_text(deny_msg)
            elif update.callback_query:
                await update.callback_query.answer(deny_msg, show_alert=True)
            return ConversationHandler.END
        return await handler_func(update, context)
    return wrapped

def validate_game_id(item_type: str, text: str) -> tuple:
    if item_type == "dia":
        match = re.match(r"^(\d{5,20})[\s\-|()]+(\d{3,6})$", text)
        if match:
            return match.groups()
        return None
    else:
        match = re.match(r"^(\d{5,20})$", text)
        if match:
            return (match.group(1), "")
        return None

def safe_user_mention(user_id: int, first_name: str = None, last_name: str = None, fallback: str = "User") -> str:
    if first_name:
        name = first_name
        if last_name:
            name += " " + last_name
    else:
        name = fallback
    safe_name = escape_html(name)
    return f'<a href="tg://user?id={user_id}">{safe_name}</a>'

def build_admin_caption(order: dict, user_id: int, first_name: str, last_name: str,
                        quantity: str, item_type: str, status: str) -> str:
    emoji = "💎" if item_type == "dia" else "💵"
    order_id_safe = escape_html(order.get("order_id", "N/A"))
    game_id = order.get("order_info", {}).get("game_id", "N/A")
    zone_id = order.get("order_info", {}).get("zone_id", "")
    game_id_safe = escape_html(game_id) if game_id != "N/A" else "N/A"
    zone_id_safe = escape_html(zone_id) if zone_id else ""
    zone_str = f" ({zone_id_safe})" if zone_id_safe else ""
    status_safe = escape_html(status)
    quantity_safe = escape_html(str(quantity))
    item_type_safe = escape_html(item_type.upper())

    # ✅ Use safe_user_mention for clickable name
    name_line = f"👤 ဝယ်သူ: {safe_user_mention(user_id, first_name, last_name)}"
    id_line = f'🆔 User ID: <a href="tg://user?id={user_id}">{user_id}</a>'

    caption = (
        f"📦 Order ID: <code>{order_id_safe}</code>\n"
        f"{name_line}\n"
        f"{id_line}\n"
        f"🆔 Game ID: <code>{game_id_safe}{zone_str}</code>\n"
        f"{emoji} ပစ္စည်း: {quantity_safe} {item_type_safe}\n"
        f"💰 အခြေအနေ: {status_safe}"
    )
    return caption

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
    except Exception as e:
        logger.error(f"Error editing admin message: {e}")

# ================== New Helpers for Fixes ==================
def check_refresh_limit(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Owner တစ်ယောက်ကို ၁ ရက်အတွင်း refresh ၃ ကြိမ်ထက်ပို မခွင့်ပြုပါ။"""
    today = date.today().isoformat()
    limits = context.bot_data.setdefault("refresh_limits", {})
    user_record = limits.get(user_id, {"date": today, "count": 0})
    if user_record["date"] != today:
        user_record = {"date": today, "count": 0}
    if user_record["count"] >= 3:
        return False
    user_record["count"] += 1
    limits[user_id] = user_record
    return True

def get_remaining_time_str(expiry_date) -> str:
    """Return human-readable remaining time string (e.g., '2လ 15ရက် ကျန်')"""
    if not expiry_date:
        return "N/A"
    now = datetime.now()
    diff = expiry_date - now
    if diff.days < 0:
        return "Expired"
    months = diff.days // 30
    days = diff.days % 30
    return f"({months}လ {days}ရက်ကျန်)"

async def _send_db_error(update: Update):
    msg = "⚠️ ဒေတာဘေ့စ် ချိတ်ဆက်မှု မရှိပါ။ ကျေးဇူးပြု၍ ခဏနေမှ ထပ်စမ်းကြည့်ပါ။"
    if update.message:
        await update.message.reply_text(msg)
    elif update.callback_query:
        await update.callback_query.answer(msg, show_alert=True)

# ================== User-Facing Handlers ==================
@handle_errors
@wrap_with_license
async def send_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_typing(update, context)
    db = get_db(context)
    if not db:
        await _send_db_error(update)
        return ConversationHandler.END
    admin_id = get_admin_id(context)
    is_open = await db.get_service_status()
    if not is_open and str(update.effective_user.id) != str(admin_id):
        text = "⚠️ Service ခေတ္တရပ်ထားပါသည်"
        if update.message:
            await update.message.reply_text(text)
        elif update.callback_query:
            await update.callback_query.answer(text, show_alert=True)
        return ConversationHandler.END
    text = "💎 Diamond Bot မှ ကြိုဆိုပါတယ်။\nရွေးချယ်ပါ👇"
    keyboard = [[
        InlineKeyboardButton("💎 စိန်ဝယ်ယူရန်", callback_data="show_dia"),
        InlineKeyboardButton("💵 UCဝယ်ယူရန်", callback_data="show_uc")
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    welcome_msg_id = await db.get_config("welcome_msg_id")
    if welcome_msg_id:
        try:
            chat_id = update.message.chat_id if update.message else update.callback_query.message.chat_id
            await context.bot.copy_message(chat_id=chat_id, from_chat_id=admin_id, message_id=welcome_msg_id, reply_markup=reply_markup)
            if update.callback_query:
                await safe_delete_message(update.callback_query.message, context)
            return ConversationHandler.END
        except Exception as e:
            logger.error(f"Welcome copy failed: {e}")
    if update.message:
        await update.message.reply_text(text, reply_markup=reply_markup)
    elif update.callback_query:
        await update.callback_query.message.reply_text(text, reply_markup=reply_markup)
    return ConversationHandler.END

@handle_errors
@wrap_with_license
async def show_items(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_typing(update, context)
    db = get_db(context)
    if not db:
        await _send_db_error(update)
        return ConversationHandler.END
    admin_id = get_admin_id(context)
    query = update.callback_query
    await query.answer()
    item_type = "dia" if query.data == "show_dia" else "uc"
    prices = await db.get_active_prices(item_type)
    if not prices:
        await query.message.reply_text("လောလောဆယ် ဈေးနှုန်း မရှိသေးပါ။")
        return ConversationHandler.END
    sorted_prices = sort_price_items(prices)
    keyboard = []
    row = []
    for p in sorted_prices:
        amount = p.get('amount') or p.get('diamond')
        btn_text = str(amount).strip()
        cb_data = f"price_{item_type}_{amount}"
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
    saved_msg_id = await db.get_config(msg_id_key)
    if saved_msg_id:
        try:
            await context.bot.copy_message(chat_id=query.message.chat_id, from_chat_id=admin_id, message_id=saved_msg_id, reply_markup=reply_markup)
            await safe_delete_message(query.message, context)
            return ConversationHandler.END
        except Exception as e:
            logger.error(f"Item menu copy failed: {e}")
    text = "ဈေးနှုန်းများ ရွေးချယ်ပါ👇"
    await query.message.edit_text(text, reply_markup=reply_markup)
    return ConversationHandler.END

@handle_errors
@wrap_with_license
async def step1_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_typing(update, context)
    db = get_db(context)
    if not db:
        await _send_db_error(update)
        return ConversationHandler.END
    admin_id = get_admin_id(context)
    query = update.callback_query
    await query.answer()
    parts = query.data.split('_', 2)
    if len(parts) < 3:
        await query.message.reply_text("⚠️ Invalid selection")
        return ConversationHandler.END
    item_type = parts[1]
    amount_str = parts[2]
    price_data = await db.get_price_by_amount(item_type, amount_str)
    if not price_data:
        await query.message.reply_text("⚠️ မတွေ့ပါ")
        return ConversationHandler.END
    order_id = f"ORD-{uuid.uuid4().hex[:6].upper()}"
    context.user_data['current_order_id'] = order_id
    context.user_data['item_type'] = item_type
    context.user_data['quantity'] = amount_str
    await db.create_order(order_id, query.from_user.id, query.from_user.first_name, amount_str, 0, item_type)
    if item_type == "uc":
        prompt_text = "🆔 Game ID ရိုက်ထည့်ပါ (ဥပမာ - 123456789)"
    else:
        prompt_text = "🆔 Game ID + Zone ID ရိုက်ထည့်ပါ\nFormat: 123456789 1234"
    await query.message.reply_text(prompt_text)
    return WAIT_GAME_ID

@handle_errors
@wrap_with_license
async def step2_id_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_typing(update, context)
    db = get_db(context)
    if not db:
        await _send_db_error(update)
        return ConversationHandler.END
    text = update.message.text.strip()
    order_id = context.user_data.get('current_order_id')
    item_type = context.user_data.get('item_type')
    if order_id is None:
        await update.message.reply_text("⚠️ အော်ဒါအချက်အလက် ပျောက်ဆုံးသွားပါသဖြင့် ကျေးဇူးပြု၍ အော်ဒါအသစ် ပြန်လုပ်ပေးပါ။")
        return ConversationHandler.END
    order = await db.get_order(order_id)
    if not order:
        await update.message.reply_text("⏰ အော်ဒါသက်တမ်းကုန်သွားပါပြီ။ ကျေးဇူးပြု၍ ပြန်လည်စတင်ပေးပါ။")
        return ConversationHandler.END
    validation_result = validate_game_id(item_type, text)
    if validation_result is None:
        if item_type == "dia":
            await update.message.reply_text("❌ Diamond အတွက် Format: `123456789 1234` (or `123456789(1234)`)")
        else:
            await update.message.reply_text("❌ UC အတွက် Format: `123456789` (ID တစ်ခုတည်း)")
        return WAIT_GAME_ID
    game_id, zone_id = validation_result
    await db.update_order_game_id(order_id, game_id, zone_id)
    safe_game_id = escape_html(game_id)
    safe_zone_id = escape_html(zone_id) if zone_id else ""
    confirm_text = f"🆔 Game ID: <b>{safe_game_id}{' (' + safe_zone_id + ')' if zone_id else ''}</b>"
    await update.message.reply_text(confirm_text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ အတည်ပြုမယ်", callback_data="confirm_id"),
        InlineKeyboardButton("✏️ ID ပြန်ပြင်မယ်", callback_data="back_id")
    ]]))
    return WAIT_CONFIRMATION

@handle_errors
@wrap_with_license
async def step3_validation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_typing(update, context)
    db = get_db(context)
    if not db:
        await _send_db_error(update)
        return ConversationHandler.END
    admin_id = get_admin_id(context)
    query = update.callback_query
    await query.answer()
    order_id = context.user_data.get('current_order_id')
    item_type = context.user_data.get('item_type')
    quantity = context.user_data.get('quantity')
    if order_id is None:
        await query.message.reply_text("⚠️ အော်ဒါအချက်အလက် ပျောက်ဆုံးသွားပါသဖြင့် ကျေးဇူးပြု၍ အော်ဒါအသစ် ပြန်လုပ်ပေးပါ။")
        return ConversationHandler.END
    order = await db.get_order(order_id)
    if not order:
        await query.message.reply_text("⏰ အော်ဒါသက်တမ်းကုန်သွားပါပြီ။ ကျေးဇူးပြု၍ ပြန်လည်စတင်ပေးပါ။")
        return ConversationHandler.END
    if query.data == "back_id":
        await query.message.reply_text("🆔 Game ID ပြန်ရိုက်ထည့်ပါ")
        return WAIT_GAME_ID
    elif query.data == "confirm_id":
        await db.update_order_status(order_id, OrderStatus.WAITING_PAYMENT)
        payment_msg_id = await db.get_price_msg_id(item_type, quantity)
        if payment_msg_id:
            try:
                await context.bot.copy_message(chat_id=query.message.chat_id, from_chat_id=admin_id, message_id=payment_msg_id, reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("❌ မဝယ်တော့ပါ", callback_data=f"cancel_user_{order_id}")
                ]]))
                await safe_delete_message(query.message, context)
            except Exception as e:
                logger.error(f"Payment image copy failed: {e}")
                await query.message.edit_text("📸 ငွေလွှဲ Screenshot ပို့ပေးပါ", reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("❌ မဝယ်တော့ပါ", callback_data=f"cancel_user_{order_id}")
                ]]))
        else:
            await query.message.edit_text("📸 ငွေလွှဲ Screenshot ပို့ပေးပါ", reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ မဝယ်တော့ပါ", callback_data=f"cancel_user_{order_id}")
            ]]))
        return WAIT_PAYMENT
    return ConversationHandler.END

@handle_errors
@wrap_with_license
async def step4_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        await _send_db_error(update)
        return ConversationHandler.END
    admin_id = get_admin_id(context)
    if update.effective_chat:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.UPLOAD_PHOTO)
    if not update.message.photo:
        await update.message.reply_text("📸 Photo only")
        return WAIT_PAYMENT
    order_id = context.user_data.get('current_order_id')
    if order_id is None:
        await update.message.reply_text("⚠️ အော်ဒါအချက်အလက် ပျောက်ဆုံးသွားပါသဖြင့် ကျေးဇူးပြု၍ အော်ဒါအသစ် ပြန်လုပ်ပေးပါ။")
        return ConversationHandler.END
    order = await db.get_order(order_id)
    if not order:
        await update.message.reply_text("⏰ အော်ဒါသက်တမ်းကုန်သွားပါပြီ။ ကျေးဇူးပြု၍ ပြန်လည်စတင်ပေးပါ။")
        return ConversationHandler.END
    await db.update_order_status(order_id, OrderStatus.VERIFYING)
    user = update.effective_user
    quantity = context.user_data.get('quantity', 'N/A')
    item_type = context.user_data.get('item_type', 'dia')

    # Caption and buttons for admin
    caption = build_admin_caption(order, user.id, user.first_name, user.last_name,
                                  quantity, item_type, "စစ်ဆေးရန်...")
    admin_markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ လက်ခံမယ်", callback_data=f"admin_approve_{order_id}"),
        InlineKeyboardButton("❌ ငြင်းပယ်မယ်", callback_data=f"admin_reject_{order_id}")
    ]])

    # Send photo with caption and inline buttons directly to admin
    try:
        sent_to_admin = await context.bot.send_photo(
            chat_id=admin_id,
            photo=update.message.photo[-1].file_id,
            caption=caption,
            parse_mode=ADMIN_PARSE_MODE,
            reply_markup=admin_markup
        )
        await db.set_order_admin_msg_id(order_id, sent_to_admin.message_id)
    except Exception as e:
        logger.error(f"Failed to send order to admin: {e}")
        await update.message.reply_text("⚠️ Admin ထံ အချက်အလက်ပို့ရာတွင် ချို့ယွင်းသွားပါသည်။ နောက်မှ ထပ်ကြိုးစားပါ။")
        return WAIT_PAYMENT

    # Order summary for the user
    game_id = order.get("order_info", {}).get("game_id", "N/A")
    zone_id = order.get("order_info", {}).get("zone_id", "")
    if zone_id:
        game_display = f"{escape_html(game_id)} ({escape_html(zone_id)})"
    else:
        game_display = escape_html(game_id)
    order_summary = (
        f"📦 Order ID: <code>{escape_html(order_id)}</code>\n"
        f"🆔 Game ID: <b>{game_display}</b>\n"
        f"{'💎' if item_type == 'dia' else '💵'} ပစ္စည်း: <b>{escape_html(quantity)} {item_type.upper()}</b>\n\n"
        f"✅ <b>လူကြီးမင်း၏ Order တင်မှု အောင်မြင်ပါသည်။</b>\n"
        f"ငွေလွှဲမှုစစ်ဆေးပြီးပါက Item ထည့်သွင်းပေးပါမည်။ ခေတ္တစောင့်ဆိုင်းပေးပါ။"
    )
    await update.message.reply_text(order_summary, parse_mode="HTML")
    return ConversationHandler.END

# ================== Admin Callback Handlers ==================
@handle_errors
async def admin_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        await _send_db_error(update)
        return
    admin_id = get_admin_id(context)
    query = update.callback_query
    user_id = query.from_user.id
    if not await db.is_license_valid(admin_id):
        await query.answer("⛔ ဆိုင်၏ လိုင်စင်သက်တမ်းကုန်သွားပါပြီ။ Master ထံ ဆက်သွယ်ပါ။", show_alert=True)
        return
    if not await is_owner_or_master(user_id, context):
        await query.answer("⛔ သင်သည် ဤလုပ်ဆောင်ချက်ကို လုပ်ပိုင်ခွင့်မရှိပါ။", show_alert=True)
        return
    await query.answer()
    data = query.data
    parts = data.split("_")
    if len(parts) < 2:
        return
    order_id = parts[-1]
    order = await db.get_order(order_id)
    if not order:
        await query.answer("အော်ဒါ မတွေ့ပါ (ဖျက်ပြီးသားဖြစ်နိုင်သည်)။", show_alert=True)
        return
    user_id_customer = order.get("user_id")
    profile_name = order.get("profile_name", "")
    first_name = profile_name if profile_name else str(user_id_customer)
    last_name = None
    order_info = order.get('order_info', {})
    quantity = order_info.get('quantity', 'N/A')
    item_type = order_info.get('item_type', 'dia')
    if data.startswith("admin_approve_"):
        await db.update_order_status(order_id, OrderStatus.PROCESSING)
        await db.increment_monthly_count(quantity, item_type)
        await context.bot.send_message(
            chat_id=user_id_customer,
            text="✅ Admin မှ ငွေလွှဲမှုကို အတည်ပြုလိုက်ပါပြီ။ ခေတ္တစောင့်ပေးပါ၊ ပစ္စည်းထည့်ပေးနေပါပြီ။"
        )
        new_caption = build_admin_caption(order, user_id_customer, first_name, last_name,
                                          quantity, item_type, "⏳ Approved - Waiting for completion")
        await edit_admin_message(query, new_caption, reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("💎 ထည့်ပြီးပြီ", callback_data=f"admin_complete_{order_id}")
        ]]))
    elif data.startswith("admin_complete_"):
        await db.update_order_status(order_id, OrderStatus.COMPLETED)
        await context.bot.send_message(
            chat_id=user_id_customer,
            text="✅ သင်ဝယ်ထားသော Item ကို အောင်မြင်စွာ ထည့်ပေးပြီးပါပြီ။",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔄 နောက်ထပ် ဝယ်ယူရန်", callback_data="new_order")
            ]])
        )
        new_caption = build_admin_caption(order, user_id_customer, first_name, last_name,
                                          quantity, item_type, "✅ Completed")
        await edit_admin_message(query, new_caption, reply_markup=None)
        await db.orders.update_one({"order_id": order_id}, {"$unset": {"admin_msg_id": ""}})
    elif data.startswith("admin_reject_"):
        await db.update_order_status(order_id, OrderStatus.FAILED)
        support_button = InlineKeyboardMarkup([[
            InlineKeyboardButton("ownerထံ ဆက်သွယ်ရန်", url=f"tg://user?id={admin_id}")
        ]])
        try:
            await context.bot.send_message(
                chat_id=user_id_customer,
                text="❌ လူကြီးမင်း၏ အော်ဒါ ငြင်းပယ်ခံရပါသည် (စလစ်မှားယွင်းခြင်း သို့မဟုတ် အချက်အလက်မစုံလင်ခြင်းကြောင့်)",
                reply_markup=support_button
            )
        except Exception as e:
            logger.error(f"Could not send rejection message to user {user_id_customer}: {e}")
        new_caption = build_admin_caption(order, user_id_customer, first_name, last_name,
                                          quantity, item_type, "❌ Rejected")
        await edit_admin_message(query, new_caption, reply_markup=None)
        await db.orders.update_one({"order_id": order_id}, {"$unset": {"admin_msg_id": ""}})
    elif data.startswith("admin_manual_"):
        await db.update_order_status(order_id, OrderStatus.COMPLETED)
        new_caption = build_admin_caption(order, user_id_customer, first_name, last_name,
                                          quantity, item_type, "✅ Manual Completed")
        await edit_admin_message(query, new_caption, reply_markup=None)
        await db.orders.update_one({"order_id": order_id}, {"$unset": {"admin_msg_id": ""}})

@handle_errors
@wrap_with_license
async def user_cancel_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        await _send_db_error(update)
        return
    query = update.callback_query
    await query.answer()
    order_id = query.data.split("_")[-1]
    order = await db.get_order(order_id)
    if not order:
        await query.message.edit_text("⏰ အော်ဒါမရှိတော့ပါ (သက်တမ်းကုန်သွားနိုင်သည်)။")
        return
    if order['status'] != OrderStatus.WAITING_PAYMENT:
        await query.message.reply_text("⚠️ ယခုအဆင့်တွင် Cancel ပြုလုပ်၍မရပါ။")
        return
    await db.delete_order(order_id)
    admin_msg_id = order.get("admin_msg_id")
    if admin_msg_id:
        admin_id = get_admin_id(context)
        user_id_customer = order.get("user_id")
        profile_name = order.get("profile_name", str(user_id_customer))
        safe_user_name = escape_html(profile_name)
        name_line = f'👤 ဝယ်သူ: <a href="tg://user?id={user_id_customer}">{safe_user_name}</a>'
        id_line = f'🆔 User ID: <a href="tg://user?id={user_id_customer}">{user_id_customer}</a>'
        order_id_safe = escape_html(order_id)
        cancel_caption = (
            f"📦 <code>{order_id_safe}</code>\n"
            f"{name_line}\n"
            f"{id_line}\n\n"
            f"❌ <b>User မှ Cancel လုပ်သွားပါသည်</b>"
        )
        try:
            await context.bot.edit_message_text(
                chat_id=admin_id,
                message_id=admin_msg_id,
                text=cancel_caption,
                parse_mode=ADMIN_PARSE_MODE,
                reply_markup=None
            )
        except BadRequest as e:
            if "message is not modified" in str(e).lower():
                pass
            else:
                try:
                    await context.bot.edit_message_caption(
                        chat_id=admin_id,
                        message_id=admin_msg_id,
                        caption=cancel_caption,
                        parse_mode=ADMIN_PARSE_MODE,
                        reply_markup=None
                    )
                except Exception as e2:
                    logger.warning(f"Could not update admin cancel message for {order_id}: {e2}")
        except Exception as e:
            logger.warning(f"Could not update admin cancel message for {order_id}: {e}")
    await query.message.edit_text("✅ Cancel ပြီး")

@handle_errors
@wrap_with_license
async def new_order_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_typing(update, context)
    query = update.callback_query
    await query.answer()
    await send_welcome(update, context)
    return ConversationHandler.END

# ================== Admin Commands ==================
@handle_errors
async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        await _send_db_error(update)
        return
    if master.is_master(update.effective_user.id) or await is_owner_or_master(update.effective_user.id, context):
        await db.set_service_status(False)
        await update.message.reply_text("🛑 Service ခေတ္တရပ်ထားပါသည် (Maintenance Mode: ON).")
    else:
        await update.message.reply_text("⛔ အခွင့်အရေးမရှိပါ။")

@handle_errors
async def open_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        await _send_db_error(update)
        return
    if master.is_master(update.effective_user.id) or await is_owner_or_master(update.effective_user.id, context):
        await db.set_service_status(True)
        await update.message.reply_text("✅ Service ပြန်လည်ဖွင့်လှစ်လိုက်ပါပြီ (Maintenance Mode: OFF).")
    else:
        await update.message.reply_text("⛔ အခွင့်အရေးမရှိပါ။")

@handle_errors
async def post_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        await _send_db_error(update)
        return
    if not (master.is_master(update.effective_user.id) or await is_owner_or_master(update.effective_user.id, context)):
        return
    if not update.message.reply_to_message:
        await update.message.reply_text("❌ ပို့ချင်သော Message ကို Reply လုပ်ပြီး /post ကိုသုံးပါ။")
        return
    all_orders = await db.orders.find({}, {"user_id": 1}).to_list(length=None)
    unique_users = list(set(o['user_id'] for o in all_orders if 'user_id' in o))
    await update.message.reply_text(f"⏳ သုံးစွဲသူ {len(unique_users)} ဦးဆီသို့ စတင်ပို့နေပါသည်...")
    success = 0
    for user_id in unique_users:
        try:
            await context.bot.copy_message(
                chat_id=user_id,
                from_chat_id=update.message.chat_id,
                message_id=update.message.reply_to_message.message_id
            )
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.warning(f"Broadcast failed to {user_id}: {e}")
    await update.message.reply_text(f"✅ ပြီးဆုံးပါသည်။ အောင်မြင်မှု - {success}/{len(unique_users)}")

@handle_errors
async def active_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        await _send_db_error(update)
        return
    if not (master.is_master(update.effective_user.id) or await is_owner_or_master(update.effective_user.id, context)):
        return
    count = await db.get_total_users_count()
    await update.message.reply_text(f"📊 လက်ရှိသုံးနေသူ {count} ယောက် ရှိပါသည်။")

@handle_errors
async def set_welcome_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        await _send_db_error(update)
        return
    if not (master.is_master(update.effective_user.id) or await is_owner_or_master(update.effective_user.id, context)):
        return
    if update.message.reply_to_message:
        await db.set_config("welcome_msg_id", update.message.reply_to_message.message_id)
        await update.message.reply_text("✅ Welcome Message ID သိမ်းဆည်းပြီးပါပြီ။")
    else:
        await update.message.reply_text("❌ Reply လုပ်ပြီးမှ /setwelcome ကိုသုံးပါ။")

@handle_errors
async def set_dia_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        await _send_db_error(update)
        return
    if not (master.is_master(update.effective_user.id) or await is_owner_or_master(update.effective_user.id, context)):
        return
    if update.message.reply_to_message:
        await db.set_config("dia_msg_id", update.message.reply_to_message.message_id)
        await update.message.reply_text("✅ Dia ဈေးနှုန်းပုံ Message ID သိမ်းဆည်းပြီးပါပြီ။")
        return
    match = re.match(r"/setdia\s+(.+)", update.message.text)
    if match:
        amount = match.group(1).strip()
        await db.add_or_update_price(amount, "dia")
        await update.message.reply_text(f"✅ Dia {amount} ထည့်သွင်း/Active လုပ်ပြီးပါပြီ။")
    else:
        await update.message.reply_text("❌ Format: /setdia [item_name]")

@handle_errors
async def set_uc_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        await _send_db_error(update)
        return
    if not (master.is_master(update.effective_user.id) or await is_owner_or_master(update.effective_user.id, context)):
        return
    if update.message.reply_to_message:
        await db.set_config("uc_msg_id", update.message.reply_to_message.message_id)
        await update.message.reply_text("✅ UC ဈေးနှုန်းပုံ Message ID သိမ်းဆည်းပြီးပါပြီ။")
        return
    match = re.match(r"/setuc\s+(.+)", update.message.text)
    if match:
        amount = match.group(1).strip()
        await db.add_or_update_price(amount, "uc")
        await update.message.reply_text(f"✅ UC {amount} ထည့်သွင်း/Active လုပ်ပြီးပါပြီ။")
    else:
        await update.message.reply_text("❌ Format: /setuc [item_name]")

@handle_errors
async def delete_dia_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        await _send_db_error(update)
        return
    if not (master.is_master(update.effective_user.id) or await is_owner_or_master(update.effective_user.id, context)):
        return
    amount = ' '.join(context.args)
    if not amount:
        await update.message.reply_text("❌ ဖျက်ရန် ပစ္စည်းအမည် ထည့်ပါ။ e.g. /deletedia Weekly Pass")
        return
    await db.set_price_active("dia", amount, False)
    await update.message.reply_text(f"✅ Dia {amount} ကို Inactive ပြုလုပ်ပြီးပါပြီ။")

@handle_errors
async def delete_uc_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        await _send_db_error(update)
        return
    if not (master.is_master(update.effective_user.id) or await is_owner_or_master(update.effective_user.id, context)):
        return
    amount = ' '.join(context.args)
    if not amount:
        await update.message.reply_text("❌ ဖျက်ရန် ပစ္စည်းအမည် ထည့်ပါ။ e.g. /deleteuc Weekly Pass")
        return
    await db.set_price_active("uc", amount, False)
    await update.message.reply_text(f"✅ UC {amount} ကို Inactive ပြုလုပ်ပြီးပါပြီ။")

@handle_errors
async def check_price_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        await _send_db_error(update)
        return
    if not (master.is_master(update.effective_user.id) or await is_owner_or_master(update.effective_user.id, context)):
        return
    dia_prices = sort_price_items(await db.get_active_prices("dia"))
    uc_prices = sort_price_items(await db.get_active_prices("uc"))
    text = "📋 Active Items:\n\n💎 Diamond:\n"
    for p in dia_prices:
        text += f"{p.get('amount') or p.get('diamond')}\n"
    text += "\n💵 UC:\n"
    for p in uc_prices:
        text += f"{p.get('amount') or p.get('diamond')}\n"
    await update.message.reply_text(text)

@handle_errors
async def paid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        await _send_db_error(update)
        return
    user_id = update.effective_user.id
    if not master.is_master(user_id):
        return
    admin_id = get_admin_id(context)
    args = context.args
    if len(args) == 2:
        try:
            target_id = int(args[0])
            months = int(args[1])
            if months <= 0:
                await update.message.reply_text("❌ လအရေအတွက် 0 ထက်ကြီးရပါမည်။")
                return
            await db.add_or_update_license(target_id, months)
            await update.message.reply_text(f"✅ User `{target_id}` အား {months} လ လိုင်စင်တိုးပေးပြီးပါပြီ။", parse_mode="Markdown")
            try:
                await context.bot.send_message(chat_id=admin_id, text=f"/license_added {target_id} {months}")
            except Exception as e:
                logger.error(f"Failed to send license signal: {e}")
        except ValueError:
            await update.message.reply_text("❌ Format: `/paid user_id months`")
        return

    licenses = await db.get_all_licenses()
    if not licenses:
        await update.message.reply_text("📭 လိုင်စင်ရှိသူ မရှိသေးပါ။")
        return
    text = "📋 <b>လိုင်စင်စာရင်း (အသေးစိတ်ကြည့်ရန် နှိပ်ပါ)</b>"
    keyboard = []
    for lic in licenses:
        uid = lic["user_id"]
        expiry = lic.get("expiry_date")
        time_left = get_remaining_time_str(expiry)
        keyboard.append([InlineKeyboardButton(f"👤 ID: {uid} {time_left}", callback_data=f"lic_view_{uid}")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(text, parse_mode=ADMIN_PARSE_MODE, reply_markup=reply_markup)

@handle_errors
async def license_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        await _send_db_error(update)
        return
    query = update.callback_query
    if not master.is_master(query.from_user.id):
        await query.answer("⛔ ခွင့်ပြုချက်မရှိပါ။", show_alert=True)
        return
    await query.answer()
    data = query.data

    if data.startswith("lic_view_"):
        target_id = int(data.split("_")[-1])
        lic = await db.licenses.find_one({"user_id": target_id})
        if not lic:
            await query.answer("မတွေ့တော့ပါ။", show_alert=True)
            return
        expiry = lic.get("expiry_date")
        text = (
            f"👤 <b>User အချက်အလက်</b>\n\n"
            f"🆔 ID: <code>{target_id}</code>\n"
            f"⏳ သက်တမ်းကုန်မည့်ရက်: {expiry.strftime('%Y-%m-%d') if expiry else 'N/A'}\n"
            f"📊 အခြေအနေ: {get_remaining_time_str(expiry)}"
        )
        keyboard = [
            [
                InlineKeyboardButton("➕ 1 လတိုး", callback_data=f"lic_add_1_{target_id}"),
                InlineKeyboardButton("➕ 3 လတိုး", callback_data=f"lic_add_3_{target_id}")
            ],
            [InlineKeyboardButton("🚫 လိုင်စင်ပိတ်မယ်", callback_data=f"lic_revoke_{target_id}")],
            [InlineKeyboardButton("🔙 စာရင်းသို့", callback_data="lic_main_list")]
        ]
        await query.edit_message_text(text, parse_mode=ADMIN_PARSE_MODE, reply_markup=InlineKeyboardMarkup(keyboard))

    elif data == "lic_main_list":
        licenses = await db.get_all_licenses()
        keyboard = []
        for lic in licenses:
            uid = lic["user_id"]
            expiry = lic.get("expiry_date")
            time_left = get_remaining_time_str(expiry)
            keyboard.append([InlineKeyboardButton(f"👤 ID: {uid} {time_left}", callback_data=f"lic_view_{uid}")])
        await query.edit_message_text(
            "📋 <b>လိုင်စင်စာရင်း</b>",
            parse_mode=ADMIN_PARSE_MODE,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif data.startswith("lic_add_") or data.startswith("lic_revoke_"):
        parts = data.split("_")
        if len(parts) < 4:
            return
        action = parts[1]
        if action == "add":
            months = int(parts[2])
            target_id = int(parts[3])
            await db.add_or_update_license(target_id, months)
            await query.answer(f"✅ {months} လ တိုးပေးပြီးပါပြီ", show_alert=True)
        elif action == "revoke":
            target_id = int(parts[2])
            await db.licenses.delete_one({"user_id": target_id})
            await query.answer("❌ လိုင်စင်ဖျက်သိမ်းပြီးပါပြီ", show_alert=True)
        query.data = f"lic_view_{target_id}"
        await license_callback_handler(update, context)

@handle_errors
async def refresh_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        await _send_db_error(update)
        return
    user_id = update.effective_user.id
    if not await is_owner_or_master(user_id, context):
        await update.message.reply_text("⛔ ဤ Command ကို အသုံးပြုခွင့် မရှိပါ။")
        return

    if not check_refresh_limit(user_id, context):
        await update.message.reply_text("⚠️ တစ်ရက်ကို ၃ ကြိမ်သာ Refresh လုပ်ခွင့်ရှိပါသည်။ မနက်ဖြန်မှ ထပ်စမ်းပါ။")
        return

    await send_typing(update, context)
    admin_id = get_admin_id(context)
    valid = await db.force_refresh_license(admin_id)
    if valid:
        _, expiry = await db.check_license_local(admin_id)
        time_str = get_remaining_time_str(expiry)
        msg = f"✅ လိုင်စင်ရှိနေပါသည်။\n⏳ သက်တမ်းကျန်: {time_str}"
    else:
        msg = "❌ လိုင်စင် မရှိပါ သို့မဟုတ် သက်တမ်းကုန်နေပါသည်။"
    await update.message.reply_text(msg)

# ================== Background Jobs ==================
async def check_timeouts(context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        return
    try:
        timeout_orders = await db.get_timeout_orders()
        for order in timeout_orders:
            await db.delete_order(order['order_id'])
    except Exception as e:
        logger.error(f"Timeout check error: {e}")

# ================== Database Migration Command (Master Only) ==================
@handle_errors
async def fix_database_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db(context)
    if not db:
        await _send_db_error(update)
        return

    # Bot Owner (Master) ဟုတ်မဟုတ် အရင်စစ်မယ်
    if not master.is_master(update.effective_user.id):
        await update.message.reply_text("⛔ ဒီခလုတ်ကို သုံးပိုင်ခွင့်မရှိပါ။")
        return

    # ဒေတာဟောင်းတွေကို အသစ်ဖြစ်အောင် စတင်ပြင်ဆင်မယ်
    count = await db.migrate_legacy_diamond_types()

    if count == -1:
        await update.message.reply_text("❌ ဒေတာပြင်ဆင်မှု မအောင်မြင်ပါ။")
    elif count == 0:
        await update.message.reply_text("✅ ပြင်စရာ ဒေတာမရှိတော့ပါ။ အားလုံး အဆင်ပြေနေပါပြီ။")
    else:
        await update.message.reply_text(f"✅ ဒေတာဟောင်း {count} ခုကို Version အသစ်ဖြစ်အောင် ပြုပြင်ပြီးပါပြီ။")
