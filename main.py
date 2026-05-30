# main.py - Production-hardened, Quart integrated into main event loop (safe)
import os
import sys
import logging
import html
import asyncio
import time as _time
import signal
import secrets
from collections import defaultdict
from datetime import datetime, time, timedelta, timezone
from logging.handlers import RotatingFileHandler
from functools import wraps

import pytz
import aiohttp
from quart import Quart, jsonify, request
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    filters,
    ContextTypes,
)
from telegram.error import Conflict

from hypercorn.asyncio import serve
from hypercorn.config import Config

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import master
from master import MASTER_ID, check_ban
from database import DatabaseManager, UTC_TZ, _ensure_utc
import handlers
from handlers import (
    WAIT_GAME_ID, WAIT_CONFIRMATION, WAIT_PAYMENT,
    send_welcome, show_items, step1_selection, step2_id_entry,
    step3_validation, step4_payment, admin_callback_handler,
    user_cancel_handler, license_callback_handler, new_order_callback_handler,
    check_timeouts, paid_command, set_dia_command, set_uc_command,
    delete_dia_command, delete_uc_command, set_welcome_command,
    check_price_command, stop_command, open_command, post_command,
    active_command, refresh_command, fix_database_command,
    timeout_handler, wrap_with_license
)

# ──────────────────────────────────────
# Logging Setup
# ──────────────────────────────────────
LOG_FILE = "bot.log"
MAX_LOG_SIZE = 3 * 1024 * 1024
BACKUP_COUNT = 1
PORT = int(os.environ.get("PORT", 8080))

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console = logging.StreamHandler()
console.setFormatter(log_format)
file_handler = RotatingFileHandler(LOG_FILE, maxBytes=MAX_LOG_SIZE, backupCount=BACKUP_COUNT, encoding='utf-8')
file_handler.setFormatter(log_format)
logging.basicConfig(level=logging.INFO, handlers=[console, file_handler])
logger = logging.getLogger(__name__)

# ──────────────────────────────────────
# Environment & Configuration
# ──────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "DiamondBotDB")
ADMIN_ID_STR = os.getenv("OWNER_ID")
API_SECRET_TOKEN = os.getenv("API_SECRET_TOKEN", "").strip()
NAME_CHECK_API = os.getenv("NAME_CHECK_API", "").strip()
EXTRA_ADMIN_IDS_STR = os.getenv("ADMIN_IDS", "").strip()
TRUST_PROXY = os.getenv("TRUST_PROXY", "0") == "1"

REQUIRED_VARS = {
    "BOT_TOKEN": BOT_TOKEN,
    "MONGO_URI": MONGO_URI,
    "OWNER_ID": ADMIN_ID_STR,
    "API_SECRET_TOKEN": API_SECRET_TOKEN,
}

missing_env = [k for k, v in REQUIRED_VARS.items() if not v]
if missing_env:
    print(f"Missing required environment variables: {', '.join(missing_env)}", file=sys.stderr)
    sys.exit(1)

if len(API_SECRET_TOKEN) < 16:
    logger.warning("⚠️ API_SECRET_TOKEN is too short (<16 chars). This is a security risk!")

try:
    PRIMARY_ADMIN_ID = int(ADMIN_ID_STR)
except ValueError:
    logger.error("OWNER_ID must be an integer.")
    sys.exit(1)

extra_admin_ids = []
if EXTRA_ADMIN_IDS_STR:
    try:
        extra_admin_ids = [int(x.strip()) for x in EXTRA_ADMIN_IDS_STR.split(",") if x.strip()]
    except ValueError:
        logger.error("❌ ADMIN_IDS contains invalid integers.")
        sys.exit(1)

ALL_ADMIN_IDS = list(set([PRIMARY_ADMIN_ID] + extra_admin_ids))

# ──────────────────────────────────────
# Constants
# ──────────────────────────────────────
CONVERSATION_TIMEOUT = 900
HTTP_TIMEOUT = 20
DB_PING_RETRIES = 3
DB_PING_RETRY_DELAY = 5
HTTP_SESSION_CLOSE_TIMEOUT = 5.0
APP_SHUTDOWN_TIMEOUT = 10.0
ERROR_DEBOUNCE_SECONDS = 300

TELEGRAM_CONNECT_TIMEOUT = 10.0
TELEGRAM_READ_TIMEOUT = 30.0
TELEGRAM_WRITE_TIMEOUT = 30.0
TELEGRAM_CONNECTION_POOL_SIZE = 32
TELEGRAM_POOL_TIMEOUT = 1.0

logger.info("=== Bot Starting ===")
logger.info(f"PRIMARY ADMIN ID: {PRIMARY_ADMIN_ID}")
logger.info(f"MASTER_ID (hardcoded): {MASTER_ID}")
logger.info(f"All admin IDs: {ALL_ADMIN_IDS}")
if NAME_CHECK_API:
    logger.info("NAME_CHECK_API loaded.")
else:
    logger.warning("NAME_CHECK_API not set; name check disabled.")

# ──────────────────────────────────────
# Access Control Decorators
# ──────────────────────────────────────
def role_required(role: str = "admin"):
    def decorator(handler_func):
        @wraps(handler_func)
        async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE):
            if update.effective_user is None:
                return
            user_id = update.effective_user.id
            allowed = master.is_master(user_id) if role == "master" else master.is_admin(user_id)
            if not allowed:
                msg = "⛔ Master အခွင့်အရေးသာ လိုအပ်ပါသည်။" if role == "master" else "⛔ အခွင့်အရေးမရှိပါ။"
                if update.message:
                    await update.message.reply_text(msg)
                elif update.callback_query:
                    try:
                        await update.callback_query.answer(msg, show_alert=True)
                    except Exception:
                        pass
                return
            return await handler_func(update, context)
        return wrapped
    return decorator

master_only = role_required("master")
admin_only  = role_required("admin")

# ──────────────────────────────────────
# Database & Session Initialization
# ──────────────────────────────────────
async def init_database(application):
    master.initialize_master(admin_ids=ALL_ADMIN_IDS)

    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
    http_session = aiohttp.ClientSession(timeout=timeout)
    application.bot_data['http_session'] = http_session

    db = DatabaseManager(
        uri=MONGO_URI,
        db_name=DB_NAME,
        http_session=http_session,
        primary_admin_id=PRIMARY_ADMIN_ID
    )
    application.bot_data['db'] = db
    application.bot_data['admin_id'] = PRIMARY_ADMIN_ID

    ping_ok = False
    for attempt in range(1, DB_PING_RETRIES + 1):
        if await db.ping():
            ping_ok = True
            break
        logger.warning(f"MongoDB ping attempt {attempt} failed. Retrying…")
        await asyncio.sleep(DB_PING_RETRY_DELAY)

    if not ping_ok:
        logger.critical("MongoDB ping failed after multiple attempts.")
        try:
            await application.bot.send_message(chat_id=PRIMARY_ADMIN_ID, text="🚨 MongoDB ချိတ်ဆက်မှု မအောင်မြင်ပါ။")
        except Exception:
            pass
        await http_session.close()
        return False

    db.start_cache_cleanup()
    logger.info("Setting up indexes…")
    try:
        await db.setup_indexes()
    except Exception:
        logger.exception("Index setup encountered an error, continuing.")
    await db.banned_repo.load_banned_users_from_db()

    now = datetime.now(UTC_TZ)
    current_month = now.strftime("%Y-%m")
    if not await db.settings_repo.get_config("last_report_month"):
        await db.settings_repo.set_config("last_report_month", current_month)

    application.bot_data['name_check_api'] = NAME_CHECK_API
    if NAME_CHECK_API:
        logger.info("Name Check API configured.")
    return True

# ──────────────────────────────────────
# Background Jobs
# ──────────────────────────────────────
async def _safe_job(func, context, name: str):
    try:
        await func(context)
    except Exception:
        logger.exception(f"Job '{name}' failed:")

async def setup_jobs(application):
    jq = application.job_queue
    if not jq:
        logger.critical("❌ PTB JobQueue not available! Install python-telegram-bot[job-queue]")
        return

    async def safe_timeout_job(context): await _safe_job(check_timeouts, context, "check_timeouts")
    async def safe_monthly_report_job(context): await _safe_job(monthly_report_job, context, "monthly_report")
    async def safe_clean_expired_job(context): await _safe_job(clean_expired_licenses, context, "clean_expired")
    async def safe_purge_job(context): await _safe_job(purge_old_data_job, context, "purge_old_data")

    jq.run_repeating(safe_timeout_job, interval=60, first=10)
    jq.run_daily(safe_monthly_report_job, time=time(hour=0, minute=0, tzinfo=pytz.UTC))
    jq.run_daily(safe_clean_expired_job, time=time(hour=3, minute=0, tzinfo=pytz.UTC))
    jq.run_daily(safe_purge_job, time=time(hour=4, minute=0, tzinfo=pytz.UTC))

    db = application.bot_data.get('db')
    if db and db.license_repo.client_mode:
        async def license_refresh(context):
            dbi = context.bot_data.get('db')
            adm = context.bot_data.get('admin_id')
            if dbi and adm:
                try:
                    await dbi.license_repo.background_refresh(adm)
                except Exception:
                    logger.exception("Background license refresh failed:")
        jq.run_repeating(license_refresh, interval=24*60*60, first=60*60)

async def monthly_report_job(context):
    db = context.bot_data.get('db')
    admin_id = context.bot_data.get('admin_id')
    if not db:
        return
    now = datetime.now(UTC_TZ)
    current_month = now.strftime("%Y-%m")
    last_month = await db.settings_repo.get_config("last_report_month")
    if not last_month or last_month == current_month:
        return
    try:
        report = await db.generate_monthly_report(last_month)
        text = (
            f"📊 <b>Monthly Report ({last_month})</b>\n\n"
            f"🔹 <b>Total Orders:</b> {report.get('Total Orders', 0)}\n\n"
            f"📦 <b>Items Sold:</b>\n{report.get('Items Sold', 'အရောင်းမရှိပါ')}"
        )
        await context.bot.send_message(chat_id=admin_id, text=text, parse_mode="HTML")
    except Exception:
        logger.exception("Monthly report job failed:")
    await db.settings_repo.set_config("last_report_month", current_month)

async def clean_expired_licenses(context):
    db = context.bot_data.get('db')
    if db:
        try:
            count = await db.license_repo.cleanup_expired()
            if count > 0:
                logger.info(f"Cleaned up {count} expired licenses.")
        except Exception:
            logger.exception("Clean expired licenses failed:")

async def purge_old_data_job(context):
    db = context.bot_data.get('db')
    if db:
        try:
            del_orders, del_reports = await db.purge_3_months_old_data()
            if del_orders:
                logger.info(f"3‑month purge: {del_orders} orders removed.")
            if del_reports:
                logger.info(f"3‑month purge: {del_reports} reports removed.")
        except Exception:
            logger.exception("Purge old data job failed:")

# ──────────────────────────────────────
# Startup Notifications
# ──────────────────────────────────────
_startup_notification_sent = False
_startup_lock = asyncio.Lock()

async def _notify_startup_task(app):
    global _startup_notification_sent, _startup_lock
    async with _startup_lock:
        if _startup_notification_sent:
            return
        _startup_notification_sent = True

    try:
        bot = app.bot
        owner_name = "Admin"
        try:
            chat = await bot.get_chat(PRIMARY_ADMIN_ID)
            owner_name = html.escape(chat.first_name or chat.username or str(PRIMARY_ADMIN_ID))
        except Exception:
            pass

        bot_info = await bot.get_me()
        bot_username = f"@{bot_info.username}" if bot_info.username else "N/A"
        mmt_now = datetime.now(pytz.timezone('Asia/Yangon')).strftime('%Y-%m-%d %H:%M:%S (MMT)')

        owner_msg = (
            f"✅ <b>Bot Started Successfully</b>\n\n"
            f"👤 <b>Owner:</b> <a href='tg://user?id={PRIMARY_ADMIN_ID}'>{owner_name}</a>\n"
            f"🆔 <b>Owner ID:</b> <code>{PRIMARY_ADMIN_ID}</code>\n"
            f"🤖 <b>Bot:</b> {bot_username}\n"
            f"🕒 <b>Time:</b> {mmt_now}\n\n"
            f"Bot is now running and accepting commands."
        )

        # owner ဆီပို့တာ fail → exception တက် → master ဆီပါမပို့ (error သိစေဖို့)
        await bot.send_message(chat_id=PRIMARY_ADMIN_ID, text=owner_msg, parse_mode="HTML")

        # ── Master API သို့ notify (client mode သာ) ──
        db = app.bot_data.get('db')
        if db and db.license_repo.client_mode:
            http_session = app.bot_data.get('http_session')
            if not getattr(app, '_master_api_notified', False):
                app._master_api_notified = True
                await _notify_master_api(bot, db.license_repo.master_url, db.license_repo.secret,
                                         PRIMARY_ADMIN_ID, http_session)

    except Exception as e:
        logger.exception("Startup notification task failed:")

async def _notify_master_api(bot, master_api_url, secret, admin_id, session):
    if not master_api_url:
        return
    owner_name = "Unknown"
    bot_username = "N/A"
    try:
        chat = await bot.get_chat(admin_id)
        owner_name = chat.first_name or chat.username or str(admin_id)
    except Exception:
        pass
    try:
        me = await bot.get_me()
        bot_username = f"@{me.username}" if me.username else "N/A"
    except Exception:
        pass
    payload = {
        "admin_id": admin_id,
        "owner_name": owner_name,
        "bot_username": bot_username,
        "time": datetime.now(pytz.timezone('Asia/Yangon')).strftime('%Y-%m-%d %H:%M:%S (MMT)')
    }
    url = master_api_url.rstrip('/') + '/api/notify_startup'
    headers = {"Authorization": f"Bearer {secret}"}
    try:
        async with session.post(url, json=payload, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT)) as resp:
            if resp.status == 200:
                logger.info("Master API startup notification sent.")
            else:
                logger.warning(f"Master API notification returned {resp.status}")
    except Exception as e:
        logger.exception("Master API notification failed:")

# ──────────────────────────────────────
# Global Error Handler
# ──────────────────────────────────────
_last_error_time = {}

async def global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    error = context.error
    if error:
        logger.error(f"Unhandled error: {type(error).__name__}", exc_info=True)
    else:
        logger.error("Global error: unknown error")
        return

    now = _time.time()
    error_type = type(error).__name__
    last = _last_error_time.get(error_type, 0)
    if now - last >= ERROR_DEBOUNCE_SECONDS:
        _last_error_time[error_type] = now
        admin_id = context.bot_data.get('admin_id', PRIMARY_ADMIN_ID)
        short_msg = f"⚠️ <b>Error:</b> <code>{error_type}</code>\n{str(error)[:200]}"
        if update and isinstance(update, Update) and update.callback_query:
            try:
                await update.callback_query.answer("An error occurred. Admin notified.", show_alert=False)
            except Exception:
                pass
        try:
            await context.bot.send_message(chat_id=admin_id, text=short_msg, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Error notification failed: {e}")

# ──────────────────────────────────────
# Quart Web Server
# ──────────────────────────────────────
quart_app = Quart(__name__)

# ✅ In-memory rate limiter အတွက်
_api_call_times: dict = defaultdict(list)
API_RATE_LIMIT = 10         # requests per minute per IP
API_RATE_WINDOW = 60        # seconds

def get_client_ip(request):
    if TRUST_PROXY:
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
    return request.remote_addr

def check_api_rate_limit(ip: str) -> bool:
    now = _time.time()
    calls = _api_call_times[ip]
    # Remove entries older than window
    _api_call_times[ip] = [t for t in calls if now - t < API_RATE_WINDOW]
    if len(_api_call_times[ip]) >= API_RATE_LIMIT:
        return False
    _api_call_times[ip].append(now)
    return True

@quart_app.route('/')
async def health_check():
    db = quart_app.config.get('DB')
    bot = quart_app.config.get('BOT_INSTANCE')
    if not bot:
        return jsonify({"status": "down", "reason": "bot instance not ready"}), 503
    if not db:
        return jsonify({"status": "down", "reason": "DB not available"}), 503
    try:
        await db.ping()
    except Exception:
        return jsonify({"status": "down", "reason": "DB ping failed"}), 503
    return jsonify({"status": "ok", "bot": True, "db": True}), 200

@quart_app.route('/api/notify_startup', methods=['POST'])
async def api_notify_startup():
    try:
        ip = get_client_ip(request)
        if not check_api_rate_limit(ip):
            return jsonify({"error": "Too many requests"}), 429

        auth = request.headers.get('Authorization', '')
        if not auth.startswith("Bearer "):
            return jsonify({"error": "Unauthorized"}), 401
        token = auth.split(" ", 1)[1]
        if not secrets.compare_digest(token, API_SECRET_TOKEN):
            return jsonify({"error": "Unauthorized"}), 401

        data = await request.get_json()
        if not data:
            return jsonify({"error": "Missing JSON"}), 400

        admin_id = data.get('admin_id')
        try:
            if int(admin_id) == MASTER_ID:
                logger.info("Ignored master self-notification.")
                return jsonify({"status": "ignored", "reason": "master self notification"}), 200
        except (ValueError, TypeError):
            pass

        owner_name = data.get('owner_name', 'Unknown')
        bot_username = data.get('bot_username', 'N/A')
        time_str = data.get('time', '')

        app_bot = quart_app.config.get('BOT_INSTANCE')
        if not app_bot:
            return jsonify({"error": "Bot unavailable"}), 503

        # ── 🛠 FIX: owner name ကို clickable link ချိတ်ထားတယ် ──
        msg = (
            f"🔔 <b>Client Bot Started</b>\n\n"
            f"👤 <b>Owner:</b> <a href='tg://user?id={admin_id}'>{html.escape(owner_name)}</a>\n"
            f"🆔 <b>User ID:</b> <code>{admin_id}</code>\n"
            f"🤖 <b>Bot:</b> {bot_username}\n"
            f"🕒 <b>Time:</b> {time_str}"
        )
        await app_bot.send_message(chat_id=MASTER_ID, text=msg, parse_mode="HTML")
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        logger.exception("api_notify_startup error:")
        return jsonify({"error": "Internal server error"}), 500

@quart_app.route('/api/license/check/<int:user_id>', methods=['GET'])
async def api_license_check(user_id: int):
    db = quart_app.config.get('DB')
    if not db:
        return jsonify({"error": "Database unavailable"}), 503

    try:
        ip = get_client_ip(request)
        if not check_api_rate_limit(ip):
            return jsonify({"error": "Too many requests"}), 429

        auth = request.headers.get('Authorization', '')
        if not auth.startswith("Bearer "):
            return jsonify({"error": "Unauthorized"}), 401
        token = auth.split(" ", 1)[1]
        if not secrets.compare_digest(token, API_SECRET_TOKEN):
            return jsonify({"error": "Unauthorized"}), 401

        valid, expiry = await db.license_repo.check_license_local(user_id)
        if valid:
            return jsonify({"valid": True, "expiry": expiry.isoformat() if expiry else None}), 200
        else:
            return jsonify({"valid": False, "expiry": expiry.isoformat() if expiry else None}), 200
    except Exception as e:
        logger.exception(f"api_license_check error for user {user_id}:")
        return jsonify({"error": "Internal server error"}), 500

# ──────────────────────────────────────
# Master‑Only Handlers
# ──────────────────────────────────────
@master_only
async def master_paid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    await paid_command(update, context)

@master_only
async def master_license_add_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    db = context.bot_data.get('db')
    if not db:
        await update.message.reply_text("⏳ Database not ready.")
        return
    args = context.args
    if len(args) == 3 and args[0].lower() == "add":
        try:
            target_id = int(args[1])
            months = int(args[2])
            await db.license_repo.add_or_update(target_id, months)
            await update.message.reply_text(
                f"✅ License updated: User <a href='tg://user?id={target_id}'>{target_id}</a> +{months} months.",
                parse_mode="HTML"
            )
        except ValueError:
            await update.message.reply_text("❌ Invalid user ID or months.")
        except Exception:
            logger.exception("master_license_add_command failed:")
            await update.message.reply_text("❌ Failed to update license.")
    else:
        await update.message.reply_text("📋 Usage: /license add <user_id> <months>")

# ──────────────────────────────────────
# Bot Application & Registration
# ──────────────────────────────────────
app = None

async def main_async():
    global app, _startup_lock, _startup_notification_sent

    _startup_lock = asyncio.Lock()
    _startup_notification_sent = False

    app = (ApplicationBuilder()
           .token(BOT_TOKEN)
           .connect_timeout(TELEGRAM_CONNECT_TIMEOUT)
           .read_timeout(TELEGRAM_READ_TIMEOUT)
           .write_timeout(TELEGRAM_WRITE_TIMEOUT)
           .connection_pool_size(TELEGRAM_CONNECTION_POOL_SIZE)
           .pool_timeout(TELEGRAM_POOL_TIMEOUT)
           .build()
    )

    if not app.job_queue:
        raise RuntimeError(
            "PTB JobQueue is not available. Install python-telegram-bot[job-queue] or ensure the extra is present."
        )

    shutdown_event = asyncio.Event()
    quart_task = None

    try:
        if not await init_database(app):
            logger.critical("Database initialization failed. Exiting.")
            return

        db = app.bot_data['db']
        quart_app.config['DB'] = db
        quart_app.config['ADMIN_ID'] = PRIMARY_ADMIN_ID
        quart_app.config['API_SECRET_TOKEN'] = API_SECRET_TOKEN

        await setup_jobs(app)

        admin_commands = [
            ("setdia", set_dia_command),
            ("setuc", set_uc_command),
            ("deletedia", delete_dia_command),
            ("deleteuc", delete_uc_command),
            ("setwelcome", set_welcome_command),
            ("check", check_price_command),
            ("stop", stop_command),
            ("open", open_command),
            ("post", post_command),
            ("active", active_command),
            ("refresh", refresh_command),
            ("fixdb", fix_database_command),
        ]
        for cmd, func in admin_commands:
            app.add_handler(CommandHandler(cmd, admin_only(func)))

        app.add_handler(CommandHandler("paid", master_paid_command))
        app.add_handler(CommandHandler("license", master_license_add_command))

        conv_handler = ConversationHandler(
            entry_points=[
                CallbackQueryHandler(check_ban(step1_selection), pattern=r"^price_(dia|uc)_.+$"),
                CallbackQueryHandler(check_ban(show_items), pattern=r"^(show_dia|show_uc)$"),
                CallbackQueryHandler(check_ban(send_welcome), pattern=r"^back_to_main$"),
                CommandHandler("start", check_ban(send_welcome)),
            ],
            states={
                WAIT_GAME_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, step2_id_entry)],
                WAIT_CONFIRMATION: [CallbackQueryHandler(step3_validation, pattern="^(confirm_id|back_id)$")],
                WAIT_PAYMENT: [MessageHandler(filters.PHOTO, step4_payment)],
                ConversationHandler.TIMEOUT: [
                    MessageHandler(filters.ALL & ~filters.COMMAND, timeout_handler),
                    CallbackQueryHandler(timeout_handler, pattern=r"^(show_dia|show_uc|price_.*|confirm_id|back_id|cancel_user_.*)$")
                ]
            },
            fallbacks=[
                CommandHandler("start", check_ban(send_welcome)),
                MessageHandler(filters.ALL & ~filters.COMMAND, timeout_handler),
            ],
            conversation_timeout=CONVERSATION_TIMEOUT,
            name="order_conversation",
            persistent=False,
        )
        app.add_handler(conv_handler)

        app.add_handler(CallbackQueryHandler(admin_callback_handler, pattern=r"^admin_"))
        app.add_handler(CallbackQueryHandler(user_cancel_handler, pattern=r"^cancel_user_"))
        app.add_handler(CallbackQueryHandler(license_callback_handler, pattern=r"^license_"))
        app.add_handler(CallbackQueryHandler(new_order_callback_handler, pattern=r"^new_order$"))

        app.add_error_handler(global_error_handler)

        await app.initialize()
        # ── 🛠 FIX: restart တိုင်း webhook ဖျက်ပြီး conflict spam လျှော့ချ ──
        await app.bot.delete_webhook(drop_pending_updates=True)
        await app.start()

        quart_app.config['BOT_INSTANCE'] = app.bot

        quart_config = Config()
        quart_config.bind = [f"0.0.0.0:{PORT}"]
        quart_task = asyncio.create_task(serve(quart_app, quart_config))
        logger.info("Quart server started on main event loop.")

        if not app.updater:
            logger.critical("Updater unavailable. Exiting.")
            return

        if app.updater.running:
            logger.critical("Updater already running. Another process may be alive. Exiting.")
            return

        try:
            # ── 🛠 FIX: drop_pending_updates=True → restart တိုင်း conflict လျှော့ချ ──
            await app.updater.start_polling(
                drop_pending_updates=True,
                allowed_updates=None
            )
        except Conflict:
            logger.critical("Another bot instance is already polling. Exiting.")
            return

        logger.info("Bot polling started.")

        if not _startup_notification_sent:
            asyncio.create_task(_notify_startup_task(app))

        def signal_handler():
            logger.info("Received termination signal. Initiating shutdown...")
            shutdown_event.set()

        try:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            pass

        try:
            await shutdown_event.wait()
        except asyncio.CancelledError:
            pass

    finally:
        logger.info("Shutting down…")

        if app:
            try:
                if app.updater and app.updater.running:
                    await app.updater.stop()
            except Exception:
                pass
            try:
                if app.running:
                    await app.stop()
            except Exception:
                pass
            try:
                await asyncio.wait_for(app.shutdown(), timeout=APP_SHUTDOWN_TIMEOUT)
            except asyncio.TimeoutError:
                logger.warning("app.shutdown() timed out, continuing.")
            except Exception:
                logger.exception("app.shutdown() error:")

        if quart_task and not quart_task.done():
            quart_task.cancel()
            try:
                await quart_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.warning(f"Quart task error during shutdown: {e}")

        if app:
            db = app.bot_data.get('db')
            if db:
                await db.close()
                logger.info("Database connection closed.")

            http_session = app.bot_data.get('http_session')
            if http_session and not http_session.closed:
                try:
                    await asyncio.wait_for(http_session.close(), timeout=HTTP_SESSION_CLOSE_TIMEOUT)
                    logger.info("HTTP session closed.")
                except Exception as e:
                    logger.warning(f"HTTP session close failed: {e}")

        logger.info("Bot shutdown complete.")

def main():
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)

if __name__ == '__main__':
    main()
