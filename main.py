# main.py (အစအဆုံး – critical fix: call_soon_threadsafe usage)
"""
main.py – Production‑ready bot entry point (Render‑optimized).
- Master ID from hardcoded master.py.
- Quart web server runs in separate thread with lightweight Motor client.
- Hypercorn ASGI server.
- Graceful shutdown using asyncio.Event + signal handlers.
- Rate limited license check API with IP leak prevention and periodic memory cleanup.
- Enhanced health check with live DB ping.
- Race condition mitigation via threading.Event for Quart DB ready.
- Polling conflict protection.
- Non-blocking thread join.
- Background Quart thread health monitor.
- Safe for external supervisor (Render auto-restart) – no internal restart loop.
- Startup notification sent only once per process lifetime (guard flag).
- Proper shutdown ordering: Telegram → Quart → DB → HTTP session.
- Quart fatal failure triggers process exit using threading.Event (only on crash).
- Reduced allowed_updates to save memory (string list, correct PTB format).
- Reduced Telegram connection pool for free‑tier RAM.
- Extra guards: safe Quart shutdown event set (loop closed), and app.initialize() fail handling.
"""

import os
import sys
import asyncio
import logging
import html
import signal
import threading
from datetime import datetime, time
from functools import wraps
from logging.handlers import RotatingFileHandler

import pytz
import aiohttp
import motor.motor_asyncio
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

# Load .env if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import master
from master import _REAL_MASTER_ID as MASTER_ID
from database import DatabaseManager, UTC_TZ
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
# Logging Setup (duplication‑free, Render‑friendly)
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

ALL_ADMIN_IDS = [PRIMARY_ADMIN_ID] + extra_admin_ids

# ──────────────────────────────────────
# Constants
# ──────────────────────────────────────
CONVERSATION_TIMEOUT = 900
HTTP_TIMEOUT = 20
DB_PING_RETRIES = 3
DB_PING_RETRY_DELAY = 5
HTTP_SESSION_CLOSE_TIMEOUT = 5.0
QUART_SHUTDOWN_TIMEOUT = 10.0
RATE_LIMIT_MAX = 10
RATE_LIMIT_WINDOW = 60
QUART_READY_TIMEOUT = 10
RATE_LIMIT_CLEANUP_INTERVAL = 300

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
# Database & Session Initialization (Main Bot)
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
    await db.setup_indexes()
    await db.banned_repo.load_banned_users_from_db()

    now = datetime.now(UTC_TZ)
    current_month = now.strftime("%Y-%m")
    if not await db.settings_repo.get_config("last_report_month"):
        await db.settings_repo.set_config("last_report_month", current_month)

#    await db.license_repo.background_refresh(PRIMARY_ADMIN_ID)
    application.bot_data['name_check_api'] = NAME_CHECK_API
    if NAME_CHECK_API:
        logger.info("Name Check API configured.")
    return True

# ──────────────────────────────────────
# Background Jobs
# ──────────────────────────────────────
async def setup_jobs(application):
    jq = application.job_queue
    if not jq:
        logger.critical("❌ PTB JobQueue not available! Install python-telegram-bot[job-queue]")
        logger.critical("Bot will continue WITHOUT background jobs (timeout check, reports, etc.)")
        return
    jq.run_repeating(check_timeouts, interval=60, first=10)
    jq.run_daily(monthly_report_job, time=time(hour=0, minute=0, tzinfo=pytz.UTC))
    jq.run_daily(clean_expired_licenses, time=time(hour=3, minute=0, tzinfo=pytz.UTC))
    jq.run_daily(purge_old_data_job, time=time(hour=4, minute=0, tzinfo=pytz.UTC))

    db = application.bot_data.get('db')
    if db and db.license_repo.client_mode:
        async def license_refresh(context):
            dbi = context.bot_data.get('db')
            adm = context.bot_data.get('admin_id')
            if dbi and adm:
                await dbi.license_repo.background_refresh(adm)
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
    report = await db.generate_monthly_report(last_month)
    text = (
        f"📊 <b>Monthly Report ({last_month})</b>\n\n"
        f"🔹 <b>Total Orders:</b> {report.get('Total Orders', 0)}\n\n"
        f"📦 <b>Items Sold:</b>\n{report.get('Items Sold', 'အရောင်းမရှိပါ')}"
    )
    await context.bot.send_message(chat_id=admin_id, text=text, parse_mode="HTML")
    await db.settings_repo.set_config("last_report_month", current_month)

async def clean_expired_licenses(context):
    db = context.bot_data.get('db')
    if db:
        count = await db.license_repo.cleanup_expired()
        if count > 0:
            logger.info(f"Cleaned up {count} expired licenses.")

async def purge_old_data_job(context):
    db = context.bot_data.get('db')
    if db:
        del_orders, del_reports = await db.purge_3_months_old_data()
        if del_orders:
            logger.info(f"3‑month purge: {del_orders} orders removed.")
        if del_reports:
            logger.info(f"3‑month purge: {del_reports} reports removed.")

# ──────────────────────────────────────
# Startup Notifications (race‑condition safe)
# ──────────────────────────────────────
_startup_notification_sent = False
_startup_lock = None  # Created in main_async

async def _notify_startup_task(app):
    global _startup_notification_sent, _startup_lock
    async with _startup_lock:
        if _startup_notification_sent:
            logger.debug("Startup notification already sent; skipping duplicate call.")
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
            f"👤 <b>Owner:</b> {owner_name}\n"
            f"🆔 <b>Owner ID:</b> <a href='tg://user?id={PRIMARY_ADMIN_ID}'>{PRIMARY_ADMIN_ID}</a>\n"
            f"🤖 <b>Bot:</b> {bot_username}\n"
            f"🕒 <b>Time:</b> {mmt_now}\n\n"
            f"Bot is now running and accepting commands."
        )
        await bot.send_message(chat_id=PRIMARY_ADMIN_ID, text=owner_msg, parse_mode="HTML")

        db = app.bot_data.get('db')
        if db and db.license_repo.client_mode:
            http_session = app.bot_data.get('http_session')
            logger.info(f"client_mode={db.license_repo.client_mode}, master_url={db.license_repo.master_url}")
            if not getattr(app, '_master_api_notified', False):
                app._master_api_notified = True
                await _notify_master_api(bot, db.license_repo.master_url, db.license_repo.secret,
                                         PRIMARY_ADMIN_ID, http_session)
    except Exception as e:
        logger.error(f"Startup notification task failed: {e}")

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
        logger.error(f"Master API notification failed: {e}")

# ──────────────────────────────────────
# Global Error Handler
# ──────────────────────────────────────
async def global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    error = context.error
    if error:
        logger.error(f"Unhandled error: {type(error).__name__}", exc_info=True)
    else:
        logger.error("Global error: unknown error")

# ──────────────────────────────────────
# Quart Web Server
# ──────────────────────────────────────
quart_app = Quart(__name__)

@quart_app.route('/api/notify_startup', methods=['POST'])
async def api_notify_startup():
    auth = request.headers.get('Authorization', '')
    if not auth.startswith("Bearer ") or auth.split(" ", 1)[1] != API_SECRET_TOKEN:
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

    msg = (
        f"🔔 <b>Client Bot Started</b>\n\n"
        f"👤 <b>Owner:</b> {html.escape(owner_name)}\n"
        f"🆔 <b>User ID:</b> <code>{admin_id}</code>\n"
        f"🤖 <b>Bot:</b> {bot_username}\n"
        f"🕒 <b>Time:</b> {time_str}"
    )
    try:
        await app_bot.send_message(
            chat_id=MASTER_ID,
            text=msg,
            parse_mode="HTML"
        )
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        logger.error(f"Notify startup failed: {e}")
        return jsonify({"error": str(e)}), 500

quart_motor_client = None
quart_licenses_col = None
quart_loop = None
quart_shutdown_event = None
quart_ready_event = threading.Event()
quart_fatal_event = threading.Event()

_rate_limit_lock = None
_rate_limit_dict = {}
rate_cleanup_task = None

def get_client_ip(request):
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.remote_addr

async def _check_rate_limit(ip: str) -> bool:
    if _rate_limit_lock is None:
        return True
    async with _rate_limit_lock:
        now = datetime.now(UTC_TZ)
        timestamps = _rate_limit_dict.get(ip, [])
        timestamps = [t for t in timestamps if (now - t).total_seconds() < RATE_LIMIT_WINDOW]
        if len(timestamps) >= RATE_LIMIT_MAX:
            _rate_limit_dict[ip] = timestamps
            return False
        timestamps.append(now)
        _rate_limit_dict[ip] = timestamps
        return True

async def _rate_limit_cleanup_loop():
    while True:
        await asyncio.sleep(RATE_LIMIT_CLEANUP_INTERVAL)
        async with _rate_limit_lock:
            now = datetime.now(UTC_TZ)
            stale_ips = []
            for ip, timestamps in list(_rate_limit_dict.items()):
                fresh = [t for t in timestamps if (now - t).total_seconds() < RATE_LIMIT_WINDOW]
                if fresh:
                    _rate_limit_dict[ip] = fresh
                else:
                    stale_ips.append(ip)
            for ip in stale_ips:
                _rate_limit_dict.pop(ip, None)
            if stale_ips:
                logger.debug(f"Rate limit cleanup: removed {len(stale_ips)} stale IPs.")

async def _init_quart_db():
    global quart_motor_client, quart_licenses_col
    client = motor.motor_asyncio.AsyncIOMotorClient(
        MONGO_URI,
        connect=False,
        serverSelectionTimeoutMS=5000,
        maxPoolSize=5,
        minPoolSize=0
    )
    try:
        await client.admin.command('ping')
    except Exception as e:
        logger.critical(f"Quart Motor ping failed: {e}")
        client.close()
        quart_fatal_event.set()
        quart_ready_event.set()
        return False

    quart_motor_client = client
    quart_licenses_col = client[DB_NAME]["Licenses"]
    logger.info("Quart lightweight DB connected.")
    quart_ready_event.set()
    return True

async def _close_quart_db():
    global quart_motor_client
    if quart_motor_client:
        quart_motor_client.close()
        logger.info("Quart Motor client closed.")

bot_running = False

@quart_app.route('/')
async def health_check():
    if not bot_running:
        return jsonify({"status": "down", "reason": "bot not started"}), 503
    if quart_motor_client is None or quart_licenses_col is None:
        return jsonify({"status": "down", "reason": "DB unavailable"}), 503
    try:
        await quart_motor_client.admin.command('ping')
    except Exception:
        return jsonify({"status": "down", "reason": "DB ping failed"}), 503
    return jsonify({"status": "ok", "bot": True, "db": True}), 200

@quart_app.route('/api/license/check/<int:user_id>', methods=['GET'])
async def api_license_check(user_id: int):
    auth = request.headers.get('Authorization', '')
    if not auth.startswith("Bearer ") or auth.split(" ", 1)[1] != API_SECRET_TOKEN:
        return jsonify({"error": "Unauthorized"}), 401

    ip = get_client_ip(request)
    if not await _check_rate_limit(ip):
        return jsonify({"error": "Too many requests"}), 429

    if quart_licenses_col is None:
        return jsonify({"error": "Database unavailable"}), 503

    try:
        doc = await quart_licenses_col.find_one({"user_id": user_id})
    except Exception as e:
        logger.error(f"Quart license lookup error: {e}")
        return jsonify({"error": "Database error"}), 500

    if not doc:
        return jsonify({"valid": False, "expiry": None}), 200

    expiry = doc.get("expiry_date")
    now_utc = datetime.now(UTC_TZ)
    if expiry and expiry > now_utc:
        return jsonify({"valid": True, "expiry": expiry.isoformat()}), 200
    else:
        return jsonify({"valid": False, "expiry": expiry.isoformat() if expiry else None}), 200

def start_quart():
    global quart_loop, quart_shutdown_event, _rate_limit_lock, _rate_limit_dict, rate_cleanup_task
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    quart_loop = loop

    _rate_limit_lock = asyncio.Lock()
    _rate_limit_dict = {}

    quart_shutdown_event = asyncio.Event()

    async def async_init_and_serve():
        ok = await _init_quart_db()
        if not ok:
            logger.critical("Quart DB init failed – Quart thread will exit.")
            return

        global rate_cleanup_task
        rate_cleanup_task = asyncio.create_task(_rate_limit_cleanup_loop())

        config = Config()
        config.bind = [f"0.0.0.0:{PORT}"]

        async def _shutdown_trigger():
            await quart_shutdown_event.wait()
            logger.info("Quart shutdown signal received.")

        try:
            await serve(quart_app, config, shutdown_trigger=_shutdown_trigger)
        finally:
            if rate_cleanup_task:
                rate_cleanup_task.cancel()
                try:
                    await rate_cleanup_task
                except asyncio.CancelledError:
                    pass

    try:
        loop.run_until_complete(async_init_and_serve())
    except Exception as e:
        logger.critical(f"Quart server crashed: {e}", exc_info=True)
        quart_fatal_event.set()
    finally:
        loop.run_until_complete(_close_quart_db())
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
        logger.info("Quart thread shut down gracefully.")

# ──────────────────────────────────────
# Master‑Only Handlers
# ──────────────────────────────────────
@master_only
async def master_paid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await paid_command(update, context)

@master_only
async def master_license_add_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    else:
        await update.message.reply_text("📋 Usage: /license add <user_id> <months>")

# ──────────────────────────────────────
# Bot Application & Registration
# ──────────────────────────────────────
quart_thread = None

async def _monitor_quart_thread():
    warned = False
    while True:
        await asyncio.sleep(30)
        if quart_thread and not quart_thread.is_alive():
            if not warned:
                logger.critical("Quart thread is not alive! Health check will fail.")
                warned = True
            return

app = None

async def main_async():
    global bot_running, quart_shutdown_event, quart_thread, quart_ready_event, quart_fatal_event
    global _startup_notification_sent, _startup_lock, app

    _startup_notification_sent = False
    quart_ready_event.clear()
    quart_fatal_event.clear()

    _startup_lock = asyncio.Lock()

    app = (ApplicationBuilder()
           .token(BOT_TOKEN)
           .connect_timeout(TELEGRAM_CONNECT_TIMEOUT)
           .read_timeout(TELEGRAM_READ_TIMEOUT)
           .write_timeout(TELEGRAM_WRITE_TIMEOUT)
           .connection_pool_size(TELEGRAM_CONNECTION_POOL_SIZE)
           .pool_timeout(TELEGRAM_POOL_TIMEOUT)
           .build()
    )

    monitor_task = None

    try:
        # ---------- Startup ----------
        if not await init_database(app):
            logger.critical("Database initialization failed. Exiting.")
            return

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
                CallbackQueryHandler(wrap_with_license(step1_selection), pattern=r"^price_(dia|uc)_.+$"),
                CallbackQueryHandler(wrap_with_license(show_items), pattern=r"^(show_dia|show_uc)$"),
                CallbackQueryHandler(wrap_with_license(send_welcome), pattern=r"^back_to_main$"),
                CommandHandler("start", wrap_with_license(send_welcome)),
            ],
            states={
                WAIT_GAME_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, wrap_with_license(step2_id_entry))],
                WAIT_CONFIRMATION: [CallbackQueryHandler(wrap_with_license(step3_validation), pattern="^(confirm_id|back_id)$")],
                WAIT_PAYMENT: [MessageHandler(filters.PHOTO, wrap_with_license(step4_payment))],
            },
            fallbacks=[
                CommandHandler("start", wrap_with_license(send_welcome)),
                MessageHandler((filters.TEXT | filters.PHOTO) & ~filters.COMMAND, timeout_handler),
            ],
            conversation_timeout=CONVERSATION_TIMEOUT,
            name="order_conversation",
            persistent=False,
        )
        app.add_handler(conv_handler)

        app.add_handler(CallbackQueryHandler(admin_callback_handler, pattern=r"^admin_"))
        app.add_handler(CallbackQueryHandler(user_cancel_handler, pattern=r"^cancel_user_"))
        app.add_handler(CallbackQueryHandler(license_callback_handler, pattern=r"^license_"))
        app.add_handler(CallbackQueryHandler(wrap_with_license(new_order_callback_handler), pattern=r"^new_order$"))

        app.add_error_handler(global_error_handler)

        quart_thread = threading.Thread(target=start_quart, daemon=True, name="QuartThread")
        quart_thread.start()

        monitor_task = asyncio.create_task(_monitor_quart_thread())

        ready = await asyncio.to_thread(quart_ready_event.wait, QUART_READY_TIMEOUT)
        if not ready:
            logger.warning("Quart DB did not become ready in time; health check may show down.")
        if quart_fatal_event.is_set():
            logger.critical("Quart DB initialization failed fatally. Exiting bot.")
            return

        await app.initialize()
        quart_app.config['BOT_INSTANCE'] = app.bot
        await app.start()

        if not app.updater:
            logger.critical("Updater unavailable. Exiting.")
            return

        try:
            await app.updater.start_polling(
                drop_pending_updates=False,
                allowed_updates=["message", "callback_query"]
            )
        except Conflict:
            logger.critical("Another bot instance is already polling. Exiting.")
            return

        bot_running = True
        logger.info("Bot polling started.")

        if not _startup_notification_sent:
            asyncio.create_task(_notify_startup_task(app))

        shutdown_event = asyncio.Event()

        def signal_handler():
            logger.info("Received termination signal. Initiating shutdown...")
            shutdown_event.set()

        try:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            pass

        async def wait_for_shutdown():
            while not shutdown_event.is_set():
                if quart_fatal_event.is_set():
                    logger.critical("Quart fatal failure detected. Shutting down bot.")
                    shutdown_event.set()
                    break
                await asyncio.sleep(0.5)

        try:
            await wait_for_shutdown()
        except asyncio.CancelledError:
            pass

    finally:
        # ---------- Shutdown cleanup ----------
        logger.info("Shutting down…")
        bot_running = False

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
                await app.shutdown()
            except Exception:
                pass

        if monitor_task:
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass

        # Stop Quart safely – CRITICAL FIX: pass the function, not the result
        if quart_loop and quart_shutdown_event:
            try:
                quart_loop.call_soon_threadsafe(quart_shutdown_event.set)
            except Exception:
                logger.debug("Quart event loop already closed (or not accessible).")

        if quart_thread and quart_thread.is_alive():
            await asyncio.to_thread(quart_thread.join, QUART_SHUTDOWN_TIMEOUT)

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
