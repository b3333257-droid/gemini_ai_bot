# main.py (Final - Clean imports, License wrap via handlers)
import os
import sys
import logging
import html
import asyncio
import time as _time
from datetime import datetime, time, timedelta
from logging.handlers import RotatingFileHandler

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

try:
    from dotenv import load_dotenv
    load_dotenv()
    logger_loading = logging.getLogger(__name__)
    logger_loading.info(".env file loaded (python-dotenv)")
except ImportError:
    pass

import master

# ==========================================
# 🔒 Environment Variables & Validation
# ==========================================
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")
ADMIN_ID_STR = os.getenv("OWNER_ID")
API_SECRET_TOKEN = os.getenv("API_SECRET_TOKEN", "").strip()

missing_vars = []
if not BOT_TOKEN:
    missing_vars.append("BOT_TOKEN")
if not MONGO_URI:
    missing_vars.append("MONGO_URI")
if not ADMIN_ID_STR:
    missing_vars.append("OWNER_ID")
if not API_SECRET_TOKEN:
    missing_vars.append("API_SECRET_TOKEN")

if missing_vars:
    raise ValueError(f"⚠️ Missing required environment variables: {', '.join(missing_vars)}")

try:
    ADMIN_ID = int(ADMIN_ID_STR)
except ValueError:
    raise ValueError("⚠️ OWNER_ID must be an integer.")

# ✅ Master ID hardcoded
master.MASTER_ID = 6510049765

# ==========================================
# 📝 Logging Configuration
# ==========================================
LOG_FILE = "bot.log"
MAX_LOG_SIZE = 5 * 1024 * 1024
BACKUP_COUNT = 3

log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_format)
file_handler = RotatingFileHandler(LOG_FILE, maxBytes=MAX_LOG_SIZE, backupCount=BACKUP_COUNT, encoding='utf-8')
file_handler.setFormatter(log_format)
logging.basicConfig(level=logging.INFO, handlers=[console_handler, file_handler])
logger = logging.getLogger(__name__)

logger.info("=== Bot Starting ===")
logger.info(f"ADMIN_ID: {ADMIN_ID}")
logger.info(f"MASTER_ID hardcoded: {master.MASTER_ID}")
logger.info(f"DB Name: DiamondBotDB (URI masked)")

# ==========================================
# 🔌 Modules Linking
# ==========================================
from database import DatabaseManager, UTC_TZ
import handlers
from handlers import (
    WAIT_GAME_ID, WAIT_CONFIRMATION, WAIT_PAYMENT, wrap_with_license,
    send_welcome, show_items, step1_selection, step2_id_entry,
    step3_validation, step4_payment, admin_callback_handler,
    user_cancel_handler, license_callback_handler, new_order_callback_handler,
    check_timeouts, paid_command, set_dia_command, set_uc_command,
    delete_dia_command, delete_uc_command, set_welcome_command,
    check_price_command, stop_command, open_command, post_command,
    active_command, refresh_command
)

# ==========================================
# 🛡 Startup Access Guard (FIXED: send to ADMIN_ID first)
# ==========================================
async def check_startup_access(app) -> bool:
    """Send startup notification to ADMIN_ID (owner) and optionally to MASTER_ID if different."""
    try:
        # Fetch owner info (needs /start prior, but will try gracefully)
        try:
            owner_chat = await app.bot.get_chat(ADMIN_ID)
            owner_name = owner_chat.first_name or owner_chat.username or str(ADMIN_ID)
        except Exception as e:
            logger.warning(f"Could not fetch owner chat info: {e}. Using fallback 'Admin'.")
            owner_name = "Admin"

        safe_owner_name = html.escape(owner_name)
        owner_id_link = f'<a href="tg://user?id={ADMIN_ID}">{ADMIN_ID}</a>'

        bot_info = await app.bot.get_me()
        bot_username = f"@{bot_info.username}" if bot_info.username else "N/A"

        mmt_tz = pytz.timezone('Asia/Yangon')
        mmt_now = datetime.now(mmt_tz)
        time_str = mmt_now.strftime('%Y-%m-%d %H:%M:%S (MMT)')

        startup_msg = (
            f"✅ <b>Bot Started Successfully</b>\n\n"
            f"👤 <b>Owner:</b> {safe_owner_name}\n"
            f"🆔 <b>Owner ID:</b> {owner_id_link}\n"
            f"🤖 <b>Bot:</b> {bot_username}\n"
            f"🕒 <b>Time:</b> {time_str}\n\n"
            f"Bot is now running and accepting commands."
        )

        # 🎯 Primary notification to ADMIN_ID (owner)
        try:
            await app.bot.send_message(
                chat_id=ADMIN_ID,
                text=startup_msg,
                parse_mode="HTML"
            )
            logger.info(f"Startup notification sent to ADMIN {ADMIN_ID}")
        except Exception as e:
            logger.warning(f"Could not send startup notification to ADMIN {ADMIN_ID}: {e}")

        # 🎯 If MASTER_ID is set and different from ADMIN, also send there
        if master.MASTER_ID is not None and master.MASTER_ID != ADMIN_ID:
            try:
                # Optionally check if master has started the bot; if fail, ignore
                await app.bot.send_message(
                    chat_id=master.MASTER_ID,
                    text=startup_msg,
                    parse_mode="HTML"
                )
                logger.info(f"Startup notification sent to MASTER {master.MASTER_ID}")
            except Exception as e:
                logger.warning(f"Could not send startup notification to MASTER: {e}")

        return True
    except Exception as e:
        logger.error(f"Failed to prepare startup notification: {e}")
        return True


# ==========================================
# ⚡ Post Init Hook
# ==========================================
async def post_init(application):
    try:
        db = DatabaseManager(uri=MONGO_URI)
        db.set_admin_id(ADMIN_ID)
        application.bot_data['db'] = db
        application.bot_data['admin_id'] = ADMIN_ID
        quart_app.config['db'] = db
        quart_app.config['ADMIN_ID'] = ADMIN_ID
        # ✅ Store bot instance for API usage
        quart_app.config['bot'] = application.bot
        db.start_cache_cleanup()

        logger.info("Checking MongoDB connection...")
        if not await db.ping():
            logger.critical("❌ MongoDB ping failed.")
            await application.bot.send_message(
                chat_id=ADMIN_ID,
                text="🚨 MongoDB ချိတ်ဆက်မှု မအောင်မြင်ပါ။"
            )
            sys.exit(1)

        logger.info("Initializing Database Indexes...")
        await db.setup_indexes()

        # ✅ Load banned users list from DB into memory
        await master.load_banned_users_from_db(db)

        # ✅ Use DatabaseManager methods instead of direct collection access
        current_month_str = datetime.now(UTC_TZ).strftime("%Y-%m")
        existing_report_month = await db.get_config("last_report_month")
        if not existing_report_month:
            await db.set_config("last_report_month", current_month_str)
            logger.info(f"Initialized last_report_month to {current_month_str}")

        logger.info("✅ Database setup completed.")
    except Exception as e:
        logger.error(f"❌ Database initialization failed: {e}")
        try:
            await application.bot.send_message(chat_id=ADMIN_ID, text=f"⚠️ DB Connection Failed: {e}")
        except:
            pass
        sys.exit(1)

    await check_startup_access(application)

    # ✅ Notify Master server about client startup via API
    db_instance = application.bot_data.get('db')
    if db_instance and db_instance.is_client_mode:
        await notify_master_startup(db_instance.MASTER_API_URL, db_instance.API_SECRET_TOKEN, ADMIN_ID)

    jq = application.job_queue
    if jq and db_instance.is_client_mode:
        async def license_refresh_job(context: ContextTypes.DEFAULT_TYPE):
            db_inner = context.bot_data.get('db')
            admin = context.bot_data.get('admin_id', ADMIN_ID)
            if db_inner:
                await db_inner.background_refresh_license(admin)
                logger.debug("Background license refresh completed.")

        jq.run_repeating(
            license_refresh_job,
            interval=24 * 60 * 60,
            first=60 * 60,
            name="license_background_refresh"
        )
        logger.info("Client mode: Background license refresh scheduled every 24h.")


# ==========================================
# 📊 Monthly Report Job (FIXED: uses get_config/set_config)
# ==========================================
async def monthly_report_job(context: ContextTypes.DEFAULT_TYPE):
    db = context.bot_data.get('db')
    admin_id = context.bot_data.get('admin_id', ADMIN_ID)
    if db is None:
        return
    now = datetime.now(UTC_TZ)
    current_month_str = now.strftime("%Y-%m")
    # ✅ Use get_config instead of db.settings.find_one
    last_report_month = await db.get_config("last_report_month")
    if not last_report_month:
        await db.set_config("last_report_month", current_month_str)
        return
    if last_report_month == current_month_str:
        return
    try:
        report = await db.generate_monthly_report(last_report_month)
        report_text = (
            f"📊 <b>Monthly Report ({last_report_month})</b>\n\n"
            f"🔹 <b>Total Orders:</b> {report.get('Total Orders', 0)}\n\n"
            f"📦 <b>Items Sold:</b>\n{report.get('Items Sold', 'အရောင်းမရှိပါ')}\n"
        )
        await context.bot.send_message(chat_id=admin_id, text=report_text, parse_mode="HTML")
        # ✅ Update using set_config
        await db.set_config("last_report_month", current_month_str)
    except Exception as e:
        logger.error(f"Error in monthly report job: {e}")


# ==========================================
# 🧹 Background Cleanup Jobs
# ==========================================
async def clean_expired_licenses(context: ContextTypes.DEFAULT_TYPE):
    """Remove licenses that have expired."""
    db = context.bot_data.get('db')
    if db is None:
        return
    try:
        result = await db.licenses.delete_many({"expiry_date": {"$lt": datetime.now(UTC_TZ)}})
        if result.deleted_count > 0:
            logger.info(f"Cleaned up {result.deleted_count} expired license(s).")
    except Exception as e:
        logger.error(f"Error cleaning expired licenses: {e}")

async def purge_old_data_job(context: ContextTypes.DEFAULT_TYPE):
    """Delete orders and reports older than 90 days (3 months)."""
    db = context.bot_data.get('db')
    if db is None:
        return
    try:
        deleted_orders, deleted_reports = await db.purge_3_months_old_data()
        if deleted_orders > 0:
            logger.info(f"3-month purge: {deleted_orders} orders removed.")
        if deleted_reports > 0:
            logger.info(f"3-month purge: {deleted_reports} reports removed.")
    except Exception as e:
        logger.error(f"Error in 3-month purge job: {e}")


# ==========================================
# 🚨 Global Error Handler (Security Improved)
# ==========================================
_last_error_notifications = {}
_ERROR_DEBOUNCE_SECONDS = 300

def _get_error_signature(error: Exception) -> str:
    error_type = type(error).__name__
    return error_type

async def global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    if context.error:
        logger.error(f"Global error: {type(context.error).__name__}", exc_info=True)
    else:
        logger.error("Global error: unknown error")

    error = context.error
    if error is not None:
        signature = _get_error_signature(error)
        now = _time.time()
        last_time = _last_error_notifications.get(signature, 0)
        if now - last_time < _ERROR_DEBOUNCE_SECONDS:
            return
        _last_error_notifications[signature] = now
        if len(_last_error_notifications) > 100:
            cutoff = now - _ERROR_DEBOUNCE_SECONDS
            for key in list(_last_error_notifications.keys()):
                if _last_error_notifications[key] < cutoff:
                    del _last_error_notifications[key]

    admin_id = context.bot_data.get('admin_id', ADMIN_ID)
    short_msg = "An unexpected error occurred."
    if context.error:
        short_msg = f"⚠️ Error: {type(context.error).__name__}"
    try:
        await context.bot.send_message(
            chat_id=admin_id,
            text=short_msg,
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Failed to send error notification: {e}")


# ==========================================
# 🌐 Quart Async Web Server
# ==========================================
quart_app = Quart(__name__)

@quart_app.route('/')
async def health_check():
    return jsonify({"status": "Bot is running"}), 200

@quart_app.route('/health')
async def health():
    return jsonify({"status": "healthy"}), 200

@quart_app.route('/api/license/check/<int:user_id>', methods=['GET'])
async def api_license_check(user_id: int):
    auth_header = request.headers.get('Authorization', '')
    expected_secret = os.getenv("API_SECRET_TOKEN", "")
    
    if not expected_secret:
        logger.error("API_SECRET_TOKEN is not configured!")
        return jsonify({"error": "Server configuration error"}), 500

    if not auth_header.startswith("Bearer "):
        logger.warning(f"Unauthorized API access attempt for user {user_id}: missing Bearer token")
        return jsonify({"error": "Unauthorized"}), 401

    token = auth_header.split(" ", 1)[1]
    if token != expected_secret:
        logger.warning(f"Invalid API token for user {user_id}")
        return jsonify({"error": "Unauthorized"}), 401

    db = quart_app.config.get('db')
    if db is None:
        return jsonify({"error": "Database unavailable"}), 503
    try:
        valid, expiry = await db.check_license_local(user_id)
    except Exception as e:
        logger.error(f"Error checking license for {user_id}: {e}")
        return jsonify({"error": "Internal server error"}), 500
    response_data = {
        "valid": valid,
        "expiry": expiry.isoformat() if expiry else None
    }
    return jsonify(response_data), 200

# ✅ New endpoint: client notifies master on startup
@quart_app.route('/api/notify_startup/<int:client_admin_id>', methods=['POST'])
async def api_notify_startup(client_admin_id: int):
    auth_header = request.headers.get('Authorization', '')
    expected_secret = os.getenv("API_SECRET_TOKEN", "")

    if not expected_secret:
        return jsonify({"error": "Server configuration error"}), 500
    if not auth_header.startswith("Bearer "):
        return jsonify({"error": "Unauthorized"}), 401
    token = auth_header.split(" ", 1)[1]
    if token != expected_secret:
        return jsonify({"error": "Unauthorized"}), 401

    bot = quart_app.config.get('bot')
    if bot is None:
        return jsonify({"error": "Bot not available"}), 503

    admin_id = quart_app.config.get('ADMIN_ID')
    if not admin_id:
        return jsonify({"error": "Admin ID not configured"}), 500

    try:
        await bot.send_message(
            chat_id=admin_id,
            text=f"🔔 <b>Client Bot Started</b>\n\n"
                 f"👤 Client Admin ID: <code>{client_admin_id}</code>\n\n"
                 f"Client bot is now running and connected to your server.",
            parse_mode="HTML"
        )
        return jsonify({"status": "notified"}), 200
    except Exception as e:
        logger.error(f"Failed to send startup notification to master: {e}")
        return jsonify({"error": "Notification failed"}), 500

async def run_quart():
    port = int(os.environ.get("PORT", 8080))
    await quart_app.run_task(host='0.0.0.0', port=port)


# ==========================================
# 🆕 Master Signal Handler
# ==========================================
async def master_signal_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles /license command from master to directly add license."""
    user_id = update.effective_user.id
    if master.MASTER_ID is None or str(user_id) != str(master.MASTER_ID):
        return
    db = context.bot_data.get('db')
    if db is None:
        await update.message.reply_text("⏳ Database not ready.")
        return
    args = context.args
    if len(args) == 3 and args[0].lower() == "add":
        try:
            target_id = int(args[1])
            months = int(args[2])
            await db.add_or_update_license(target_id, months)
            await update.message.reply_text(f"✅ License updated: User {target_id} +{months} months.")
        except ValueError:
            await update.message.reply_text("❌ Invalid user ID or months. Must be numbers.")
    else:
        await update.message.reply_text("📋 Usage: /license add <user_id> <months>")
    try:
        if update.message.chat.type in ['group', 'supergroup']:
            await update.message.delete()
    except Exception as e:
        logger.debug(f"Could not delete master signal message: {e}")

# ==========================================
# 📞 Client startup notification helper
# ==========================================
async def notify_master_startup(master_api_url: str, secret_token: str, admin_id: int):
    """Call master server to notify that this client bot has started."""
    if not master_api_url:
        logger.warning("MASTER_API_URL not set, cannot notify master about startup.")
        return
    url = f"{master_api_url.rstrip('/')}/api/notify_startup/{admin_id}"
    headers = {"Authorization": f"Bearer {secret_token}"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    logger.info("Master notified of client startup successfully.")
                else:
                    logger.warning(f"Master notification returned status {resp.status}")
    except Exception as e:
        logger.error(f"Failed to notify master about startup: {e}")

# ==========================================
# 🚀 Main Entry Point
# ==========================================
async def main_async():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    await post_init(app)

    # Admin & Special Commands
    app.add_handler(CommandHandler("paid", paid_command))
    app.add_handler(CommandHandler("license", master_signal_handler))

    ADMIN_COMMANDS = [
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
    ]
    for cmd, handler_func in ADMIN_COMMANDS:
        app.add_handler(CommandHandler(cmd, handler_func))

    # User-facing handlers with License Wrap
    conv_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(wrap_with_license(step1_selection), pattern=r"^price_(dia|uc)_.+$"),
            CallbackQueryHandler(wrap_with_license(show_items), pattern=r"^(show_dia|show_uc)$"),
            CallbackQueryHandler(wrap_with_license(send_welcome), pattern=r"^back_to_main$"),
            CommandHandler("start", wrap_with_license(send_welcome))
        ],
        states={
            WAIT_GAME_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, wrap_with_license(step2_id_entry))],
            WAIT_CONFIRMATION: [CallbackQueryHandler(wrap_with_license(step3_validation), pattern="^(confirm_id|back_id)$")],
            WAIT_PAYMENT: [MessageHandler(filters.PHOTO, wrap_with_license(step4_payment))]
        },
        fallbacks=[
            CommandHandler("start", wrap_with_license(send_welcome)),
        ]
    )
    app.add_handler(conv_handler)

    # Order management callbacks (Admin only, own permission check inside)
    app.add_handler(CallbackQueryHandler(admin_callback_handler, pattern=r"^admin_"))
    app.add_handler(CallbackQueryHandler(user_cancel_handler, pattern=r"^cancel_user_"))
    app.add_handler(CallbackQueryHandler(license_callback_handler, pattern=r"^license_"))
    app.add_handler(CallbackQueryHandler(wrap_with_license(new_order_callback_handler), pattern=r"^new_order$"))

    app.add_error_handler(global_error_handler)

    jq = app.job_queue
    if jq:
        # Existing jobs
        jq.run_repeating(check_timeouts, interval=60, first=10)
        jq.run_daily(monthly_report_job, time=time(hour=0, minute=0, tzinfo=pytz.UTC))

        # Cleanup jobs
        jq.run_daily(clean_expired_licenses, time=time(hour=3, minute=0, tzinfo=pytz.UTC))
        # 3-month purge replaces old 7-day clean_old_orders
        jq.run_daily(purge_old_data_job, time=time(hour=4, minute=0, tzinfo=pytz.UTC))

    quart_task = asyncio.create_task(run_quart())
    logger.info("Quart server starting on port 8080...")
    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)

    try:
        await asyncio.Event().wait()
    finally:
        logger.info("Shutting down gracefully...")
        db = app.bot_data.get('db')
        if db:
            await db.close()
        quart_task.cancel()
        await app.stop()
        await app.shutdown()
        logger.info("Bot shutdown complete.")


def main():
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")


if __name__ == '__main__':
    main()
