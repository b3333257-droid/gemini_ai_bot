# master.py (Clean – hardcoded MASTER_ID from main.py, no env read)
import logging
from datetime import datetime
from telegram import Bot
from telegram.error import TelegramError, NetworkError, TimedOut

logger = logging.getLogger(__name__)

# ==========================================
# 🔐 Master Configuration
# ==========================================
# MASTER_ID will be set by main.py after module import.
# Do NOT read environment here; client bot hardcodes it.
MASTER_ID = None

# ✅ Banned users set (populated from DB at startup)
BANNED_USERS = set()  # stores integer user IDs

# ==========================================
# 🛡 Master & Banned Check Functions
# ==========================================
def is_master(user_id: int) -> bool:
    """
    Check if given user_id is the master.
    Both IDs are converted to string with strip for safe comparison.
    """
    if MASTER_ID is None:
        return False
    # ✅ Safer: directly convert to string without int() to avoid None/TypeError
    try:
        return str(user_id).strip() == str(MASTER_ID).strip()
    except (ValueError, TypeError):
        return False

def is_banned(user_id: int) -> bool:
    """Check if user is banned (in-memory set, stores integers)."""
    try:
        return int(user_id) in BANNED_USERS
    except (ValueError, TypeError):
        return False

# ==========================================
# 🔧 Banned User Memory Helpers
# ==========================================
def add_banned_user(user_id: int) -> None:
    try:
        BANNED_USERS.add(int(user_id))
        logger.info(f"User {user_id} added to banned list in memory.")
    except (ValueError, TypeError) as e:
        logger.error(f"Failed to add banned user {user_id}: {e}")

def remove_banned_user(user_id: int) -> None:
    try:
        BANNED_USERS.discard(int(user_id))
        logger.info(f"User {user_id} removed from banned list in memory.")
    except (ValueError, TypeError) as e:
        logger.error(f"Failed to remove banned user {user_id}: {e}")

# ==========================================
# 📥 Load Banned Users from Database
# ==========================================
async def load_banned_users_from_db(db) -> None:
    """
    Bot startup တွင် DB ထဲမှ banned user များကို memory သို့ဆွဲတင်သည်။
    """
    global BANNED_USERS
    if db is None:
        logger.warning("Database instance is None, cannot load banned users.")
        return

    try:
        cursor = db.banned_users.find({})
        banned_docs = await cursor.to_list(length=10000)
        BANNED_USERS = {int(doc["user_id"]) for doc in banned_docs}
        logger.info(f"✅ Loaded {len(BANNED_USERS)} banned users from database into memory.")
    except Exception as e:
        logger.error(f"❌ Failed to load banned users from DB: {e}")
        BANNED_USERS = set()

# ==========================================
# 📢 Startup Notification (Optional, not used in main flow)
# ==========================================
async def notify_master_on_startup(bot: Bot) -> bool:
    """
    (Optional) Send startup notification to master.
    Not actively called in current main.py – kept for external use.
    """
    if MASTER_ID is None:
        logger.error("MASTER_ID is not set. Cannot send startup notification.")
        return False
    try:
        bot_info = await bot.get_me()
        bot_username = bot_info.username
        bot_link = f"https://t.me/{bot_username}" if bot_username else "N/A"
        message = (
            f"✅ <b>Bot Started Successfully</b>\n\n"
            f"🤖 <b>Bot:</b> @{bot_username}\n"
            f"🔗 <b>Link:</b> <a href='{bot_link}'>{bot_link}</a>\n"
            f"🕒 <b>Time:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"Bot is now running and accepting commands."
        )
        await bot.send_message(chat_id=MASTER_ID, text=message, parse_mode="HTML")
        logger.info(f"Startup notification sent to master {MASTER_ID}")
        return True
    except (NetworkError, TimedOut, TelegramError) as e:
        logger.error(f"Failed to send startup notification: {e}")
        return False

# ==========================================
# 🚫 Access Denied Messages
# ==========================================
def get_access_denied_message(user_id: int) -> str:
    if is_banned(user_id):
        return "⛔ သင်သည် ဤ Bot ကို အသုံးပြုခွင့် ပိတ်ထားခြင်း ခံရပါသည်။\nကျေးဇူးပြု၍ Master ထံ ဆက်သွယ်ပါ။"
    else:
        return "⛔ ဒီ Bot ကို အသုံးပြုခွင့် မရှိသေးပါ (သို့) သက်တမ်းကုန်သွားပါပြီ။\nကျေးဇူးပြု၍ Master ထံ ဆက်သွယ်ပါ။"
