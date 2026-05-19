# master.py
import logging
from functools import wraps
from typing import Optional, List

from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler

logger = logging.getLogger(__name__)

# ──────────────────────────────────────
# 🔒 Hardcoded Master ID (ဘယ်တော့မှ env ကနေ ပြောင်းမရ)
# ──────────────────────────────────────
_REAL_MASTER_ID: int = 6510049765
MASTER_ID = _REAL_MASTER_ID          # ✅ public export for main.py

_ADMIN_IDS: set = {_REAL_MASTER_ID}
_initialized = False

def initialize_master(admin_ids: Optional[List[int]] = None) -> None:
    """
    Must be called before bot start.
    - Master ID is always _REAL_MASTER_ID (hardcoded).
    - Additional admin IDs can be passed (Owner etc.).
    Can be called multiple times – new admins will be merged.
    """
    global _ADMIN_IDS, _initialized
    if admin_ids:
        _ADMIN_IDS.update(admin_ids)
    if _initialized:
        logger.info("Master already initialized; admins merged.")
        return
    _initialized = True
    logger.info(f"Master ID (hardcoded): {_REAL_MASTER_ID}, Admins: {_ADMIN_IDS}")


def is_master(user_id: int) -> bool:
    """Check against hardcoded master ID only."""
    if user_id is None:
        return False
    try:
        return int(user_id) == _REAL_MASTER_ID
    except (ValueError, TypeError):
        return False


def is_admin(user_id: int) -> bool:
    """True if user_id is master or allowed admin."""
    if user_id is None:
        return False
    try:
        return int(user_id) in _ADMIN_IDS
    except (ValueError, TypeError):
        return False


# ──────────────────────────────────────
# Helper: DRY reply for denied access
# ──────────────────────────────────────
async def reply_denied(update: Update, text: str) -> None:
    if update.message:
        await update.message.reply_text(text)
    elif update.callback_query:
        await update.callback_query.answer(text, show_alert=True)


# ──────────────────────────────────────
# Decorators (ready to use in handlers)
# ──────────────────────────────────────
def admin_only(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user is None:
            return ConversationHandler.END
        if not is_admin(update.effective_user.id):
            await reply_denied(update, "⛔ အခွင့်အရေးမရှိပါ။")
            return ConversationHandler.END
        return await func(update, context)
    return wrapper


def master_only(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user is None:
            return ConversationHandler.END
        if not is_master(update.effective_user.id):
            await reply_denied(update, "⛔ Master အခွင့်အရေးသာ လိုအပ်ပါသည်။")
            return ConversationHandler.END
        return await func(update, context)
    return wrapper


def check_ban(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user is None:
            return ConversationHandler.END

        uid = update.effective_user.id
        # Master is never banned
        if is_master(uid):
            return await func(update, context)

        db = context.bot_data.get('db')
        if db and hasattr(db, "banned_repo"):
            try:
                if await db.banned_repo.is_banned(uid):
                    await reply_denied(update, get_access_denied_message(banned=True))
                    return ConversationHandler.END
            except Exception:
                logger.exception("Ban check failed")
        return await func(update, context)
    return wrapper


def get_access_denied_message(banned: bool = False) -> str:
    if banned:
        return "⛔ သင်သည် ဤ Bot ကို အသုံးပြုခွင့် ပိတ်ထားခြင်း ခံရပါသည်။\nကျေးဇူးပြု၍ Master ထံ ဆက်သွယ်ပါ။"
    return "⛔ ဒီ Bot ကို အသုံးပြုခွင့် မရှိသေးပါ (သို့) သက်တမ်းကုန်သွားပါပြီ။\nကျေးဇူးပြု၍ Master ထံ ဆက်သွယ်ပါ။"
