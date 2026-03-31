# bot.py
import asyncio
import os
import threading
import time
from datetime import datetime
from flask import Flask
from pymongo import MongoClient
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ContextTypes, filters
)
import sec

# ---------------- CONFIG ----------------
TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID = int(os.environ.get("OWNER_ID", 123456789))
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "gemini_ai_bot_db")

# ---------------- DATABASE ----------------
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
users_col = db["users"]

# Link DB to sec.py
sec.users_col = users_col

# ---------------- USER SYNC ----------------
async def sync_user(update: Update):
    user = update.effective_user
    if not user:
        return
    try:
        users_col.update_one(
            {"user_id": user.id},
            {"$set": {
                "last_seen": datetime.utcnow(),
                "username": user.username,
                "first_name": user.first_name
            }},
            upsert=True
        )
    except Exception as e:
        print(f"❌ User Sync Error: {e}")

# ---------------- COMMANDS ----------------
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await sync_user(update)
    try:
        await update.message.reply_text("🤖 AI Bot Ready! စာရိုက်ပြီး စကားပြောနိုင်ပါပြီ။")
    except Exception as e:
        print(f"❌ Start Command Error: {e}")

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_data = users_col.find_one({"user_id": user_id}) or {}
    personality = user_data.get("personality", "Gemini AI Bot")

    display_name = personality if len(personality) < 40 else "Custom AI Bot"

    help_text = (
        f"🤖 *{display_name} Help*\n\n"
        "• /start - Bot စတင်ရန်\n"
        "• /help - အသုံးပြုပုံကြည့်ရန်\n"
        "• /delete_all - Chat history နှင့် စရိုက်အားလုံးဖျက်ရန်\n"
        "• စာရိုက်ပို့ရုံဖြင့် AI နှင့် စကားပြောနိုင်ပါသည်။"
    )

    try:
        await update.message.reply_text(help_text, parse_mode="Markdown")
    except Exception as e:
        print(f"❌ Help Command Error: {e}")

# ---------------- AI HANDLER ----------------
async def ai_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    if update.effective_chat.type != "private":
        return

    await sync_user(update)

    try:
        await sec.handle_ai(update, context)
    except Exception as e:
        print(f"❌ AI Error: {e}")
        try:
            await update.message.reply_text("❌ AI Error occurred.")
        except Exception as reply_e:
            print(f"❌ Failed to send AI Error message: {reply_e}")

# ---------------- WEB SERVER ----------------
app_flask = Flask(__name__)

@app_flask.route("/")
def home():
    return "Bot is running."

def run_flask():
    try:
        app_flask.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
    except Exception as e:
        print(f"❌ Flask Error: {e}")

def self_ping():
    import requests
    while True:
        url = os.environ.get("SELF_URL")
        if url:
            try:
                requests.get(url, timeout=10)
            except Exception as e:
                print(f"❌ Ping Error: {e}")
        time.sleep(300)

# ---------------- MAIN ----------------
def main():
    # ✅ Load Gemini Keys
    sec.update_keys()

    # ✅ DB index
    try:
        users_col.create_index("user_id", unique=True)
    except Exception as e:
        print(f"❌ Index Error: {e}")

    application = ApplicationBuilder().token(TOKEN).concurrent_updates(True).build()

    # Handlers
    application.add_handler(CommandHandler("start", start_cmd))
    application.add_handler(CommandHandler("help", help_cmd))
    application.add_handler(CommandHandler("delete_all", sec.delete_all))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, ai_handler))

    # Background tasks
    threading.Thread(target=run_flask, daemon=True).start()
    threading.Thread(target=self_ping, daemon=True).start()

    print("🚀 Bot starting...")

    # ✅ Render Event Loop Fix (Removed explicit loop handling as run_polling manages it)
    application.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
