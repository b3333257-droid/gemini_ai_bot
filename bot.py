# bot.py
import asyncio
import os
import threading
import time
from datetime import datetime, timedelta
from flask import Flask
from pymongo import MongoClient
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ContextTypes, filters
)
import sec
import thd
import random

# ---------------- CONFIG ----------------
TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID = int(os.environ.get("OWNER_ID", 123456789))
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "gemini_ai_bot_db")

# ✅ KEY ROTATION
GEMINI_KEYS = [
    v for k, v in os.environ.items()
    if k.startswith("KEY") and v.strip()
]

# ---------------- DATABASE ----------------
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
users_col = db["users"]

# ---------------- SEC.PY ----------------
sec.users_col = users_col
sec.GEMINI_KEYS = GEMINI_KEYS

# ---------------- SYNC ----------------
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

# ---------------- BASIC ----------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await sync_user(update)
    await update.message.reply_text("🤖 AI Bot Ready (DM Only)")

# ---------------- AI HANDLER ----------------
async def ai_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    # ✅ DM Only
    if update.effective_chat.type != "private":
        return

    await sync_user(update)

    try:
        await sec.handle_ai(update, context)
    except Exception as e:
        print(f"❌ AI Error: {e}")
        await update.message.reply_text("❌ AI Error occurred.")

# ---------------- HANDLERS ----------------
def register_handlers(app):
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", thd.help_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, ai_handler))

# ---------------- FLASK & SELF-PING ----------------
app_flask = Flask(__name__)

@app_flask.route("/")
def home():
    return "Bot is running."

def run_flask():
    try:
        app_flask.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
    except Exception as e:
        print(f"❌ Flask Server Error: {e}")

def self_ping():
    while True:
        try:
            import requests
            url = os.environ.get("SELF_URL")
            if url:
                requests.get(url, timeout=10)
            else:
                print("⚠️ SELF_URL not set!")
        except Exception as e:
            print(f"❌ Self-Ping Error: {e}")
        time.sleep(300)

# ---------------- PRUNE ----------------
def prune_inactive_users():
    cutoff = datetime.utcnow() - timedelta(days=90)
    users_col.delete_many({"last_seen": {"$lt": cutoff}})

def auto_prune_scheduler():
    while True:
        try:
            prune_inactive_users()
            print("🧹 Users cleaned.")
        except Exception as e:
            print(f"❌ Prune Error: {e}")
        time.sleep(86400)

# ---------------- INDEX ----------------
def setup_indexes():
    try:
        users_col.create_index("user_id", unique=True)
        print("✅ MongoDB Index ready.")
    except Exception as e:
        print(f"❌ Index Error: {e}")

# ---------------- MAIN ----------------
def main():
    setup_indexes()
    thd.init_db(MONGO_URI, DB_NAME)

    application = ApplicationBuilder().token(TOKEN).concurrent_updates(True).build()
    register_handlers(application)

    threading.Thread(target=run_flask, daemon=True).start()
    threading.Thread(target=self_ping, daemon=True).start()
    threading.Thread(target=auto_prune_scheduler, daemon=True).start()

    print("🚀 Bot starting...")
    application.run_polling(allowed_updates=["message"])

if __name__ == "__main__":
    main()
