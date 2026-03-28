# sec.py
import os
from datetime import datetime
import google.generativeai as genai
from telegram import Update
from telegram.ext import ContextTypes
from telegram.constants import ChatAction

# ---------------- DATABASE ----------------
users_col = None
GEMINI_KEYS = []

# ---------------- KEY LOADER ----------------
def update_keys():
    global GEMINI_KEYS
    temp_keys = []
    for i in range(1, 11):
        k = os.environ.get(f"KEY{i}")
        if k and k.strip():
            temp_keys.append(k.strip())
    GEMINI_KEYS = temp_keys
    print(f"🔑 Loaded {len(GEMINI_KEYS)} Gemini Keys.")

# ---------------- ROLE SANITIZER ----------------
def fix_role(role: str):
    return "model" if role == "model" else "user"

# ---------------- GEMINI RESPONSE ----------------
async def get_gemini_response(prompt: str, history: list, personality: str):
    if not GEMINI_KEYS:
        return "❌ No API Keys found."

    cleaned_history = []
    for h in history:
        try:
            raw_parts = h.get("parts", "")
            if isinstance(raw_parts, list):
                raw_parts = raw_parts[0] if raw_parts else ""

            cleaned_history.append({
                "role": fix_role(h.get("role")),
                "parts": [{"text": str(raw_parts)}]
            })
        except:
            continue

    for key in GEMINI_KEYS:
        try:
            genai.configure(api_key=key)

            system_text = (
                f"You must follow the user's latest identity request: {personality}"
                if personality else
                "You are a helpful AI assistant."
            )

            model = genai.GenerativeModel(
                "gemini-1.5-flash",
                system_instruction=system_text
            )

            chat = model.start_chat(history=cleaned_history)
            response = chat.send_message(prompt)

            return response.text.strip()

        except Exception as e:
            err = str(e).lower()

            # ✅ Only skip rate limit / quota
            if any(x in err for x in ["429", "quota", "rate", "limit"]):
                print("⚠️ Rate limit → next key")
                continue

            # ✅ Show real error (debug)
            return f"❌ AI Error Detail: {e}"

    return "❌ All API keys failed."

# ---------------- SMART PERSONALITY DETECTOR ----------------
def detect_personality_update(message: str):
    msg = message.strip()

    if len(msg) > 80:
        return None

    triggers = [
        "မင်းနာမည်",
        "နာမည်ကို",
        "မင်းကို",
        "ခေါ်မယ်",
        "ဖြစ်အောင်",
        "act as",
        "you are",
        "your name"
    ]

    if any(t in msg.lower() for t in triggers):
        return msg

    return None

# ---------------- AI HANDLER ----------------
async def handle_ai(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message.text

    if not user or not message:
        return

    # typing status
    try:
        await context.bot.send_chat_action(
            chat_id=update.effective_message.chat_id,
            action=ChatAction.TYPING
        )
    except:
        pass

    user_id = user.id

    # load data
    user_data = users_col.find_one({"user_id": user_id}) or {}
    history = user_data.get("chat_history", [])
    personality = user_data.get("personality", "")

    # personality update
    new_personality = detect_personality_update(message)
    if new_personality:
        personality = new_personality
        try:
            users_col.update_one(
                {"user_id": user_id},
                {"$set": {"personality": personality}},
                upsert=True
            )
        except Exception as e:
            print(f"❌ Personality Error: {e}")

    # AI response
    reply = await get_gemini_response(message, history, personality)

    # send
    sent = False
    try:
        await update.message.reply_text(reply)
        sent = True
    except Exception as e:
        print(f"❌ Send Error: {e}")

    # save history
    if sent:
        try:
            users_col.update_one(
                {"user_id": user_id},
                {
                    "$push": {
                        "chat_history": {
                            "$each": [
                                {"role": "user", "parts": [message]},
                                {"role": "model", "parts": [reply]}
                            ],
                            "$slice": -20
                        }
                    },
                    "$set": {
                        "last_seen": datetime.utcnow()
                    }
                },
                upsert=True
            )
        except Exception as e:
            print(f"❌ History Error: {e}")

# ---------------- HARD RESET ----------------
async def delete_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return

    try:
        users_col.delete_one({"user_id": user.id})
        await update.message.reply_text("🗑️ Reset complete.")
    except Exception as e:
        print(f"❌ Delete Error: {e}")
        await update.message.reply_text("❌ Reset failed.")
