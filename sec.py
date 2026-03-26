# sec.py
import os
from datetime import datetime
import google.generativeai as genai
from telegram import Update
from telegram.ext import ContextTypes
from telegram.constants import ChatAction  # ✅ ADD THIS

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

# ---------------- GEMINI RESPONSE ----------------
async def get_gemini_response(prompt: str, history: list, personality: str):
    update_keys()

    if not GEMINI_KEYS:
        return "❌ No API Keys found in Environment Variables."

    # Clean history
    cleaned_history = []
    for h in history:
        try:
            parts = h.get("parts", "")
            if isinstance(parts, list):
                parts = parts[0] if parts else ""
            cleaned_history.append({
                "role": h.get("role", "user"),
                "parts": [str(parts)]
            })
        except:
            continue

    # Loop keys
    for key in GEMINI_KEYS:
        try:
            genai.configure(api_key=key)

            system_text = (
                f"You must follow the user's latest identity request: {personality}"
                if personality else
                "You are a helpful AI assistant."
            )

            model = genai.GenerativeModel(
                "model/gemini-1.5-flash",
                system_instruction=system_text
            )

            chat = model.start_chat(history=cleaned_history)
            response = chat.send_message(prompt)

            return response.text.strip()

        except Exception as e:
            err_msg = str(e).lower()

            if any(x in err_msg for x in [
                "429", "quota", "limit", "rate",
                "key_invalid", "api_key_invalid"
            ]):
                print(f"⚠️ Key failed → next ({err_msg[:60]})")
                continue

            return f"❌ AI Error: {e}"

    return "❌ All API keys failed (Rate-limited or Invalid)."

# ---------------- PERSONALITY DETECTOR ----------------
def detect_personality_update(message: str):
    keywords = ["ပြောင်းလိုက်", "မှတ်ထား", "ခေါ်မယ်", "ဖြစ်အောင်လုပ်", "နေပါ"]

    if any(k in message for k in keywords):
        return message.strip()

    return None

# ---------------- AI HANDLER ----------------
async def handle_ai(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message.text
    if not user or not message:
        return

    # ✅ SHOW "typing..." STATUS
    try:
        await context.bot.send_chat_action(
            chat_id=update.effective_message.chat_id,
            action=ChatAction.TYPING
        )
    except:
        pass

    user_id = user.id

    # ---------------- LOAD USER DATA ----------------
    user_data = users_col.find_one({"user_id": user_id}) or {}
    history = user_data.get("chat_history", [])
    personality = user_data.get("personality", "")

    # ---------------- PERSONALITY UPDATE ----------------
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
            print(f"❌ Personality Save Error: {e}")

    # ---------------- GET AI RESPONSE ----------------
    reply = await get_gemini_response(message, history, personality)

    # ---------------- SEND MESSAGE ----------------
    sent = False
    try:
        await update.message.reply_text(reply)
        sent = True
    except Exception as e:
        print(f"❌ Telegram Send Error: {e}")

    # ---------------- SAVE HISTORY ----------------
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
            print(f"❌ History Save Error: {e}")

# ---------------- HARD RESET ----------------
async def delete_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return

    try:
        users_col.delete_one({"user_id": user.id})
        await update.message.reply_text("🗑️ Your data has been fully reset.")
    except Exception as e:
        print(f"❌ Delete Error: {e}")
        await update.message.reply_text("❌ Failed to reset data.")
