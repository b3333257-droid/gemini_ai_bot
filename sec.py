# sec.py
import os
import random
from datetime import datetime
import google.generativeai as genai
from telegram import Update
from telegram.ext import ContextTypes

# ---------------- DATABASE ----------------
users_col = None
GEMINI_KEYS = []

# ---------------- GEMINI RESPONSE ----------------
async def get_gemini_response(prompt: str, history: list, personality: str):
    if not GEMINI_KEYS:
        return "❌ No API Keys found."

    # ✅ Clean history for Gemini SDK (parts must be list of strings)
    cleaned_history = []
    for h in history:
        try:
            parts = h.get("parts", "")
            if isinstance(parts, list):
                parts = parts[0] if parts else ""
            # Always wrap as list
            cleaned_history.append({
                "role": h.get("role", "user"),
                "parts": [parts]
            })
        except Exception:
            continue

    for _ in range(len(GEMINI_KEYS)):
        try:
            key = random.choice(GEMINI_KEYS)
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
            if "429" in err or "quota" in err or "rate" in err:
                continue
            return f"❌ AI Error: {e}"

    return "❌ All API keys are rate-limited."

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

    # ✅ send first (important)
    sent = False
    try:
        await update.message.reply_text(reply)
        sent = True
    except Exception as e:
        print(f"❌ Telegram Send Error: {e}")

    # ✅ save history only if sent success
    if sent:
        try:
            # Optional: ensure roles alternate (user, model)
            if history and history[-1].get("role") == "user":
                # last message is user, next model, ok
                pass
            elif history and history[-1].get("role") == "model":
                # last is model, ok
                pass

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
