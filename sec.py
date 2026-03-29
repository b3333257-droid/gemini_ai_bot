# sec.py
import os
import itertools
import asyncio
from datetime import datetime
import google.generativeai as genai
from telegram import Update
from telegram.ext import ContextTypes
from telegram.constants import ChatAction

# ---------------- DATABASE ----------------
users_col = None
GEMINI_KEYS = []
key_cycle = None  # ✅ global key rotation iterator

# ---------------- KEY LOADER ----------------
def update_keys():
    global GEMINI_KEYS, key_cycle

    temp_keys = []
    for i in range(1, 11):
        k = os.environ.get(f"KEY{i}")
        if k and k.strip():
            temp_keys.append(k.strip())

    GEMINI_KEYS = temp_keys
    key_cycle = itertools.cycle(GEMINI_KEYS)
    print(f"🔑 Loaded {len(GEMINI_KEYS)} Gemini Keys.")

# ---------------- ROLE SANITIZER ----------------
def fix_role(role: str):
    return "model" if role == "model" else "user"

# ---------------- GEMINI RESPONSE ----------------
async def get_gemini_response(prompt: str, history: list, personality: str):
    global key_cycle

    # ✅ Cold start / key_cycle check
    if key_cycle is None:
        update_keys()

    if not GEMINI_KEYS:
        return "❌ No API Keys found."

    # Clean history
    cleaned_history = []
    for h in history:
        try:
            raw_parts = h.get("parts", [])
            text_content = ""

            if isinstance(raw_parts, list) and len(raw_parts) > 0:
                if isinstance(raw_parts[0], dict):
                    text_content = raw_parts[0].get("text", "")
                else:
                    text_content = str(raw_parts[0])
            else:
                text_content = str(raw_parts)

            if text_content:
                cleaned_history.append({
                    "role": fix_role(h.get("role")),
                    "parts": [{"text": text_content}]
                })

        except Exception:
            continue

    # 🔥 KEY ROTATION LOOP
    for _ in range(len(GEMINI_KEYS)):
        key = next(key_cycle)
        try:
            print(f"🚀 Using key: {key[:8]}...")
            genai.configure(api_key=key, transport="rest")

            model = genai.GenerativeModel(
                model_name="models/gemini-1.5-flash-latest",
                system_instruction=personality if personality else "You are a helpful assistant."
            )

            chat = model.start_chat(history=cleaned_history)
            response = await asyncio.to_thread(chat.send_message, prompt)

            return response.text.strip()

        except Exception as e:
            err_msg = str(e)
            print(f"⚠️ Key Error ({key[:8]}...): {err_msg}")

            # Quota error → next key
            if "429" in err_msg or "quota" in err_msg.lower():
                continue

            # Model error → skip
            if "404" in err_msg:
                continue

            return f"❌ AI Error: {err_msg}"

    return "❌ All API keys quota exceeded."

# ---------------- PERSONALITY ----------------
def detect_personality_update(message: str):
    msg = message.strip()
    
    if len(msg) > 80:
        return None

    # ✅ tightened triggers (explicit only)
    triggers = [
        "မှတ်ထား",
        "ပြောင်းလိုက်",
        "set personality"
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

    # typing indicator
    try:
        await context.bot.send_chat_action(
            chat_id=update.effective_message.chat_id,
            action=ChatAction.TYPING
        )
    except:
        pass

    user_id = user.id

    # load user data
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

    # AI reply
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
                                {"role": "user", "parts": [{"text": message}]},
                                {"role": "model", "parts": [{"text": reply}]}
                            ],
                            "$slice": -20
                        }
                    },
                    "$set": {"last_seen": datetime.utcnow()}
                },
                upsert=True
            )
        except Exception as e:
            print(f"❌ History Error: {e}")

# ---------------- RESET ----------------
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
