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
key_cycle = None

# ---------------- KEY LOADER ----------------

def update_keys():
    global GEMINI_KEYS, key_cycle

    temp_keys = []
    for i in range(1, 11):
        k = os.environ.get(f"KEY{i}")
        if k and k.strip():
            temp_keys.append(k.strip())

    GEMINI_KEYS = temp_keys

    if GEMINI_KEYS:
        key_cycle = itertools.cycle(GEMINI_KEYS)
    else:
        key_cycle = None

    print(f"🔑 Loaded {len(GEMINI_KEYS)} Gemini Keys.")

# ---------------- ROLE SANITIZER ----------------

def fix_role(role: str):
    return "model" if role == "model" else "user"

# ---------------- GEMINI RESPONSE ----------------

async def get_gemini_response(prompt: str, history: list, personality: str):
    global key_cycle

    # cold start
    if key_cycle is None:
        update_keys()

    if not GEMINI_KEYS or key_cycle is None:
        return "❌ No API Keys found or key_cycle not initialized."

    # clean history
    cleaned_history = []
    for h in history:
        try:
            role = h.get("role")
            parts = h.get("parts")

            if (
                role
                and parts
                and isinstance(parts, list)
                and len(parts) > 0
                and isinstance(parts[0], dict)
                and "text" in parts[0]
            ):
                cleaned_history.append({
                    "role": fix_role(role),
                    "parts": parts
                })
        except Exception as e:
            print(f"⚠️ History cleaning error: {e}")
            continue

    # key rotation
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

            if "429" in err_msg or "quota" in err_msg.lower():
                continue

            if "404" in err_msg or "invalid model" in err_msg.lower():
                continue

            return f"❌ AI Error: {err_msg}"

    return "❌ All API keys quota exceeded or invalid."

# ---------------- PERSONALITY ----------------

def detect_personality_update(message: str):
    msg = message.strip()

    triggers = [
        "မှတ်ထား",
        "ပြောင်းလိုက်",
        "set personality"
    ]

    for t in triggers:
        if t in msg.lower():
            start_index = msg.lower().find(t) + len(t)
            personality_text = msg[start_index:].strip()

            if personality_text:
                return personality_text
            else:
                return t

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
            chat_id=update.effective_chat.id,
            action=ChatAction.TYPING
        )
    except Exception as e:
        print(f"❌ Send chat action error: {e}")

    user_id = user.id

    # load user data
    user_data = users_col.find_one({"user_id": user_id}) or {}
    history = user_data.get("chat_history", [])
    personality = user_data.get("personality", "")

    # ---------------- PERSONALITY UPDATE ----------------

    new_personality = detect_personality_update(message)

    if new_personality:
        triggers = ["မှတ်ထား", "ပြောင်းလိုက်", "set personality"]

        # prevent empty personality
        if new_personality.lower().strip() in triggers:
            await update.message.reply_text("❗ Personality text ထည့်ပါ။")
            return

        personality = new_personality

        try:
            users_col.update_one(
                {"user_id": user_id},
                {"$set": {"personality": personality}},
                upsert=True
            )

            await update.message.reply_text(
                f"✅ စရိုက်ကို ပြောင်းလဲလိုက်ပါပြီ:\n{personality}"
            )

        except Exception as e:
            print(f"❌ Personality Error: {e}")

        return

    # ---------------- AI RESPONSE ----------------

    reply = await get_gemini_response(message, history, personality)

    if not reply:
        return

    try:
        await update.message.reply_text(reply)
    except Exception as e:
        print(f"❌ Reply error: {e}")

    # ---------------- SAVE HISTORY ----------------

    try:
        history.append({"role": "user", "parts": [{"text": message}]})
        history.append({"role": "model", "parts": [{"text": reply}]})

        # keep last 20 messages
        history = history[-20:]

        users_col.update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "chat_history": history,
                    "updated_at": datetime.utcnow()
                }
            },
            upsert=True
        )

    except Exception as e:
        print(f"❌ History Save Error: {e}")
