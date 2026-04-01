
import os
import logging
import asyncio
from datetime import datetime, timedelta

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters, CallbackQueryHandler
from pymongo import MongoClient
from dotenv_values import dotenv_values
import requests

# Load environment variables
config = dotenv_values(".env")

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Environment variables
BOT_TOKEN = os.getenv("BOT_TOKEN", config.get("BOT_TOKEN"))
MONGO_URI = os.getenv("MONGO_URI", config.get("MONGO_URI"))
SELF_URL = os.getenv("SELF_URL", config.get("SELF_URL"))
PORT = int(os.getenv("PORT", config.get("PORT", "5000")))

# MongoDB setup
client = MongoClient(MONGO_URI)
db = client.blind_date_bot
users_collection = db.users
matches_collection = db.matches

# --- Bot Commands and Handlers ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a welcome message and prompts user to register."""
    user = update.effective_user
    if not users_collection.find_one({"user_id": user.id}):
        await update.message.reply_html(
            f"Hi {user.mention_html()}! Welcome to the Blind Date Bot. "
            "Let's find you a perfect match! Please register to get started."
        )
        await register_gender(update, context)
    else:
        await update.message.reply_text("You are already registered! Use /find_match to find a date.")

async def register_gender(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Asks the user for their gender."""
    keyboard = [
        [InlineKeyboardButton("Male", callback_data="gender_Male")],
        [InlineKeyboardButton("Female", callback_data="gender_Female")],
        [InlineKeyboardButton("Other", callback_data="gender_Other")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("What is your gender?", reply_markup=reply_markup)

async def button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles button presses for registration and matching."""
    query = update.callback_query
    await query.answer()

    data = query.data
    user_id = query.from_user.id

    if data.startswith("gender_"):
        gender = data.split("_")[1]
        context.user_data["gender"] = gender
        await query.edit_message_text(text=f"Your gender: {gender}")
        await register_preference(query, context)
    elif data.startswith("preference_"):
        preference = data.split("_")[1]
        context.user_data["preference"] = preference
        await query.edit_message_text(text=f"Looking for: {preference}")
        await register_name(query, context)
    elif data.startswith("match_action_"):
        action, matched_user_id = data.split("_")[1:]
        await handle_match_action(query, context, action, int(matched_user_id))

async def register_preference(query: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Asks the user for their preferred match gender."""
    keyboard = [
        [InlineKeyboardButton("Male", callback_data="preference_Male")],
        [InlineKeyboardButton("Female", callback_data="preference_Female")],
        [InlineKeyboardButton("Both", callback_data="preference_Both")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.message.reply_text("Who are you looking to meet?", reply_markup=reply_markup)

async def register_name(query: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Asks the user for their name."""
    await query.message.reply_text("What is your name?")
    context.user_data["awaiting_input"] = "name"

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles incoming messages for registration and general chat."""
    user = update.effective_user
    if context.user_data.get("awaiting_input") == "name":
        name = update.message.text
        context.user_data["name"] = name
        context.user_data["awaiting_input"] = None
        await update.message.reply_text(f"Nice to meet you, {name}!")
        await register_age(update, context)
    elif context.user_data.get("awaiting_input") == "age":
        try:
            age = int(update.message.text)
            if 18 <= age <= 99:
                context.user_data["age"] = age
                context.user_data["awaiting_input"] = None
                await update.message.reply_text(f"Got it, you are {age} years old.")
                await register_bio(update, context)
            else:
                await update.message.reply_text("Please enter a valid age between 18 and 99.")
        except ValueError:
            await update.message.reply_text("Please enter a number for your age.")
    elif context.user_data.get("awaiting_input") == "bio":
        bio = update.message.text
        context.user_data["bio"] = bio
        context.user_data["awaiting_input"] = None
        await update.message.reply_text("Thanks for your bio!")
        await complete_registration(update, context)
    else:
        await update.message.reply_text("I don't understand that command. Use /start to begin or /find_match to find a date.")

async def register_age(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Asks the user for their age."""
    await update.message.reply_text("How old are you? (Enter a number between 18 and 99)")
    context.user_data["awaiting_input"] = "age"

async def register_bio(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Asks the user for a short bio."""
    await update.message.reply_text("Tell me a little about yourself (a short bio).")
    context.user_data["awaiting_input"] = "bio"

async def complete_registration(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Saves user data to MongoDB and confirms registration."""
    user_id = update.effective_user.id
    user_data = {
        "user_id": user_id,
        "username": update.effective_user.username,
        "first_name": update.effective_user.first_name,
        "gender": context.user_data.get("gender"),
        "preference": context.user_data.get("preference"),
        "name": context.user_data.get("name"),
        "age": context.user_data.get("age"),
        "bio": context.user_data.get("bio"),
        "registered_at": datetime.now(),
        "last_match_request": None,
        "active": True,
    }
    users_collection.update_one({"user_id": user_id}, {"$set": user_data}, upsert=True)
    await update.message.reply_text(
        "Registration complete! You can now use /find_match to find a date."
    )

async def find_match(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Finds a potential match for the user."""
    user = users_collection.find_one({"user_id": update.effective_user.id})
    if not user:
        await update.message.reply_text("Please register first using /start.")
        return

    # Prevent rapid match requests
    last_request = user.get("last_match_request")
    if last_request and datetime.now() - last_request < timedelta(minutes=1):
        await update.message.reply_text("Please wait a minute before requesting another match.")
        return

    users_collection.update_one(
        {"user_id": user["user_id"]},
        {"$set": {"last_match_request": datetime.now()}}
    )

    # Logic to find a match based on gender and preference
    query = {"user_id": {"$ne": user["user_id"]}, "active": True}

    if user["preference"] == "Male":
        query["gender"] = "Male"
    elif user["preference"] == "Female":
        query["gender"] = "Female"
    # If preference is "Both" or "Other", no gender filter is applied

    # Also, ensure the matched user's preference aligns with the current user's gender
    # This is a simplified reciprocal matching. A more complex system would check for mutual likes.
    if user["gender"] == "Male":
        query["preference"] = {"$in": ["Female", "Both"]}
    elif user["gender"] == "Female":
        query["preference"] = {"$in": ["Male", "Both"]}
    else: # Other gender
        query["preference"] = {"$in": ["Male", "Female", "Both", "Other"]}

    potential_matches = list(users_collection.find(query))

    if not potential_matches:
        await update.message.reply_text("Sorry, no matches found at the moment. Try again later!")
        return

    # Simple random match for now
    import random
    matched_user = random.choice(potential_matches)

    # Store potential match for review
    matches_collection.insert_one({
        "user1_id": user["user_id"],
        "user2_id": matched_user["user_id"],
        "status": "pending", # pending, liked, disliked, connected
        "created_at": datetime.now()
    })

    keyboard = [
        [InlineKeyboardButton("Like", callback_data=f"match_action_like_{matched_user['user_id']}")],
        [InlineKeyboardButton("Dislike", callback_data=f"match_action_dislike_{matched_user['user_id']}")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    match_info = (
        f"You've got a potential match!\n\n"
        f"Name: {matched_user['name']}\n"
        f"Age: {matched_user['age']}\n"
        f"Gender: {matched_user['gender']}\n"
        f"Bio: {matched_user['bio']}\n\n"
        "What do you think?"
    )
    await update.message.reply_text(match_info, reply_markup=reply_markup)

async def handle_match_action(query: Update, context: ContextTypes.DEFAULT_TYPE, action: str, matched_user_id: int) -> None:
    """Handles user's like/dislike action on a match."""
    user_id = query.from_user.id

    # Find the most recent pending match between these two users
    match_record = matches_collection.find_one({
        "user1_id": user_id,
        "user2_id": matched_user_id,
        "status": "pending"
    })

    if not match_record:
        await query.edit_message_text("This match is no longer active or has been acted upon.")
        return

    if action == "like":
        # Check if the matched user has also liked the current user
        reverse_match = matches_collection.find_one({
            "user1_id": matched_user_id,
            "user2_id": user_id,
            "status": "liked"
        })

        if reverse_match:
            # It's a mutual match!
            matches_collection.update_one(
                {"_id": match_record["_id"]},
                {"$set": {"status": "connected", "connected_at": datetime.now()}}
            )
            matches_collection.update_one(
                {"_id": reverse_match["_id"]},
                {"$set": {"status": "connected", "connected_at": datetime.now()}}
            )

            matched_user = users_collection.find_one({"user_id": matched_user_id})
            user_info = users_collection.find_one({"user_id": user_id})

            await query.edit_message_text("It's a mutual match! You are connected!")
            await context.bot.send_message(
                chat_id=matched_user_id,
                text=f"It's a mutual match with {user_info['name']}! You can now chat with them. "
                     f"Their Telegram username is @{user_info['username'] or user_info['first_name']}."
            )
            await context.bot.send_message(
                chat_id=user_id,
                text=f"It's a mutual match with {matched_user['name']}! You can now chat with them. "
                     f"Their Telegram username is @{matched_user['username'] or matched_user['first_name']}."
            )
        else:
            matches_collection.update_one(
                {"_id": match_record["_id"]},
                {"$set": {"status": "liked"}}
            )
            await query.edit_message_text("You liked this person! Waiting for them to respond.")
            # Notify the matched user that someone liked them (optional, can be a premium feature)
            # await context.bot.send_message(chat_id=matched_user_id, text="Someone liked you!")

    elif action == "dislike":
        matches_collection.update_one(
            {"_id": match_record["_id"]},
            {"$set": {"status": "disliked"}}
        )
        await query.edit_message_text("You disliked this person. Try /find_match for another one.")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a help message."""
    help_text = (
        "Welcome to the Blind Date Bot!\n\n"
        "Here are the commands you can use:\n"
        "/start - Register or check your registration status.\n"
        "/find_match - Find a potential blind date.\n"
        "/help - Show this help message.\n\n"
        "Once you find a match, you can choose to 'Like' or 'Dislike' them. "
        "If it's a mutual like, you'll be connected!"
    )
    await update.message.reply_text(help_text)

async def self_ping() -> None:
    """Pings the bot's own URL to keep it alive on Render free tier."""
    if SELF_URL:
        while True:
            try:
                response = requests.get(SELF_URL)
                logger.info(f"Self-ping successful: {response.status_code}")
            except requests.exceptions.RequestException as e:
                logger.error(f"Self-ping failed: {e}")
            await asyncio.sleep(600)  # Ping every 10 minutes
    else:
        logger.warning("SELF_URL not set. Self-ping will not be active.")

def main() -> None:
    """Starts the bot."""
    if not BOT_TOKEN or not MONGO_URI:
        logger.error("BOT_TOKEN or MONGO_URI environment variables are not set. Exiting.")
        return

    application = Application.builder().token(BOT_TOKEN).build()

    # Register handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("find_match", find_match))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CallbackQueryHandler(button))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Start self-ping in a separate task
    if SELF_URL:
        asyncio.create_task(self_ping())

    # Run the bot
    logger.info("Bot started polling...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
