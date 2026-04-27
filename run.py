import asyncio
import logging
from main import main_async, logger

# Render မှာ error တက်ရင် ကြည့်လို့ရအောင် logging ကို ပြန်ဖွင့်တာပါ
logging.basicConfig(level=logging.INFO)

async def start_bot():
    try:
        logger.info("Starting bot from compiled .so file...")
        await main_async()
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")

if __name__ == "__main__":
    try:
        # main.py ထဲက main_async() ကို တိုက်ရိုက် run တာပါ
        asyncio.run(start_bot())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
    except Exception as e:
        print(f"Critical Error: {e}")
