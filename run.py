import asyncio
import sys
from main import main_async, logger

if __name__ == "__main__":
    try:
        logger.info("Starting bot from compiled .so file...")
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
    except Exception as e:
        logger.error(f"Critical Error: {e}")
        sys.exit(1)
