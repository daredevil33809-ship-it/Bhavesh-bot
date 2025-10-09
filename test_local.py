import os
import asyncio
from bot import dp, bot, load_movies, load_users

async def main():
    print("Testing bot locally in polling mode...")
    print("Loading movies...")
    load_movies()
    print("Loading users...")
    load_users()
    print("Bot is running! Press Ctrl+C to stop.")
    
    try:
        await dp.start_polling(bot)
    except KeyboardInterrupt:
        print("\nBot stopped.")
    finally:
        await bot.session.close()

if __name__ == "__main__":
    if not os.getenv("BOT_TOKEN"):
        print("ERROR: BOT_TOKEN not set!")
        exit(1)
    
    asyncio.run(main())
