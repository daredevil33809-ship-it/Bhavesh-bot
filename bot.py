import os
import json
import asyncio
import time
import logging
from datetime import datetime
from typing import List, Dict, Optional
from collections import defaultdict
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from aiogram.enums import ParseMode
from rapidfuzz import fuzz, process
from Levenshtein import distance as levenshtein_distance
from flask import Flask, request, abort
import requests as http_requests 

# --- Configuration and Environment Variables ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
APP_URL = os.getenv("APP_URL")

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN environment variable is not set")

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('bot')

# --- Constants ---
ADMIN_IDS = [7263519581]
LIBRARY_CHANNEL_USERNAME = "@MOVIEMAZA19"
LIBRARY_CHANNEL_ID = -1002970735025  
JOIN_CHANNEL_USERNAME = "@MOVIEMAZASU"
JOIN_CHANNEL_ID = -1003124931164
JOIN_GROUP_USERNAME = "@THEGREATMOVIESL9"
JOIN_GROUP_ID = -1002970735025

# Use /tmp for ephemeral storage on cloud environments
if os.path.exists("/tmp"):
    MOVIES_FILE = "/tmp/movies.json"
    BACKUP_FILE = "/tmp/movies_backup.json"
    USERS_FILE = "/tmp/users.json"
    INITIAL_DATA_FILE = "movies.json" 
else:
    MOVIES_FILE = "movies.json"
    BACKUP_FILE = "movies_backup.json"
    USERS_FILE = "users.json"
    INITIAL_DATA_FILE = None

# --- Aiogram and Flask Initialization ---
bot = Bot(
    token=BOT_TOKEN, 
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()
app = Flask(__name__)

# --- Global State and Cache ---
movies_cache: List[Dict[str, str]] = []
movies_index: Dict[str, List[int]] = {}
user_sessions: Dict[int, Dict] = defaultdict(dict)
search_cache: Dict[str, List[Dict]] = {}
verified_users: set = set()
users_database: Dict[int, Dict] = {}
user_last_action: Dict[int, float] = {}
bot_stats = {
    "start_time": time.time(),
    "total_searches": 0,
    "cache_hits": 0
}

RATE_LIMIT_SECONDS = 1

# --- Storage and Initialization Functions ---

def load_movies():
    global movies_cache
    try:
        if os.path.exists(MOVIES_FILE):
            with open(MOVIES_FILE, 'r', encoding='utf-8') as f:
                movies_cache = json.load(f)
            logger.info(f"Loaded {len(movies_cache)} movies from {MOVIES_FILE}")
        elif INITIAL_DATA_FILE and os.path.exists(INITIAL_DATA_FILE):
            with open(INITIAL_DATA_FILE, 'r', encoding='utf-8') as f:
                movies_cache = json.load(f)
            save_movies()
            logger.info(f"Loaded {len(movies_cache)} movies from {INITIAL_DATA_FILE} and saved to {MOVIES_FILE}")
        else:
            movies_cache = []
            save_movies()
            logger.info(f"Created new {MOVIES_FILE}")
    except json.JSONDecodeError:
        logger.error(f"Error: Corrupted {MOVIES_FILE}")
        movies_cache = []
        save_movies()
    except Exception as e:
        logger.error(f"Error loading movies: {e}")
        movies_cache = []
    
    build_movies_index()
    logger.info(f"Built search index with {len(movies_index)} unique terms")


def save_movies():
    try:
        if os.path.exists(MOVIES_FILE):
            with open(MOVIES_FILE, 'r', encoding='utf-8') as f:
                backup_data = f.read()
            with open(BACKUP_FILE, 'w', encoding='utf-8') as f:
                f.write(backup_data)
        
        with open(MOVIES_FILE, 'w', encoding='utf-8') as f:
            json.dump(movies_cache, f, ensure_ascii=False, separators=(',', ':'))
        logger.info(f"Saved {len(movies_cache)} movies to {MOVIES_FILE}")
    except Exception as e:
        logger.error(f"Error saving movies: {e}")

def build_movies_index():
    global movies_index
    movies_index = {}
    for idx, movie in enumerate(movies_cache):
        title_normalized = normalize_abbreviations(movie['title'].lower())
        words = title_normalized.split()
        for word in words:
            if len(word) > 2:
                if word not in movies_index:
                    movies_index[word] = []
                movies_index[word].append(idx)

def load_users():
    global users_database
    try:
        if os.path.exists(USERS_FILE):
            with open(USERS_FILE, 'r', encoding='utf-8') as f:
                users_database = json.load(f)
            logger.info(f"Loaded {len(users_database)} users from {USERS_FILE}")
        else:
            users_database = {}
            save_users()
            logger.info(f"Created new {USERS_FILE}")
    except Exception as e:
        logger.error(f"Error loading users: {e}")
        users_database = {}

def save_users():
    try:
        with open(USERS_FILE, 'w', encoding='utf-8') as f:
            json.dump(users_database, f, ensure_ascii=False, separators=(',', ':'))
        logger.info(f"Saved {len(users_database)} users to {USERS_FILE}")
    except Exception as e:
        logger.error(f"Error saving users: {e}")

def add_user(user_id: int, username: str = None, first_name: str = None):
    user_id_str = str(user_id)
    if user_id_str not in users_database:
        users_database[user_id_str] = {
            "user_id": user_id,
            "username": username,
            "first_name": first_name,
            "joined_date": datetime.now().isoformat(),
            "last_active": datetime.now().isoformat()
        }
        save_users()
        logger.info(f"New user added: {user_id} (@{username})")
    else:
        users_database[user_id_str]["last_active"] = datetime.now().isoformat()
        save_users()

def add_movie(title: str, file_id: str) -> bool:
    normalized_title = title.strip().lower()
    for movie in movies_cache:
        if movie['title'].lower() == normalized_title:
            logger.warning(f"Duplicate movie prevented: {title}")
            return False
    
    movies_cache.append({"title": title, "file_id": file_id})
    save_movies()
    build_movies_index()
    search_cache.clear()
    logger.info(f"Added new movie: {title}")
    return True

def normalize_abbreviations(text: str) -> str:
    import re
    abbrev_map = {
        r'\bs(\d+)\b': r'season \1',
        r'\bse(\d+)\b': r'season \1',
        r'\bseason(\d+)\b': r'season \1',
        r'\bpt(\d+)\b': r'part \1',
        r'\bpart(\d+)\b': r'part \1',
        r'\bep(\d+)\b': r'episode \1',
        r'\bepisode(\d+)\b': r'episode \1',
        r'\be(\d+)\b': r'episode \1',
        r'\bvol(\d+)\b': r'volume \1',
        r'\bvolume(\d+)\b': r'volume \1',
        r'\bch(\d+)\b': r'chapter \1',
        r'\bchapter(\d+)\b': r'chapter \1',
    }
    normalized = text.lower()
    for pattern, replacement in abbrev_map.items():
        normalized = re.sub(pattern, replacement, normalized)
    return normalized

def phonetic_similarity(s1: str, s2: str) -> float:
    s1_lower = s1.lower()
    s2_lower = s2.lower()
    consonants = "bcdfghjklmnpqrstvwxyz"
    vowels = "aeiou"
    
    def extract_consonants(s):
        return ''.join([c for c in s if c in consonants])
    
    def extract_vowels(s):
        return ''.join([c for c in s if c in vowels])
    
    cons1, cons2 = extract_consonants(s1_lower), extract_consonants(s2_lower)
    vowel1, vowel2 = extract_vowels(s1_lower), extract_vowels(s2_lower)
    
    if len(cons1) == 0 or len(cons2) == 0:
        return 0
    
    cons_sim = fuzz.ratio(cons1, cons2)
    vowel_sim = fuzz.ratio(vowel1, vowel2) if vowel1 and vowel2 else 50
    
    return (cons_sim * 0.7 + vowel_sim * 0.3)

def advanced_phonetic_match(s1: str, s2: str) -> float:
    s1_clean = ''.join(c for c in s1.lower() if c.isalnum())
    s2_clean = ''.join(c for c in s2.lower() if c.isalnum())
    
    if not s1_clean or not s2_clean:
        return 0
    
    if s1_clean == s2_clean:
        return 100
    
    consonant_groups = {
        'ptkbdg': 'stop',
        'fvszh': 'fricative',
        'mnl': 'nasal',
        'wy': 'glide'
    }
    
    def simplify_sound(char):
        for group, _ in consonant_groups.items():
            if char in group:
                return group[0]
        return char
    
    s1_sound = ''.join(simplify_sound(c) for c in s1_clean)
    s2_sound = ''.join(simplify_sound(c) for c in s2_clean)
    
    return fuzz.ratio(s1_sound, s2_sound)

def check_rate_limit(user_id: int) -> bool:
    current_time = time.time()
    if user_id in user_last_action:
        if current_time - user_last_action[user_id] < RATE_LIMIT_SECONDS:
            return False
    user_last_action[user_id] = current_time
    return True

async def check_user_membership(user_id: int) -> bool:
    if user_id in verified_users:
        return True
    
    try:
        channel_member = await bot.get_chat_member(JOIN_CHANNEL_ID, user_id)
        group_member = await bot.get_chat_member(JOIN_GROUP_ID, user_id)
        
        if channel_member.status not in ['left', 'kicked'] and group_member.status not in ['left', 'kicked']:
            verified_users.add(user_id)
            return True
    except Exception as e:
        logger.error(f"Error checking membership for user {user_id}: {e}")
    
    return False

def advanced_fuzzy_search(query: str, limit: int = 15) -> List[Dict]:
    if not query or not movies_cache:
        return []
    
    cache_key = query.lower().strip()
    if cache_key in search_cache:
        bot_stats["cache_hits"] += 1
        return search_cache[cache_key][:limit]
    
    bot_stats["total_searches"] += 1
    query_lower = query.lower().strip()
    query_normalized = normalize_abbreviations(query_lower)
    query_words = query_normalized.split()
    
    scored_movies = []
    
    for movie in movies_cache:
        title = movie['title']
        title_normalized = normalize_abbreviations(title.lower())
        
        ratio_score = fuzz.ratio(query_normalized, title_normalized)
        partial_ratio = fuzz.partial_ratio(query_normalized, title_normalized)
        token_sort = fuzz.token_sort_ratio(query_normalized, title_normalized)
        token_set = fuzz.token_set_ratio(query_normalized, title_normalized)
        phonetic_score = phonetic_similarity(query_normalized, title_normalized)
        adv_phonetic = advanced_phonetic_match(query_normalized, title_normalized)
        
        final_score = (
            ratio_score * 0.20 +
            partial_ratio * 0.18 +
            token_sort * 0.15 +
            token_set * 0.15 +
            phonetic_score * 0.08 +
            adv_phonetic * 0.07 
        )
        
        if title_normalized == query_normalized:
            final_score += 300
        elif title_normalized.startswith(query_normalized):
            final_score += 150
        elif query_normalized in title_normalized:
            final_score += 80
        
        if final_score > 25:
            scored_movies.append({
                "title": title,
                "file_id": movie['file_id'],
                "score": final_score
            })
    
    scored_movies.sort(key=lambda x: x['score'], reverse=True)
    results = scored_movies[:limit]
    
    search_cache[cache_key] = results
    if len(search_cache) > 1000:
        oldest_key = next(iter(search_cache))
        del search_cache[oldest_key]
    
    return results

# --- Aiogram Handlers ---

@dp.message(Command("start"))
async def cmd_start(message: Message):
    if message.from_user:
        add_user(
            user_id=message.from_user.id,
            username=message.from_user.username,
            first_name=message.from_user.first_name
        )
    
    if message.from_user and message.from_user.id in ADMIN_IDS:
        admin_text = f"""🎬 Welcome Back, Admin!

👤 Admin ID: {message.from_user.id}
📊 Total Movies: {len(movies_cache)}
👥 Total Users: {len(users_database)}

🔧 Admin Commands:
/stats - View detailed statistics
/refresh - Reload movie database
/broadcast - Send message to all users

You have full access to all bot features."""
        await message.answer(admin_text)
    else:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"🔗 Join Channel", url=f"https://t.me/{JOIN_CHANNEL_USERNAME.replace('@', '')}")],
            [InlineKeyboardButton(text=f"👥 Join Group", url=f"https://t.me/{JOIN_GROUP_USERNAME.replace('@', '')}")],
            [InlineKeyboardButton(text="✅ I Joined", callback_data="joined")]
        ])
        
        await message.answer(
            "Welcome! Please join our channel and group to continue:",
            reply_markup=keyboard
        )


@dp.callback_query(F.data == "joined")
async def process_joined(callback: types.CallbackQuery):
    welcome_text = """Hello! In this bot, you will find all kinds of movies. Even if you type the spelling wrong, you will still get your movie. 
And best of all, everything is free for now. Just type the movie name and enjoy!  

नमस्ते! इस बोट में आपको हर तरह की फ़िल्में मिलेंगी। यदि आप स्पेलिंग गलत भी लिखें तो भी आपकी फ़िल्म मिलेगी। 
अभी सब कुछ फ्री है। बस फ़िल्म का नाम टाइप करें और एंजॉय करें!"""
    
    if callback.message and isinstance(callback.message, Message):
        await callback.message.edit_text(welcome_text)
    await callback.answer()


@dp.message(Command("refresh"))
async def cmd_refresh(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ You are not authorized to use this command.")
        return
    
    load_movies()
    search_cache.clear()
    await message.answer(f"✅ Refreshed! Loaded {len(movies_cache)} movies\n📇 Index: {len(movies_index)} terms")


@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ You are not authorized to use this command.")
        return
    
    uptime_seconds = int(time.time() - bot_stats["start_time"])
    hours = uptime_seconds // 3600
    minutes = (uptime_seconds % 3600) // 60
    
    stats_text = f"""📊 Bot Statistics:

🎬 Total Movies: {len(movies_cache)}
👥 Total Users: {len(users_database)}
🔍 Total Searches: {bot_stats['total_searches']}
⚡ Cache Hits: {bot_stats['cache_hits']}
💾 Cache Size: {len(search_cache)} queries
⏱ Uptime: {hours}h {minutes}m"""
    
    await message.answer(stats_text)


@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message):
    if not message.from_user or message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ You are not authorized to use this command.")
        return
    
    broadcast_text = message.text.replace("/broadcast", "").strip()
    broadcast_photo = None
    broadcast_video = None
    
    if message.reply_to_message:
        if message.reply_to_message.photo:
            broadcast_photo = message.reply_to_message.photo[-1].file_id
            if message.reply_to_message.caption:
                broadcast_text = broadcast_text or message.reply_to_message.caption
        elif message.reply_to_message.video:
            broadcast_video = message.reply_to_message.video.file_id
            if message.reply_to_message.caption:
                broadcast_text = broadcast_text or message.reply_to_message.caption
    
    if not broadcast_text and not broadcast_photo and not broadcast_video:
        help_text = """⚠️ Broadcast Usage:

📝 Text Message:
/broadcast Your message here

📸 With Photo:
Reply to a photo with /broadcast [optional message]

🎥 With Video:
Reply to a video with /broadcast [optional message]

Example:
/broadcast 🎬 100+ new movies added!"""
        await message.answer(help_text)
        return
    
    if not users_database:
        await message.answer("⚠️ No users in database yet.")
        return
    
    sent_count = 0
    failed_count = 0
    blocked_count = 0
    
    media_type = "📸 photo" if broadcast_photo else ("🎥 video" if broadcast_video else "📝 text")
    status_msg = await message.answer(f"📡 Broadcasting {media_type} to {len(users_database)} users...")
    
    for user_id_str, user_data in users_database.items():
        try:
            user_id = int(user_id_str)
            
            if broadcast_photo:
                await bot.send_photo(
                    chat_id=user_id,
                    photo=broadcast_photo,
                    caption=f"📢 Broadcast:\n\n{broadcast_text}" if broadcast_text else "📢 Broadcast"
                )
            elif broadcast_video:
                await bot.send_video(
                    chat_id=user_id,
                    video=broadcast_video,
                    caption=f"📢 Broadcast:\n\n{broadcast_text}" if broadcast_text else "📢 Broadcast"
                )
            else:
                await bot.send_message(
                    chat_id=user_id,
                    text=f"📢 Broadcast:\n\n{broadcast_text}"
                )
            
            sent_count += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            error_msg = str(e).lower()
            if "blocked" in error_msg or "deactivated" in error_msg or "user is deactivated" in error_msg:
                blocked_count += 1
            else:
                failed_count += 1
            logger.error(f"Failed to send to {user_id}: {e}")
    
    summary = f"""✅ Broadcast Complete!

📊 Results:
✅ Sent: {sent_count}
🚫 Blocked: {blocked_count}
❌ Failed: {failed_count}
👥 Total Users: {len(users_database)}
📤 Type: {media_type}"""
    
    await status_msg.edit_text(summary)


@dp.channel_post()
async def handle_channel_post(message: Message):
    try:
        if not message.chat:
            return
            
        if message.chat.id == LIBRARY_CHANNEL_ID or (message.chat.username and message.chat.username.lower() == LIBRARY_CHANNEL_USERNAME.lower().replace('@', '')):
            if message.document or message.video:
                caption = message.caption or ""
                title = caption.split('\n')[0].strip() if caption else "Unknown Movie"
                
                file_id = None
                if message.document and message.document.file_id:
                    file_id = message.document.file_id
                elif message.video and message.video.file_id:
                    file_id = message.video.file_id
                
                if title and title != "Unknown Movie" and file_id:
                    if add_movie(title, file_id):
                        logger.info(f"✅ Auto-indexed: {title}")
                    else:
                        logger.warning(f"⚠️ Duplicate skipped: {title}")
    except Exception as e:
        logger.error(f"Error in handle_channel_post: {e}")


@dp.message(F.text)
async def handle_search(message: Message):
    try:
        if not message.text or message.text.startswith('/'):
            return
        
        query = message.text.strip()
        if not query or not message.from_user:
            return
        
        if not check_rate_limit(message.from_user.id):
            return
        
        if len(query) > 100:
            await message.answer("⚠️ Query too long. Please use less than 100 characters.")
            return
        
        results = advanced_fuzzy_search(query, limit=15)
        
        if not results:
            await message.answer(f"❌ No movies found for: {query}\n\nTry checking the spelling or use a different name.")
            return
        
        keyboard_buttons = []
        for result in results:
            try:
                button_text = f"{result['title']} ({int(result['score'])}%)"
                # Find the index safely for callback data
                try:
                    movie_idx = movies_cache.index([m for m in movies_cache if m['file_id'] == result['file_id']][0])
                except (ValueError, IndexError):
                    continue
                
                callback_data = f"movie_{movie_idx}"
                keyboard_buttons.append([InlineKeyboardButton(text=button_text, callback_data=callback_data)])
            except (ValueError, IndexError):
                continue
        
        if not keyboard_buttons:
            await message.answer(f"❌ No movies found for: {query}")
            return
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
        
        sent_msg = await message.answer(
            f"🔍 Found {len(keyboard_buttons)} results for: {query}",
            reply_markup=keyboard
        )
        
        user_sessions[message.from_user.id]['last_search_msg'] = sent_msg.message_id
    
    except Exception as e:
        logger.error(f"Error in handle_search: {e}")
        if message.from_user:
            try:
                await message.answer("❌ An error occurred. Please try again.")
            except:
                pass


@dp.callback_query(F.data.startswith("movie_"))
async def send_movie(callback: types.CallbackQuery):
    try:
        if not callback.data or not callback.from_user:
            await callback.answer("❌ Error: Invalid request")
            return
        
        try:
            movie_index = int(callback.data.split('_')[1])
        except (ValueError, IndexError):
            await callback.answer("❌ Invalid movie selection")
            return
        
        if movie_index < 0 or movie_index >= len(movies_cache):
            await callback.answer("❌ Movie not found")
            return
        
        movie = movies_cache[movie_index]
        
        # Try sending as document, then fall back to video
        try:
            await bot.send_document(
                chat_id=callback.from_user.id,
                document=movie['file_id'],
                caption=f"🎬 {movie['title']}"
            )
        except Exception as doc_err:
            try:
                await bot.send_video(
                    chat_id=callback.from_user.id,
                    video=movie['file_id'],
                    caption=f"🎬 {movie['title']}"
                )
            except Exception as vid_err:
                logger.error(f"Error sending as document or video: {doc_err}, {vid_err}")
                await callback.answer("❌ Failed to send movie file")
                return
        
        if 'last_search_msg' in user_sessions.get(callback.from_user.id, {}):
            try:
                await bot.delete_message(
                    chat_id=callback.from_user.id,
                    message_id=user_sessions[callback.from_user.id]['last_search_msg']
                )
            except:
                pass
        
        await callback.answer(f"✅ Sent: {movie['title']}")
        
    except Exception as e:
        logger.error(f"Error sending movie: {e}")
        try:
            await callback.answer("❌ Error sending movie. Please try again.")
        except:
            pass

# --- Flask Webhook and Keep Alive ---

@app.route(f"/webhook/{BOT_TOKEN}", methods=["POST"])
async def telegram_webhook():
    """Telegram webhook endpoint."""
    if request.method == "POST":
        update_data = request.json
        if not update_data:
            return "OK", 200
        
        try:
            # Validate and create Update object
            update = types.Update.model_validate(update_data, context={"bot": bot})
            
            # FINAL FIX: Removed 'bot=bot' from feed_update for clean aiogram v3 integration.
            await dp.feed_update(update) # <- Corrected Line
            
            return "OK", 200
        except Exception as e:
            logger.error(f"Error processing update: {e}", exc_info=True)
            return "Error", 500
    
    return abort(405) # Method Not Allowed


@app.route("/", methods=["GET"])
def index():
    """Health check endpoint."""
    return "Movie Bot is running!", 200

# Function to ping the app itself (for 24/7 keep alive)
async def keep_alive():
    if not APP_URL:
        logger.warning("APP_URL environment variable is not set. 24/7 Keep Alive will not work.")
        return

    # Try to set webhook automatically on startup
    webhook_url = f"{APP_URL}/webhook/{BOT_TOKEN}"
    try:
        current_webhook = await bot.get_webhook_info()
        if current_webhook.url != webhook_url:
            await bot.set_webhook(url=webhook_url)
            logger.info(f"Webhook set successfully to: {webhook_url}")
    except Exception as e:
        logger.error(f"Failed to set webhook on startup: {e}")
    
    # Start the continuous ping loop
    while True:
        try:
            # Ping the index route
            response = http_requests.get(APP_URL, timeout=10)
            if response.status_code == 200:
                logger.info("🟢 Ping successful.")
            else:
                logger.warning(f"🟡 Ping failed with status code: {response.status_code}")
        except Exception as e:
            logger.error(f"🔴 Keep alive ping failed: {e}")
        
        await asyncio.sleep(300) # Ping every 5 minutes

# --- Startup Logic ---

# Run the initialization on application startup
def initialize_app():
    logger.info("Initializing Movie Bot...")
    load_movies()
    load_users()
    logger.info(f"Bot ready with {len(movies_cache)} movies and {len(users_database)} users in memory")
    
    # Start the keep_alive coroutine in the background
    if APP_URL:
        import threading
        def run_keep_alive():
            asyncio.run(keep_alive())
        
        thread = threading.Thread(target=run_keep_alive, daemon=True)
        thread.start()
        
initialize_app()
```eof

---

## 2. `requirements.txt` (Final)

```markdown:Project Dependencies:requirements.txt
aiogram==3.22.0
rapidfuzz==3.14.1
python-Levenshtein==0.27.1
boto3>=1.28.0
Flask[async]
gunicorn
requests
gevent
```eof
