import os
import json
import asyncio
import time
from datetime import datetime
from collections import defaultdict
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from aiogram.enums import ParseMode
from rapidfuzz import fuzz
from Levenshtein import distance as levenshtein_distance

# --- S3 Storage Check ---
try:
    from s3_storage import load_movies_from_s3, save_movies_to_s3
    S3_ENABLED = True
except ImportError:
    S3_ENABLED = False
    def load_movies_from_s3(): return None
    def save_movies_to_s3(movies): return False

# --- Bot Token ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN environment variable is not set")

# --- Admin & Channels ---
ADMIN_IDS = [7263519581]
LIBRARY_CHANNEL_ID = -1002970735025
LIBRARY_CHANNEL_USERNAME = "@MOVIEMAZA19"
JOIN_CHANNEL_ID = -1003124931164
JOIN_CHANNEL_USERNAME = "@MOVIEMAZASU"
JOIN_GROUP_ID = -1002970735025
JOIN_GROUP_USERNAME = "@THEGREATMOVIESL9"

# --- File Paths ---
import platform
if platform.system() == "Linux" and os.path.exists("/tmp"):
    MOVIES_FILE = "/tmp/movies.json"
    BACKUP_FILE = "/tmp/movies_backup.json"
    USERS_FILE = "/tmp/users.json"
    INITIAL_DATA_FILE = "movies.json"
else:
    MOVIES_FILE = "movies.json"
    BACKUP_FILE = "movies_backup.json"
    USERS_FILE = "users.json"
    INITIAL_DATA_FILE = None

# --- Bot & Dispatcher ---
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- Global Data ---
movies_cache = []
movies_index = {}
user_sessions = defaultdict(dict)
search_cache = {}
verified_users = set()
users_database = {}
user_last_action = {}
bot_stats = {"start_time": time.time(), "total_searches": 0, "cache_hits": 0}
RATE_LIMIT_SECONDS = 1

# ----------------- Helper Functions -----------------
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

def build_movies_index():
    global movies_index
    movies_index = {}
    for idx, movie in enumerate(movies_cache):
        title_normalized = normalize_abbreviations(movie['title'].lower())
        for word in title_normalized.split():
            if len(word) > 2:
                movies_index.setdefault(word, []).append(idx)

def load_movies():
    global movies_cache
    if S3_ENABLED:
        s3_movies = load_movies_from_s3()
        if s3_movies is not None:
            movies_cache = s3_movies
            with open(MOVIES_FILE, 'w', encoding='utf-8') as f:
                json.dump(movies_cache, f, ensure_ascii=False, separators=(',', ':'))
            build_movies_index()
            return
    try:
        if os.path.exists(MOVIES_FILE):
            with open(MOVIES_FILE, 'r', encoding='utf-8') as f:
                movies_cache = json.load(f)
        elif INITIAL_DATA_FILE and os.path.exists(INITIAL_DATA_FILE):
            with open(INITIAL_DATA_FILE, 'r', encoding='utf-8') as f:
                movies_cache = json.load(f)
            save_movies()
        else:
            movies_cache = []
            save_movies()
    except Exception:
        movies_cache = []
        save_movies()
    build_movies_index()

def save_movies():
    try:
        if os.path.exists(MOVIES_FILE):
            with open(MOVIES_FILE, 'r', encoding='utf-8') as f:
                backup_data = f.read()
            with open(BACKUP_FILE, 'w', encoding='utf-8') as f:
                f.write(backup_data)
        with open(MOVIES_FILE, 'w', encoding='utf-8') as f:
            json.dump(movies_cache, f, ensure_ascii=False, separators=(',', ':'))
        if S3_ENABLED:
            save_movies_to_s3(movies_cache)
    except Exception as e:
        print(f"Error saving movies: {e}")

def load_users():
    global users_database
    try:
        if os.path.exists(USERS_FILE):
            with open(USERS_FILE, 'r', encoding='utf-8') as f:
                users_database = json.load(f)
        else:
            users_database = {}
            save_users()
    except Exception:
        users_database = {}

def save_users():
    try:
        with open(USERS_FILE, 'w', encoding='utf-8') as f:
            json.dump(users_database, f, ensure_ascii=False, separators=(',', ':'))
    except Exception as e:
        print(f"Error saving users: {e}")

def add_user(user_id: int, username=None, first_name=None):
    uid = str(user_id)
    if uid not in users_database:
        users_database[uid] = {
            "user_id": user_id,
            "username": username,
            "first_name": first_name,
            "joined_date": datetime.now().isoformat(),
            "last_active": datetime.now().isoformat()
        }
        save_users()
    else:
        users_database[uid]["last_active"] = datetime.now().isoformat()
        save_users()

def add_movie(title: str, file_id: str) -> bool:
    normalized_title = title.strip().lower()
    for movie in movies_cache:
        if movie['title'].lower() == normalized_title:
            return False
    movies_cache.append({"title": title, "file_id": file_id})
    save_movies()
    build_movies_index()
    search_cache.clear()
    return True

def check_rate_limit(user_id: int) -> bool:
    current_time = time.time()
    if user_id in user_last_action and current_time - user_last_action[user_id] < RATE_LIMIT_SECONDS:
        return False
    user_last_action[user_id] = current_time
    return True

async def check_user_membership(user_id: int) -> bool:
    if user_id in verified_users: return True
    try:
        channel_member = await bot.get_chat_member(JOIN_CHANNEL_ID, user_id)
        group_member = await bot.get_chat_member(JOIN_GROUP_ID, user_id)
        if channel_member.status not in ['left', 'kicked'] and group_member.status not in ['left', 'kicked']:
            verified_users.add(user_id)
            return True
    except: pass
    return False

def advanced_fuzzy_search(query: str, limit=15):
    if not query or not movies_cache: return []
    key = query.lower().strip()
    if key in search_cache:
        bot_stats["cache_hits"] += 1
        return search_cache[key][:limit]
    bot_stats["total_searches"] += 1
    query_norm = normalize_abbreviations(query.lower())
    results = []
    for movie in movies_cache:
        title_norm = normalize_abbreviations(movie['title'].lower())
        ratio = fuzz.ratio(query_norm, title_norm)
        partial = fuzz.partial_ratio(query_norm, title_norm)
        lev = levenshtein_distance(query_norm, title_norm)
        score = ratio * 0.6 + partial * 0.3 + max(0, (100 - lev)) * 0.1
        if score > 25:
            results.append({"title": movie['title'], "file_id": movie['file_id'], "score": score})
    results.sort(key=lambda x: x['score'], reverse=True)
    search_cache[key] = results[:limit]
    if len(search_cache) > 1000: search_cache.pop(next(iter(search_cache)))
    return results[:limit]

# ----------------- Handlers -----------------
@dp.message(Command("start"))
async def cmd_start(message: Message):
    if message.from_user:
        add_user(message.from_user.id, message.from_user.username, message.from_user.first_name)
    if message.from_user and message.from_user.id in ADMIN_IDS:
        text = f"""🎬 Welcome Back, Admin!
👤 Admin ID: {message.from_user.id}
🎬 Total Movies: {len(movies_cache)}
👥 Total Users: {len(users_database)}
🔧 Commands: /stats /refresh /broadcast"""
        await message.answer(text)
    else:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton("🔗 Join Channel", url=f"https://t.me/{JOIN_CHANNEL_USERNAME.replace('@','')}")],
            [InlineKeyboardButton("👥 Join Group", url=f"https://t.me/{JOIN_GROUP_USERNAME.replace('@','')}")],
            [InlineKeyboardButton("✅ I Joined", callback_data="joined")]
        ])
        await message.answer("Welcome! Join channel/group to continue:", reply_markup=kb)

@dp.callback_query(F.data == "joined")
async def joined_cb(callback: types.CallbackQuery):
    text = """Hello! This bot gives all movies, spelling mistakes allowed.
Just type movie name and enjoy!"""
    if callback.message: await callback.message.edit_text(text)
    await callback.answer()

@dp.message(Command("refresh"))
async def cmd_refresh(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ Not authorized")
        return
    load_movies()
    search_cache.clear()
    await message.answer(f"✅ Refreshed! {len(movies_cache)} movies")

@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ Not authorized")
        return
    uptime = int(time.time() - bot_stats["start_time"])
    h, m = uptime//3600, (uptime%3600)//60
    text = f"""📊 Stats:
🎬 Movies: {len(movies_cache)}
👥 Users: {len(users_database)}
🔍 Searches: {bot_stats['total_searches']}
⚡ Cache hits: {bot_stats['cache_hits']}
⏱ Uptime: {h}h {m}m"""
    await message.answer(text)

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ Not authorized")
        return
    text = message.text.replace("/broadcast","").strip()
    media_photo = message.reply_to_message.photo[-1].file_id if message.reply_to_message and message.reply_to_message.photo else None
    media_video = message.reply_to_message.video.file_id if message.reply_to_message and message.reply_to_message.video else None
    sent, blocked, failed = 0,0,0
    for uid_str in users_database:
        try:
            uid = int(uid_str)
            if media_photo:
                await bot.send_photo(uid, media_photo, caption=text)
            elif media_video:
                await bot.send_video(uid, media_video, caption=text)
            else:
                await bot.send_message(uid, text)
            sent +=1
            await asyncio.sleep(0.05)
        except Exception as e:
            err = str(e).lower()
            if "blocked" in err: blocked+=1
            else: failed+=1
    await message.answer(f"✅ Broadcast complete!\nSent: {sent}, Blocked: {blocked}, Failed: {failed}")

@dp.channel_post()
async def handle_channel_post(message: Message):
    try:
        if message.chat.id == LIBRARY_CHANNEL_ID:
            if message.document or message.video:
                caption = message.caption or ""
                title = caption.split('\n')[0].strip() if caption else "Unknown Movie"
                file_id = message.document.file_id if message.document else message.video.file_id
                add_movie(title, file_id)
    except: pass

@dp.message(F.text)
async def handle_search(message: Message):
    if not message.text or message.text.startswith("/"): return
    if not check_rate_limit(message.from_user.id): return
    results = advanced_fuzzy_search(message.text, limit=15)
    if not results:
        await message.answer(f"❌ No movies found for: {message.text}")
        return
    kb_buttons = [[InlineKeyboardButton(f"{r['title']} ({int(r['score'])}%)", callback_data=f"movie_{movies_cache.index(next(m for m in movies_cache if m['file_id']==r['file_id']))}")] for r in results]
    kb = InlineKeyboardMarkup(inline_keyboard=kb_buttons)
    msg = await message.answer(f"🔍 Found {len(kb_buttons)} results for: {message.text}", reply_markup=kb)
    user_sessions[message.from_user.id]['last_search_msg'] = msg.message_id

@dp.callback_query(F.data.startswith("movie_"))
async def send_movie(callback: types.CallbackQuery):
    try:
        idx = int(callback.data.split("_")[1])
        movie = movies_cache[idx]
        try:
            await bot.send_document(callback.from_user.id, movie['file_id'], caption=f"🎬 {movie['title']}")
        except:
            await bot.send_video(callback.from_user.id, movie['file_id'], caption=f"🎬 {movie['title']}")
        if 'last_search_msg' in user_sessions.get(callback.from_user.id,{}):
            try:
                await bot.delete_message(callback.from_user.id, user_sessions[callback.from_user.id]['last_search_msg'])
            except: pass
        await callback.answer(f"✅ Sent: {movie['title']}")
    except: await callback.answer("❌ Failed")

# ----------------- Lambda / Webhook -----------------
def lambda_handler(event, context):
    try:
        load_movies()
        load_users()
        if 'body' not in event: return {'statusCode':400,'body':json.dumps({'error':'No body'})}
        update_data = json.loads(event['body'])
        update = types.Update(**update_data)
        asyncio.run(dp.feed_update(bot=bot, update=update))
        return {'statusCode':200,'body':json.dumps({'status':'ok'})}
    except Exception as e:
        return {'statusCode':500,'body':json.dumps({'error': str(e)})}}

# ----------------- Main -----------------
if __name__ == "__main__":
    print("Loading movies...")
    load_movies()
    print("Loading users...")
    load_users()
    print(f"Bot ready with {len(movies_cache)} movies and {len(users_database)} users")
    print("Webhook-ready. Designed for Koyeb 24/7 hosting.")
