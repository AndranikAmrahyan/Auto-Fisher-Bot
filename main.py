# main.py | Auto Fisher Bot + Render Keep-Alive
import os
import re
import time
import asyncio
import logging
import threading
from datetime import datetime, timedelta, timezone

# Сторонние библиотеки
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from PIL import Image
import aiohttp
from flask import Flask

# Google GenAI (новая версия)
from google import genai
from google.genai import types

# ----------------- Настройка -----------------
load_dotenv()

# --- Config for Telethon ---
SESSION_STRING = os.getenv("SESSION_STRING_SERVER")
API_ID = int(os.getenv("API_ID") or 0)
API_HASH = os.getenv("API_HASH") or ""
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# --- Config for Render Keep-Alive ---
RENDER_APP_URL = os.getenv("RENDER_APP_URL") # Например: https://my-bot.onrender.com

if not SESSION_STRING:
    raise RuntimeError("SESSION_STRING_SERVER not found in environment")

if API_ID == 0 or API_HASH == "":
    print("⚠️ Укажи API_ID и API_HASH в .env.")

# Инициализация Gemini
genai_client = None
if not GEMINI_API_KEY:
    print("⚠️ ВНИМАНИЕ: Не найден GEMINI_API_KEY. Решение капчи работать не будет!")
else:
    genai_client = genai.Client(api_key=GEMINI_API_KEY)

QALAIS_BOT_ID = 6964500387

CMD_START = {"начать", "начинать", "старт", "start", "запуск", "go"}
CMD_STOPS = {"закончить", "завершить", "остановить", "стоп", "stop", "конец", "финиш"}

FISH_CMD = "рыбалка"

# Tunables
FIND_EMOJI_TIMEOUT = 35.0
BOT_RESPONSE_TIMEOUT = 35.0

# Cooldowns
COOLDOWN_AFTER_CLICK = 3.5
MIN_SEND_INTERVAL = 0.6

# Логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("auto_fisher")
# Отключаем лишний шум
logging.getLogger("telethon").setLevel(logging.WARNING)
logging.getLogger("werkzeug").setLevel(logging.WARNING) # Логи Flask

# ----------------- Flask Server (Keep-Alive) -----------------
app_flask = Flask(__name__)

@app_flask.route("/")
def home():
    return "Bot is running!", 200

@app_flask.route("/ping")
def ping():
    return "pong", 200

def run_web_server():
    # Render предоставляет порт через переменную окружения PORT
    port = int(os.environ.get("PORT", 8080))
    app_flask.run(host="0.0.0.0", port=port)

async def self_ping():
    """Периодически пингует сам себя, чтобы Render не усыплял сервис."""
    if not RENDER_APP_URL:
        logger.warning("⚠️ RENDER_APP_URL не задан! Бот может уснуть.")
        return

    logger.info(f"🔄 Self-ping запущен для: {RENDER_APP_URL}")
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(f"{RENDER_APP_URL}/ping") as resp:
                    if resp.status == 200:
                        logger.info("Ping OK")
                    else:
                        logger.warning(f"Ping failed with status: {resp.status}")
            except Exception as e:
                logger.error(f"Ping error: {str(e)}")
            
            # Ждем 3 минуты (180 сек)
            await asyncio.sleep(180)

# ----------------- Telethon client -----------------
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

_worker_task = None
_worker_running = False
_stop_event = asyncio.Event()

bot_msg_queue: asyncio.Queue = asyncio.Queue(maxsize=64)

# ----------------- Event handlers -----------------
def _resolve_peer_user_id(msg):
    try:
        peer = getattr(msg, "peer_id", None)
        if peer is None: return None
        return getattr(peer, "user_id", None)
    except Exception:
        return None

def is_private_with_bot(msg):
    try:
        peer_user = _resolve_peer_user_id(msg)
        if peer_user == QALAIS_BOT_ID: return True
        if getattr(msg, "chat_id", None) == QALAIS_BOT_ID: return True
        sender_id = getattr(msg, "sender_id", None)
        if sender_id == QALAIS_BOT_ID: return False
        if getattr(msg, "from_id", None):
            fid = getattr(msg.from_id, "user_id", None)
            if fid == QALAIS_BOT_ID:
                return _resolve_peer_user_id(msg) == QALAIS_BOT_ID or getattr(msg, "chat_id", None) == QALAIS_BOT_ID
    except Exception:
        return False
    return False

@client.on(events.NewMessage(incoming=True, chats=QALAIS_BOT_ID))
async def _on_any_new_message(event):
    try:
        m = event.message
        if is_private_with_bot(m):
            try:
                bot_msg_queue.put_nowait(m)
            except asyncio.QueueFull:
                try:
                    _ = bot_msg_queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    bot_msg_queue.put_nowait(m)
                except asyncio.QueueFull:
                    logger.warning("Queue still full after cleanup, dropping message")
    except Exception as e:
        logger.debug("error in _on_any_new_message: %s", e)

@client.on(events.MessageEdited(chats=QALAIS_BOT_ID))
async def _on_any_edited_message(event):
    try:
        m = getattr(event, "message", None) or await event.get_message()
        if not m: return
        if is_private_with_bot(m):
            try:
                bot_msg_queue.put_nowait(m)
            except asyncio.QueueFull:
                try:
                    _ = bot_msg_queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    bot_msg_queue.put_nowait(m)
                except asyncio.QueueFull:
                    logger.warning("Queue still full after cleanup, dropping edited message")
    except Exception as e:
        logger.debug("error in _on_any_edited_message: %s", e)

# ----------------- Utils -----------------
EMOJI_RE = re.compile(
    "[" 
    "\U0001F300-\U0001F5FF"
    "\U0001F600-\U0001F64F"
    "\U0001F680-\U0001F6FF"
    "\U0001F700-\U0001F77F"
    "\U0001F780-\U0001F7FF"
    "\U0001F900-\U0001F9FF"
    "\U0001FA00-\U0001FA6F"
    "]+",
    flags=re.UNICODE,
)

def msg_text_lower(message) -> str:
    try:
        return (message.message or message.raw_text or "").lower()
    except Exception:
        return ""

async def click_button_by_flat_index(message, flat_index: int) -> bool:
    MAX_ATTEMPTS = 3
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            mid = getattr(message, "id", None)
            if mid:
                try:
                    fresh = await client.get_messages(QALAIS_BOT_ID, ids=mid)
                    if fresh: message = fresh
                except Exception: pass

            try:
                await message.click(flat_index)
                return True
            except Exception: pass

            # try row/col fallback
            try:
                cum = 0
                for ri, row in enumerate(getattr(message, "buttons", [])):
                    if flat_index < cum + len(row):
                        ci = flat_index - cum
                        await message.click((ri, ci))
                        return True
                    cum += len(row)
            except Exception: pass
            
            # fallback send text
            try:
                flat_buttons = []
                for row in getattr(message, "buttons", []):
                    for b in row: flat_buttons.append(getattr(b, "text", "") or "")
                if 0 <= flat_index < len(flat_buttons) and flat_buttons[flat_index]:
                    await client.send_message(message.chat_id, flat_buttons[flat_index], reply_to=message.id)
                    return True
            except Exception: pass
            
        except Exception: pass
        await asyncio.sleep(0.15)
    return False

async def find_button_index_with_keyword(message, keyword: str):
    flat = []
    for row in getattr(message, "buttons", []):
        for b in row: flat.append(getattr(b, "text", "") or "")
    for i, t in enumerate(flat):
        if keyword.lower() in t.lower():
            return i, t
    return None, None

async def find_button_has_emoji(message):
    flat = []
    for row in getattr(message, "buttons", []):
        for b in row: flat.append((getattr(b, "text", "") or "").strip())

    button_stats = {}
    for s in flat:
        if s: button_stats[s] = button_stats.get(s, 0) + 1
    
    most_common = max(button_stats.items(), key=lambda x: x[1])[0] if button_stats else ""
    
    for i, s in enumerate(flat):
        if not s: continue
        if all(ch in ("\u2800", "⠀") for ch in s): continue
        if any(ch.isalpha() for ch in s.lower()): continue
        if s != most_common and len(s) <= 3:
            return i, s
    return None, None

# ----------------- Waiters -----------------
async def _same_message_equiv(a, b) -> bool:
    if a is None or b is None: return False
    try:
        if getattr(a, "id", None) != getattr(b, "id", None): return False
        ta = (a.message or a.raw_text or "") or ""
        tb = (b.message or b.raw_text or "") or ""
        if ta != tb: return False
        ba = [[getattr(x, "text", "") or "" for x in row] for row in getattr(a, "buttons", [])]
        bb = [[getattr(x, "text", "") or "" for x in row] for row in getattr(b, "buttons", [])]
        return ba == bb
    except Exception: return False

async def wait_for_bot_message(after_dt: datetime = None, timeout=BOT_RESPONSE_TIMEOUT, prev_msg=None):
    # Добавляем запас в 2 секунды назад, чтобы не пропустить сообщения из-за разницы во времени серверов
    if after_dt is None: 
        after_dt = datetime.now(timezone.utc) - timedelta(seconds=2)
    else:
        after_dt = after_dt - timedelta(seconds=2)
        
    deadline = time.time() + timeout
    
    # Сначала проверяем последние сообщения, вдруг ответ уже пришел
    try:
        recent = await client.get_messages(QALAIS_BOT_ID, limit=8)
        if recent:
            for m in recent:
                if getattr(m, 'date', None) and m.date > after_dt:
                    if prev_msg is not None and getattr(m, 'id', None) == getattr(prev_msg, 'id', None):
                        # Если это то же сообщение, проверяем, изменились ли кнопки
                        if not await _same_message_equiv(m, prev_msg):
                            return m
                        continue
                    return m
    except Exception: pass

    while time.time() < deadline and not _stop_event.is_set():
        remaining = deadline - time.time()
        try:
            msg = await asyncio.wait_for(bot_msg_queue.get(), timeout=min(remaining, 2.0))
        except asyncio.TimeoutError: continue
        
        if not msg: continue
        
        # Если пришло обновление того же сообщения (редактирование)
        if prev_msg is not None and getattr(msg, 'id', None) == getattr(prev_msg, 'id', None):
            if not await _same_message_equiv(msg, prev_msg):
                return msg
            continue

        mdate = getattr(msg, 'date', None)
        if mdate and mdate > after_dt:
            return msg

    return None

async def poll_for_button_emoji(timeout=FIND_EMOJI_TIMEOUT):
    try:
        recent = await client.get_messages(QALAIS_BOT_ID, limit=8)
    except Exception: recent = []

    if recent:
        for m in recent:
            if m and getattr(m, "buttons", None):
                idx, txt = await find_button_has_emoji(m)
                if idx is not None: return m, idx, txt

    deadline = time.time() + timeout
    while time.time() < deadline and not _stop_event.is_set():
        remaining = deadline - time.time()
        try:
            msg = await asyncio.wait_for(bot_msg_queue.get(), timeout=min(remaining, BOT_RESPONSE_TIMEOUT))
        except asyncio.TimeoutError: return None, None, None
        except Exception: continue
        
        if msg and getattr(msg, "buttons", None):
            idx, txt = await find_button_has_emoji(msg)
            if idx is not None: return msg, idx, txt
    return None, None, None

# ----------------- Решение капчи (Google GenAI) -----------------
async def solve_captcha_message(message) -> bool:
    if not genai_client:
        logger.error("CAPTCHA: Клиент Gemini не инициализирован.")
        return False

    flat_buttons = []
    for row in getattr(message, "buttons", []):
        for b in row:
            txt = getattr(b, "text", None)
            flat_buttons.append(txt.strip() if txt else "")

    unique_options = [b for b in flat_buttons if b and not b.isspace()]
    
    if not unique_options:
        logger.info("CAPTCHA: Кнопки не найдены.")
        return False

    tmp = "captcha_tmp.jpg"
    try:
        await client.download_media(message.media, file=tmp)
        with open(tmp, "rb") as f:
            image_data = f.read()
        
        prompt = (
            f"Look at the object in this image. "
            f"Select the most appropriate emoji from this list: {unique_options}. "
            f"Return only the emoji character itself."
        )
        
        logger.info(f"CAPTCHA: Запрос к Gemini API... Варианты: {unique_options}")
        
        response = await asyncio.to_thread(
            genai_client.models.generate_content,
            model="gemini-1.5-flash",
            contents=[
                types.Part.from_bytes(data=image_data, mime_type="image/jpeg"),
                prompt
            ]
        )
        
        predicted_emoji = response.text.strip()
        logger.info(f"CAPTCHA: Ответ API: '{predicted_emoji}'")

        best_idx = -1
        for i, btn_txt in enumerate(flat_buttons):
            if predicted_emoji in btn_txt:
                best_idx = i
                break
        
        if best_idx != -1:
            logger.info(f"CAPTCHA: Нажимаем кнопку {best_idx}")
            return await click_button_by_flat_index(message, best_idx)
        else:
            logger.warning("CAPTCHA: Соответствующая кнопка не найдена.")
            return False

    except Exception as e:
        logger.warning(f"CAPTCHA: Ошибка: {e}")
        return False
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)

# ----------------- keywords -----------------
MENU_KEYWORDS = ["меню рыбалки", "уровень рыбака", "поймано рыбы", "уникальные виды"]
FISH_WAIT_KEYWORDS = ["вы закинули удочку в воду", "подсекайте ее", "подсечь рыбу"]
CATCH_SUCCESS_KEYWORDS = ["вы поймали", "поздравляем", "сорвалась", "не успели", "оборвалась"]
CAPTCHA_KEYWORDS = ["нам нужно убедиться", "нажмите на кнопку с эмодзи", "нажмите на кнопку ниже"]

def contains_any(text: str, keywords):
    if not text: return False
    for k in keywords:
        if k in text: return True
    return False

# ----------------- Основной воркер -----------------
async def fisher_worker():
    logger.info("🚀 Fisher worker started")
    fishing_in_progress = False
    last_click_time = None
    last_send_time = None

    try:
        while not _stop_event.is_set():
            # Получаем актуальный текст сообщения
            txt = msg_text_lower(menu_msg)
            
            # 1. ПРОВЕРКА КАПЧИ
            if contains_any(txt, CAPTCHA_KEYWORDS):
                await solve_captcha_message(menu_msg)
                fishing_in_progress = False 
                # Ждем обновления после капчи
                menu_msg = await wait_for_bot_message(timeout=15, prev_msg=menu_msg) or menu_msg
                continue
    
            # 2. ЕСЛИ ИДЕТ РЫБАЛКА (ждем рыбу)
            if fishing_in_progress:
                # poll_for_button_emoji сам ждет появления кнопки с рыбой
                found_msg, found_idx, found_text = await poll_for_button_emoji(timeout=FIND_EMOJI_TIMEOUT)
                
                if found_msg:
                    # Нажимаем на рыбу
                    await click_button_by_flat_index(found_msg, found_idx)
                    fishing_in_progress = False # Сбрасываем флаг, так как фаза ожидания рыбы окончена
                    
                    # Ждем, когда появится сообщение об улове (CATCH_SUCCESS) или неудаче
                    res = await wait_for_bot_message(timeout=15, prev_msg=found_msg)
                    if res:
                        menu_msg = res
                    continue
                else:
                    # Если за FIND_EMOJI_TIMEOUT рыба не появилась, сбрасываем состояние
                    fishing_in_progress = False
                    continue
    
            # 3. ЕСЛИ МЫ В МЕНЮ ИЛИ ПОСЛЕ УЛОВА (ищем кнопку "Рыбачить")
            idx, btn_text = await find_button_index_with_keyword(menu_msg, "рыбач")
            if idx is not None:
                # Очистка очереди перед важным кликом (для Render)
                while not bot_msg_queue.empty(): 
                    try: bot_msg_queue.get_nowait()
                    except asyncio.QueueEmpty: break
                
                success = await click_button_by_flat_index(menu_msg, idx)
                if success:
                    fishing_in_progress = True
                    last_click_time = datetime.now(timezone.utc)
                    # Даем время на анимацию заброса
                    await asyncio.sleep(1.5)
                    # Ждем изменения сообщения на "Вы закинули удочку..."
                    res = await wait_for_bot_message(timeout=15, prev_msg=menu_msg)
                    if res:
                        menu_msg = res
                continue
    
            # 4. СТРАХОВКА (если кнопок нет или бот завис)
            now = datetime.now(timezone.utc)
            if (now - last_send_time).total_seconds() >= 60: # Если 1 минуту ничего не происходило
                try:
                    await client.send_message(QALAIS_BOT_ID, FISH_CMD)
                    last_send_time = now
                    # Ждем любое новое сообщение от бота
                    res = await wait_for_bot_message(timeout=10)
                    if res:
                        menu_msg = res
                        fishing_in_progress = False
                except Exception: pass
    
            await asyncio.sleep(1.0)

    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.exception("❌ Unexpected error in fisher_worker: %s", e)
    finally:
        logger.info("🛑 Fisher worker stopped")

# ----------------- Commands -----------------
CMD_START_PATTERN = r'(?i)^(' + '|'.join(re.escape(cmd) for cmd in CMD_START) + r')$'

@client.on(events.NewMessage(outgoing=True, chats=QALAIS_BOT_ID, pattern=CMD_START_PATTERN))
async def cmd_start(event):
    global _worker_task, _worker_running, _stop_event, bot_msg_queue
    if _worker_running:
        await event.reply("Бот уже запущен.")
        return
    
    # Очищаем очередь сообщений перед запуском
    while not bot_msg_queue.empty():
        try:
            bot_msg_queue.get_nowait()
        except asyncio.QueueEmpty:
            break
    
    _stop_event.clear()
    _worker_running = True
    _worker_task = asyncio.create_task(fisher_worker())
    logger.info("✅ Авто-рыбалка запущена по команде")
    await event.reply("✅ Авто-рыбалка запущена.")

@client.on(events.NewMessage(outgoing=True, chats=QALAIS_BOT_ID))
async def cmd_stop_listener(event):
    global _worker_task, _worker_running, _stop_event, bot_msg_queue
    txt = (event.raw_text or "").strip().lower()
    if txt in CMD_STOPS:
        if not _worker_running:
            await event.reply("Бот не запущен.")
            return
        
        logger.info("🛑 Получена команда остановки")
        _stop_event.set()
        
        if _worker_task:
            # Даем воркеру время завершиться корректно
            try:
                await asyncio.wait_for(_worker_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                # Если не завершился за 5 сек, отменяем
                _worker_task.cancel()
                try:
                    await _worker_task
                except asyncio.CancelledError:
                    pass
        
        # Очищаем очередь сообщений после остановки
        while not bot_msg_queue.empty():
            try:
                bot_msg_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        
        _worker_running = False
        _worker_task = None
        await event.reply("⛔ Авто-рыбалка остановлена.")

async def main():
    print("Connecting to Telegram...")
    await client.start()
    
    # Запускаем задачу самопингования
    if RENDER_APP_URL:
        asyncio.create_task(self_ping())
    else:
        print("⚠️ RENDER_APP_URL не задан, самопингование отключено.")

    print("Client started. Send 'начать' (in the private chat with the game bot) to run.")
    await client.run_until_disconnected()

if __name__ == "__main__":
    # Запускаем веб-сервер Flask в отдельном потоке
    threading.Thread(target=run_web_server, daemon=True).start()
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrupted, exiting...")
