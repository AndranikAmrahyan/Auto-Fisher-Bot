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

# https://aistudio.google.com/u/1/usage?project=gen-lang-client-0290532217&timeRange=last-28-days&tab=rate-limit
CAPTCHA_MODEL = "gemini-2.5-flash-lite"

QALAIS_BOT_ID = 6964500387

CMD_START = {"начать", "начинать", "старт", "start", "запуск", "go"}
CMD_STOPS = {"закончить", "завершить", "остановить", "стоп", "stop", "конец", "финиш"}

FISH_CMD = "рыбалка"

# Tunables
FIND_EMOJI_TIMEOUT = 50.0
BOT_RESPONSE_TIMEOUT = 50.0

# Cooldowns
COOLDOWN_AFTER_CLICK = 4.5
MIN_SEND_INTERVAL = 0.8

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
                        logger.debug("Ping OK")
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

bot_msg_queue: asyncio.Queue = asyncio.Queue(maxsize=128)

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
                    pass
    except Exception:
        pass

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
                    pass
    except Exception:
        pass

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
    """Улучшенная функция клика с повторными попытками"""
    MAX_ATTEMPTS = 5
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            mid = getattr(message, "id", None)
            if mid:
                try:
                    fresh = await asyncio.wait_for(
                        client.get_messages(QALAIS_BOT_ID, ids=mid),
                        timeout=3.0
                    )
                    if fresh: message = fresh
                except (asyncio.TimeoutError, Exception):
                    pass

            try:
                await asyncio.wait_for(
                    message.click(flat_index),
                    timeout=5.0
                )
                logger.info(f"✅ Успешный клик по кнопке {flat_index}")
                return True
            except (asyncio.TimeoutError, Exception) as e:
                pass
            
        except Exception:
            pass
        
        if attempt < MAX_ATTEMPTS:
            await asyncio.sleep(0.3 * attempt)
    
    logger.warning(f"❌ Не удалось нажать кнопку {flat_index} после {MAX_ATTEMPTS} попыток")
    return False

async def find_button_index_with_keyword(message, keyword: str):
    flat = []
    for row in getattr(message, "buttons", []):
        for b in row: flat.append(getattr(b, "text", "") or "")
    for i, t in enumerate(flat):
        if keyword.lower() in t.lower():
            return i, t
    
    # Дополнительная проверка с похожими словами
    similar_keywords = ["рыба", "ловить", "удочка", "закинуть", "начать рыбалку"]
    for i, t in enumerate(flat):
        for similar in similar_keywords:
            if similar in t.lower():
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
        return ta == tb
    except Exception:
        return False

async def wait_for_bot_message(after_dt: datetime = None, timeout=BOT_RESPONSE_TIMEOUT, prev_msg=None):
    if after_dt is None: after_dt = datetime.now(timezone.utc) - timedelta(seconds=10)
    deadline = time.time() + timeout
    
    try:
        recent = await asyncio.wait_for(
            client.get_messages(QALAIS_BOT_ID, limit=10),
            timeout=5.0
        )
    except (asyncio.TimeoutError, Exception): 
        recent = []
    
    if recent:
        for m in recent:
            if getattr(m, "date", None) and m.date > after_dt:
                if prev_msg is not None and await _same_message_equiv(m, prev_msg): 
                    continue
                return m

    while time.time() < deadline and not _stop_event.is_set():
        remaining = deadline - time.time()
        try:
            msg = await asyncio.wait_for(
                bot_msg_queue.get(),
                timeout=min(remaining, 2.0)
            )
        except asyncio.TimeoutError: 
            continue
        except Exception: 
            continue
        
        if not msg: 
            continue
        
        mdate = getattr(msg, "date", None)
        if prev_msg is not None and getattr(msg, "id", None) == getattr(prev_msg, "id", None):
            if not await _same_message_equiv(msg, prev_msg): 
                return msg
            else: 
                continue

        if mdate and mdate > after_dt: 
            return msg
        if getattr(msg, "buttons", None): 
            return msg

    return None

async def poll_for_button_emoji(timeout=FIND_EMOJI_TIMEOUT):
    try:
        recent = await asyncio.wait_for(
            client.get_messages(QALAIS_BOT_ID, limit=12),
            timeout=5.0
        )
    except (asyncio.TimeoutError, Exception): 
        recent = []

    if recent:
        for m in recent:
            if m and getattr(m, "buttons", None):
                idx, txt = await find_button_has_emoji(m)
                if idx is not None: 
                    return m, idx, txt

    deadline = time.time() + timeout
    while time.time() < deadline and not _stop_event.is_set():
        remaining = deadline - time.time()
        try:
            msg = await asyncio.wait_for(
                bot_msg_queue.get(),
                timeout=min(remaining, 2.0)
            )
        except asyncio.TimeoutError: 
            continue
        except Exception: 
            continue
        
        if msg and getattr(msg, "buttons", None):
            idx, txt = await find_button_has_emoji(msg)
            if idx is not None: 
                return msg, idx, txt
    
    return None, None, None

# ========== ФУНКЦИИ ДЛЯ ОБРАБОТКИ ЦИКЛА РЫБАЛКИ ==========
async def wait_for_fish_result(fish_msg_id, timeout=25.0):
    """
    Ожидает результат рыбалки, отслеживая редактирование сообщения с ID fish_msg_id
    или появление нового сообщения с результатом
    """
    deadline = time.time() + timeout
    
    while time.time() < deadline and not _stop_event.is_set():
        # Сначала проверяем, не изменилось ли исходное сообщение
        try:
            fresh_msg = await asyncio.wait_for(
                client.get_messages(QALAIS_BOT_ID, ids=fish_msg_id),
                timeout=3.0
            )
            if fresh_msg and fresh_msg.id == fish_msg_id:
                txt = msg_text_lower(fresh_msg)
                if contains_any(txt, CATCH_SUCCESS_KEYWORDS):
                    logger.info("🎣 Сообщение с рыбой отредактировано в результат")
                    return fresh_msg
        except (asyncio.TimeoutError, Exception):
            pass
        
        # Затем проверяем новые сообщения
        try:
            recent = await asyncio.wait_for(
                client.get_messages(QALAIS_BOT_ID, limit=6),
                timeout=3.0
            )
            for msg in recent:
                txt = msg_text_lower(msg)
                if contains_any(txt, CATCH_SUCCESS_KEYWORDS):
                    logger.info("🎣 Найден результат рыбалки в истории")
                    return msg
        except (asyncio.TimeoutError, Exception):
            pass
        
        # Проверяем очередь сообщений
        try:
            msg = await asyncio.wait_for(
                bot_msg_queue.get(),
                timeout=2.0
            )
            if msg:
                txt = msg_text_lower(msg)
                if contains_any(txt, CATCH_SUCCESS_KEYWORDS):
                    logger.info("🎣 Результат рыбалки получен через очередь")
                    return msg
        except asyncio.TimeoutError:
            continue
        except Exception:
            continue
        
        await asyncio.sleep(1.0)
    
    return None

async def click_fish_button_after_result(result_msg, fish_msg_id=None):
    """
    Пытается нажать кнопку "рыбачить" после результата рыбалки
    с отслеживанием возможного редактирования сообщения
    """
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            # Получаем свежую версию сообщения
            if result_msg and hasattr(result_msg, 'id'):
                fresh_msg = await asyncio.wait_for(
                    client.get_messages(QALAIS_BOT_ID, ids=result_msg.id),
                    timeout=3.0
                )
                if fresh_msg:
                    result_msg = fresh_msg
            
            # Ищем кнопку "рыбачить"
            idx, btn_text = await find_button_index_with_keyword(result_msg, "рыбач")
            if idx is not None:
                logger.info(f"🎯 Найдена кнопка 'рыбачить': {btn_text}")
                success = await click_button_by_flat_index(result_msg, idx)
                if success:
                    return True
                else:
                    logger.warning(f"❌ Попытка {attempt+1}: не удалось нажать кнопку")
            else:
                logger.warning(f"❌ Попытка {attempt+1}: кнопка 'рыбачить' не найдена")
                
                # Проверяем, не изменилось ли сообщение
                if attempt < max_attempts - 1:
                    await asyncio.sleep(1.0)
                    
        except (asyncio.TimeoutError, Exception) as e:
            logger.warning(f"❌ Ошибка при попытке {attempt+1}: {e}")
            if attempt < max_attempts - 1:
                await asyncio.sleep(1.0)
    
    return False

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
        logger.error("CAPTCHA: Кнопки не найдены.")
        return False

    tmp = "captcha_tmp.jpg"
    try:
        await asyncio.wait_for(
            client.download_media(message.media, file=tmp),
            timeout=10.0
        )
        
        with open(tmp, "rb") as f:
            image_data = f.read()
        
        prompt = (
            f"Look at the object in this image. "
            f"Select the most appropriate emoji from this list: {unique_options}. "
            f"Return only the emoji character itself."
        )
        
        logger.info(f"CAPTCHA: Запрос к Gemini API... Варианты: {unique_options}")
        
        response = await asyncio.wait_for(
            asyncio.to_thread(
                genai_client.models.generate_content,
                model=CAPTCHA_MODEL,
                contents=[
                    types.Part.from_bytes(data=image_data, mime_type="image/jpeg"),
                    prompt
                ]
            ),
            timeout=15.0
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
            try:
                await asyncio.wait_for(
                    message.click(best_idx),
                    timeout=5.0
                )
                logger.info(f"✅ Капча решена успешно")
                return True
            except (asyncio.TimeoutError, Exception) as e:
                logger.warning(f"❌ Не удалось нажать кнопку капчи: {e}")
                return False
        else:
            logger.warning("CAPTCHA: Соответствующая кнопка не найдена.")
            return False

    except asyncio.TimeoutError:
        logger.warning("CAPTCHA: Таймаут при решении капчи")
        return False
    except Exception as e:
        logger.warning(f"CAPTCHA: Ошибка: {e}")
        return False
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except:
                pass

# ----------------- keywords -----------------
MENU_KEYWORDS = ["меню рыбалки", "уровень рыбака", "поймано рыбы", "уникальные виды"]
FISH_WAIT_KEYWORDS = ["вы закинули удочку в воду",
                      "дождитесь момента, когда рыба зацепится за крючок и подсекайте ее",
                      "у вас будет пару секунд, чтобы подсечь рыбу"]
CATCH_SUCCESS_KEYWORDS = ["вы поймали рыбу", "вы поймали предмет", "поздравляем с удачной рыбалкой",
                          "леска не выдержала и оборвалась", "сорвалась с крючка",
                          "подсечь рыбу"]
CAPTCHA_KEYWORDS = ["нам нужно убедиться, что вы не робот",
                    "нажмите на кнопку с эмодзи, который отображен",
                    "нажмите на кнопку ниже, чтобы продолжить"]

def contains_any(text: str, keywords):
    if not text: return False
    text_lower = text.lower()
    for k in keywords:
        if k in text_lower: return True
    return False

# ========== ОПТИМИЗИРОВАННЫЙ ОСНОВНОЙ ВОРКЕР ==========
async def fisher_worker():
    logger.info("🚀 Fisher worker started")
    fishing_in_progress = False
    last_click_time = None
    last_send_time = None
    consecutive_fails = 0
    last_captcha_time = None  # Время последней капчи

    try:
        while not _stop_event.is_set():
            now = datetime.now(timezone.utc)
            
            # Если слишком много неудач подряд - делаем паузу
            if consecutive_fails >= 3:
                logger.warning(f"⚠️ {consecutive_fails} неудач подряд, пауза 10 секунд")
                await asyncio.sleep(10)
                consecutive_fails = 0
                continue
            
            # Проверяем кулдаун
            if last_click_time and (now - last_click_time).total_seconds() < COOLDOWN_AFTER_CLICK:
                # В режиме ожидания проверяем новые сообщения
                try:
                    menu_msg = await asyncio.wait_for(
                        wait_for_bot_message(timeout=3.0),
                        timeout=3.5
                    )
                except (asyncio.TimeoutError, Exception):
                    menu_msg = None
            else:
                # Отправляем команду рыбалки
                if last_send_time and (now - last_send_time).total_seconds() < MIN_SEND_INTERVAL:
                    await asyncio.sleep(0.3)
                    continue
                    
                try:
                    await asyncio.wait_for(
                        client.send_message(QALAIS_BOT_ID, FISH_CMD),
                        timeout=5.0
                    )
                    last_send_time = datetime.now(timezone.utc)
                    fishing_in_progress = True
                    last_click_time = datetime.now(timezone.utc)
                    consecutive_fails = 0
                    logger.info("🎣 Отправлена команда 'рыбалка'")
                except Exception as e:
                    logger.warning(f"send_message failed: {e}")
                    consecutive_fails += 1
                    await asyncio.sleep(2)
                    continue

                await asyncio.sleep(2.0)
                
                try:
                    menu_msg = await asyncio.wait_for(
                        wait_for_bot_message(timeout=10.0),
                        timeout=10.5
                    )
                except (asyncio.TimeoutError, Exception):
                    menu_msg = None

            if menu_msg is None:
                consecutive_fails += 1
                await asyncio.sleep(1)
                continue

            txt = msg_text_lower(menu_msg)

            # ========== ОПТИМИЗИРОВАННАЯ ЛОГИКА ОБРАБОТКИ ==========
            
            # 1. Капча (самый высокий приоритет)
            if contains_any(txt, CAPTCHA_KEYWORDS):
                logger.info("🔐 Обнаружена капча")
                
                # Запоминаем время капчи
                last_captcha_time = datetime.now(timezone.utc)
                
                # Очищаем очередь сообщений перед решением капчи
                while not bot_msg_queue.empty():
                    try:
                        bot_msg_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                
                success = await solve_captcha_message(menu_msg)
                if success:
                    consecutive_fails = 0
                    # После успешного решения капчи ждем новое сообщение
                    await asyncio.sleep(3.0)
                    
                    # Очищаем очередь снова, чтобы избавиться от старых сообщений
                    while not bot_msg_queue.empty():
                        try:
                            bot_msg_queue.get_nowait()
                        except asyncio.QueueEmpty:
                            break
                    
                    # Сбрасываем состояние, чтобы начать новую рыбалку
                    fishing_in_progress = False
                    last_click_time = None
                    
                    logger.info("✅ Капча решена, начинаем новую рыбалку")
                else:
                    consecutive_fails += 1
                    logger.warning("❌ Не удалось решить капчу")
                
                await asyncio.sleep(1)
                continue
            
            # 2. Меню рыбалки (нужно нажать "рыбачить")
            if contains_any(txt, MENU_KEYWORDS):
                logger.info("📋 Обнаружено меню рыбалки")
                idx, btn_text = await find_button_index_with_keyword(menu_msg, "рыбач")
                
                if idx is None:
                    # Ищем любую активную кнопку
                    for row in getattr(menu_msg, "buttons", []):
                        for i, b in enumerate(row):
                            txt_btn = getattr(b, "text", "") or ""
                            if txt_btn and not txt_btn.isspace():
                                idx = i
                                break
                        if idx is not None:
                            break
                
                if idx is not None:
                    success = await click_button_by_flat_index(menu_msg, idx)
                    if success:
                        fishing_in_progress = True
                        last_click_time = datetime.now(timezone.utc)
                        consecutive_fails = 0
                        logger.info("✅ Нажата кнопка 'рыбачить'")
                        
                        # Ждем сообщение о закинутой удочке
                        await asyncio.sleep(2.0)
                        fish_wait_msg = await wait_for_bot_message(timeout=8.0)
                        if fish_wait_msg:
                            txt_fish = msg_text_lower(fish_wait_msg)
                            if contains_any(txt_fish, FISH_WAIT_KEYWORDS):
                                logger.info("🎣 Удочка закинута, ждем рыбу...")
                                # Ищем кнопку с рыбой
                                found_msg, found_idx, found_text = await poll_for_button_emoji(timeout=25.0)
                                if found_msg:
                                    logger.info(f"🐟 Найдена кнопка с рыбой: {found_text}")
                                    fish_msg_id = found_msg.id
                                    
                                    # Нажимаем на рыбу
                                    success_fish = await click_button_by_flat_index(found_msg, found_idx)
                                    if success_fish:
                                        logger.info("✅ Нажата кнопка с рыбой")
                                        last_click_time = datetime.now(timezone.utc)
                                        
                                        # Ждем результат рыбалки, отслеживая редактирование сообщения
                                        await asyncio.sleep(2.0)
                                        
                                        result_msg = await wait_for_fish_result(fish_msg_id, timeout=20.0)
                                        
                                        if result_msg:
                                            logger.info("🎣 Получен результат рыбалки")
                                            
                                            # Пытаемся нажать кнопку "рыбачить" после результата
                                            fish_button_success = await click_fish_button_after_result(result_msg, fish_msg_id)
                                            
                                            if fish_button_success:
                                                fishing_in_progress = True
                                                last_click_time = datetime.now(timezone.utc)
                                                consecutive_fails = 0
                                                logger.info("✅ Нажата кнопка 'рыбачить' после результата")
                                                
                                                # Короткая пауза перед продолжением
                                                await asyncio.sleep(1.5)
                                                continue
                                            else:
                                                logger.warning("❌ Не удалось нажать 'рыбачить' после результата")
                                                # Пробуем начать новую рыбалку через кулдаун
                                                fishing_in_progress = False
                                                consecutive_fails += 1
                                        else:
                                            logger.warning("❌ Результат рыбалки не получен")
                                            consecutive_fails += 1
                                    else:
                                        logger.warning("❌ Не удалось нажать кнопку с рыбой")
                                        consecutive_fails += 1
                                else:
                                    logger.warning("❌ Кнопка с рыбой не найдена")
                                    consecutive_fails += 1
                            else:
                                # Возможно, это капча
                                if contains_any(txt_fish, CAPTCHA_KEYWORDS):
                                    await solve_captcha_message(fish_wait_msg)
                                consecutive_fails += 1
                        else:
                            consecutive_fails += 1
                    else:
                        consecutive_fails += 1
                else:
                    consecutive_fails += 1
                
                await asyncio.sleep(0.5)
                continue
            
            # 3. Ожидание поклевки (сообщение с FISH_WAIT_KEYWORDS)
            if contains_any(txt, FISH_WAIT_KEYWORDS):
                logger.info("🎣 Обнаружено ожидание поклевки")
                
                # Ищем кнопку с рыбой в текущем сообщении
                idx, btn_text = await find_button_has_emoji(menu_msg)
                if idx is not None:
                    found_msg, found_idx, found_text = menu_msg, idx, btn_text
                else:
                    # Ищем в истории
                    found_msg, found_idx, found_text = await poll_for_button_emoji(timeout=20.0)
                
                if found_msg and found_idx is not None:
                    logger.info(f"🐟 Найдена кнопка с рыбой: {found_text}")
                    fish_msg_id = found_msg.id
                    
                    # Нажимаем на рыбу
                    success_fish = await click_button_by_flat_index(found_msg, found_idx)
                    if success_fish:
                        logger.info("✅ Нажата кнопка с рыбой")
                        last_click_time = datetime.now(timezone.utc)
                        
                        # Ждем результат рыбалки, отслеживая редактирование сообщения
                        await asyncio.sleep(2.0)
                        
                        result_msg = await wait_for_fish_result(fish_msg_id, timeout=20.0)
                        
                        if result_msg:
                            logger.info("🎣 Получен результат рыбалки")
                            
                            # Пытаемся нажать кнопку "рыбачить" после результата
                            fish_button_success = await click_fish_button_after_result(result_msg, fish_msg_id)
                            
                            if fish_button_success:
                                fishing_in_progress = True
                                last_click_time = datetime.now(timezone.utc)
                                consecutive_fails = 0
                                logger.info("✅ Нажата кнопка 'рыбачить' после результата")
                                
                                # Короткая пауза перед продолжением
                                await asyncio.sleep(1.5)
                                continue
                            else:
                                logger.warning("❌ Не удалось нажать 'рыбачить' после результата")
                                # Пробуем начать новую рыбалку через кулдаун
                                fishing_in_progress = False
                                consecutive_fails += 1
                        else:
                            logger.warning("❌ Результат рыбалки не получен")
                            consecutive_fails += 1
                    else:
                        logger.warning("❌ Не удалось нажать кнопку с рыбой")
                        consecutive_fails += 1
                else:
                    logger.warning("❌ Кнопка с рыбой не найдена")
                    consecutive_fails += 1
                
                await asyncio.sleep(0.5)
                continue
            
            # 4. Результат рыбалки (если мы пропустили предыдущие шаги)
            if contains_any(txt, CATCH_SUCCESS_KEYWORDS):
                logger.info("🎣 Обнаружен результат рыбалки")
                
                # Пытаемся нажать кнопку "рыбачить"
                fish_button_success = await click_fish_button_after_result(menu_msg)
                
                if fish_button_success:
                    fishing_in_progress = True
                    last_click_time = datetime.now(timezone.utc)
                    consecutive_fails = 0
                    logger.info("✅ Нажата кнопка 'рыбачить' после результата")
                    await asyncio.sleep(1.5)
                    continue
                else:
                    logger.warning("❌ Не удалось нажать 'рыбачить' после результата")
                    fishing_in_progress = False
                    consecutive_fails += 1
                
                await asyncio.sleep(0.5)
                continue
            
            # 5. Неопознанное состояние
            consecutive_fails += 1
            logger.warning(f"❓ Неизвестное состояние: {txt[:50]}...")
            await asyncio.sleep(1)

    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.exception(f"❌ Critical error in fisher_worker: {e}")
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
    logger.info("Connecting to Telegram...")
    
    # Несколько попыток подключения
    for attempt in range(1, 6):
        try:
            await client.start()
            logger.info("✅ Подключение к Telegram успешно")
            break
        except Exception as e:
            logger.warning(f"⚠️ Попытка {attempt}/5 подключения не удалась: {e}")
            if attempt < 5:
                await asyncio.sleep(5 * attempt)
            else:
                logger.error("❌ Не удалось подключиться к Telegram после 5 попыток")
                return
    
    # Запускаем задачу самопингования
    if RENDER_APP_URL:
        asyncio.create_task(self_ping())
        logger.info("🔄 Самопинг запущен")
    else:
        logger.warning("⚠️ RENDER_APP_URL не задан, самопингование отключено.")

    logger.info("🤖 Бот запущен. Отправьте 'начать' в личном чате с игровым ботом.")
    
    # Очищаем очередь при старте
    while not bot_msg_queue.empty():
        try:
            bot_msg_queue.get_nowait()
        except asyncio.QueueEmpty:
            break
    
    await client.run_until_disconnected()

if __name__ == "__main__":
    # Запускаем веб-сервер Flask в отдельном потоке
    web_thread = threading.Thread(target=run_web_server, daemon=True)
    web_thread.start()
    logger.info("🌐 Веб-сервер запущен")
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Завершение работы по запросу пользователя")
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}")
    finally:
        logger.info("👋 Бот остановлен")
