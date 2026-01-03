# main.py | Auto Fisher Bot + Render Keep-Alive
import os
import re
import time
import asyncio
import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Optional

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

# ========== МОДЕЛИ ДЛЯ КАПЧИ ==========
# https://aistudio.google.com/u/1/usage?project=gen-lang-client-0290532217&timeRange=last-1-day&tab=rate-limit
CAPTCHA_MODELS = [
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite", 
    "gemini-robotics-er-1.5-preview"
]
current_model_index = 0  # Начинаем с первой модели
successful_model_index = None  # Индекс успешной модели

SUPPORT_CONTACT = "@andranik_amrahyan"  # Контакт поддержки

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

bot_msg_queue: asyncio.Queue = asyncio.Queue(maxsize=128)

# ========== ФУНКЦИИ УПРАВЛЕНИЯ МОДЕЛЯМИ КАПЧИ ==========
async def rotate_captcha_model() -> bool:
    """Переключает на следующую модель капчи. Возвращает True если есть еще модели, False если все исчерпаны."""
    global current_model_index, successful_model_index
    
    logger.info(f"🔄 Ротация модели капчи. Текущая: {CAPTCHA_MODELS[current_model_index]}")
    
    # Сохраняем начальный индекс для проверки полного цикла
    start_index = current_model_index
    
    # Пробуем следующую модель
    next_index = (current_model_index + 1) % len(CAPTCHA_MODELS)
    
    # Если мы вернулись к успешной модели или прошли полный цикл
    if next_index == start_index:
        logger.error("❌ Все модели капчи исчерпаны!")
        return False
    
    current_model_index = next_index
    logger.info(f"✅ Переключено на модель: {CAPTCHA_MODELS[current_model_index]}")
    return True

async def get_current_captcha_model() -> str:
    """Возвращает текущую модель капчи."""
    global current_model_index
    return CAPTCHA_MODELS[current_model_index]

def set_successful_captcha_model():
    """Сохраняет текущую модель как успешную."""
    global successful_model_index, current_model_index
    successful_model_index = current_model_index
    logger.info(f"💾 Сохранена успешная модель: {CAPTCHA_MODELS[successful_model_index]}")

# Переменные для отслеживания повторяющихся некритических ошибок
last_captcha_error_type = None
captcha_error_count = 0

async def stop_bot_with_captcha_error(error_message: str, is_limit_exhausted: bool = False):
    """Останавливает бота с сообщением об ошибке капчи."""
    global _worker_task, _worker_running, _stop_event, _worker_task
    
    logger.error(f"🛑 Остановка бота из-за ошибки капчи: {error_message}")
    
    if is_limit_exhausted:
        message_text = (
            "❌ Достигнут лимит всех моделей для решения капчи!\n\n"
            "Все доступные модели ИИ исчерпали свои лимиты:\n"
            f"- {', '.join(CAPTCHA_MODELS)}\n\n"
            "⛔ Авто-рыбалка остановлена.\n"
            "Попробуйте снова через некоторое время."
        )
    else:
        message_text = (
            "❌ Критическая ошибка при решении капчи!\n\n"
            f"Ошибка: {error_message}\n\n"
            "⚠️ Пожалуйста, свяжитесь со службой поддержки и сообщите об этой ошибке.\n"
            f"Поддержка: {SUPPORT_CONTACT}\n\n"
            "⛔ Авто-рыбалка остановлена."
        )
    
    # Отправляем сообщение об ошибке в чат
    if error_message:
        try:
            await client.send_message(QALAIS_BOT_ID, message_text)
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение об ошибке: {e}")
    
    # Останавливаем воркер
    if _worker_running:
        _stop_event.set()
        if _worker_task:
            _worker_task.cancel()
            try:
                await _worker_task
            except asyncio.CancelledError:
                pass
            _worker_task = None
        
        # Очищаем очередь сообщений
        while not bot_msg_queue.empty():
            try:
                bot_msg_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        
        _worker_running = False
        logger.error("🛑 Бот остановлен из-за ошибки капчи")

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

# ========== УЛУЧШЕННОЕ РЕШЕНИЕ КАПЧИ С РОТАЦИЕЙ МОДЕЛЕЙ ==========
async def solve_captcha_message(message) -> Optional[bool]:
    """
    Решает капчу с ротацией моделей.
    Возвращает:
    - True: капча решена успешно
    - False: капча не решена (но не критическая ошибка)
    - None: критическая ошибка, бот должен остановиться
    """
    global last_captcha_error_type, captcha_error_count, current_model_index, successful_model_index
    
    if not genai_client:
        logger.error("CAPTCHA: Клиент Gemini не инициализирован.")
        await stop_bot_with_captcha_error("Клиент Gemini не инициализирован")
        return None

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
    
    # Загружаем изображение капчи
    try:
        await asyncio.wait_for(
            client.download_media(message.media, file=tmp),
            timeout=10.0
        )
        
        with open(tmp, "rb") as f:
            image_data = f.read()
    except Exception as e:
        logger.warning(f"CAPTCHA: Ошибка загрузки изображения: {e}")
        # Проверяем, была ли такая же ошибка в прошлый раз
        if last_captcha_error_type == "image_load_error":
            captcha_error_count += 1
            if captcha_error_count >= 2:
                logger.error("CAPTCHA: Повторная ошибка загрузки изображения капчи!")
                error_message = (
                    "❌ Критическая ошибка при решении капчи!\n\n"
                    "Не удалось загрузить изображение капчи дважды подряд.\n\n"
                    "⚠️ Пожалуйста, свяжитесь со службой поддержки и сообщите об этой ошибке.\n"
                    f"Поддержка: {SUPPORT_CONTACT}\n\n"
                    "⛔ Авто-рыбалка остановлена."
                )
                try:
                    await client.send_message(QALAIS_BOT_ID, error_message)
                except Exception as send_err:
                    logger.error(f"Не удалось отправить сообщение об ошибке: {send_err}")
                
                # Останавливаем бота
                await stop_bot_with_captcha_error("Повторная ошибка загрузки изображения капчи")
                return None
        else:
            last_captcha_error_type = "image_load_error"
            captcha_error_count = 1
        return False
    
    prompt = (
        f"Look at the object in this image. "
        f"Select the most appropriate emoji from this list: {unique_options}. "
        f"Return only the emoji character itself."
    )
    
    # Сохраняем начальный индекс для проверки полного цикла
    start_model_index = current_model_index
    models_tried = 0
    
    # Пробуем решить капчу с ротацией моделей
    while models_tried < len(CAPTCHA_MODELS):
        current_model = await get_current_captcha_model()
        logger.info(f"🔍 CAPTCHA: Используем модель {current_model} (попытка {models_tried + 1}/{len(CAPTCHA_MODELS)})")
        
        try:
            response = await asyncio.wait_for(
                asyncio.to_thread(
                    genai_client.models.generate_content,
                    model=current_model,
                    contents=[
                        types.Part.from_bytes(data=image_data, mime_type="image/jpeg"),
                        prompt
                    ]
                ),
                timeout=15.0
            )
            
            predicted_emoji = response.text.strip()
            logger.info(f"✅ CAPTCHA: Ответ API: '{predicted_emoji}'")
            
            best_idx = -1
            for i, btn_txt in enumerate(flat_buttons):
                if predicted_emoji in btn_txt:
                    best_idx = i
                    break
            
            if best_idx != -1:
                logger.info(f"🎯 CAPTCHA: Нажимаем кнопку {best_idx}")
                try:
                    await asyncio.wait_for(
                        message.click(best_idx),
                        timeout=5.0
                    )
                    logger.info(f"✅ Капча решена успешно с моделью {current_model}")
                    
                    # Сбрасываем счетчик ошибок при успешном решении
                    last_captcha_error_type = None
                    captcha_error_count = 0
                    
                    # Сохраняем успешную модель для будущего использования
                    set_successful_captcha_model()
                    
                    # Если использовали не первую модель, возвращаемся к ней для следующих капч
                    if successful_model_index is not None and successful_model_index != 0:
                        current_model_index = successful_model_index
                        logger.info(f"🔄 Возвращаемся к успешной модели: {CAPTCHA_MODELS[current_model_index]}")
                    
                    return True
                except (asyncio.TimeoutError, Exception) as e:
                    logger.warning(f"❌ Не удалось нажать кнопку капчи: {e}")
                    
                    # Проверяем, была ли такая же ошибка в прошлый раз
                    if last_captcha_error_type == "button_click_error":
                        captcha_error_count += 1
                        if captcha_error_count >= 2:
                            logger.error("CAPTCHA: Повторная ошибка нажатия кнопки капчи!")
                            error_message = (
                                "❌ Критическая ошибка при решении капчи!\n\n"
                                "Не удалось нажать кнопку капчи дважды подряд.\n\n"
                                "⚠️ Пожалуйста, свяжитесь со службой поддержки и сообщите об этой ошибке.\n"
                                f"Поддержка: {SUPPORT_CONTACT}\n\n"
                                "⛔ Авто-рыбалка остановлена."
                            )
                            try:
                                await client.send_message(QALAIS_BOT_ID, error_message)
                            except Exception as send_err:
                                logger.error(f"Не удалось отправить сообщение об ошибке: {send_err}")
                            
                            # Останавливаем бота
                            await stop_bot_with_captcha_error("Повторная ошибка нажатия кнопки капчи")
                            return None
                    else:
                        last_captcha_error_type = "button_click_error"
                        captcha_error_count = 1
                    return False
            else:
                logger.error("❌ CAPTCHA: Соответствующая кнопка не найдена в ответе API")
                
                # Отправляем сообщение пользователю о необходимости решить капчу вручную
                error_message = (
                    "❌ Не удалось решить капчу автоматически.\n\n"
                    "Пожалуйста, решите капчу вручную и снова запустите авто рыбалку.\n"
                    "Если это случается часто, свяжитесь со службой поддержки.\n"
                    f"Поддержка: {SUPPORT_CONTACT}\n\n"
                    "⛔ Авто-рыбалка остановлена."
                )
                try:
                    await client.send_message(QALAIS_BOT_ID, error_message)
                except Exception as send_err:
                    logger.error(f"Не удалось отправить сообщение об ошибке: {send_err}")
                
                # Останавливаем бота
                await stop_bot_with_captcha_error("")
                return None
                
        except Exception as e:
            error_str = str(e)
            logger.warning(f"⚠️ CAPTCHA: Ошибка с моделью {current_model}: {error_str}")
            
            # Проверяем тип ошибки
            is_404_error = '404' in error_str and 'NOT_FOUND' in error_str.upper()
            is_resource_exhausted = 'RESOURCE_EXHAUSTED' in error_str.upper()
            
            if is_404_error or is_resource_exhausted:
                logger.warning(f"⚠️ CAPTCHA: Модель {current_model} недоступна или лимит исчерпан")
                
                # Пробуем следующую модель
                has_more_models = await rotate_captcha_model()
                models_tried += 1
                
                # Если прошли полный цикл и вернулись к началу
                if not has_more_models or (current_model_index == start_model_index and models_tried >= len(CAPTCHA_MODELS)):
                    await stop_bot_with_captcha_error(
                        f"Все модели исчерпаны. Последняя ошибка: {error_str}",
                        is_limit_exhausted=True
                    )
                    return None
                
                # Продолжаем с следующей моделью
                continue
            else:
                # Другие ошибки - критическая ситуация
                logger.error(f"❌ CAPTCHA: Критическая ошибка с моделью {current_model}: {error_str}")
                await stop_bot_with_captcha_error(
                    f"Критическая ошибка с моделью {current_model}: {error_str}",
                    is_limit_exhausted=False
                )
                return None
    
    # Если дошли сюда, значит все модели были перепробованы без успеха
    await stop_bot_with_captcha_error(
        "Все модели перепробованы, но ни одна не сработала",
        is_limit_exhausted=True
    )
    return None

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
    last_captcha_time = None

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
                try:
                    menu_msg = await asyncio.wait_for(
                        wait_for_bot_message(timeout=3.0),
                        timeout=3.5
                    )
                except (asyncio.TimeoutError, Exception):
                    menu_msg = None
            else:
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
                
                last_captcha_time = datetime.now(timezone.utc)
                
                while not bot_msg_queue.empty():
                    try:
                        bot_msg_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                
                result = await solve_captcha_message(menu_msg)
                
                if result is None:
                    # Критическая ошибка, бот уже остановлен
                    return
                elif result:
                    consecutive_fails = 0
                    await asyncio.sleep(3.0)
                    
                    while not bot_msg_queue.empty():
                        try:
                            bot_msg_queue.get_nowait()
                        except asyncio.QueueEmpty:
                            break
                    
                    fishing_in_progress = False
                    last_click_time = None
                    
                    logger.info("✅ Капча решена, начинаем новую рыбалку")
                else:
                    consecutive_fails += 1
                    logger.warning("❌ Не удалось решить капчу (не критическая ошибка)")
                
                await asyncio.sleep(1)
                continue
            
            # 2. Меню рыбалки (нужно нажать "рыбачить")
            if contains_any(txt, MENU_KEYWORDS):
                idx, btn_text = await find_button_index_with_keyword(menu_msg, "рыбач")
                
                if idx is None:
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
                        
                        await asyncio.sleep(2.0)
                        fish_wait_msg = await wait_for_bot_message(timeout=8.0)
                        if fish_wait_msg:
                            txt_fish = msg_text_lower(fish_wait_msg)
                            if contains_any(txt_fish, FISH_WAIT_KEYWORDS):
                                found_msg, found_idx, found_text = await poll_for_button_emoji(timeout=30.0)
                                if found_msg:
                                    fish_msg_id = found_msg.id
                                    
                                    success_fish = await click_button_by_flat_index(found_msg, found_idx)
                                    if success_fish:
                                        last_click_time = datetime.now(timezone.utc)
                                        
                                        await asyncio.sleep(2.0)
                                        
                                        result_msg = await wait_for_fish_result(fish_msg_id, timeout=20.0)
                                        
                                        if result_msg:                                            
                                            fish_button_success = await click_fish_button_after_result(result_msg, fish_msg_id)
                                            
                                            if fish_button_success:
                                                fishing_in_progress = True
                                                last_click_time = datetime.now(timezone.utc)
                                                consecutive_fails = 0
                                                
                                                await asyncio.sleep(1.5)
                                                continue
                                            else:
                                                logger.warning("❌ Не удалось нажать 'рыбачить' после результата")
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
                                if contains_any(txt_fish, CAPTCHA_KEYWORDS):
                                    result = await solve_captcha_message(fish_wait_msg)
                                    if result is None:
                                        return
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
                idx, btn_text = await find_button_has_emoji(menu_msg)
                if idx is not None:
                    found_msg, found_idx, found_text = menu_msg, idx, btn_text
                else:
                    found_msg, found_idx, found_text = await poll_for_button_emoji(timeout=30.0)
                
                if found_msg and found_idx is not None:
                    fish_msg_id = found_msg.id
                    
                    success_fish = await click_button_by_flat_index(found_msg, found_idx)
                    if success_fish:
                        last_click_time = datetime.now(timezone.utc)
                        
                        await asyncio.sleep(2.0)
                        
                        result_msg = await wait_for_fish_result(fish_msg_id, timeout=20.0)
                        
                        if result_msg:                            
                            fish_button_success = await click_fish_button_after_result(result_msg, fish_msg_id)
                            
                            if fish_button_success:
                                fishing_in_progress = True
                                last_click_time = datetime.now(timezone.utc)
                                consecutive_fails = 0
                                
                                await asyncio.sleep(1.5)
                                continue
                            else:
                                logger.warning("❌ Не удалось нажать 'рыбачить' после результата")
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
                fish_button_success = await click_fish_button_after_result(menu_msg)
                
                if fish_button_success:
                    fishing_in_progress = True
                    last_click_time = datetime.now(timezone.utc)
                    consecutive_fails = 0
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
    global last_captcha_error_type, captcha_error_count
    
    if _worker_running:
        await event.reply("Бот уже запущен.")
        return
    
    # Сбрасываем счетчики ошибок капчи при запуске
    last_captcha_error_type = None
    captcha_error_count = 0
    
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
            try:
                await asyncio.wait_for(_worker_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                _worker_task.cancel()
                try:
                    await _worker_task
                except asyncio.CancelledError:
                    pass
        
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
    
    # Логируем информацию о моделях капчи
    logger.info(f"🤖 Доступные модели капчи: {', '.join(CAPTCHA_MODELS)}")
    logger.info(f"🔧 Начинаем с модели: {CAPTCHA_MODELS[current_model_index]}")
    
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
    
    if RENDER_APP_URL:
        asyncio.create_task(self_ping())
        logger.info("🔄 Самопинг запущен")
    else:
        logger.warning("⚠️ RENDER_APP_URL не задан, самопингование отключено.")

    logger.info("🤖 Бот запущен. Отправьте 'начать' в личном чате с игровым ботом.")
    
    while not bot_msg_queue.empty():
        try:
            bot_msg_queue.get_nowait()
        except asyncio.QueueEmpty:
            break
    
    await client.run_until_disconnected()

if __name__ == "__main__":
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
