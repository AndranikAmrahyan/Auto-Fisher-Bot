# event_bot.py
import asyncio
import logging
from datetime import datetime, timezone
from telethon import events, TelegramClient

# ================= КОНФИГУРАЦИЯ =================

# 1. Режим игры:
# 1 = Точное совпадение (Сообщение == Слово)
# 2 = Поиск вхождения (Слово внутри сообщения)
EVENT_MODE = 2

# 2. Загаданные слова (массив строк)
SECRET_WORDS = ["рыба", "удочка", "клев"]

# 3. Команды управления
CMD_START_EVENT = "старт ивент"
CMD_STOP_EVENT = "стоп ивент"

# 4. ID администраторов (кто может управлять ботом)
ADMIN_IDS = [5553779390, 1057267401]

# 5. ID группы для отслеживания
TARGET_GROUP_ID = -1002157100033

# 6. ID бота, который переводит голосовые/кружочки в текст
TRANSCRIPTION_BOT_ID = 5244379085

# 7. Интервал обновления сообщения админа (в секундах)
UI_UPDATE_INTERVAL = 4

# ================================================

logger = logging.getLogger("event_bot")

# Хранилище состояния (в памяти)
class EventState:
    def __init__(self):
        self.is_running = False
        self.start_time = None
        self.initiator_id = None  # Кто запустил (админ)
        self.status_msg = None    # Объект сообщения в ЛС админа для редактирования
        
        # Данные статистики
        # scores: {user_id: {"name": str, "count": int}}
        self.scores = {}
        
        # word_stats: {word: count}
        self.word_stats = {w.lower(): 0 for w in SECRET_WORDS}
        
        # user_word_stats: {word: {user_id: count}} - кто сколько раз какое слово сказал
        self.user_word_stats = {w.lower(): {} for w in SECRET_WORDS}

        # Для защиты от FloodWait (UI Update Loop)
        self.needs_update = False
        self.ui_task = None

    def reset(self, initiator_id):
        self.is_running = True
        self.start_time = datetime.now(timezone.utc)
        self.initiator_id = initiator_id
        self.status_msg = None
        self.scores = {}
        self.word_stats = {w.lower(): 0 for w in SECRET_WORDS}
        self.user_word_stats = {w.lower(): {} for w in SECRET_WORDS}
        self.needs_update = True

state = EventState()

def get_time_str(start_dt):
    if not start_dt:
        return "0ч 0м"
    diff = datetime.now(timezone.utc) - start_dt
    days = diff.days
    seconds = diff.seconds
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    
    time_str = ""
    if days > 0: time_str += f"{days}д. "
    time_str += f"{hours}ч. {minutes}м."
    return time_str

def generate_report(is_final=False):
    title = "🏁 <b>ИТОГИ ИВЕНТА</b>" if is_final else "📊 <b>LIVE СТАТИСТИКА</b>"
    timer = f"⏱ Время работы: <b>{get_time_str(state.start_time)}</b>"
    if is_final:
        timer += " (Завершен)"

    # Сортировка участников по баллам
    sorted_users = sorted(state.scores.items(), key=lambda item: item[1]['count'], reverse=True)
    
    users_text = ""
    if sorted_users:
        users_text += "\n\n🏆 <b>Лидерборд:</b>\n"
        for idx, (uid, data) in enumerate(sorted_users, 1):
            # Ссылка на профиль tg://user?id=...
            name_link = f"<a href='tg://user?id={uid}'>{data['name']}</a>"
            users_text += f"{idx}. {name_link} — <b>{data['count']}</b>\n"
    else:
        users_text += "\n\n💤 Пока никто ничего не угадал."

    # Аналитика по словам
    analytics_text = "\n📉 <b>Аналитика по словам:</b>\n"
    for word in SECRET_WORDS:
        w_lower = word.lower()
        total_uses = state.word_stats.get(w_lower, 0)
        
        # Находим лидера по этому слову
        top_user_for_word = "Никто"
        u_stats = state.user_word_stats.get(w_lower, {})
        if u_stats:
            top_user_id = max(u_stats, key=u_stats.get)
            top_count = u_stats[top_user_id]
            # Пытаемся достать имя из общего скора
            if top_user_id in state.scores:
                u_name = state.scores[top_user_id]['name']
                top_user_for_word = f"<a href='tg://user?id={top_user_id}'>{u_name}</a> ({top_count})"
        
        analytics_text += f"▪️ <i>{word}</i>: использовано {total_uses} раз. Лидер: {top_user_for_word}\n"

    return f"{title}\n{timer}{users_text}\n{analytics_text}"

async def ui_updater_loop(client: TelegramClient):
    """Фоновая задача: обновляет сообщение админа раз в N секунд, если есть изменения."""
    logger.info("UI Updater Loop started")
    while state.is_running:
        try:
            if state.needs_update and state.status_msg:
                text = generate_report(is_final=False)
                # Проверяем, отличается ли текст, чтобы не дергать API зря
                if state.status_msg.text != text.replace("<b>", "**").replace("</b>", "**"):
                    try:
                        await state.status_msg.edit(text, parse_mode='html')
                        state.needs_update = False
                    except Exception as e:
                        logger.warning(f"UI Update error: {e}")
            
            # Ждем заданное количество секунд перед следующей проверкой
            await asyncio.sleep(UI_UPDATE_INTERVAL)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in UI loop: {e}")
            await asyncio.sleep(5)

# ================= ГЛАВНАЯ ФУНКЦИЯ ПОДКЛЮЧЕНИЯ =================

def init_event_bot(client: TelegramClient):
    """Подключает хендлеры ивента к существующему клиенту."""
    logger.info("🎮 Event Bot module loaded")

    @client.on(events.NewMessage(chats=ADMIN_IDS))
    async def admin_commands_handler(event):
        sender_id = event.sender_id
        text = event.raw_text.lower().strip()
        
        # --- КОМАНДА СТАРТ ---
        if text == CMD_START_EVENT:
            if state.is_running:
                await event.reply("⚠️ Ивент уже запущен!")
                return
            
            state.reset(sender_id)
            report = generate_report(is_final=False)
            state.status_msg = await client.send_message(sender_id, report, parse_mode='html')
            
            # Запускаем фоновый цикл обновления UI
            state.ui_task = asyncio.create_task(ui_updater_loop(client))
            logger.info(f"Ивент запущен администратором {sender_id}")

        # --- КОМАНДА СТОП ---
        elif text == CMD_STOP_EVENT:
            if not state.is_running:
                await event.reply("⚠️ Ивент не запущен.")
                return

            state.is_running = False

            # Останавливаем цикл обновления
            if state.ui_task:
                state.ui_task.cancel()
                state.ui_task = None

            # Формируем финальный отчет
            final_report = generate_report(is_final=True)
            
            # 1. Удаляем лайв-сообщение в ЛС (если есть)
            if state.status_msg:
                try:
                    await state.status_msg.delete()
                except Exception:
                    pass
            
            # 2. Отправляем итоги в ЛС инициатору
            if state.initiator_id:
                await client.send_message(state.initiator_id, final_report, parse_mode='html')
            
            # 3. Если команду написали в Группе, дублируем туда
            if event.chat_id == TARGET_GROUP_ID:
                await event.reply(final_report, parse_mode='html')
            
            logger.info("Ивент остановлен.")

    @client.on(events.NewMessage(chats=TARGET_GROUP_ID))
    async def group_watcher_handler(event):
        # Игнорируем, если ивент не запущен
        if not state.is_running:
            return

        sender = await event.get_sender()
        if not sender:
            return

        is_transcription_bot = (sender.id == TRANSCRIPTION_BOT_ID)

        # Логика фильтрации ботов:
        # Если пишет бот и это НЕ бот-переводчик -> игнорируем
        if sender.bot and not is_transcription_bot:
            return
            
        # Игнорируем команды управления
        if event.raw_text.lower().strip() in [CMD_START_EVENT, CMD_STOP_EVENT]:
            return

        # --- ОПРЕДЕЛЕНИЕ РЕАЛЬНОГО АВТОРА И ТЕКСТА ---
        target_user = None
        
        if is_transcription_bot:
            # Если пишет бот-переводчик, ищем автора оригинального сообщения (reply)
            reply_msg = await event.get_reply_message()
            if reply_msg:
                target_user = await reply_msg.get_sender()
            else:
                # Если реплая нет (странно для этого бота), игнорируем
                return
        else:
            # Обычный пользователь
            target_user = sender

        if not target_user:
            return
            
        # Игнорируем, если "реальный автор" тоже бот (на всякий случай)
        if target_user.bot:
            return

        user_id = target_user.id
        import html
        full_name = html.escape(f"{target_user.first_name} {target_user.last_name or ''}".strip())

        # Получаем текст. 
        # event.raw_text берет:
        # 1. Текст обычного сообщения
        # 2. Caption (подпись) к картинке/видео
        # 3. Текст внутри цитирования (blockquote) без Markdown-символов
        msg_text = event.raw_text.lower().strip()
        
        found_matches = 0
        
        # --- ЛОГИКА ПОИСКА ---
        if EVENT_MODE == 1:
            # Точное совпадение
            for secret in SECRET_WORDS:
                s_lower = secret.lower()
                if msg_text == s_lower:
                    found_matches += 1
                    state.word_stats[s_lower] += 1
                    state.user_word_stats[s_lower][user_id] = state.user_word_stats[s_lower].get(user_id, 0) + 1

        elif EVENT_MODE == 2:
            # Вхождение (для бота-транскрибатора это основной вариант, так как там много текста)
            for secret in SECRET_WORDS:
                s_lower = secret.lower()
                count_in_msg = msg_text.count(s_lower)
                
                if count_in_msg > 0:
                    found_matches += count_in_msg
                    state.word_stats[s_lower] += count_in_msg
                    state.user_word_stats[s_lower][user_id] = state.user_word_stats[s_lower].get(user_id, 0) + count_in_msg

        # --- ОБНОВЛЕНИЕ СЧЕТА ПОЛЬЗОВАТЕЛЯ ---
        if found_matches > 0:
            if user_id not in state.scores:
                state.scores[user_id] = {"name": full_name, "count": 0}
            
            state.scores[user_id]["count"] += found_matches
            
            # Ставим флаг обновления, вместо прямого вызова
            state.needs_update = True