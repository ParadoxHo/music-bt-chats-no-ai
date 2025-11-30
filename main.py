# -*- coding: utf-8 -*-
import os
import sys
import json
import logging
import tempfile
import re
import random
import asyncio
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

# ==================== CONFIG ====================
BOT_TOKEN = os.environ.get('BOT_TOKEN')

if not BOT_TOKEN:
    print("❌ Ошибка: BOT_TOKEN не установлен")
    sys.exit(1)

print("🔧 Универсальный Music Bot запускается...")

# Оптимизированные настройки
MAX_FILE_SIZE_MB = int(os.environ.get('MAX_FILE_SIZE_MB', 50))
DOWNLOAD_TIMEOUT = int(os.environ.get('DOWNLOAD_TIMEOUT', 120))
SEARCH_TIMEOUT = int(os.environ.get('SEARCH_TIMEOUT', 20))
REQUESTS_PER_MINUTE = int(os.environ.get('REQUESTS_PER_MINUTE', 10))

# Ускоренные настройки для SoundCloud
SOUNDCLOUD_OPTS = {
    'format': 'bestaudio[ext=mp3]/bestaudio[ext=m4a]/bestaudio/best',
    'outtmpl': os.path.join(tempfile.gettempdir(), '%(id)s.%(ext)s'),
    'quiet': True,
    'no_warnings': True,
    'retries': 2,
    'fragment_retries': 2,
    'skip_unavailable_fragments': True,
    'noprogress': True,
    'nopart': True,
    'noplaylist': True,
    'max_filesize': MAX_FILE_SIZE_MB * 1024 * 1024,
    'ignoreerrors': True,
    'socket_timeout': 20,
    'extractaudio': True,
    'audioformat': 'best',
}

# Список для случайных треков
RANDOM_SEARCHES = [
    'lo fi beats', 'chillhop', 'deep house', 'synthwave', 'indie rock',
    'electronic music', 'jazz lounge', 'ambient', 'study music',
    'focus music', 'relaxing music', 'instrumental', 'acoustic',
    'piano covers', 'guitar music', 'vocal trance', 'dubstep',
    'tropical house', 'future bass', 'retro wave', 'city pop',
    'latin music', 'reggaeton', 'k-pop', 'j-pop', 'classical piano',
    'orchestral', 'film scores', 'video game music'
]

# Список из 100 приятных пожеланий
WISHES = [
    "Хорошего дня! 🌟", "Отличного настроения! 😊", "Пусть день будет прекрасным! ✨",
    "Удачи во всех начинаниях! 🍀", "Прекрасной музыки! 🎵", "Наслаждайтесь моментом! 🌈",
    "Пусть этот день принесет радость! 🌞", "Отличного прослушивания! 🎧", "Вдохновения и творчества! 🎨",
    "Прекрасного дня и хорошей музыки! 🎶", "Пусть всё получается! 💫", "Хорошего отдыха! 🌴",
    "Отличного настроения и вдохновения! 🌟", "Приятного прослушивания! 🎵", "Пусть день будет полон гармонии! 🎼",
    "Наслаждайтесь музыкой! 🎹", "Прекрасных музыкальных открытий! 🔍", "Хорошего дня и отличного настроения! 😄",
    "Пусть музыка наполнит день радостью! 🎵", "Отличного звучания! 🎶", "Приятного музыкального путешествия! 🌍",
    "Пусть этот трек поднимет настроение! 📈", "Наслаждайтесь каждым моментом! ⏳", "Хорошего дня и вдохновения! 💡",
    "Пусть музыка принесет умиротворение! 🕊️", "Отличного прослушивания и хорошего дня! 🌞", "Приятных музыкальных впечатлений! 🎭",
    "Пусть день будет мелодичным! 🎵", "Наслаждайтесь музыкой и отдыхом! 🛋️", "Хорошего настроения и отличного дня! 🌈",
    "Пусть музыка станет саундтреком вашего дня! 🎬", "Приятного прослушивания и творчества! 🎨", "Отличного дня и хорошей музыки! 🎵",
    "Пусть этот трек зарядит энергией! ⚡", "Наслаждайтесь моментом и музыкой! 🎧", "Хорошего дня и приятных эмоций! 😊",
    "Пусть музыка принесет вдохновение! 💫", "Отличного настроения и прекрасного дня! 🌟", "Приятного музыкального опыта! 🎼",
    "Пусть день будет наполнен гармонией! 🎵", "Наслаждайтесь звучанием! 🎶", "Хорошего прослушивания и отдыха! 🌴",
    "Пусть музыка поднимет настроение! 📈", "Отличного дня и музыкальных открытий! 🔍", "Приятных музыкальных моментов! ⏰",
    "Пусть этот трек станет любимым! ❤️", "Наслаждайтесь музыкой и расслаблением! 🛀", "Хорошего дня и прекрасного настроения! 😄",
    "Пусть музыка принесет радость! 🎵", "Отличного прослушивания и вдохновения! 💡", "Приятного знакомства с новой музыкой! 🆕",
    "Пусть день будет ритмичным! 🥁", "Наслаждайтесь музыкальным разнообразием! 🌈", "Хорошего настроения и приятного дня! 🌞",
    "Пусть музыка станет источником радости! 🎵", "Отличного звучания и хорошего дня! 🎶", "Приятного погружения в музыку! 🌊",
    "Пусть этот трек запомнится надолго! 📅", "Наслаждайтесь музыкальной атмосферой! 🌌", "Хорошего дня и творческих успехов! 🎨",
    "Пусть музыка принесет уют и комфорт! 🏠", "Отличного настроения и музыкального наслаждения! 😊", "Приятного прослушивания и релаксации! 🧘",
    "Пусть день будет наполнен красивыми мелодиями! 🎵", "Наслаждайтесь каждым аккордом! 🎹", "Хорошего дня и позитивных вибраций! 🌟",
    "Пусть музыка станет верным спутником! 👣", "Отличного прослушивания и хорошего настроения! 🎧", "Приятных музыкальных эмоций! ❤️",
    "Пусть этот трек вдохновит на великие дела! 💪", "Наслаждайтесь музыкой и гармонией! 🎵", "Хорошего дня и прекрасных моментов! ⏳",
    "Пусть музыка принесет свет и радость! 💡", "Отличного настроения и приятного прослушивания! 😄", "Приятного знакомства с новыми звуками! 👂",
    "Пусть день будет музыкальным праздником! 🎉", "Наслаждайтесь ритмом и мелодией! 🥁", "Хорошего дня и вдохновляющей музыки! 💫",
    "Пусть музыка станет источником вдохновения! 🎵", "Отличного прослушивания и творческого подъема! 📈", "Приятных музыкальных открытий! 🔍",
    "Пусть этот трек поднимет настроение на весь день! ☀️", "Наслаждайтесь музыкой и позитивом! 😊", "Хорошего дня и мелодичного настроения! 🎶",
    "Пусть музыка принесет умиротворение и радость! 🕊️", "Отличного настроения и приятных звуков! 🎵", "Приятного прослушивания и хорошего отдыха! 🌴",
    "Пусть день будет наполнен красивой музыкой! 🎼", "Наслаждайтесь каждым музыкальным моментом! ⏰", "Хорошего дня и гармонии в душе! 🎵",
    "Пусть музыка станет лекарством для души! 💊", "Отличного прослушивания и позитивных эмоций! 🌈", "Приятного погружения в мир музыки! 🌍",
    "Пусть этот трек запомнится своей красотой! 🌸", "Наслаждайтесь музыкой и вдохновением! 💡", "Хорошего дня и прекрасного звучания! 🎶"
]

# ==================== IMPORT TELEGRAM & YT-DLP ====================
try:
    from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
    from telegram.ext import (
        Application, CommandHandler, MessageHandler, 
        filters, ContextTypes, CallbackQueryHandler
    )
    from telegram.error import Conflict, TimedOut, NetworkError
    import yt_dlp
    print("✅ Все зависимости загружены")
except ImportError as exc:
    print(f"❌ Ошибка импорта: {exc}")
    os.system("pip install python-telegram-bot yt-dlp")
    try:
        from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
        from telegram.ext import (
            Application, CommandHandler, MessageHandler,
            filters, ContextTypes, CallbackQueryHandler
        )
        from telegram.error import Conflict, TimedOut, NetworkError
        import yt_dlp
        print("✅ Зависимости успешно установлены")
    except ImportError as exc2:
        print(f"❌ Ошибка импорта после установки: {exc2}")
        sys.exit(1)

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== RATE LIMITER ====================
class RateLimiter:
    def __init__(self):
        self.user_requests = defaultdict(list)
    
    def is_limited(self, user_id: int, limit: int = REQUESTS_PER_MINUTE, period: int = 60):
        now = datetime.now()
        user_requests = self.user_requests[user_id]
        user_requests = [req for req in user_requests if now - req < timedelta(seconds=period)]
        self.user_requests[user_id] = user_requests
        
        if len(user_requests) >= limit:
            return True
            
        user_requests.append(now)
        return False

# ==================== ТРАНСЛИТЕРАЦИЯ ====================
class Transliterator:
    def __init__(self):
        self.translit_map = {
            'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'e', 'ж': 'zh', 'з': 'z',
            'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r',
            'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch',
            'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya',
            ' ': ' ', '-': '-', '_': '_'
        }
        
        # Обратная транслитерация (латиница -> кириллица)
        self.reverse_map = {v: k for k, v in self.translit_map.items() if v}
        # Особые случаи для обратной транслитерации
        self.reverse_map.update({
            'sch': 'щ', 'zh': 'ж', 'ts': 'ц', 'ch': 'ч', 'sh': 'ш', 'yu': 'ю', 'ya': 'я'
        })

    def to_latin(self, text: str) -> str:
        """Транслитерирует кириллицу в латиницу"""
        result = []
        text = text.lower()
        i = 0
        while i < len(text):
            # Проверяем специальные комбинации (2-3 символа)
            if i + 2 <= len(text) and text[i:i+2] in ['шч', 'жч']:
                result.append('shch')
                i += 2
            elif i + 1 < len(text) and text[i:i+2] in ['йо', 'йе']:
                result.append('yo' if text[i:i+2] == 'йо' else 'ye')
                i += 2
            elif i + 1 < len(text):
                # Обычные двухсимвольные комбинации
                two_chars = text[i:i+2]
                if two_chars in self.translit_map:
                    result.append(self.translit_map[two_chars])
                    i += 2
                else:
                    # Одиночные символы
                    result.append(self.translit_map.get(text[i], text[i]))
                    i += 1
            else:
                # Одиночные символы
                result.append(self.translit_map.get(text[i], text[i]))
                i += 1
        return ''.join(result)

    def to_cyrillic(self, text: str) -> str:
        """Транслитерирует латиницу в кириллицу (для обратного поиска)"""
        result = []
        text = text.lower()
        i = 0
        while i < len(text):
            # Проверяем специальные комбинации (3 символа)
            if i + 3 <= len(text) and text[i:i+3] in ['shch', 'sch']:
                result.append('щ')
                i += 4 if text[i:i+3] == 'shch' else 3
            # Проверяем двухсимвольные комбинации
            elif i + 2 <= len(text) and text[i:i+2] in ['zh', 'ts', 'ch', 'sh', 'yu', 'ya']:
                result.append(self.reverse_map.get(text[i:i+2], text[i:i+2]))
                i += 2
            else:
                # Одиночные символы
                result.append(self.reverse_map.get(text[i], text[i]))
                i += 1
        return ''.join(result)

    def generate_search_variants(self, query: str) -> list:
        """Генерирует варианты поиска с транслитерацией"""
        variants = [query]
        
        # Проверяем, содержит ли запрос кириллицу
        has_cyrillic = any('а' <= char <= 'я' or char == 'ё' for char in query.lower())
        
        if has_cyrillic:
            # Если есть кириллица - добавляем латинский вариант
            latin_version = self.to_latin(query)
            if latin_version and latin_version != query:
                variants.append(latin_version)
        else:
            # Если нет кириллицы - добавляем кириллический вариант
            cyrillic_version = self.to_cyrillic(query)
            if cyrillic_version and cyrillic_version != query:
                variants.append(cyrillic_version)
        
        return variants

# ==================== UNIVERSAL MUSIC BOT ====================
class UniversalMusicBot:
    def __init__(self):
        self.download_semaphore = asyncio.Semaphore(2)
        self.search_semaphore = asyncio.Semaphore(3)
        self.rate_limiter = RateLimiter()
        self.transliterator = Transliterator()
        self.app = None
        self.active_searches = {}  # Хранит результаты поиска по chat_id
        logger.info('✅ Универсальный бот инициализирован')

    def get_random_wish(self):
        """Возвращает случайное пожелание из списка"""
        return random.choice(WISHES)

    @staticmethod
    def clean_title(title: str) -> str:
        if not title:
            return 'Неизвестный трек'
        title = re.sub(r"[^\w\s\-\.\(\)\[\]]", '', title)
        tags = ['official video', 'official music video', 'lyric video', 'hd', '4k',
                '1080p', '720p', 'official audio', 'audio', 'video', 'clip', 'mv']
        for tag in tags:
            title = re.sub(tag, '', title, flags=re.IGNORECASE)
        return ' '.join(title.split()).strip()

    @staticmethod
    def format_duration(seconds) -> str:
        try:
            sec = int(float(seconds))
            minutes = sec // 60
            sec = sec % 60
            return f"{minutes:02d}:{sec:02d}"
        except Exception:
            return '00:00'

    @staticmethod
    def is_valid_url(url: str) -> bool:
        """Проверяет валидность URL"""
        if not url:
            return False
        return bool(re.match(r'^https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+', url))

    def _create_application(self):
        """Создает и настраивает приложение Telegram"""
        self.app = Application.builder().token(BOT_TOKEN).build()

        # Обработчик ВСЕХ текстовых сообщений ВО ВСЕХ чатах
        self.app.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            self.handle_all_messages
        ))

        # Команды
        self.app.add_handler(CommandHandler('start', self.start_command))
        self.app.add_handler(CommandHandler('find', self.handle_find_short))
        self.app.add_handler(CommandHandler('random', self.handle_random_short))
        
        # Обработчик нажатий на кнопки
        self.app.add_handler(CallbackQueryHandler(self.handle_button_click, pattern="^download_"))

    async def handle_button_click(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обрабатывает нажатия на кнопки"""
        query = update.callback_query
        await query.answer()
        
        data = query.data
        chat_id = query.message.chat_id
        
        # Обработка скачивания выбранного трека
        track_index = int(data.split('_')[1])
        await self.download_selected_track(update, context, track_index, chat_id)

    async def download_selected_track(self, update: Update, context: ContextTypes.DEFAULT_TYPE, track_index: int, chat_id: int):
        """Скачивает выбранный трек"""
        query = update.callback_query
        user = query.from_user
        
        # Получаем сохраненные результаты поиска
        if chat_id not in self.active_searches:
            await query.edit_message_text("❌ Результаты поиска устарели. Начни новый поиск.")
            return
        
        search_data = self.active_searches[chat_id]
        tracks = search_data.get('tracks', [])
        
        if track_index < 0 or track_index >= len(tracks):
            await query.edit_message_text("❌ Неверный выбор трека.")
            return
        
        track = tracks[track_index]
        
        # Редактируем сообщение для отображения статуса скачивания
        await query.edit_message_text(
            f"⏬ Скачивается: <b>{track['title']}</b>\n"
            f"⏱️ Длительность: {self.format_duration(track.get('duration'))}\n\n"
            f"⏳ Пожалуйста, подожди...",
            parse_mode='HTML'
        )
        
        # Скачиваем трек
        file_path = await self.download_track(track.get('webpage_url'))
        if not file_path:
            await query.edit_message_text(
                f"❌ Не удалось скачать трек: {track['title']}\n"
                f"💡 Попробуй выбрать другой трек"
            )
            return
        
        # Отправляем аудио
        wish = self.get_random_wish()
        try:
            with open(file_path, 'rb') as audio_file:
                await context.bot.send_audio(
                    chat_id=chat_id,
                    audio=audio_file,
                    title=(track.get('title') or 'Неизвестный трек')[:64],
                    performer=(track.get('artist') or 'Неизвестный исполнитель')[:64],
                    caption=f"🎵 <b>{track.get('title', 'Неизвестный трек')}</b>\n"
                           f"⏱️ {self.format_duration(track.get('duration'))}\n\n{wish}",
                    parse_mode='HTML'
                )
            
            # Удаляем временный файл
            try:
                os.remove(file_path)
            except:
                pass
            
            # Удаляем сообщение с кнопками
            try:
                await query.message.delete()
            except:
                pass
            
        except Exception as e:
            logger.exception(f'Ошибка отправки аудио: {e}')
            await query.edit_message_text(
                f"❌ Ошибка отправки трека\n"
                f"💡 Попробуй еще раз"
            )

    def create_tracks_keyboard(self, tracks):
        """Создает клавиатуру только с треками (кнопки на всю ширину)"""
        keyboard = []
        
        # Добавляем кнопки для каждого трека (каждая на всю ширину)
        for i, track in enumerate(tracks):
            title = track.get('title', 'Неизвестный трек')
            duration = self.format_duration(track.get('duration'))
            
            # Обрезаем длинные названия и добавляем длительность
            button_text = f"{i+1}. {title[:30]}{'...' if len(title) > 30 else ''} ({duration})"
            
            keyboard.append([InlineKeyboardButton(
                button_text, 
                callback_data=f"download_{i}"
            )])
        
        return InlineKeyboardMarkup(keyboard)

    async def handle_find_short(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обрабатывает команду /find"""
        query = ' '.join(context.args)
        if not query:
            await update.message.reply_text(
                "❌ Укажи запрос для поиска\n💡 Пример: <code>/find coldplay</code>",
                parse_mode='HTML'
            )
            return
        await self.handle_find_command(update, context, f"найди {query}")

    async def handle_random_short(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обрабатывает команду /random"""
        await self.handle_random_command(update, context)

    # ==================== ОБРАБОТКА ВСЕХ СООБЩЕНИЙ ====================

    async def handle_all_messages(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обрабатывает ВСЕ сообщения из любых чатов"""
        try:
            if not update.message or not update.message.text:
                return
                
            message_text = update.message.text.strip().lower()
            chat_id = update.effective_chat.id
            user = update.effective_user
            
            print(f"🎯 Сообщение от {user.first_name}: {message_text}")

            # Rate limiting
            if self.rate_limiter.is_limited(user.id):
                await update.message.reply_text(
                    f"⏳ {user.mention_html()}, слишком много запросов!\n"
                    f"Подожди 1 минуту перед следующим запросом.",
                    parse_mode='HTML'
                )
                return

            # Реагируем ТОЛЬКО на команды "найди" и "рандом"
            if message_text.startswith('найди'):
                await self.handle_find_command(update, context, message_text)
            
            elif message_text.startswith('рандом'):
                await self.handle_random_command(update, context)
            
            # Игнорируем все остальные сообщения
            else:
                return
                
        except Exception as e:
            logger.exception(f'Ошибка обработки сообщения: {e}')

    async def handle_find_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_text: str):
        """Обрабатывает поиск трека по запросу"""
        status_msg = None
        try:
            user = update.effective_user
            chat_id = update.effective_chat.id
            original_message = update.message
            
            # Извлекаем запрос после "найди"
            query = self.extract_search_query(message_text)
            
            if not query:
                await original_message.reply_text(
                    f"❌ {user.mention_html()}, не указано что искать\n"
                    f"💡 Напиши: найди [название трека или исполнителя]",
                    parse_mode='HTML'
                )
                return

            # Отправляем статус
            status_msg = await original_message.reply_text(
                f"🔍 Ищу: <code>{query}</code>\n⏳ Пожалуйста, подожди...", 
                parse_mode='HTML'
            )

            # Ищем три трека
            tracks = await self.find_multiple_tracks(query)
            
            if not tracks:
                await status_msg.edit_text(
                    f"❌ Не найдено треков по запросу: <code>{query}</code>\n"
                    f"💡 Попробуй другой запрос",
                    parse_mode='HTML'
                )
                return

            print(f"✅ Найдено {len(tracks)} треков по запросу: {query}")

            # Сохраняем результаты поиска
            self.active_searches[chat_id] = {
                'query': query,
                'tracks': tracks
            }

            # Создаем клавиатуру с результатами
            keyboard = self.create_tracks_keyboard(tracks)

            # Редактируем сообщение с результатами - ТОЛЬКО КНОПКИ
            await status_msg.edit_text(
                f"🎵 Выбери трек для скачивания:",
                reply_markup=keyboard,
                parse_mode='HTML'
            )

        except Exception as e:
            logger.exception(f'Ошибка при поиске: {e}')
            print(f"❌ Критическая ошибка в handle_find_command: {e}")
            if status_msg:
                await status_msg.edit_text(
                    f"❌ Ошибка при поиске\n"
                    f"💡 Попробуй еще раз",
                    parse_mode='HTML'
                )

    async def handle_random_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обрабатывает запрос на случайный трек"""
        status_msg = None
        try:
            user = update.effective_user
            chat_id = update.effective_chat.id
            original_message = update.message

            # Отправляем статус
            status_msg = await original_message.reply_text("🎲 Ищу случайный трек...", parse_mode='HTML')

            # Случайный запрос
            random_query = random.choice(RANDOM_SEARCHES)
            print(f"🎲 Случайный запрос: {random_query}")
            
            # Ищем треки
            tracks = await self.find_multiple_tracks(random_query, limit=1)
            
            if not tracks:
                await status_msg.edit_text(
                    f"❌ Не удалось найти случайный трек\n"
                    f"💡 Попробуй еще раз",
                    parse_mode='HTML'
                )
                return

            track = tracks[0]
            print(f"✅ Найден случайный трек: {track['title']}")

            # Скачиваем трек
            file_path = await self.download_track(track.get('webpage_url'))
            if not file_path:
                print(f"❌ Не удалось скачать случайный трек: {track['title']}")
                await status_msg.edit_text(
                    f"❌ Не удалось скачать случайный трек\n"
                    f"🎵 {track.get('title', 'Неизвестный трек')}",
                    parse_mode='HTML'
                )
                return

            print(f"✅ Случайный трек скачан: {file_path}")

            # Получаем случайное пожелание
            wish = self.get_random_wish()

            # Отправляем аудио
            try:
                with open(file_path, 'rb') as audio_file:
                    await context.bot.send_audio(
                        chat_id=chat_id,
                        audio=audio_file,
                        title=(track.get('title') or 'Неизвестный трек')[:64],
                        performer=(track.get('artist') or 'Неизвестный исполнитель')[:64],
                        caption=f"🎵 <b>{track.get('title', 'Неизвестный трек')}</b>\n"
                               f"⏱️ {self.format_duration(track.get('duration'))}\n\n{wish}",
                        parse_mode='HTML'
                    )
                print(f"✅ Случайное аудио отправлено в чат {chat_id}")
            except Exception as e:
                print(f"❌ Ошибка отправки случайного аудио: {e}")
                await status_msg.edit_text(
                    f"❌ Ошибка отправки трека\n"
                    f"💡 Попробуй еще раз",
                    parse_mode='HTML'
                )
                return

            # Удаляем временный файл
            try:
                os.remove(file_path)
            except:
                pass

            # Удаляем статус-сообщение
            try:
                await status_msg.delete()
            except:
                pass

        except Exception as e:
            logger.exception(f'Ошибка при поиске случайного трека: {e}')
            print(f"❌ Критическая ошибка в handle_random_command: {e}")
            if status_msg:
                await status_msg.edit_text(
                    f"❌ Ошибка при поиске\n"
                    f"💡 Попробуй еще раз",
                    parse_mode='HTML'
                )

    def extract_search_query(self, message_text: str) -> str:
        """Извлекает поисковый запрос из сообщения"""
        query = message_text.replace('найди', '').strip()
        stop_words = ['пожалуйста', 'мне', 'трек', 'песню', 'музыку', 'плз', 'plz']
        for word in stop_words:
            query = query.replace(word, '')
        return query.strip()

    # ==================== ПОИСК ТРЕКОВ С ТРАНСЛИТЕРАЦИЕЙ ====================

    async def find_multiple_tracks(self, query: str, limit: int = 3):
        """Находит несколько треков по запросу с использованием транслитерации"""
        async with self.search_semaphore:
            # Генерируем варианты поиска с транслитерацией
            search_variants = self.transliterator.generate_search_variants(query)
            print(f"🔍 Варианты поиска: {search_variants}")
            
            # Пробуем каждый вариант поиска до тех пор, пока не найдем результаты
            for search_query in search_variants:
                print(f"🔍 Пробуем поиск: {search_query}")
                tracks = await self._search_tracks(search_query, limit * 2)  # Ищем больше, чтобы выбрать лучшие
                
                if tracks:
                    print(f"✅ Найдено {len(tracks)} треков по запросу: {search_query}")
                    # Возвращаем лучшие треки (уже отсортированные по релевантности)
                    return tracks[:limit]
            
            print(f"❌ Не найдено треков ни по одному варианту: {search_variants}")
            return None

    async def _search_tracks(self, query: str, limit: int = 6):
        """Внутренняя функция поиска треков"""
        ydl_opts = {
            'format': 'bestaudio/best',
            'quiet': True,
            'no_warnings': True,
            'extract_flat': True,
            'ignoreerrors': True,
            'noplaylist': True,
            'socket_timeout': 15,
        }

        try:
            print(f"🔍 Выполняем поиск: {query}")
            
            def perform_search():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    return ydl.extract_info(f"scsearch{limit}:{query}", download=False)

            loop = asyncio.get_event_loop()
            info = await asyncio.wait_for(
                loop.run_in_executor(None, perform_search),
                timeout=SEARCH_TIMEOUT
            )

            if not info:
                print(f"❌ Поиск не дал результатов: {query}")
                return None

            entries = info.get('entries', [])
            if not entries and info.get('_type') != 'playlist':
                entries = [info]

            print(f"✅ Найдено {len(entries)} результатов по запросу: {query}")

            # Фильтрация и сортировка для лучшей релевантности
            filtered_entries = []
            for entry in entries:
                if not entry:
                    continue

                # Фильтруем по длительности (минимум 30 секунд, максимум 1 час)
                duration = entry.get('duration') or 0
                if duration < 30 or duration > 3600:
                    continue

                title = self.clean_title(entry.get('title') or '')
                if not title:
                    continue

                # Приоритет для "official" треков
                priority = 0
                title_lower = title.lower()
                query_lower = query.lower()
                
                # Повышаем приоритет, если запрос точно совпадает с названием
                if query_lower in title_lower:
                    priority += 2
                
                if 'official' in title_lower:
                    priority += 3
                elif 'original' in title_lower:
                    priority += 2
                elif 'cover' not in title_lower and 'remix' not in title_lower:
                    priority += 1

                filtered_entries.append({
                    'entry': entry,
                    'priority': priority,
                    'duration': duration,
                    'title': title
                })

            if not filtered_entries:
                print("❌ Нет подходящих треков после фильтрации")
                return None

            # Сортируем по приоритету и длительности
            filtered_entries.sort(key=lambda x: (-x['priority'], -x['duration']))

            # Берем нужное количество результатов
            results = []
            for i in range(min(limit, len(filtered_entries))):
                best_entry = filtered_entries[i]['entry']
                title = self.clean_title(best_entry.get('title') or '')
                webpage_url = best_entry.get('webpage_url') or best_entry.get('url') or ''
                duration = best_entry.get('duration') or 0
                artist = best_entry.get('uploader') or best_entry.get('uploader_id') or 'Неизвестно'

                if not webpage_url:
                    continue

                results.append({
                    'title': title,
                    'webpage_url': webpage_url,
                    'duration': duration,
                    'artist': artist
                })

            print(f"🎵 Выбрано {len(results)} лучших треков")
            return results

        except asyncio.TimeoutError:
            logger.warning(f"Таймаут поиска: {query}")
            print(f"❌ Таймаут поиска: {query}")
            return None
        except Exception as e:
            logger.warning(f'Ошибка поиска: {e}')
            print(f"❌ Ошибка поиска: {e}")
            return None

    # ==================== СКАЧИВАНИЕ ====================

    async def download_track(self, url: str) -> str:
        """Скачивает трек и возвращает путь к файлу"""
        if not self.is_valid_url(url):
            print(f"❌ Невалидный URL: {url}")
            return None

        loop = asyncio.get_event_loop()
        tmpdir = tempfile.mkdtemp()
        
        try:
            ydl_opts = SOUNDCLOUD_OPTS.copy()
            ydl_opts['outtmpl'] = os.path.join(tmpdir, '%(title).100s.%(ext)s')

            print(f"⏬ Начинаем скачивание: {url}")

            def download_track():
                try:
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        result = ydl.extract_info(url, download=True)
                        print(f"✅ yt-dlp завершил скачивание")
                        return result
                except Exception as e:
                    print(f"❌ Ошибка в yt-dlp: {e}")
                    return None

            info = await asyncio.wait_for(
                loop.run_in_executor(None, download_track),
                timeout=DOWNLOAD_TIMEOUT
            )

            if not info:
                print("❌ yt-dlp не вернул информацию")
                return None

            # Ищем Telegram-совместимые файлы
            telegram_audio_extensions = ['.mp3', '.m4a', '.ogg', '.wav', '.flac']
            
            for file in os.listdir(tmpdir):
                file_ext = os.path.splitext(file)[1].lower()
                if file_ext in telegram_audio_extensions:
                    file_path = os.path.join(tmpdir, file)
                    
                    # Проверяем размер файла
                    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
                    print(f"📁 Найден файл: {file} ({file_size_mb:.2f} MB)")
                    
                    if file_size_mb >= MAX_FILE_SIZE_MB:
                        print(f"❌ Файл слишком большой: {file_size_mb} MB")
                        continue
                    
                    print(f"✅ Файл подходит: {file_path}")
                    return file_path

            print(f"❌ Не найдено подходящих файлов в {tmpdir}")
            return None

        except asyncio.TimeoutError:
            print(f"❌ Таймаут скачивания: {url}")
            return None
        except Exception as e:
            logger.exception(f'Ошибка скачивания: {e}')
            print(f"❌ Ошибка скачивания: {e}")
            return None
        finally:
            # Очищаем временную директорию
            async def cleanup():
                await asyncio.sleep(2)
                try:
                    shutil.rmtree(tmpdir, ignore_errors=True)
                    print(f"✅ Очищена временная директори
