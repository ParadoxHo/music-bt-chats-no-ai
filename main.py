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
import aiofiles
from concurrent.futures import ThreadPoolExecutor

# ==================== CONFIG ====================
BOT_TOKEN = os.environ.get('BOT_TOKEN')

if not BOT_TOKEN:
    print("❌ Ошибка: BOT_TOKEN не установлен")
    sys.exit(1)

print("🔧 Универсальный Music Bot запускается...")

# Оптимизированные настройки для скорости
MAX_FILE_SIZE_MB = int(os.environ.get('MAX_FILE_SIZE_MB', 50))
DOWNLOAD_TIMEOUT = int(os.environ.get('DOWNLOAD_TIMEOUT', 90))  # Уменьшен таймаут
SEARCH_TIMEOUT = int(os.environ.get('SEARCH_TIMEOUT', 15))      # Уменьшен таймаут
REQUESTS_PER_MINUTE = int(os.environ.get('REQUESTS_PER_MINUTE', 15))  # Увеличен лимит

# Оптимизированные настройки для SoundCloud
SOUNDCLOUD_OPTS = {
    'format': 'bestaudio[ext=mp3]/bestaudio[ext=m4a]/bestaudio/best',
    'outtmpl': os.path.join(tempfile.gettempdir(), '%(id)s.%(ext)s'),
    'quiet': True,
    'no_warnings': True,
    'retries': 1,  # Уменьшено количество попыток
    'fragment_retries': 1,  # Уменьшено количество попыток
    'skip_unavailable_fragments': True,
    'noprogress': True,
    'nopart': True,
    'noplaylist': True,
    'max_filesize': MAX_FILE_SIZE_MB * 1024 * 1024,
    'ignoreerrors': True,
    'socket_timeout': 10,  # Уменьшен таймаут
    'extractaudio': True,
    'audioformat': 'mp3',  # Приоритет MP3 для скорости
    'concurrent_fragment_downloads': 3,  # Параллельная загрузка фрагментов
    'throttledratelimit': 0,  # Отключено ограничение скорости
    'buffersize': 1024 * 1024,  # Увеличен буфер
    'http_chunk_size': 10485760,  # Увеличен размер чанка
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

# ==================== IMPORT TELEGRAM & YT-DLP ====================
try:
    from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
    from telegram.ext import (
        Application, CommandHandler, MessageHandler, 
        filters, ContextTypes, CallbackQueryHandler
    )
    from telegram.error import Conflict, TimedOut, NetworkError
    import yt_dlp
    print("✅ Все зависимости загружены")
except ImportError as exc:
    print(f"❌ Ошибка импорта: {exc}")
    os.system("pip install python-telegram-bot yt-dlp aiofiles")
    try:
        from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
        from telegram.ext import (
            Application, CommandHandler, MessageHandler,
            filters, ContextTypes, CallbackQueryHandler
        )
        from telegram.error import Conflict, TimedOut, NetworkError
        import yt_dlp
        import aiofiles
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

# ==================== КЭШИРОВАНИЕ ====================
class SearchCache:
    def __init__(self, ttl_minutes=10):
        self.cache = {}
        self.ttl = timedelta(minutes=ttl_minutes)
    
    def get(self, query):
        now = datetime.now()
        if query in self.cache:
            data, timestamp = self.cache[query]
            if now - timestamp < self.ttl:
                return data
            else:
                del self.cache[query]
        return None
    
    def set(self, query, data):
        self.cache[query] = (data, datetime.now())
    
    def clear_old(self):
        now = datetime.now()
        expired = [key for key, (_, timestamp) in self.cache.items() 
                  if now - timestamp > self.ttl]
        for key in expired:
            del self.cache[key]

# ==================== ТРАНСЛИТЕРАЦИЯ ====================
class Transliterator:
    def __init__(self):
        self.translit_map = {
            'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo', 'ж': 'zh', 'з': 'z',
            'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r',
            'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch',
            'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya',
            ' ': ' ', '-': '-', '_': '_', '.': '.', ',': ','
        }

    def to_latin(self, text: str) -> str:
        """Транслитерирует кириллицу в латиницу"""
        result = []
        text = text.lower()
        for char in text:
            result.append(self.translit_map.get(char, char))
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
        
        return variants

# ==================== UNIVERSAL MUSIC BOT ====================
class UniversalMusicBot:
    def __init__(self):
        self.download_semaphore = asyncio.Semaphore(4)  # Увеличено с 2 до 4
        self.search_semaphore = asyncio.Semaphore(5)    # Увеличено с 3 до 5
        self.rate_limiter = RateLimiter()
        self.transliterator = Transliterator()
        self.search_cache = SearchCache()
        self.app = None
        self.active_searches = {}
        self.thread_pool = ThreadPoolExecutor(max_workers=8)  # Пул потоков для CPU-bound операций
        logger.info('✅ Универсальный бот инициализирован')

    @staticmethod
    def clean_title(title: str) -> str:
        if not title:
            return 'Неизвестный трек'
        # Упрощенная очистка для скорости
        title = re.sub(r'[^\w\s\-\.\(\)\[\]]', '', title)
        tags = ['official video', 'official music video', 'lyric video', 'hd', '4k',
                '1080p', '720p', 'official audio', 'audio', 'video', 'clip', 'mv']
        for tag in tags:
            title = re.sub(tag, '', title, flags=re.IGNORECASE)
        return ' '.join(title.split()).strip()[:100]  # Ограничение длины

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

    async def setup_bot_commands(self, application):
        """Устанавливает список команд для бота"""
        commands = [
            BotCommand("start", "Запустить бота"),
            BotCommand("find", "Найти треки"),
            BotCommand("random", "Случайный трек")
        ]
        await application.bot.set_my_commands(commands)
        print("✅ Команды бота обновлены")

    async def handle_button_click(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обрабатывает нажатия на кнопки"""
        query = update.callback_query
        await query.answer()
        
        data = query.data
        chat_id = query.message.chat_id
        user_id = query.from_user.id
        
        # Получаем сохраненные результаты поиска
        if chat_id not in self.active_searches:
            await query.edit_message_text("❌ Результаты поиска устарели. Начни новый поиск.")
            return
        
        search_data = self.active_searches[chat_id]
        
        # Проверяем, что пользователь, который нажал кнопку, тот же, что и запускал поиск
        if user_id != search_data.get('user_id'):
            await query.answer("❌ Только пользователь, который запустил поиск, может выбирать трек.", show_alert=True)
            return
        
        tracks = search_data.get('tracks', [])
        
        # Обработка скачивания выбранного трека
        track_index = int(data.split('_')[1])
        await self.download_selected_track(update, context, track_index, chat_id, user_id)

    async def download_selected_track(self, update: Update, context: ContextTypes.DEFAULT_TYPE, track_index: int, chat_id: int, user_id: int):
        """Скачивает выбранный трек"""
        query = update.callback_query
        
        # Получаем сохраненные результаты поиска
        if chat_id not in self.active_searches:
            await query.edit_message_text("❌ Результаты поиска устарели. Начни новый поиск.")
            return
        
        search_data = self.active_searches[chat_id]
        
        # Дополнительная проверка на случай, если что-то пошло не так
        if user_id != search_data.get('user_id'):
            await query.edit_message_text("❌ Ошибка доступа. Начни новый поиск.")
            return
        
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
        try:
            # Используем асинхронное чтение файла
            async with aiofiles.open(file_path, 'rb') as audio_file:
                audio_data = await audio_file.read()
                await context.bot.send_audio(
                    chat_id=chat_id,
                    audio=audio_data,
                    title=(track.get('title') or 'Неизвестный трек')[:64],
                    performer=(track.get('artist') or 'Неизвестный исполнитель')[:64],
                    caption=f"🎵 <b>{track.get('title', 'Неизвестный трек')}</b>\n"
                           f"⏱️ {self.format_duration(track.get('duration'))}",
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
            
            print(f"🎯 Сообщение от {user.first_name} (ID: {user.id}): {message_text}")

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

            # Проверяем кэш
            cached_tracks = self.search_cache.get(query)
            if cached_tracks:
                print(f"✅ Используем кэшированные результаты для: {query}")
                tracks = cached_tracks
            else:
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

                # Сохраняем в кэш
                self.search_cache.set(query, tracks)
                print(f"✅ Найдено {len(tracks)} треков по запросу: {query}")

            # Сохраняем результаты поиска вместе с ID пользователя
            self.active_searches[chat_id] = {
                'query': query,
                'tracks': tracks,
                'user_id': user.id
            }

            # Создаем клавиатуру с результатами
            keyboard = self.create_tracks_keyboard(tracks)

            # Если статусное сообщение уже есть, редактируем его
            if status_msg:
                await status_msg.edit_text(
                    f"🎵 Выбери трек для скачивания:",
                    reply_markup=keyboard,
                    parse_mode='HTML'
                )
            else:
                # Иначе отправляем новое сообщение
                await original_message.reply_text(
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

            # Отправляем аудио
            try:
                async with aiofiles.open(file_path, 'rb') as audio_file:
                    audio_data = await audio_file.read()
                    await context.bot.send_audio(
                        chat_id=chat_id,
                        audio=audio_data,
                        title=(track.get('title') or 'Неизвестный трек')[:64],
                        performer=(track.get('artist') or 'Неизвестный исполнитель')[:64],
                        caption=f"🎵 <b>{track.get('title', 'Неизвестный трек')}</b>\n"
                               f"⏱️ {self.format_duration(track.get('duration'))}",
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
            
            all_tracks = []
            
            # Пробуем каждый вариант поиска
            for search_query in search_variants:
                print(f"🔍 Пробуем поиск: {search_query}")
                tracks = await self._search_tracks(search_query, limit * 2)
                
                if tracks:
                    print(f"✅ Найдено {len(tracks)} треков по запросу: {search_query}")
                    all_tracks.extend(tracks)
                    # Если нашли достаточно треков, прерываем поиск
                    if len(all_tracks) >= limit * 2:
                        break
            
            if not all_tracks:
                print(f"❌ Не найдено треков ни по одному варианту: {search_variants}")
                return None
            
            # Убираем дубликаты по URL
            unique_tracks = {}
            for track in all_tracks:
                url = track.get('webpage_url')
                if url and url not in unique_tracks:
                    unique_tracks[url] = track
            
            # Сортируем по релевантности
            sorted_tracks = self._sort_tracks_by_relevance(list(unique_tracks.values()), query)
            
            # Возвращаем лучшие треки
            return sorted_tracks[:limit]

    async def _search_tracks(self, query: str, limit: int = 6):
        """Внутренняя функция поиска треков"""
        ydl_opts = {
            'format': 'bestaudio/best',
            'quiet': True,
            'no_warnings': True,
            'extract_flat': True,
            'ignoreerrors': True,
            'noplaylist': True,
            'socket_timeout': 8,  # Уменьшен таймаут
            'extractaudio': True,
            'audioformat': 'mp3',
        }

        try:
            print(f"🔍 Выполняем поиск: {query}")
            
            def perform_search():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    return ydl.extract_info(f"scsearch{limit}:{query}", download=False)

            # Используем ThreadPoolExecutor для CPU-bound операций
            loop = asyncio.get_event_loop()
            info = await asyncio.wait_for(
                loop.run_in_executor(self.thread_pool, perform_search),
                timeout=SEARCH_TIMEOUT
            )

            if not info:
                print(f"❌ Поиск не дал результатов: {query}")
                return None

            entries = info.get('entries', [])
            if not entries and info.get('_type') != 'playlist':
                entries = [info]

            print(f"✅ Найдено {len(entries)} результатов по запросу: {query}")

            # Ускоренная фильтрация
            results = []
            for entry in entries:
                if not entry:
                    continue

                # Быстрая проверка длительности
                duration = entry.get('duration') or 0
                if duration < 30 or duration > 3600:
                    continue

                title = self.clean_title(entry.get('title') or '')
                if not title:
                    continue

                webpage_url = entry.get('webpage_url') or entry.get('url') or ''
                if not webpage_url:
                    continue

                results.append({
                    'title': title,
                    'webpage_url': webpage_url,
                    'duration': duration,
                    'artist': entry.get('uploader') or entry.get('uploader_id') or 'Неизвестно'
                })

                # Ограничиваем количество обрабатываемых записей для скорости
                if len(results) >= limit:
                    break

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

    def _sort_tracks_by_relevance(self, tracks: list, original_query: str) -> list:
        """Сортирует треки по релевантности запросу"""
        original_query_lower = original_query.lower()
        
        for track in tracks:
            title_lower = track.get('title', '').lower()
            relevance_score = 0
            
            # Высокий приоритет для точного совпадения
            if original_query_lower in title_lower:
                relevance_score += 10
            
            # Приоритет для официальных релизов
            if 'official' in title_lower:
                relevance_score += 5
            elif 'original' in title_lower:
                relevance_score += 3
            
            track['relevance_score'] = relevance_score
        
        # Сортируем по релевантности (убывание) и длительности (убывание)
        tracks.sort(key=lambda x: (-x.get('relevance_score', 0), -x.get('duration', 0)))
        
        return tracks

    # ==================== СКАЧИВАНИЕ ====================

    async def download_track(self, url: str) -> str:
        """Скачивает трек и возвращает путь к файлу"""
        if not self.is_valid_url(url):
            print(f"❌ Невалидный URL: {url}")
            return None

        async with self.download_semaphore:
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

                # Используем ThreadPoolExecutor для скачивания
                loop = asyncio.get_event_loop()
                info = await asyncio.wait_for(
                    loop.run_in_executor(self.thread_pool, download_track),
                    timeout=DOWNLOAD_TIMEOUT
                )

                if not info:
                    print("❌ yt-dlp не вернул информацию")
                    return None

                # Быстрый поиск файлов
                telegram_audio_extensions = ['.mp3', '.m4a']
                
                for file in os.listdir(tmpdir):
                    file_ext = os.path.splitext(file)[1].lower()
                    if file_ext in telegram_audio_extensions:
                        file_path = os.path.join(tmpdir, file)
                        
                        # Быстрая проверка размера файла
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
                # Асинхронная очистка временной директории
                async def cleanup():
                    await asyncio.sleep(1)  # Уменьшена задержка
                    try:
                        shutil.rmtree(tmpdir, ignore_errors=True)
                        print(f"✅ Очищена временная директория: {tmpdir}")
                    except Exception as e:
                        print(f"⚠️ Не удалось очистить временную директорию: {e}")
                
                asyncio.create_task(cleanup())

    # ==================== КОМАНДЫ ====================

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /start"""
        user = update.effective_user
        await update.message.reply_text(
            f"🎵 <b>Универсальный музыкальный бот</b>\n\n"
            f"👋 Привет, {user.mention_html()}!\n\n"
            f"📢 <b>Доступные команды:</b>\n"
            f"• <code>найди [запрос]</code> - найти треки (показывает 3 варианта)\n"
            f"• <code>/find [запрос]</code> - найти треки (команда)\n"
            f"• <code>рандом</code> - случайный трек\n"
            f"• <code>/random</code> - случайный трек (команда)\n\n"
            f"🚀 <b>Начни поиск музыки!</b>",
            parse_mode='HTML'
        )

    # ==================== ЗАПУСК БОТА ====================

    def run(self):
        print('🚀 Запуск УСКОРЕННОГО Music Bot...')
        print('💡 Бот работает ВО ВСЕХ чатах (ЛС и группы)')
        print('🎯 Реагирует на: "найди", "/find", "рандом", "/random"')
        print('🛡️  Rate limiting: {} запросов/минуту'.format(REQUESTS_PER_MINUTE))
        print('🎵 Показывает 3 трека на кнопках для выбора')
        print('🔍 Ускоренный поиск: кэширование + оптимизированные запросы')
        print('⚡ Ускоренное скачивание: 4 одновременных загрузки')
        print('🔤 ТРАНСЛИТЕРАЦИЯ: Поиск работает как по кириллице, так и по латинице')
        print('🔒 БЕЗОПАСНОСТЬ: Только пользователь, запустивший поиск, может выбирать треки')
        print('💾 КЭШИРОВАНИЕ: Результаты поиска кэшируются на 10 минут')
        print('🚀 ThreadPool: 8 рабочих потоков для CPU-bound операций')

        self._create_application()

        # Устанавливаем команды бота
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.setup_bot_commands(self.app))
        finally:
            loop.close()

        max_retries = 3
        for attempt in range(max_retries):
            try:
                print(f'🔄 Попытка запуска {attempt + 1}/{max_retries}...')
                self.app.run_polling(
                    poll_interval=0.5,  # Уменьшен интервал опроса
                    timeout=20,         # Уменьшен таймаут
                    drop_pending_updates=True
                )
                break
                
            except Conflict as e:
                if attempt < max_retries - 1:
                    wait_time = 10 * (attempt + 1)
                    print(f'⚠️ Конфликт: {e}')
                    print(f'⏳ Ждем {wait_time} секунд перед повторной попыткой...')
                    time.sleep(wait_time)
                else:
                    print('❌ Не удалось запустить бота из-за конфликта. Убедитесь, что не запущено других инстансов бота.')
                    raise
                    
            except (TimedOut, NetworkError) as e:
                if attempt < max_retries - 1:
                    wait_time = 5 * (attempt + 1)
                    print(f'⚠️ Сетевая ошибка: {e}')
                    print(f'⏳ Ждем {wait_time} секунд перед повторной попыткой...')
                    time.sleep(wait_time)
                else:
                    print('❌ Не удалось запустить бота из-за сетевых ошибок')
                    raise
                    
            except Exception as e:
                print(f'❌ Непредвиденная ошибка: {e}')
                raise

if __name__ == '__main__':
    bot = UniversalMusicBot()
    bot.run()
