FROM python:3.11-slim

WORKDIR /app

# Устанавливаем ffmpeg и системные зависимости
RUN apt-get update && apt-get install -y \
    ffmpeg \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Копируем requirements и устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем основной код
COPY . .

# Создаем директорию для данных
RUN mkdir -p /app/data

# Создаем не-root пользователя для безопасности
RUN useradd -m -r bot && chown -R bot:bot /app
USER bot

# Запускаем бота
CMD ["python", "main.py"]
