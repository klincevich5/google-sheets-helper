FROM python:3.10-slim

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y gcc libffi-dev libssl-dev libpq-dev

# Создаем рабочую директорию
WORKDIR /app

# Копируем файлы проекта
COPY . .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Запускаем Flask-сервер
CMD ["python", "main.py"]
