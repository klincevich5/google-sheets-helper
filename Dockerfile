# Dockerfile

FROM python:3.10-alpine

# Устанавливаем системные зависимости
RUN apk add --no-cache gcc musl-dev libffi-dev openssl-dev postgresql-dev

# Создаем рабочую директорию
WORKDIR /app

# Копируем файлы проекта
COPY . .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Запускаем Flask-сервер
CMD ["flask", "run", "--host=0.0.0.0", "--port=5000"]