FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Confluent Cloud credentials come from the platform's env vars/secrets at runtime
# (see config.py) - never baked into the image or committed to config/local.env.
ENV PORT=5050
EXPOSE 5050

CMD ["sh", "-c", "gunicorn --bind 0.0.0.0:${PORT} --workers 2 app:app"]
