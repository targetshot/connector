FROM python:3.11-slim
WORKDIR /app
# Systemdeps schlank halten
RUN apt-get update \
    && apt-get install -y --no-install-recommends git docker.io docker-compose-plugin \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir --upgrade pip
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8080
CMD ["uvicorn","app:app","--host","0.0.0.0","--port","8080"]
