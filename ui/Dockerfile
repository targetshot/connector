FROM python:3.11-slim
WORKDIR /app

# Systemdeps schlank halten, Docker CLI & Compose manuell installieren
ARG DOCKER_CLI_VERSION=26.1.3
ARG DOCKER_COMPOSE_VERSION=2.29.7
RUN apt-get update \
    && apt-get install -y --no-install-recommends git curl ca-certificates tar \
    && curl -fsSL "https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_CLI_VERSION}.tgz" -o /tmp/docker-cli.tgz \
    && tar -xzf /tmp/docker-cli.tgz -C /tmp docker/docker \
    && mv /tmp/docker/docker /usr/bin/docker \
    && chmod +x /usr/bin/docker \
    && rm -rf /tmp/docker /tmp/docker-cli.tgz \
    && mkdir -p /usr/lib/docker/cli-plugins \
    && curl -fsSL "https://github.com/docker/compose/releases/download/v${DOCKER_COMPOSE_VERSION}/docker-compose-linux-x86_64" -o /usr/lib/docker/cli-plugins/docker-compose \
    && chmod +x /usr/lib/docker/cli-plugins/docker-compose \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8080
CMD ["uvicorn","app:app","--host","0.0.0.0","--port","8080"]
