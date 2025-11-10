FROM python:3.11-slim
WORKDIR /app

# Systemdeps schlank halten, Docker CLI & Compose manuell installieren
ARG DOCKER_CLI_VERSION=26.1.3
ARG DOCKER_COMPOSE_VERSION=2.29.7
ARG DOCKER_BUILDX_VERSION=0.17.1
ARG TARGETOS
ARG TARGETARCH
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) DOCKER_RELEASE_ARCH="x86_64"; DOCKER_BUILDX_ARCH="amd64" ;; \
      arm64) DOCKER_RELEASE_ARCH="aarch64"; DOCKER_BUILDX_ARCH="arm64" ;; \
      *) echo "Unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    apt-get update \
    && apt-get install -y --no-install-recommends git curl ca-certificates tar \
    && curl -fsSL "https://download.docker.com/linux/static/stable/${DOCKER_RELEASE_ARCH}/docker-${DOCKER_CLI_VERSION}.tgz" -o /tmp/docker-cli.tgz \
    && tar -xzf /tmp/docker-cli.tgz -C /tmp docker/docker \
    && mv /tmp/docker/docker /usr/bin/docker \
    && chmod +x /usr/bin/docker \
    && rm -rf /tmp/docker /tmp/docker-cli.tgz \
    && mkdir -p /usr/lib/docker/cli-plugins \
    && curl -fsSL "https://github.com/docker/compose/releases/download/v${DOCKER_COMPOSE_VERSION}/docker-compose-linux-${DOCKER_RELEASE_ARCH}" -o /usr/lib/docker/cli-plugins/docker-compose \
    && chmod +x /usr/lib/docker/cli-plugins/docker-compose \
    && curl -fsSL "https://github.com/docker/buildx/releases/download/v${DOCKER_BUILDX_VERSION}/buildx-v${DOCKER_BUILDX_VERSION}.linux-${DOCKER_BUILDX_ARCH}" -o /usr/lib/docker/cli-plugins/docker-buildx \
    && chmod +x /usr/lib/docker/cli-plugins/docker-buildx \
    && git config --global --add safe.directory /workspace \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8080
CMD ["uvicorn","app:app","--host","0.0.0.0","--port","8080"]
