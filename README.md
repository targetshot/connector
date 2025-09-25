# TargetShot Connect Docker Compose Package

This package bundles TargetShot Connect (Kafka Connect + MariaDB Debezium connector + web UI) for self-hosted deployments.

## Quick Start

```bash
git clone git@github.com:targetshot/connector.git
cd connector
cp .env.example .env  # adjust secrets
cp ui/.env.example ui/.env  # optional: UI-specific overrides
cp compose.env.example compose.env  # optional: compose overrides
docker compose up -d
```

### Services
- `redpanda`: local Kafka (single node) for offsets/history
- `kafka-connect`: Confluent Kafka Connect with Debezium MySQL plugin
- `ui`: FastAPI web UI to manage connector, tests, secrets

## Folder Structure
```
compose.yml
compose.env.example
Dockerfile (under connect/ and ui/)
ui/
connect/
README.md
```

Publish versioned releases by tagging branches (e.g., `connector-compose-v0.1`).
