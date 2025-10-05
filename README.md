# TargetShot Connect Docker Compose Package

This package bundles TargetShot Connect (Kafka Connect + MariaDB Debezium connector + web UI) for self-hosted deployments.

## Quick Start

```bash
git clone https://github.com/targetshot/connector.git
cd connector
cp .env.example .env  # adjust secrets
cp ui/.env.example ui/.env  # optional: UI-specific overrides
cp compose.env.example compose.env  # optional: compose overrides (e.g. UI_BIND_IP)
docker compose up -d
```

### Versioning & Releases
- Update the version defaults once per release. Run `./scripts/bump_version.sh vX.Y.Z [ReleaseName]` from the repo root. This updates `ts-connect/VERSION` (and optionally `ts-connect/RELEASE`).
- Commit the change, create an annotated tag (`git tag -a vX.Y.Z -m "Release vX.Y.Z"`), and push it (`git push origin vX.Y.Z`).
- The UI reads these files automatically when no explicit `TS_CONNECT_VERSION/TS_CONNECT_RELEASE` environment variables are set.
- GitHub releases should be created from the pushed tag so the auto-update job can discover the new version.

### Services
- `redpanda`: local Kafka (single node) for offsets/history
- `kafka-connect`: Confluent Kafka Connect with Debezium MySQL plugin
- `ui`: FastAPI web UI to manage connector, tests, secrets

### Notes
- UI binds to `${UI_BIND_IP:-0.0.0.0}` by default. Set `UI_BIND_IP=127.0.0.1` in `compose.env` for localhost-only access.

## Folder Structure
```
compose.yml
compose.env.example
Dockerfile (under connect/ and ui/)
ui/
connect/
README.md
```

Ausführliche Dokumentation: <https://docs.targetshot.app/install/docker-compose/>

Publish versioned releases by tagging branches (e.g., `connector-compose-v0.1`).
