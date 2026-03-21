# TargetShot Connect

`ts-connect` bundles the local mirror MariaDB, Kafka Connect/Debezium, Redpanda, the FastAPI admin UI, MirrorMaker, and the update sidecar for self-hosted club deployments.

## Quick Start

```bash
git clone https://github.com/targetshot/connector.git
cd connector
cp .env.example .env
docker compose up -d
cd update-agent
docker compose --env-file ../.env up -d
```

`compose.env` is no longer used. Maintain runtime configuration only in `./.env`.

## Documentation Map

- Operator startup, updates, backups, logs, and recovery:
  [docs/operator-operations-runbook.md](./docs/operator-operations-runbook.md)
- Club Plus / Keygen activation, machine moves, and recovery:
  [docs/keygen-activation-runbook.md](./docs/keygen-activation-runbook.md)
- Kafka / MirrorMaker topic contract and cleanup boundaries:
  [docs/topic-contract.md](./docs/topic-contract.md)
- Current module boundaries and future extraction rules:
  [docs/runtime-architecture.md](./docs/runtime-architecture.md)

## Core Flow

- Club main DB replicates into the local `mariadb-mirror`.
- Debezium reads from `mariadb-mirror` into local Kafka/Redpanda.
- `streams-transform` normalizes the local topics.
- MirrorMaker sends the normalized product topics to the cloud when the connector is licensed and connected.
- `backup-db` keeps the offline buffer locally.

## Main Services

- `redpanda`: local Kafka broker
- `kafka-connect`: Debezium / Kafka Connect runtime
- `mariadb-mirror`: local MariaDB mirror with binlog enabled
- `ui`: FastAPI admin UI
- `update-agent`: sidecar for pull/restart/update orchestration
- `schema-registry`: local schema registry
- `mirror-maker`: MM2 replication into the cloud
- `streams-transform`: maps club-specific source topics to the shared cloud topic contract
- `backup-db`: local PostgreSQL offline buffer

## Runtime Notes

- UI binds to `${UI_BIND_IP:-0.0.0.0}`. Set `UI_BIND_IP=127.0.0.1` for localhost-only access.
- `UI_TRUSTED_CIDRS` is validated at startup. Invalid CIDRs are ignored with a warning. Loopback stays allowed.
- For in-app updates from inside the container, set `TS_CONNECT_WORKSPACE_HOST` to the absolute host path of the checkout, e.g. `/opt/ts-connect`.
- Runtime logs are stored under `${TS_CONNECT_LOG_DIR:-/app/logs}` and mounted host-persistent from `${TS_CONNECT_WORKSPACE_HOST:-.}/ui/logs`.
- Mirror-DB-Backups are written as `sql.gz` into `${TS_CONNECT_MIRROR_BACKUP_DIR:-/workspace/ui/backups/mirror-db}` and rotated after `${TS_CONNECT_MIRROR_BACKUP_RETENTION_DAYS:-30}` days.
- Supported UI exposure models are documented in the operator runbook. Preferred setups are either `UI_BIND_IP=127.0.0.1` behind a reverse proxy/VPN or a specific private LAN IP for direct club-network access.
- The UI now separates:
  - `Offline-Puffer-Export`: NDJSON export of `buffer_events`
  - `Mirror-DB-Backup`: full dump of the local `ts-mariadb-mirror`
- Storage ownership issues on `ui/data/backup-db` and `ui/data/mariadb` are surfaced in the UI and can be repaired in a targeted way without touching the rest of the workspace.

## Club Plus / Licensing

- Club Plus is managed cloud-side in `beta.targetshot.app` under `Club Billing`.
- `ts-connect` still expects the operator to paste the assigned key locally and activate the installation as a machine.
- A valid and activated Club Plus license unlocks the full connector path and extends offline-buffer retention.
- Detailed operator flow:
  [docs/keygen-activation-runbook.md](./docs/keygen-activation-runbook.md)

Live verification on a running connector:

```bash
python3 scripts/verify_club_plus_runtime.py
python3 scripts/verify_club_plus_runtime.py --json
```

## Host-Agent

The host-agent remains optional and is only needed when you want OS package updates and orchestrated host reboots from the UI.

- Install and wire it as documented in:
  [docs/operator-operations-runbook.md](./docs/operator-operations-runbook.md)
- Default listen example: `127.0.0.1:9010`
- The UI expects:
  - `TS_CONNECT_HOST_AGENT_URL`
  - `TS_CONNECT_HOST_AGENT_TOKEN`

## Releases

- Bump the version with:

```bash
./scripts/bump_version.sh vX.Y.Z [ReleaseName]
```

- Or run the automated release helper:

```bash
./scripts/release.sh vX.Y.Z [ReleaseName]
```

- Pushes to `main` trigger `Build & publish ts-connect`.
- Stable/beta/lts promotions happen via the `Promote ts-connect image` workflow.
- Production installs should pin to a channel tag such as `targetshot.azurecr.io/ts-connect:stable`.

## Secret Inventory

Runtime secrets live in `ts-connect/.env`.

Important runtime secrets:

- `TS_CONNECT_UI_ADMIN_PASSWORD`
- `TS_CONNECT_UI_SESSION_SECRET`
- `TS_CONNECT_UPDATE_AGENT_TOKEN`
- `TS_CONNECT_HOST_AGENT_TOKEN`
- `TS_CONNECT_ACR_USERNAME`
- `TS_CONNECT_ACR_PASSWORD`
- `TS_CONNECT_MIRROR_ROOT_PASSWORD`
- `TS_CONNECT_MIRROR_DB_PASSWORD`
- `TS_CONNECT_SOURCE_DB_REPL_PASSWORD`
- `TS_CONNECT_BACKUP_PASSWORD`
- `TS_CONNECT_KEYGEN_ACCOUNT`
- `TS_CONNECT_KEYGEN_POLICY_ID`
- optional `TS_CONNECT_KEYGEN_LICENSE_TOKEN`
- optional `TS_CONNECT_GITHUB_TOKEN`

Auto-generated local secret files:

- `ui/data/admin_password.generated`
- `ui/data/session_secret`
- `ui/data/update-agent.token`
- `ui/data/host-agent.token`
- `ui/data/license.key`
- `/etc/ts-connect-host-agent/token`

Persisted connector/cloud secrets:

- `ui/data/secrets.properties`

GitHub Actions release secrets live only in `targetshot/connector` repository settings:

- `AZURE_CLIENT_ID`
- `AZURE_TENANT_ID`

## Folder Structure

```text
compose.yml
.env.example
ui/
connect/
docs/
update-agent/
host-agent/
README.md
```

Extended installation docs: <https://docs.targetshot.app/install/docker-compose/>
