# Operator Operations Runbook

This runbook covers the normal day-2 operator workflow for `ts-connect`.

## First Startup

1. Prepare `.env` from `.env.example`.
2. Start the main stack:

```bash
docker compose up -d
```

3. Start the update-agent separately:

```bash
cd update-agent
docker compose --env-file ../.env up -d
```

4. Open the UI and read the generated admin password from:

```bash
ui/data/admin_password.generated
```

5. Change the admin password after first login or provide `TS_CONNECT_UI_ADMIN_PASSWORD` in `.env`.

## Required Host Path

For in-app updates and host-persistent mounts, set:

```env
TS_CONNECT_WORKSPACE_HOST=/absolute/path/to/connector
```

Without it, Docker bind mounts may resolve against `/workspace/...` inside the running container instead of the real host path.

## Network Exposure

Supported exposure models:

1. localhost-only plus reverse proxy or VPN
   - set `UI_BIND_IP=127.0.0.1`
   - publish the UI only through a deliberate reverse-proxy or VPN layer

2. direct access inside the club LAN
   - bind the UI to a specific private LAN IP
   - keep `UI_TRUSTED_CIDRS` aligned to the club subnet

Avoid treating `0.0.0.0` as a public default. If you bind to all interfaces, you are expected to enforce the real network boundary with firewall rules, VPN, or a reverse proxy.

Additional rules:

- keep the update-agent host publish on `127.0.0.1:9000`
- keep the host-agent on `127.0.0.1:9010` unless you have a very deliberate tunnel/proxy setup
- do not expose the host-agent directly on a public interface

## Daily Operator Checks

In the UI, check:

- connector status
- license status
- offline buffer status
- mirror backup status
- update status

Important host-persistent paths:

- logs: `${TS_CONNECT_WORKSPACE_HOST}/ui/logs`
- mirror DB backups: `${TS_CONNECT_WORKSPACE_HOST}/ui/backups/mirror-db`
- local data and secrets: `${TS_CONNECT_WORKSPACE_HOST}/ui/data`

## Updating

Normal path:

1. Open the update card in the UI.
2. Run `Nach Update suchen`.
3. Start the update when a newer stable release is available.

The update flow performs:

- git refresh of the checkout
- image pull from ACR
- `docker compose up -d`

If a manual correction is needed:

```bash
cd /opt/ts-connect
git pull --ff-only origin main
cd update-agent
docker compose --env-file ../.env up -d
cd ..
docker compose --env-file .env pull
docker compose --env-file .env up -d
```

## Logs

Important log files:

- `ui.log`
- `health.log`
- `update-agent.log`
- `update-runner.log`
- `host-agent.log` when the host-agent is installed

Default host locations:

- `${TS_CONNECT_WORKSPACE_HOST}/ui/logs`
- `${TS_HOST_AGENT_LOG_DIR:-/var/lib/ts-connect-host-agent/logs}`

## Backups

There are two different backup/export paths:

### Offline-Puffer-Export

- exports `buffer_events` as NDJSON
- this is not a full MariaDB dump

### Mirror-DB-Backup

- creates a full `sql.gz` dump of the local `ts-mariadb-mirror`
- runs automatically once per day
- retention defaults to 30 days

Relevant env vars:

- `TS_CONNECT_MIRROR_BACKUP_ENABLED`
- `TS_CONNECT_MIRROR_BACKUP_RETENTION_DAYS`
- `TS_CONNECT_MIRROR_BACKUP_HOUR`
- `TS_CONNECT_MIRROR_BACKUP_MINUTE`

## Storage Ownership Repair

Two bind-mounted DB data directories must belong to the DB container users, not to the repo owner:

- `ui/data/backup-db`
- `ui/data/mariadb`

If their ownership is broken, the UI now shows a targeted warning and a repair button.

Important rule:

- never run `chown -R` over the whole connector workspace

Only the affected DB data directories should be repaired.

## Host-Agent

The host-agent is optional. Install it only if you want:

- OS package updates from the UI
- orchestrated host reboot

Recommended listen endpoint:

```text
127.0.0.1:9010
```

Required UI config:

```env
TS_CONNECT_HOST_AGENT_URL=http://127.0.0.1:9010
TS_CONNECT_HOST_AGENT_TOKEN=...
```

## Licensing

The Club Plus operator flow is documented separately:

- [keygen-activation-runbook.md](./keygen-activation-runbook.md)

Use the live verification helper on a running connector:

```bash
python3 scripts/verify_club_plus_runtime.py
python3 scripts/verify_club_plus_runtime.py --json
```

## Topic Contract

MirrorMaker/Kafka topic cleanup boundaries are documented separately:

- [topic-contract.md](./topic-contract.md)

Do not delete MM2 worker state topics while MM2 is active.
