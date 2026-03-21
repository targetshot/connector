# TargetShot Connect Docker Compose Package

This package bundles TargetShot Connect (Kafka Connect + local mirror MariaDB + Debezium connector + web UI) for self-hosted deployments.

## Quick Start

```bash
git clone https://github.com/targetshot/connector.git
cd connector
cp .env.example .env  # adjust secrets
docker compose up -d
# Update-Agent separat starten (damit `docker compose down` den Runner nicht stoppt)
cd update-agent
docker compose --env-file ../.env up -d
```

Hinweis: `compose.env` wird nicht mehr verwendet. Nutze ab sofort ausschließlich `./.env`.

Die empfohlene Pipeline ist jetzt:
- Vereins-Meyton-DB -> lokale `mariadb-mirror` auf dem Server
- Debezium Connector -> liest aus `mariadb-mirror`
- Kafka Topics -> lokal (Redpanda) und optional weiter in die Cloud (MirrorMaker)

### Versioning & Releases
- Update the version defaults once per release. Run `./scripts/bump_version.sh vX.Y.Z [ReleaseName]` from the repo root. This updates `ts-connect/VERSION` (and optionally `ts-connect/RELEASE`).

- For a fully automated workflow (stage dev changes → merge main → bump version → wait for the build → promote `stable`), use `./scripts/release.sh vX.Y.Z [ReleaseName]`. The script requires `git`, `gh`, and `jq` to be installed and authenticated.
- Commit the version file changes and merge them into `main`. Pushing to `main` automatically triggers the **Build & publish ts-connect** workflow.
- The UI reads `VERSION`/`RELEASE` automatically when `TS_CONNECT_VERSION`/`TS_CONNECT_RELEASE` are not set.
- If you need to run `docker compose` from another directory (e.g. via systemd), set `TS_CONNECT_WORKSPACE_HOST=/absolute/path/to/connector` in `.env`. Otherwise the default bind `.:/workspace` (relative to `compose.yml`) is used.

### Container Images & Channels
- Pushes to `main` run the **Build & publish ts-connect** workflow. It logs into Azure via OIDC (Environment `release`), builds AMD64/ARM64 images with Buildx, signs the resulting manifest with Cosign, and publishes it to `targetshot.azurecr.io/ts-connect:<version>` and `:<commit>`. The digest is immediately re-tagged as `targetshot.azurecr.io/ts-connect:beta` without rebuilding.
- Promotions (beta → stable → lts) happen via the manual **Promote ts-connect image** workflow. Supply either a tag (`beta`, `stable`, `lts`, or a version) or a raw digest (`sha256:...`) and choose the target channel. The workflow simply re-tags the digest inside ACR, so rollbacks are just another promotion run.
- Customers should pin to the desired channel, e.g.:
  ```yaml
  services:
    ts-connect:
      image: targetshot.azurecr.io/ts-connect:stable
  ```
  With Watchtower: `containrrr/watchtower --interval 900 --rolling-restart`.
- Cosign signatures stay valid across promotions because tags all reference the same digest; verification tooling should pin to the digest rather than the floating channel tag.

### Updates & Rollout
- `compose.yml` references `${TS_CONNECT_UI_IMAGE:-targetshot.azurecr.io/ts-connect:stable}` for the UI container. Production installs keep the default channel; local development can override `TS_CONNECT_UI_IMAGE` and still run `docker compose build` to create a bespoke image.
- The UI’s manual update button (and the nightly auto-update, if enabled) now performs `git pull`, `docker compose pull`, and `docker compose up -d` to roll out the latest `stable` channel from ACR. Set `TS_CONNECT_UPDATE_BUILD_LOCAL=true` only when you explicitly want the update runner to rebuild the image from source.
- The update card compares your local `ts-connect/VERSION` with the `org.opencontainers.image.version` label of `${TS_CONNECT_UPDATE_CHECK_IMAGE:-TS_CONNECT_UI_IMAGE}` (default `targetshot.azurecr.io/ts-connect:stable`). Override this variable if you want the UI to monitor another tag (e.g. `beta`).
- Environments without Watchtower can continue to rely on the built-in manual/auto-update flow; Watchtower is optional and simply adds continuous polling for the channel tag.
- Azure Container Registry authentication: either log in once per host (`docker login targetshot.azurecr.io -u <user> --password-stdin`) using an ACR token/service principal, or set `TS_CONNECT_ACR_USERNAME/TS_CONNECT_ACR_PASSWORD` (and optionally `TS_CONNECT_ACR_REGISTRY`) in `.env` so the update runner performs `docker login` automatically before `docker compose pull`. Using a Docker credential helper (`pass`, `secretservice`, `osxkeychain`, …) is recommended to store the token securely.

### Host-Agent für OS-Updates & Neustart
Der neue Host-Agent läuft außerhalb von Docker direkt auf dem Server und kümmert sich um sichere Betriebssystem-Updates sowie orchestrierte Reboots. Die UI spricht ihn über eine geschützte HTTP-API an.

1. **Installieren**
   ```bash
   cd /opt/ts-connect/host-agent
   python3 -m venv /opt/ts-connect/.venv-host-agent
   source /opt/ts-connect/.venv-host-agent/bin/activate
   pip install -r requirements.txt
   ```
   Beispiel-Unit `/etc/systemd/system/ts-connect-host-agent.service` (läuft als `tsconnect`):
   ```ini
   [Unit]
   Description=TargetShot Host Agent
   After=network.target docker.service

   [Service]
   Type=simple
   User=tsconnect
   Group=tsconnect
   WorkingDirectory=/opt/ts-connect/host-agent
   Environment=TS_HOST_WORKSPACE=/opt/ts-connect
   Environment=TS_HOST_COMPOSE_ENV=.env
   Environment=TS_HOST_UPDATE_AGENT_PATH=/opt/ts-connect/update-agent
   ExecStart=/opt/ts-connect/.venv-host-agent/bin/python host_agent.py --host 127.0.0.1 --port 9010
   Restart=on-failure

   [Install]
   WantedBy=multi-user.target
   ```
   Der Agent erstellt automatisch `/etc/ts-connect-host-agent/token` und `/var/lib/ts-connect-host-agent/state.json`.

2. **UI anbinden**
   - Setze in `.env`:  
     ```
     TS_CONNECT_HOST_AGENT_URL=http://127.0.0.1:9010
     TS_CONNECT_HOST_AGENT_TOKEN=<Inhalt aus /etc/ts-connect-host-agent/token>
     ```
     (Alternativ den Token nach `ui/data/host-agent.token` kopieren.)
     Falls `host.docker.internal` auf deiner Docker-Version nicht auf den Host zeigt, setze zusätzlich
     `TS_CONNECT_HOST_GATEWAY_OVERRIDE=mein-hostname:meine-ip` (Standard `host.docker.internal:host-gateway`).
   - UI + Update-Agent neu starten:
     ```bash
     docker compose up -d
     cd update-agent && docker compose --env-file ../.env up -d
     ```

3. **Funktionsumfang**
   - `Systemcheck` ruft `apt-get update && apt list --upgradable` direkt auf dem Host auf.
   - `Pakete aktualisieren` startet `apt-get upgrade && apt-get autoremove`.
   - `Server neu starten` stoppt zuerst den Haupt-Stack (`docker compose down`), anschließend den Update-Agent, synchronisiert das Dateisystem und triggert einen zeitnahen Reboot (Standardverzögerung `TS_CONNECT_HOST_REBOOT_DELAY`, Default 60 s).

Fehlt die Konfiguration, zeigt die UI einen entsprechenden Hinweis. Der Host-Agent benötigt Root-Rechte (oder passende sudo-Regeln), damit apt und `shutdown` ausgeführt werden können.

### Services
- `redpanda`: local Kafka (single node) for offsets/history
- `kafka-connect`: Confluent Kafka Connect with Debezium MySQL plugin
- `mariadb-mirror`: lokale MariaDB mit aktiviertem Binlog (Debezium-Quelle)
- `ui`: FastAPI web UI to manage connector, tests, secrets
- `update-agent`: Sidecar API (jetzt als eigenes Compose-Projekt unter `update-agent/compose.yml`) – kümmert sich um Pull/Restart, ohne sich selbst zu stoppen
- `schema-registry`: lokaler Schema Registry Dienst für Avro/JSON Converter
- `mirror-maker`: Kafka MirrorMaker 2, spiegelt die normalisierten Topics (`ts.sds-test.*`) in die Confluent Cloud sobald erreichbar
- `streams-transform`: Kafka Streams Anwendung, die Vereins-Topics auf einheitliche Confluent-Topics mapped
- `backup-db`: lokaler PostgreSQL-Puffer für Offline-Backups

### Notes
- UI binds to `${UI_BIND_IP:-0.0.0.0}` by default. Set `UI_BIND_IP=127.0.0.1` in `.env` for localhost-only access.
- `UI_TRUSTED_CIDRS` is now validated at startup. Invalid CIDR entries are ignored with a warning, and loopback (`127.0.0.0/8`, `::1/128`) stays allowed so local admin access does not lock itself out.
- Debezium nutzt die lokale Mirror-DB ausschließlich über `.env` (`TS_CONNECT_DEFAULT_DB_HOST`, `TS_CONNECT_DEFAULT_DB_PORT`, `TS_CONNECT_DEFAULT_DB_USER`, `TS_CONNECT_MIRROR_DB_PASSWORD`).
- Port intern bleibt standardmäßig `3306`, extern `${TS_CONNECT_MIRROR_PORT:-3307}`.
- On first start the UI now generates a random admin password and stores it inside `ui/data/admin_password.generated` (container path `/app/data/admin_password.generated`). Read the file once, log in, and immediately rotate the password via the Admin section or by setting `TS_CONNECT_UI_ADMIN_PASSWORD` in `.env`. The legacy alias `UI_ADMIN_PASSWORD` is still accepted for compatibility.
- Sessions are signed with `TS_CONNECT_UI_SESSION_SECRET`. If the variable is absent, a random value is written to `/app/data/session_secret`. Supplying your own secret in `.env` keeps logins valid across re-installs. The legacy alias `UI_SESSION_SECRET` is still accepted for compatibility.
- Cross-container secrets (e.g. `secrets.properties`) are automatically written with UID/GID `1000`. Override this via `TS_CONNECT_SECRETS_UID`/`TS_CONNECT_SECRETS_GID` if your Kafka Connect container runs with another user.
- Docker socket access moved into the dedicated `update-agent` service. The UI talks to it via `TS_CONNECT_UPDATE_AGENT_URL` (defaults to `http://update-agent:9000`) and authenticates with `TS_CONNECT_UPDATE_AGENT_TOKEN`. Leave the token empty to auto-generate a shared secret in `ui/data/update-agent.token`.
- Operative Logs werden standardmäßig host-persistent aus `${TS_CONNECT_WORKSPACE_HOST:-.}/ui/logs` gemountet und liegen im Container unter `${TS_CONNECT_LOG_DIR:-/app/logs}`.
- Vollständige Mirror-DB-Backups werden standardmäßig täglich als `sql.gz` nach `${TS_CONNECT_MIRROR_BACKUP_DIR:-/workspace/ui/backups/mirror-db}` geschrieben; auf dem Host entspricht das `${TS_CONNECT_MIRROR_BACKUP_HOST_DIR:-${TS_CONNECT_WORKSPACE_HOST}/ui/backups/mirror-db}`.
- Die Rotation entfernt Mirror-DB-Backups automatisch nach `${TS_CONNECT_MIRROR_BACKUP_RETENTION_DAYS:-30}` Tagen. Der Zeitplan wird über `TS_CONNECT_MIRROR_BACKUP_HOUR` und `TS_CONNECT_MIRROR_BACKUP_MINUTE` gesteuert.
- Für In-App-Updates aus dem laufenden Container heraus muss `TS_CONNECT_WORKSPACE_HOST` in `.env` auf den absoluten Host-Pfad des Connector-Workspaces gesetzt sein, z.B. `/opt/ts-connect`. Sonst werden Docker-Bind-Mounts aus `/workspace/...` aufgelöst und `docker compose up -d` schlägt fehl.
- Wichtige Dateien dort:
  - `ui.log`
  - `health.log`
  - `update-agent.log`
  - `update-runner.log`
- Die UI trennt jetzt zwei Export-Arten:
  - `Offline-Puffer exportieren`: `buffer_events` als `.ndjson`
  - `Mirror-DB sichern`: vollständiger Dump der lokalen `ts-mariadb-mirror` als serverseitig abgelegtes `sql.gz`
- `ui.log` lässt sich weiterhin direkt im UI-Bereich "System-Logs" anzeigen.
- Der Host-Agent schreibt standardmäßig nach `${TS_HOST_AGENT_LOG_DIR:-/var/lib/ts-connect-host-agent/logs}/host-agent.log`.
- Gespeicherte Confluent-Zugangsdaten bleiben in `secrets.properties` erhalten; beim UI-/Container-Start wird die Connector-/MirrorMaker-Konfiguration automatisch erneut angewendet, sobald Lizenz und lokale Mirror-DB verfügbar sind.

### Secret Inventory
- Kanonische Laufzeit-Secrets liegen ausschließlich in `ts-connect/.env`.
- GitHub-Actions-Secrets für Releases leben ausschließlich im Repository `targetshot/connector` unter `Settings -> Secrets and variables -> Actions`:
  - `AZURE_CLIENT_ID`
  - `AZURE_TENANT_ID`
- Wichtige Laufzeit-Secrets in `.env`:
  - `TS_CONNECT_UI_ADMIN_PASSWORD`
  - `TS_CONNECT_UI_SESSION_SECRET`
  - `TS_CONNECT_UPDATE_AGENT_TOKEN`
  - `TS_CONNECT_HOST_AGENT_TOKEN`
  - `TS_CONNECT_ACR_USERNAME`, `TS_CONNECT_ACR_PASSWORD`
  - `TS_CONNECT_MIRROR_ROOT_PASSWORD`, `TS_CONNECT_MIRROR_DB_PASSWORD`
  - `TS_CONNECT_SOURCE_DB_REPL_PASSWORD`
  - `TS_CONNECT_BACKUP_PASSWORD`
  - `TS_CONNECT_KEYGEN_ACCOUNT`, `TS_CONNECT_KEYGEN_POLICY_ID`
  - optional `TS_CONNECT_KEYGEN_LICENSE_TOKEN`
  - optional `TS_CONNECT_GITHUB_TOKEN`
- Automatisch erzeugte lokale Secret-Dateien:
  - `ui/data/admin_password.generated`
  - `ui/data/session_secret`
  - `ui/data/update-agent.token`
  - `ui/data/host-agent.token`
  - `ui/data/license.key`
  - `/etc/ts-connect-host-agent/token`
- Persistierte Connector-/Cloud-Secrets:
  - `ui/data/secrets.properties`
  - enthaltend u. a. `db_password`, `source_db_repl_password`, `confluent_sasl_password`, `schema_registry_secret`, `backup_pg_password`
- `ui/.env.example` wird nicht mehr verwendet und wurde entfernt. Pflege erfolgt nur noch über `./.env`.

### Kafka Streams Transformation
- Der Dienst `streams-transform` abonniert sowohl Legacy-Debezium-Topics (`<Vereinsnummer>.(SMDB|SSMDB2).(Schuetze|Treffer|Scheiben|Serien)`) als auch geroutete `ts.raw.*`-Topics und leitet sie in die Standard-Topics `ts.sds-test.{schuetze,treffer,scheiben,serien}` weiter.
- Standardmäßig verbindet sich die Anwendung mit `redpanda:9092`; per `TS_STREAMS_TARGET_PREFIX` lässt sich das Zielpräfix anpassen.
- Die erzeugten Ziele werden zusätzlich über MirrorMaker 2 in die Confluent Cloud repliziert (`ts.sds-test.*`).
- Feintuning (Application ID, Pattern, Threads, Commit-Intervalle) erfolgt über die optionalen `TS_STREAMS_*` Variablen in `.env`.
- Der Topic-Vertrag ist bewusst zweigeteilt: `ts.sds-test.*` bleibt das Produktpräfix, während MM2/Kafka-Connect-Internthemen separat geführt werden. Details und Cleanup-Hinweise stehen in [docs/topic-contract.md](docs/topic-contract.md).

### Mirror-MariaDB Replikation
- Die Debezium-Connector-Zugangsdaten für die lokale Mirror-DB werden nicht über die UI geändert, sondern über `.env` gesetzt.
- Die Replikationsquelle kann direkt in der UI unter **Vereins-MainDB-Replikation** gepflegt und live angewendet werden.
- GTID/Binlog/Connect-Retry werden in der UI nicht mehr abgefragt und intern mit stabilen Defaults betrieben.
- Der Container `mariadb-mirror` kann beim ersten Start automatisch als Replikat der Vereins-Meyton-DB konfiguriert werden.
- Dafür in `.env` setzen:
  - `TS_CONNECT_SOURCE_DB_HOST`, `TS_CONNECT_SOURCE_DB_PORT`
  - `TS_CONNECT_SOURCE_DB_REPL_USER`, `TS_CONNECT_SOURCE_DB_REPL_PASSWORD`
  - `TS_CONNECT_SOURCE_DB_GTID_MODE=true` (Standard)
- Falls die Quell-DB kein GTID verwendet:
  - `TS_CONNECT_SOURCE_DB_GTID_MODE=false`
  - `TS_CONNECT_SOURCE_DB_LOG_FILE`, `TS_CONNECT_SOURCE_DB_LOG_POS`
- Nach dem Start kannst du den Replikationsstatus prüfen:
  ```bash
  docker exec -it ts-mariadb-mirror mariadb -uroot -p"$TS_CONNECT_MIRROR_ROOT_PASSWORD" -e "SHOW SLAVE STATUS\\G"
  ```

### Offline-Puffer konfigurieren
- Der Offline-Puffer ist dauerhaft aktiv. Alle Debezium-Events werden lokal in der Postgres-Datenbank `buffer_events` zwischengespeichert und bei verfügbarer Verbindung automatisch hochgeladen.
- MirrorMaker 2 repliziert die lokal gepufferten Topics nach Confluent, sobald eine Verbindung besteht. Die benötigten Zugangsdaten bleiben in `secrets.properties` hinterlegt.
- Das Backup-Passwort wird beim ersten Start zufällig generiert und intern verwaltet. Optional kann ein initialer Wert über `TS_CONNECT_BACKUP_PASSWORD` gesetzt werden.
- Über den Button *Backup exportieren* in der UI lässt sich ein NDJSON-Dump der Tabelle `buffer_events` herunterladen.
- Die Aufbewahrungszeit richtet sich nach der Lizenz:
  - **Nicht lizenziert**: 14 Tage
  - **Club Plus**: 30 Tage
- Standard-Credentials für den Postgres-Puffer können über `TS_CONNECT_BACKUP_DB/USER/PORT` in `.env` angepasst werden.
- Die UI legt die Tabelle `buffer_events` automatisch an und entfernt alte Einträge gemäß Lizenzlaufzeit.

### Lizenzprüfung (Keygen)
- Hinterlege den vereinsgebundenen Club-Plus-Lizenzschlüssel im Abschnitt *Lizenzverwaltung*. Die UI prüft den Schlüssel gegen Keygen und kann die aktuelle Installation als Maschine aktivieren.
- Die Club-Lizenz wird cloudseitig in `beta.targetshot.app` (`Club Billing`) verwaltet. Der Connector zieht den Schlüssel derzeit nicht automatisch aus der Cloud, sondern erwartet weiterhin, dass der Operator den zugewiesenen Key lokal einfügt.
- Eine aktive Club-Plus-Lizenz steuert die Aufbewahrungsdauer des Offline-Puffers. Ohne aktive Lizenz bleibt TargetShot Connect im lokalen Pufferbetrieb.
- Der Save-Pfad prüft zunächst die Club-Lizenz selbst. Die lokale Maschinenbindung erfolgt erst über *Installation aktivieren*.
- Umgebungskonfiguration:
  - `TS_CONNECT_KEYGEN_ACCOUNT`: Keygen Account-ID oder Slug.
  - `TS_CONNECT_KEYGEN_API_URL`: optionaler Override für die Keygen-API-URL.
  - `TS_CONNECT_KEYGEN_POLICY_ID`: Policy-ID der Club-Plus-Lizenz.
  - `TS_CONNECT_KEYGEN_POLICY_NAME`: Anzeigename der Policy (optional, Standard: `Club Plus`).
  - `TS_CONNECT_KEYGEN_LICENSE_TOKEN`: optionaler Keygen-Token. Für die Aktivierung reicht in der Regel die Lizenzschlüssel-Authentifizierung.
  - `TS_CONNECT_KEYGEN_MACHINE_NAME`: optionaler Anzeigename der Maschine.
  - `TS_CONNECT_KEYGEN_MACHINE_FINGERPRINT`: optionaler fester Fingerprint. Ohne Override wird ein stabiler Fingerprint lokal erzeugt und unter `/app/data/machine_fingerprint` gespeichert.
  - `TS_CONNECT_KEYGEN_AUTO_ACTIVATE`: aktiviert die Maschine nach erfolgreicher Prüfung (Standard: `true`).
- Die Cloud-Replikation (MirrorMaker) startet erst, wenn die Lizenz aktiviert wurde; bis dahin verbleiben alle Events ausschließlich im lokalen Puffer.
- Ausführlicher Operator-Ablauf für Erstaktivierung, Schlüsseltausch, Maschinenwechsel und Recovery: [docs/keygen-activation-runbook.md](./docs/keygen-activation-runbook.md)
- Für die Live-Verifikation auf einem laufenden Connector steht zusätzlich ein lokales Prüfsignal bereit:
  ```bash
  python3 scripts/verify_club_plus_runtime.py
  python3 scripts/verify_club_plus_runtime.py --json
  ```
  Das Script liest `ui/data/config.db`, `license.key`, `license-meta.json` und `machine_fingerprint` und zeigt, ob der Club-Plus-Key lokal gespeichert, aktiviert und für die Laufzeit wirklich freigeschaltet ist.

## Folder Structure
```
compose.yml
.env.example
Dockerfile (under connect/ and ui/)
ui/
connect/
README.md
docs/
```

Ausführliche Dokumentation: <https://docs.targetshot.app/install/docker-compose/>
