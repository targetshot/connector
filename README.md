# TargetShot Connect Docker Compose Package

This package bundles TargetShot Connect (Kafka Connect + MariaDB Debezium connector + web UI) for self-hosted deployments.

## Quick Start

```bash
git clone https://github.com/targetshot/connector.git
cd connector
cp .env.example .env  # adjust secrets
cp ui/.env.example ui/.env  # optional: UI-specific overrides
cp compose.env.example compose.env  # optional: compose overrides (e.g. UI_BIND_IP)
# Tipp: Wenn du `compose.env` nutzt, jeden Compose-Befehl mit `--env-file compose.env` aufrufen
# (oder den Inhalt nach `.env` verschieben), damit Variablen auch in den Containern landen.
docker compose up -d
# Update-Agent separat starten (damit `docker compose down` den Runner nicht stoppt)
cd update-agent
docker compose --env-file ../compose.env up -d
```

### Versioning & Releases
- Update the version defaults once per release. Run `./scripts/bump_version.sh vX.Y.Z [ReleaseName]` from the repo root. This updates `ts-connect/VERSION` (and optionally `ts-connect/RELEASE`).

- For a fully automated workflow (stage dev changes → merge main → bump version → wait for the build → promote `stable`), use `./scripts/release.sh vX.Y.Z [ReleaseName]`. The script requires `git`, `gh`, and `jq` to be installed and authenticated.
- Commit the version file changes and merge them into `main`. Pushing to `main` automatically triggers the **Build & publish ts-connect** workflow.
- The UI reads `VERSION`/`RELEASE` automatically when `TS_CONNECT_VERSION`/`TS_CONNECT_RELEASE` are not set.
- If you need to run `docker compose` from another directory (e.g. via systemd), set `TS_CONNECT_WORKSPACE_HOST=/absolute/path/to/connector` in `.env` or `compose.env`. Otherwise the default bind `.:/workspace` (relative to `compose.yml`) is used.

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
- Azure Container Registry authentication: either log in once per host (`docker login targetshot.azurecr.io -u <user> --password-stdin`) using an ACR token/service principal, or set `TS_CONNECT_ACR_USERNAME/TS_CONNECT_ACR_PASSWORD` (and optionally `TS_CONNECT_ACR_REGISTRY`) in `compose.env` so the update runner performs `docker login` automatically before `docker compose pull`. Using a Docker credential helper (`pass`, `secretservice`, `osxkeychain`, …) is recommended to store the token securely.

### Host-Agent für OS-Updates & Neustart
Der neue Host-Agent läuft außerhalb von Docker direkt auf dem Server und kümmert sich um sichere Betriebssystem-Updates sowie orchestrierte Reboots. Die UI spricht ihn über eine geschützte HTTP-API an.

1. **Installieren**
   ```bash
   cd /opt/ts-connect/host-agent
   python3 -m venv /opt/ts-host-agent
   source /opt/ts-host-agent/bin/activate
   pip install -r requirements.txt
   ```
   Beispiel-Unit `/etc/systemd/system/ts-connect-host-agent.service`:
   ```ini
   [Unit]
   Description=TargetShot Host Agent
   After=network.target docker.service

   [Service]
   Type=simple
   WorkingDirectory=/opt/ts-connect/host-agent
   Environment=TS_HOST_WORKSPACE=/opt/ts-connect
   Environment=TS_HOST_COMPOSE_ENV=compose.env
   Environment=TS_HOST_UPDATE_AGENT_PATH=/opt/ts-connect/update-agent
   ExecStart=/opt/ts-host-agent/bin/python host_agent.py --host 127.0.0.1 --port 9010
   Restart=on-failure

   [Install]
   WantedBy=multi-user.target
   ```
   Der Agent erstellt automatisch `/etc/ts-connect-host-agent/token` und `/var/lib/ts-connect-host-agent/state.json`.

2. **UI anbinden**
   - Setze in `compose.env`:  
     ```
     TS_CONNECT_HOST_AGENT_URL=http://127.0.0.1:9010
     TS_CONNECT_HOST_AGENT_TOKEN=<Inhalt aus /etc/ts-connect-host-agent/token>
     ```
     (Alternativ den Token nach `ui/data/host-agent.token` kopieren.)
     Falls `host.docker.internal` auf deiner Docker-Version nicht auf den Host zeigt, setze zusätzlich
     `TS_CONNECT_HOST_GATEWAY_OVERRIDE=mein-hostname:meine-ip` (Standard `host.docker.internal:host-gateway`).
   - UI + Update-Agent neu starten:
     ```bash
     docker compose --env-file compose.env up -d
     cd update-agent && docker compose --env-file ../compose.env up -d
     ```

3. **Funktionsumfang**
   - `Systemcheck` ruft `apt-get update && apt list --upgradable` direkt auf dem Host auf.
   - `Pakete aktualisieren` startet `apt-get upgrade && apt-get autoremove`.
   - `Server neu starten` stoppt zuerst den Haupt-Stack (`docker compose down`), anschließend den Update-Agent, synchronisiert das Dateisystem und triggert einen zeitnahen Reboot (Standardverzögerung `TS_CONNECT_HOST_REBOOT_DELAY`, Default 60 s).

Fehlt die Konfiguration, zeigt die UI einen entsprechenden Hinweis. Der Host-Agent benötigt Root-Rechte (oder passende sudo-Regeln), damit apt und `shutdown` ausgeführt werden können.

### Services
- `redpanda`: local Kafka (single node) for offsets/history
- `kafka-connect`: Confluent Kafka Connect with Debezium MySQL plugin
- `ui`: FastAPI web UI to manage connector, tests, secrets
- `update-agent`: Sidecar API (jetzt als eigenes Compose-Projekt unter `update-agent/compose.yml`) – kümmert sich um Pull/Restart, ohne sich selbst zu stoppen
- `schema-registry`: lokaler Schema Registry Dienst für Avro/JSON Converter
- `mirror-maker`: Kafka MirrorMaker 2, spiegelt `ts.raw.*`-Topics in die Confluent Cloud sobald erreichbar
- `streams-transform`: Kafka Streams Anwendung, die Vereins-Topics auf einheitliche Confluent-Topics mapped
- `backup-db`: lokaler PostgreSQL-Puffer für Offline-Backups
- *(optional)* `elastic-agent`: Elastic Fleet Client für Logs & Metriken (per `docker compose --profile elastic up -d` aktivieren)

### Notes
- UI binds to `${UI_BIND_IP:-0.0.0.0}` by default. Set `UI_BIND_IP=127.0.0.1` in `compose.env` for localhost-only access.
- On first start the UI now generates a random admin password and stores it inside `ui/data/admin_password.generated` (container path `/app/data/admin_password.generated`). Read the file once, log in, and immediately rotate the password via the Admin section or by setting `UI_ADMIN_PASSWORD`.
- Sessions are signed with `UI_SESSION_SECRET`. If the variable is absent, a random value is written to `/app/data/session_secret`. Supplying your own secret in `compose.env` keeps logins valid across re-installs.
- Cross-container secrets (e.g. `secrets.properties`) are automatically written with UID/GID `1000`. Override this via `TS_CONNECT_SECRETS_UID`/`TS_CONNECT_SECRETS_GID` if your Kafka Connect container runs with another user.
- Docker socket access moved into the dedicated `update-agent` service. The UI talks to it via `TS_CONNECT_UPDATE_AGENT_URL` (defaults to `http://update-agent:9000`) and authenticates with `TS_CONNECT_UPDATE_AGENT_TOKEN`. Leave the token empty to auto-generate a shared secret in `ui/data/update-agent.token`.

### Kafka Streams Transformation
- Der Dienst `streams-transform` abonniert alle Topics nach dem Muster `<Vereinsnummer>.SMDB.(Schuetze|Treffer|Scheiben|Serien)` und leitet sie in die Standard-Topics `ts.sds-test.{schuetze,treffer,scheiben,serien}` weiter.
- Standardmäßig verbindet sich die Anwendung mit `redpanda:9092`; per `TS_STREAMS_TARGET_PREFIX` lässt sich das Zielpräfix anpassen.
- Die erzeugten Ziele werden zusätzlich über MirrorMaker 2 in die Confluent Cloud repliziert (`ts.sds-test.*`).
- Feintuning (Application ID, Pattern, Threads, Commit-Intervalle) erfolgt über die optionalen `TS_STREAMS_*` Variablen in `compose.env`.

### Offline-Puffer konfigurieren
- Der Offline-Puffer ist dauerhaft aktiv. Alle Debezium-Events werden lokal in der Postgres-Datenbank `buffer_events` zwischengespeichert und bei verfügbarer Verbindung automatisch hochgeladen.
- MirrorMaker 2 repliziert die lokal gepufferten Topics nach Confluent, sobald eine Verbindung besteht. Die benötigten Zugangsdaten bleiben in `secrets.properties` hinterlegt.
- Das Backup-Passwort wird beim ersten Start zufällig generiert und intern verwaltet. Optional kann ein initialer Wert über `TS_CONNECT_BACKUP_PASSWORD` gesetzt werden.
- Über den Button *Backup exportieren* in der UI lässt sich ein NDJSON-Dump der Tabelle `buffer_events` herunterladen.
- Die Aufbewahrungszeit richtet sich nach der Lizenz:
  - **Basic**: 14 Tage
  - **Plus**: 30 Tage
  - **Pro**: 90 Tage
- Standard-Credentials für den Postgres-Puffer können über `TS_CONNECT_BACKUP_DB/USER/PORT` im `compose.env` angepasst werden.
- Die UI legt die Tabelle `buffer_events` automatisch an und entfernt alte Einträge gemäß Lizenzlaufzeit.

### Lizenzprüfung (Lemon Squeezy)
- Hinterlege deinen Lemon-Squeezy-Lizenzschlüssel im neuen Abschnitt *Lizenzverwaltung*. Die UI prüft den Schlüssel gegen die Lemon-Squeezy-API und zeigt Status, Laufzeit und den zugehörigen Plan an.
- Der aktive Plan (Basic / Plus / Pro) steuert automatisch die Aufbewahrungsdauer des Offline-Puffers. Abgelaufene oder ungültige Lizenzen fallen auf den Basisplan zurück.
- Umgebungskonfiguration:
  - `TS_LICENSE_API_KEY`: (optional, empfohlen) Lemon-Squeezy API-Key für die Lizenzprüfung.
  - `TS_LICENSE_VARIANT_PLAN_MAP`: Zuordnung von Produkt- oder Varianten-IDs zum Plan, z.&nbsp;B. `123=basic,234=plus`.
  - `TS_LICENSE_INSTANCE_NAME` / `TS_LICENSE_INSTANCE_ID`: optionale Angaben, die an Lemon Squeezy übertragen werden.
  - `TS_LICENSE_AUTO_ACTIVATE`: aktiviert nach erfolgreicher Prüfung automatisch eine neue Installation (Standard: `true`).
  - `TS_LICENSE_ACTIVATION_URL`: Endpoint für Aktivierungen (Standard: `https://api.lemonsqueezy.com/v1/licenses/activate`).
- Die Cloud-Replikation (MirrorMaker) startet erst, wenn die Lizenz aktiviert wurde; bis dahin verbleiben alle Events ausschließlich im lokalen Puffer.

### Zentrales Monitoring mit Elastic Agent
- Standardmäßig bleibt der Elastic Agent deaktiviert (Health-Badge zeigt „Deaktiviert“).
- Aktiviere ihn bei Bedarf mit `docker compose --profile elastic up -d` oder setze `COMPOSE_PROFILES=elastic`.
- Setze zusätzlich `ELASTIC_AGENT_ENABLED=true`, damit die UI den Health-Check aktiviert.
- `elastic-agent` joined automatisch deine Elastic-Fleet, sobald `ELASTIC_FLEET_URL` und `ELASTIC_FLEET_ENROLLMENT_TOKEN` gesetzt sind.
- Der Agent läuft vollständig im Fleet-Modus (kein `inputs.d`). Konfiguriere Docker-, System- und Log-Integrationen direkt in Kibana → Fleet für deine Policy.
- Persistenter Agent-State liegt unter `./elastic-agent/state`, damit Upgrades und Reboots sauber durchlaufen.
- Health- und Lizenzdaten schreibt die UI als NDJSON nach `/app/data/logs/health.log`. Binde diesen Pfad in der Logs-Integration ein, um Status-Dashboards aufzubauen.

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
