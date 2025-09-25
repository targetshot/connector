import os, socket, ssl, ipaddress, stat, sqlite3, hashlib, secrets
from pathlib import Path
from fastapi import FastAPI, Request, Form, HTTPException, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
import httpx
from connector_config import CONNECT_SECRETS_PATH, build_connector_config

APP_PORT = int(os.getenv("PORT", "8080"))
CONNECT_BASE_URL = os.getenv("CONNECT_BASE_URL", "http://kafka-connect:8083")
DEFAULT_CONNECTOR_NAME = os.getenv("DEFAULT_CONNECTOR_NAME", "targetshot-debezium")
ADMIN_PASSWORD = os.getenv("UI_ADMIN_PASSWORD", "change-me")
TRUSTED_CIDRS = [c.strip() for c in os.getenv(
    "UI_TRUSTED_CIDRS",
    "192.168.0.0/16,10.0.0.0/8,172.16.0.0/12"
).split(",")]
CONNECT_VERSION = os.getenv("TS_CONNECT_VERSION", "0.9.0")
CONNECT_RELEASE = os.getenv("TS_CONNECT_RELEASE", "Beta")
SESSION_SECRET = os.getenv("UI_SESSION_SECRET", "targetshot-connect-ui-secret")
CONFLUENT_CLUSTER_URL = os.getenv(
    "TS_CONNECT_CLUSTER_URL",
    "https://pkc-w7d6j.germanywestcentral.azure.confluent.cloud",
)
CONFLUENT_CLUSTER_ID = os.getenv("TS_CONNECT_CLUSTER_ID", "lkc-g8p3n1")
CONFLUENT_BOOTSTRAP_DEFAULT = os.getenv(
    "TS_CONNECT_BOOTSTRAP_DEFAULT",
    "pkc-w7d6j.germanywestcentral.azure.confluent.cloud:9092",
)
DOCS_URL = os.getenv("TS_CONNECT_DOCS_URL", "https://docs.targetshot.app/connect/")

DATA_DIR = Path("/app/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "config.db"
SECRETS_PATH = Path(CONNECT_SECRETS_PATH)
ADMIN_PASSWORD_FILE = DATA_DIR / "admin_password.txt"

app = FastAPI()
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET, same_site="lax")

# --------- Middleware: nur Vereinsnetz zulassen ----------
@app.middleware("http")
async def ip_allowlist(request: Request, call_next):
    client_ip = request.client.host
    ip_obj = ipaddress.ip_address(client_ip)
    allowed = any(ip_obj in ipaddress.ip_network(cidr) for cidr in TRUSTED_CIDRS)
    if not allowed and client_ip != "127.0.0.1":
        return JSONResponse({"detail": "Forbidden (CIDR)"}, status_code=403)
    return await call_next(request)

# --------- DB Helpers ----------
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS settings (
        id INTEGER PRIMARY KEY CHECK (id=1),
        db_host TEXT, db_port INTEGER, db_user TEXT,
        confluent_bootstrap TEXT, confluent_sasl_username TEXT,
        topic_prefix TEXT, server_id INTEGER, server_name TEXT
    )""")
    cur = conn.execute("SELECT COUNT(*) FROM settings")
    if cur.fetchone()[0] == 0:
        conn.execute(
            """
            INSERT INTO settings(
                id, db_host, db_port, db_user,
                confluent_bootstrap, confluent_sasl_username,
                topic_prefix, server_id, server_name
            )
            VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                os.getenv("TS_CONNECT_DEFAULT_DB_HOST", "192.168.10.200"),
                3306,
                os.getenv("TS_CONNECT_DEFAULT_DB_USER", "debezium_sync"),
                CONFLUENT_BOOTSTRAP_DEFAULT,
                "YOUR-API-KEY",
                os.getenv("TS_CONNECT_DEFAULT_TOPIC_PREFIX", "413067"),
                int(os.getenv("TS_CONNECT_DEFAULT_SERVER_ID", "413067")),
                os.getenv("TS_CONNECT_DEFAULT_SERVER_NAME", "targetshot-mysql"),
            ),
        )
        conn.commit()
    return conn


def fetch_settings() -> dict:
    conn = get_db()
    cur = conn.execute(
        """
        SELECT db_host, db_port, db_user,
               confluent_bootstrap, confluent_sasl_username,
               topic_prefix, server_id, server_name
        FROM settings WHERE id=1
        """
    )
    row = cur.fetchone()
    conn.close()
    return {
        "db_host": row[0],
        "db_port": row[1],
        "db_user": row[2],
        "confluent_bootstrap": row[3],
        "confluent_sasl_username": row[4],
        "topic_prefix": row[5],
        "server_id": row[6],
        "server_name": row[7],
    }

def write_secrets_file(db_password: str, confluent_bootstrap: str,
                       confluent_user: str, confluent_pass: str):
    lines = [
        f"db_password={db_password}",
        f"confluent_bootstrap={confluent_bootstrap}",
        f"confluent_sasl_username={confluent_user}",
        f"confluent_sasl_password={confluent_pass}"
    ]
    SECRETS_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.chmod(SECRETS_PATH, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)  # 0644 allows read across containers


def read_secrets_file() -> dict:
    if not SECRETS_PATH.exists():
        return {}
    data: dict[str, str] = {}
    for line in SECRETS_PATH.read_text(encoding="utf-8").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key.strip()] = value.strip()
    return data


def _hash_admin_password(plain: str, salt_hex: str | None = None) -> tuple[str, str]:
    if not plain:
        raise ValueError("Admin-Passwort darf nicht leer sein")
    if salt_hex is None:
        salt_bytes = secrets.token_bytes(16)
    else:
        salt_bytes = bytes.fromhex(salt_hex)
    digest = hashlib.pbkdf2_hmac("sha256", plain.encode("utf-8"), salt_bytes, 260000)
    return salt_bytes.hex(), digest.hex()


def set_admin_password(new_password: str) -> None:
    salt_hex, hash_hex = _hash_admin_password(new_password)
    ADMIN_PASSWORD_FILE.write_text(f"{salt_hex}:{hash_hex}\n", encoding="utf-8")
    os.chmod(ADMIN_PASSWORD_FILE, stat.S_IRUSR | stat.S_IWUSR)


def _read_admin_password_record() -> str:
    if not ADMIN_PASSWORD_FILE.exists():
        return ""
    return ADMIN_PASSWORD_FILE.read_text(encoding="utf-8").strip()


def ensure_admin_password_file() -> None:
    if ADMIN_PASSWORD_FILE.exists():
        return
    set_admin_password(ADMIN_PASSWORD)


def verify_admin_password(candidate: str) -> bool:
    ensure_admin_password_file()
    record = _read_admin_password_record()
    if not record:
        return False
    if ":" not in record:
        return secrets.compare_digest(record, candidate)
    salt_hex, hash_hex = record.split(":", 1)
    try:
        _, candidate_hash = _hash_admin_password(candidate, salt_hex=salt_hex)
    except ValueError:
        return False
    return secrets.compare_digest(hash_hex, candidate_hash)


@app.on_event("startup")
async def init_admin_password() -> None:
    ensure_admin_password_file()


async def apply_connector_config() -> None:
    settings = fetch_settings()
    secrets = read_secrets_file()

    required = {
        "db_password": "DB-Passwort",
        "confluent_bootstrap": "Confluent Bootstrap",
        "confluent_sasl_username": "Confluent API Key",
        "confluent_sasl_password": "Confluent API Secret",
    }
    missing = [label for key, label in required.items() if not secrets.get(key)]
    if missing:
        raise ValueError("Es fehlen Werte in secrets.properties: " + ", ".join(missing))

    connector_cfg = {
        "name": DEFAULT_CONNECTOR_NAME,
        "config": build_connector_config(settings),
    }

    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(f"{CONNECT_BASE_URL}/connectors/{DEFAULT_CONNECTOR_NAME}")
        if r.status_code == 200:
            await client.put(f"{CONNECT_BASE_URL}/connectors/{DEFAULT_CONNECTOR_NAME}/pause")
            await client.put(
                f"{CONNECT_BASE_URL}/connectors/{DEFAULT_CONNECTOR_NAME}/config",
                json=connector_cfg["config"],
            )
            await client.put(f"{CONNECT_BASE_URL}/connectors/{DEFAULT_CONNECTOR_NAME}/resume")
        else:
            await client.post(f"{CONNECT_BASE_URL}/connectors", json=connector_cfg)

# --------- Views ----------
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    if not request.session.get("authenticated"):
        return RedirectResponse("/login", status_code=303)
    context = build_index_context(request)
    return templates.TemplateResponse("index.html", context)

def require_admin(pw: str, *, raise_exc: bool = True) -> bool:
    if verify_admin_password(pw):
        return True
    if raise_exc:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return False

def require_session(request: Request):
    if not request.session.get("authenticated"):
        raise HTTPException(status_code=401, detail="Unauthorized")


def build_index_context(request: Request) -> dict:
    data = fetch_settings().copy()
    placeholders = {
        "YOUR-BOOTSTRAP:9092",
        "YOUR-BOOTSTRAP",
        "pkc-xxxxx.eu-central-1.aws.confluent.cloud:9092",
    }
    if data["confluent_bootstrap"] in placeholders or not data["confluent_bootstrap"]:
        data["confluent_bootstrap"] = CONFLUENT_BOOTSTRAP_DEFAULT

    has_secrets = SECRETS_PATH.exists()
    flash_message = request.session.pop("flash_message", None)
    error_message = request.session.pop("error_message", None)

    return {
        "request": request,
        "data": data,
        "has_secrets": has_secrets,
        "connect_version": CONNECT_VERSION,
        "connect_release": CONNECT_RELEASE,
        "confluent_cluster_url": CONFLUENT_CLUSTER_URL,
        "confluent_cluster_id": CONFLUENT_CLUSTER_ID,
        "flash_message": flash_message,
        "error_message": error_message,
        "docs_url": DOCS_URL,
    }


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if request.session.get("authenticated"):
        return RedirectResponse("/", status_code=303)
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@app.post("/login")
async def login_submit(request: Request, pw: str = Form(...)):
    if not verify_admin_password(pw):
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "error": "Falsches Passwort. Bitte erneut versuchen.",
            },
            status_code=401,
        )
    request.session["authenticated"] = True
    return RedirectResponse("/", status_code=303)


@app.post("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=303)

# --------- Preflight Tests ----------
@app.get("/api/test/db", dependencies=[Depends(require_session)])
async def test_db(host: str, port: int, user: str, password: str):
    import pymysql
    try:
        conn = pymysql.connect(host=host, port=port, user=user, password=password,
                               connect_timeout=3, read_timeout=3, write_timeout=3)
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        conn.close()
        return {"ok": True, "msg": "DB OK"}
    except Exception as e:
        return {"ok": False, "msg": str(e)}

@app.get("/api/test/confluent", dependencies=[Depends(require_session)])
async def test_confluent(bootstrap: str):
    # reachability + TLS handshake only (Credentials prüft später der Connector)
    try:
        bootstrap_value = bootstrap.strip()
        for scheme in ("https://", "http://"):
            if bootstrap_value.startswith(scheme):
                bootstrap_value = bootstrap_value[len(scheme):]
                break
        bootstrap_value = bootstrap_value.rstrip("/")
        if ":" in bootstrap_value:
            host, port_raw = bootstrap_value.rsplit(":", 1)
            port = int(port_raw)
        else:
            host, port = bootstrap_value, 9092
        ctx = ssl.create_default_context()
        with socket.create_connection((host, port), timeout=3) as sock:
            with ctx.wrap_socket(sock, server_hostname=host) as ssock:
                # handshake ok
                return {"ok": True, "msg": "TLS handshake OK"}
    except Exception as e:
        return {"ok": False, "msg": str(e)}

# --------- Save & Apply ----------
@app.post("/save", dependencies=[Depends(require_session)])
async def save(
    request: Request,
    section: str = Form(...),
    pw: str | None = Form(default=None),
    db_host: str | None = Form(default=None),
    db_port: int | None = Form(default=None),
    db_user: str | None = Form(default=None),
    db_password: str | None = Form(default=None),
    confluent_bootstrap: str | None = Form(default=None),
    confluent_sasl_username: str | None = Form(default=None),
    confluent_sasl_password: str | None = Form(default=None),
    topic_prefix: str | None = Form(default=None),
    server_id: int | None = Form(default=None),
    server_name: str | None = Form(default=None),
    new_admin_password: str = Form(default=""),
    confirm_admin_password: str = Form(default=""),
):
    section_key = section.strip().lower()
    settings = fetch_settings()
    secrets_data = read_secrets_file()

    if section_key == "admin":
        if not pw or not require_admin(pw, raise_exc=False):
            request.session["error_message"] = "Aktuelles Admin-Passwort ist ungültig."
            return RedirectResponse("/", status_code=303)
        new_admin_password = new_admin_password.strip()
        confirm_admin_password = confirm_admin_password.strip()
        if not new_admin_password:
            request.session["error_message"] = "Bitte ein neues Admin-Passwort eingeben."
            return RedirectResponse("/", status_code=303)
        if new_admin_password != confirm_admin_password:
            request.session["error_message"] = "Neues Admin-Passwort stimmt nicht überein."
            return RedirectResponse("/", status_code=303)
        if len(new_admin_password) < 8:
            request.session["error_message"] = "Das neue Admin-Passwort muss mindestens 8 Zeichen enthalten."
            return RedirectResponse("/", status_code=303)
        set_admin_password(new_admin_password)
        request.session["flash_message"] = "Admin-Passwort aktualisiert."
        return RedirectResponse("/", status_code=303)

    if section_key == "db":
        if not all([db_host, db_port is not None, db_user, db_password]):
            request.session["error_message"] = "Bitte alle MariaDB-Felder ausfüllen."
            return RedirectResponse("/", status_code=303)

        db_host = db_host.strip()
        db_user = db_user.strip()

        conn = get_db()
        conn.execute(
            "UPDATE settings SET db_host=?, db_port=?, db_user=? WHERE id=1",
            (db_host, db_port, db_user),
        )
        conn.commit()
        conn.close()

        settings = fetch_settings()
        confluent_bootstrap_val = settings["confluent_bootstrap"] or CONFLUENT_BOOTSTRAP_DEFAULT
        confluent_user_val = settings["confluent_sasl_username"] or secrets_data.get("confluent_sasl_username", "")
        confluent_pass_val = secrets_data.get("confluent_sasl_password", "")

        write_secrets_file(db_password, confluent_bootstrap_val, confluent_user_val, confluent_pass_val)

        try:
            await apply_connector_config()
            request.session["flash_message"] = "MariaDB-Einstellungen gespeichert & Connector aktualisiert."
        except ValueError as exc:
            request.session["flash_message"] = (
                "MariaDB-Einstellungen gespeichert. Connector-Update übersprungen: "
                f"{exc}"
            )
        except Exception as exc:
            request.session["error_message"] = f"Connector-Update fehlgeschlagen: {exc}"
        return RedirectResponse("/", status_code=303)

    if section_key == "confluent":
        confluent_sasl_username = (confluent_sasl_username or "").strip()
        confluent_sasl_password = (confluent_sasl_password or "").strip()
        topic_prefix = (topic_prefix or "").strip()
        server_name = (server_name or "").strip()
        bootstrap_value = (confluent_bootstrap or settings["confluent_bootstrap"] or CONFLUENT_BOOTSTRAP_DEFAULT).strip()

        if not confluent_sasl_username or not confluent_sasl_password:
            request.session["error_message"] = "Bitte API Key und Secret für Confluent ausfüllen."
            return RedirectResponse("/", status_code=303)

        conn = get_db()
        conn.execute(
            """
            UPDATE settings
            SET confluent_bootstrap=?, confluent_sasl_username=?, topic_prefix=?,
                server_id=?, server_name=?
            WHERE id=1
            """,
            (
                bootstrap_value,
                confluent_sasl_username,
                topic_prefix,
                server_id if server_id is not None else settings["server_id"],
                server_name or settings["server_name"],
            ),
        )
        conn.commit()
        conn.close()

        settings = fetch_settings()
        db_password_val = db_password if db_password else secrets_data.get("db_password")
        if not db_password_val:
            request.session["error_message"] = "Bitte zuerst die MariaDB-Zugangsdaten speichern (DB-Passwort fehlt)."
            return RedirectResponse("/", status_code=303)

        write_secrets_file(
            db_password_val,
            settings["confluent_bootstrap"],
            confluent_sasl_username,
            confluent_sasl_password,
        )

        try:
            await apply_connector_config()
        except Exception as exc:
            request.session["error_message"] = f"Connector-Update fehlgeschlagen: {exc}"
            return RedirectResponse("/", status_code=303)

        request.session["flash_message"] = "Confluent-Einstellungen gespeichert & Connector aktualisiert."
        return RedirectResponse("/", status_code=303)

    request.session["error_message"] = "Unbekannter Abschnitt."
    return RedirectResponse("/", status_code=303)

# --------- Connector Control ----------
@app.post("/api/connector/{action}", dependencies=[Depends(require_session)])
async def connector_control(action: str, pw: str = Form(...)):
    require_admin(pw)
    valid = {"pause", "resume", "restart"}
    if action not in valid: raise HTTPException(400, "invalid action")
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(f"{CONNECT_BASE_URL}/connectors/{DEFAULT_CONNECTOR_NAME}/{action}")
        return {"ok": r.status_code in (200, 202), "status": r.status_code}

# --------- Status ----------
@app.get("/api/status", dependencies=[Depends(require_session)])
async def status():
    async with httpx.AsyncClient(timeout=5) as client:
        try:
            w = await client.get(f"{CONNECT_BASE_URL}/connectors")
            if w.status_code != 200:
                return {"worker": "unavailable"}
            s = await client.get(f"{CONNECT_BASE_URL}/connectors/{DEFAULT_CONNECTOR_NAME}/status")
            return {"worker": "ok", "connectorStatus": s.json() if s.status_code == 200 else None}
        except Exception as e:
            return {"worker": "unavailable", "error": str(e)}


@app.get("/api/connector/config", dependencies=[Depends(require_session)])
async def connector_config():
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            resp = await client.get(f"{CONNECT_BASE_URL}/connectors/{DEFAULT_CONNECTOR_NAME}")
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"Connector REST nicht erreichbar: {exc}")
    if resp.status_code == 404:
        return {"exists": False, "config": None}
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return {"exists": True, "config": resp.json()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=APP_PORT)
