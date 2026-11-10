#!/bin/bash
# Ubuntu 22.04 / 24.04 one-command systemd installer, updater, and uninstaller for AstraG.
# Supports two installation modes without persistent venv:
#   1) --apt    (default) : System Ubuntu APT packages only, zero pip / zero venv
#   2) --binary           : Single standalone compiled binary (/opt/astrag/astrag) like Cesbo Astra
set -euo pipefail

START=true
UNINSTALL=false
INSTALL_MODE=""
CUSTOM_PORT="${ASTRA_PORT:-8000}"
REPO_URL="${ASTRA_REPO_URL:-https://github.com/udemika/astrag.git}"

while [ "$#" -gt 0 ]; do
    case "$1" in
        --apt|--system) INSTALL_MODE="apt"; shift ;;
        --binary|--bin|--single-binary) INSTALL_MODE="binary"; shift ;;
        --mode)
            if [ "$#" -lt 2 ]; then echo "Missing mode ('apt' or 'binary') after --mode" >&2; exit 2; fi
            INSTALL_MODE="$2"; shift 2 ;;
        --start|--restart|--update) START=true; shift ;;
        --no-start) START=false; shift ;;
        --uninstall|--remove|--purge) UNINSTALL=true; shift ;;
        --port)
            if [ "$#" -lt 2 ]; then echo "Missing port number after --port" >&2; exit 2; fi
            CUSTOM_PORT="$2"; shift 2 ;;
        --repo)
            if [ "$#" -lt 2 ]; then echo "Missing repository URL after --repo" >&2; exit 2; fi
            REPO_URL="$2"; shift 2 ;;
        *)
            echo "Usage: sudo bash install.sh [--apt|--binary] [--start|--update|--no-start|--uninstall] [--port PORT] [--repo GIT_URL]" >&2
            exit 2
            ;;
    esac
done

if [ "$(id -u)" -ne 0 ]; then
    echo "Run as root (sudo)." >&2
    exit 1
fi

APP_DIR=/opt/astrag
STATIC_DIR="$APP_DIR/www"
CONFIG_DIR=/etc/astrag
STATE_DIR=/var/lib/astrag
SERVICE_USER=astrag

if "$UNINSTALL"; then
    echo "==> Stopping and removing AstraG services..."
    for unit_p in /etc/systemd/system/astrag*.service /etc/systemd/system/astra-clone.service; do
        if [ -f "$unit_p" ]; then
            u_name="$(basename "$unit_p")"
            systemctl stop "$u_name" 2>/dev/null || true
            systemctl disable "$u_name" 2>/dev/null || true
            rm -f "$unit_p"
        fi
    done
    pkill -f "astra_clone_backend.py" 2>/dev/null || true
    pkill -f "astrag_backend.py" 2>/dev/null || true
    pkill -f "/opt/astrag/astrag" 2>/dev/null || true
    systemctl daemon-reload || true
    systemctl reset-failed 2>/dev/null || true
    rm -f /usr/bin/astrag /usr/bin/astra-clone
    rm -rf /opt/astrag /opt/astra-clone /etc/astrag /etc/astra-clone /var/lib/astrag /var/lib/astra-clone
    if id "$SERVICE_USER" >/dev/null 2>&1; then userdel "$SERVICE_USER" 2>/dev/null || true; fi
    if getent group "$SERVICE_USER" >/dev/null 2>&1; then groupdel "$SERVICE_USER" 2>/dev/null || true; fi
    if id "astra-clone" >/dev/null 2>&1; then userdel "astra-clone" 2>/dev/null || true; fi
    if getent group "astra-clone" >/dev/null 2>&1; then groupdel "astra-clone" 2>/dev/null || true; fi
    echo "==> AstraG has been completely removed from this server."
    exit 0
fi

# Auto-detect previous install mode on --update if --apt/--binary was not explicitly specified
if [ -z "$INSTALL_MODE" ]; then
    if [ -x "$APP_DIR/astrag" ] && [ ! -d "$APP_DIR/venv" ]; then
        INSTALL_MODE="binary"
    else
        INSTALL_MODE="apt"
    fi
fi

if [ "$INSTALL_MODE" != "apt" ] && [ "$INSTALL_MODE" != "binary" ]; then
    echo "Invalid install mode '$INSTALL_MODE' (expected 'apt' or 'binary')." >&2
    exit 2
fi

SCRIPT_DIR=""
if [ -n "${BASH_SOURCE[0]:-}" ] && [ -f "${BASH_SOURCE[0]}" ]; then
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fi

TMP_CLONE=""
TMP_BUILD=""
cleanup_tmp() {
    if [ -n "$TMP_CLONE" ] && [ -d "$TMP_CLONE" ]; then
        rm -rf "$TMP_CLONE"
    fi
    if [ -n "$TMP_BUILD" ] && [ -d "$TMP_BUILD" ]; then
        rm -rf "$TMP_BUILD"
    fi
}
trap cleanup_tmp EXIT

if [ -z "$SCRIPT_DIR" ] || [ ! -f "$SCRIPT_DIR/astra_clone_backend.py" ]; then
    echo "==> Downloading AstraG source from GitHub ($REPO_URL)..."
    apt-get update -qq
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq git curl ca-certificates
    TMP_CLONE="$(mktemp -d /tmp/astrag-src.XXXXXX)"
    git clone --depth 1 "$REPO_URL" "$TMP_CLONE"
    SCRIPT_DIR="$TMP_CLONE"
fi

for file in astra_clone_backend.py stream_engine.py account_security.py create_admin.py www/index.html www/account.html www/mod.js; do
    if [ ! -f "$SCRIPT_DIR/$file" ]; then echo "Missing $SCRIPT_DIR/$file" >&2; exit 1; fi
done

if ! getent group "$SERVICE_USER" >/dev/null; then groupadd --system "$SERVICE_USER"; fi
if ! id "$SERVICE_USER" >/dev/null 2>&1; then
    useradd --system --gid "$SERVICE_USER" --home-dir "$STATE_DIR" --no-create-home --shell /usr/sbin/nologin "$SERVICE_USER"
fi
if [ "$(id -u "$SERVICE_USER")" -eq 0 ]; then echo "Refusing root service user" >&2; exit 1; fi

install -d -o root -g root -m 0755 "$APP_DIR" "$STATIC_DIR"
install -d -o "$SERVICE_USER" -g "$SERVICE_USER" -m 0750 "$CONFIG_DIR"
install -d -o "$SERVICE_USER" -g "$SERVICE_USER" -m 0750 "$STATE_DIR"

# Seamless migration from legacy /var/lib/astra-clone and /etc/astra-clone if present
if [ ! -f "$STATE_DIR/state.json" ] && [ -f /var/lib/astra-clone/state.json ]; then
    cp -a /var/lib/astra-clone/state.json "$STATE_DIR/state.json"
    chown "$SERVICE_USER:$SERVICE_USER" "$STATE_DIR/state.json"
    chmod 0640 "$STATE_DIR/state.json"
fi
if systemctl is-active --quiet astra-clone.service 2>/dev/null; then
    systemctl stop astra-clone.service || true
    systemctl disable astra-clone.service || true
fi

# Preserve installed static files before updating, never recursively delete them.
if [ -f "$STATIC_DIR/index.html" ]; then
    install -d -o root -g root -m 0700 "$APP_DIR/backups"
    saved="$(mktemp -d "$APP_DIR/backups/www.XXXXXXXX")"
    cp -a "$STATIC_DIR/." "$saved/"
fi
cp -R --no-preserve=ownership "$SCRIPT_DIR/www/." "$STATIC_DIR/"
chown -R root:root "$STATIC_DIR"
find "$STATIC_DIR" -type d -exec chmod 0755 {} +
find "$STATIC_DIR" -type f -exec chmod 0644 {} +

# Remove any legacy /opt/astrag/venv so neither mode uses a persistent virtualenv
rm -rf "$APP_DIR/venv"

# Native data plane (astrag-core). The backend uses it automatically when /opt/astrag/astrag-core is
# executable and falls back to the Python engine otherwise.
install_astrag_core() {
    if [ "$(uname -m)" = "x86_64" ] && [ -f "$SCRIPT_DIR/astrag-core" ] && "$SCRIPT_DIR/astrag-core" --version >/dev/null 2>&1; then
        install -o root -g root -m 0755 "$SCRIPT_DIR/astrag-core" "$APP_DIR/astrag-core"
    elif [ -f "$SCRIPT_DIR/astrag_core.c" ]; then
        echo "==> Building astrag-core from source..."
        DEBIAN_FRONTEND=noninteractive apt-get install -y gcc libc6-dev libssl-dev >/dev/null 2>&1 || true
        if gcc -O2 -s -o "$APP_DIR/astrag-core.new" "$SCRIPT_DIR/astrag_core.c" -lssl -lcrypto -lpthread -lm; then
            chmod 0755 "$APP_DIR/astrag-core.new"
            mv -f "$APP_DIR/astrag-core.new" "$APP_DIR/astrag-core"
        else
            rm -f "$APP_DIR/astrag-core.new"
            echo "WARNING: astrag-core build failed; the Python stream engine will be used." >&2
        fi
    fi
}

if [ "$INSTALL_MODE" = "apt" ]; then
    echo "==> [Mode 1: APT-Only] Installing Ubuntu system APT packages (zero pip, zero venv)..."
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        python3 \
        python3-fastapi \
        python3-uvicorn \
        python3-pydantic \
        python3-starlette \
        python3-anyio \
        python3-h11 \
        python3-click \
        curl \
        ca-certificates
    # Optional WebSocket / multipart / HTTPX system packages across Ubuntu 22.04 and 24.04
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        python3-websockets python3-wsproto python3-multipart python3-httpx uvicorn 2>/dev/null || true

    python3 -c 'import sys, fastapi, uvicorn, pydantic; assert sys.version_info >= (3, 10), "Python 3.10+ required"'

    rm -f "$APP_DIR/astrag"
    for file in astra_clone_backend.py stream_engine.py account_security.py create_admin.py; do
        install -o root -g root -m 0644 "$SCRIPT_DIR/$file" "$APP_DIR/$file"
    done
    if [ -f "$SCRIPT_DIR/astrag_backend.py" ]; then
        install -o root -g root -m 0644 "$SCRIPT_DIR/astrag_backend.py" "$APP_DIR/astrag_backend.py"
    else
        install -o root -g root -m 0644 "$SCRIPT_DIR/astra_clone_backend.py" "$APP_DIR/astrag_backend.py"
    fi
    install_astrag_core

    EXEC_START_CMD="/usr/bin/python3 /opt/astrag/astra_clone_backend.py"
    cat > /usr/bin/astrag <<'EOF'
#!/bin/sh
export MALLOC_MMAP_THRESHOLD_=131072
export MALLOC_TRIM_THRESHOLD_=131072
export MALLOC_ARENA_MAX=2
exec /usr/bin/python3 /opt/astrag/astra_clone_backend.py "$@"
EOF

else
    echo "==> [Mode 2: Single Binary] Compiling AstraG into a standalone binary (/opt/astrag/astrag) like Cesbo Astra..."
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        python3 python3-dev python3-venv python3-pip binutils build-essential patchelf curl ca-certificates
    python3 -c 'import sys; assert sys.version_info >= (3, 10), "Python 3.10+ required"'

    TMP_BUILD="$(mktemp -d /tmp/astrag-build.XXXXXX)"
    python3 -m venv "$TMP_BUILD/venv"
    "$TMP_BUILD/venv/bin/python" -m pip install --upgrade pip
    "$TMP_BUILD/venv/bin/python" -m pip install -r "$SCRIPT_DIR/requirements.txt" pyinstaller

    "$TMP_BUILD/venv/bin/pyinstaller" \
        --onefile --clean --noconfirm \
        --name astrag \
        --distpath "$TMP_BUILD/dist" \
        --workpath "$TMP_BUILD/work" \
        --specpath "$TMP_BUILD" \
        --paths "$SCRIPT_DIR" \
        --add-data "$SCRIPT_DIR/www:www" \
        --hidden-import stream_engine \
        --hidden-import account_security \
        --hidden-import astra_clone_backend \
        --hidden-import uvicorn.logging \
        --hidden-import uvicorn.loops \
        --hidden-import uvicorn.loops.auto \
        --hidden-import uvicorn.loops.asyncio \
        --hidden-import uvicorn.protocols \
        --hidden-import uvicorn.protocols.http \
        --hidden-import uvicorn.protocols.http.auto \
        --hidden-import uvicorn.protocols.http.h11_impl \
        --hidden-import uvicorn.protocols.websockets \
        --hidden-import uvicorn.protocols.websockets.auto \
        --hidden-import uvicorn.protocols.websockets.websockets_impl \
        --hidden-import uvicorn.protocols.websockets.wsproto_impl \
        --hidden-import uvicorn.lifespan \
        --hidden-import uvicorn.lifespan.on \
        "$SCRIPT_DIR/astra_clone_backend.py"

    systemctl stop astrag.service 2>/dev/null || true
    install -o root -g root -m 0755 "$TMP_BUILD/dist/astrag" "$APP_DIR/astrag"
    install -o root -g root -m 0644 "$SCRIPT_DIR/create_admin.py" "$APP_DIR/create_admin.py"
    install -o root -g root -m 0644 "$SCRIPT_DIR/account_security.py" "$APP_DIR/account_security.py"
    install_astrag_core
    rm -f "$APP_DIR/astra_clone_backend.py" "$APP_DIR/astrag_backend.py" "$APP_DIR/stream_engine.py" "$APP_DIR/requirements.txt"
    rm -rf "$TMP_BUILD"
    TMP_BUILD=""

    EXEC_START_CMD="/opt/astrag/astrag"
    cat > /usr/bin/astrag <<'EOF'
#!/bin/sh
export MALLOC_MMAP_THRESHOLD_=131072
export MALLOC_TRIM_THRESHOLD_=131072
export MALLOC_ARENA_MAX=2
exec /opt/astrag/astrag "$@"
EOF
fi

chown root:root /usr/bin/astrag
chmod 0755 /usr/bin/astrag
ln -sf /usr/bin/astrag /usr/bin/astra-clone

if [ "${CUSTOM_PORT:-8000}" = "8000" ]; then
    SERVICE_NAME="astrag"
else
    SERVICE_NAME="astrag${CUSTOM_PORT}"
fi
CONF_FILE="$CONFIG_DIR/${SERVICE_NAME}.conf"

python3 - "$CONF_FILE" "${CUSTOM_PORT:-8000}" "$SERVICE_NAME" "$STATE_DIR/state.json" "$APP_DIR" <<'PYEOF'
import json, os, sys, time
from pathlib import Path

conf_file = Path(sys.argv[1])
port = int(sys.argv[2])
service_name = sys.argv[3]
legacy_state = Path(sys.argv[4])
app_dir = Path(sys.argv[5])
sys.path.insert(0, str(app_dir))
try:
    from account_security import verify_password
except Exception:
    verify_password = lambda p, s: False

default_cfg = {
    "port": port,
    "host": "0.0.0.0",
    "gid": 466560,
    "auth": {"enable": True},
    "users": {
        "admin": {
            "type": 1,
            "role": "admin",
            "enable": True,
            "password": "admin",
            "created": int(time.time()),
        }
    },
    "make_stream": [],
    "categories": [],
    "servers": [],
    "softcam": [],
    "cas": [],
    "dvb_tune": [],
    "settings": {},
    "hls": {},
}

existing = None
if conf_file.exists():
    raw = conf_file.read_text(encoding="utf-8-sig").strip()
    if raw.startswith("{"):
        try:
            d = json.loads(raw)
            if isinstance(d, dict):
                existing = d.get("config") if isinstance(d.get("config"), dict) else d
        except Exception:
            existing = None

if existing is None and service_name == "astrag" and legacy_state.exists():
    try:
        st_raw = legacy_state.read_text(encoding="utf-8-sig").strip()
        st_d = json.loads(st_raw) if st_raw else {}
        if isinstance(st_d, dict):
            existing = st_d.get("config") if isinstance(st_d.get("config"), dict) else st_d
    except Exception:
        existing = None

if isinstance(existing, dict):
    merged = dict(default_cfg)
    merged.update(existing)
    merged["port"] = port
    merged.pop("control_port", None)
    if isinstance(merged.get("users"), dict):
        for uinfo in merged["users"].values():
            if isinstance(uinfo, dict):
                pw = uinfo.get("password", "")
                if isinstance(pw, str) and pw.startswith("pbkdf2_sha256$") and verify_password("admin", pw):
                    uinfo["password"] = "admin"
    ordered = {
        "port": merged["port"],
        "host": merged.get("host", "0.0.0.0"),
        "gid": merged.get("gid", 466560),
        "auth": merged.get("auth", {"enable": True}),
        "users": merged.get("users", default_cfg["users"]),
        "make_stream": merged.get("make_stream", []),
        "categories": merged.get("categories", []),
        "servers": merged.get("servers", []),
        "softcam": merged.get("softcam", []),
        "cas": merged.get("cas", []),
        "dvb_tune": merged.get("dvb_tune", []),
        "settings": merged.get("settings", {}),
        "hls": merged.get("hls", {}),
    }
    conf_file.write_text(json.dumps(ordered, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
else:
    conf_file.write_text(json.dumps(default_cfg, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
PYEOF

chown -R "$SERVICE_USER:$SERVICE_USER" "$CONFIG_DIR"
chmod 0640 "$CONF_FILE"

UNIT_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
SERVICE_SRC="$SCRIPT_DIR/astrag.service"
install -o root -g root -m 0644 "$SERVICE_SRC" "$UNIT_FILE"
sed -i "s|^Description=.*|Description=AstraG Control Panel and IPTV Streaming Server (${SERVICE_NAME})|" "$UNIT_FILE"
sed -i "s|^Environment=\"ASTRA_STATE_FILE=.*|Environment=\"ASTRA_STATE_FILE=${CONF_FILE}\"|" "$UNIT_FILE"
sed -i "s|^Environment=\"ASTRA_CONFIG_FILE=.*|Environment=\"ASTRA_CONFIG_FILE=${CONF_FILE}\"|" "$UNIT_FILE"
sed -i "s|^ExecStart=.*|ExecStart=${EXEC_START_CMD} -c ${CONF_FILE}|" "$UNIT_FILE"

if command -v ufw >/dev/null 2>&1 && ufw status 2>/dev/null | grep -q "Status: active"; then
    ufw allow "${CUSTOM_PORT:-8000}/tcp" >/dev/null 2>&1 || true
fi

systemctl daemon-reload
if "$START"; then
    systemctl enable "${SERVICE_NAME}.service"
    systemctl restart "${SERVICE_NAME}.service"
    sleep 1
    if ! systemctl is-active --quiet "${SERVICE_NAME}.service"; then
        echo "ERROR: ${SERVICE_NAME}.service failed to start. Recent logs:" >&2
        journalctl -u "${SERVICE_NAME}.service" -n 30 --no-pager >&2 || true
        exit 1
    fi
    SERVER_IP="$(hostname -I 2>/dev/null | awk '{print $1}' || echo '127.0.0.1')"
    echo "================================================================"
    echo " AstraG (${SERVICE_NAME}) successfully installed and started!"
    echo " Config File       : ${CONF_FILE}"
    echo " Web Control Panel : http://${SERVER_IP}:${CUSTOM_PORT:-8000}"
    echo " Default Login     : admin"
    echo " Default Password  : admin"
    echo " Playlist M3U8     : http://${SERVER_IP}:${CUSTOM_PORT:-8000}/playlist.m3u8"
    echo "================================================================"
else
    echo "Installed AstraG (${SERVICE_NAME}) with config ${CONF_FILE}. Start with: sudo systemctl enable --now ${SERVICE_NAME}.service"
fi
echo "Logs: sudo journalctl -u ${SERVICE_NAME}.service -f"


