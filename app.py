import asyncio
import json
import math
import os
import random
import struct
import threading
import time
import unicodedata
from datetime import datetime
from typing import Any, Dict, List

import snap7
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

CFG_PATH = os.getenv("CFG_PATH", "json_teste.json")
SIM_MODE = os.getenv("SIM_MODE", "0") == "1"

# ======================= Util =======================
def slug(s: str) -> str:
    # remove acentos, troca não-alfanum por _
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    out = []
    for ch in s:
        if ch.isalnum():
            out.append(ch.lower())
        else:
            out.append("_")
    t = "_".join(filter(None, "".join(out).split("_")))
    return t.strip("_")


def parse_value(raw: bytes, typ: str):
    try:
        if typ == "REAL" and len(raw) >= 4:
            return round(struct.unpack(">f", raw[:4])[0], 2)
        if typ == "INT" and len(raw) >= 2:
            return int.from_bytes(raw[:2], "big", signed=True)
        if typ == "UINT" and len(raw) >= 2:
            return int.from_bytes(raw[:2], "big", signed=False)
        if typ == "USINT" and len(raw) >= 1:
            return raw[0]
        if typ == "DTL" and len(raw) >= 8:
            year = int.from_bytes(raw[0:2], "big")
            month, day = raw[2], raw[3]
            hour, minute, second = raw[5], raw[6], raw[7]
            return datetime(year, month, day, hour, minute, second).isoformat()
    except Exception:
        pass
    return None


def simulate_value(clp_slug: str, var_slug: str, typ: str, t: float) -> Any:
    """Gera valores coerentes para modo simulado."""
    base = sum(ord(c) for c in (clp_slug + var_slug)) % 10
    noise = random.uniform(-0.05, 0.05)
    if typ == "REAL":
        return round(5.0 + base * 0.3 + math.sin(t / 3 + base) + noise, 2)
    if typ in ("INT", "UINT", "USINT"):
        return int(100 + base * 2 + 10 * math.sin(t / 5 + base))
    return None


# ======================= S7 pool =======================
class ClientEntry:
    def __init__(self, ip, rack, slot, min_interval_ms=10):
        self.ip = ip
        self.rack = rack
        self.slot = slot
        self.client = snap7.client.Client()
        self.lock = threading.Lock()
        self.connected = False
        self.last_op = 0.0
        self.min_interval = max(0.0, min_interval_ms / 1000.0)

    def connect(self):
        if not self.connected or not self.client.get_connected():
            self.client.connect(self.ip, self.rack, self.slot)
            self.connected = True

    def safe_db_read(self, dbnum, start, size, retries=3):
        backoff = 0.05
        for i in range(retries):
            try:
                with self.lock:
                    now = time.monotonic()
                    delta = now - self.last_op
                    if delta < self.min_interval:
                        time.sleep(self.min_interval - delta)
                    data = self.client.db_read(dbnum, start, size)
                    self.last_op = time.monotonic()
                    return data
            except RuntimeError as e:
                if "Job pending" in str(e):
                    time.sleep(backoff * (i + 1))
                    continue
                with self.lock:
                    try:
                        self.client.disconnect()
                    except Exception:
                        pass
                    self.connected = False
                time.sleep(backoff * (i + 1))
                self.connect()
            except Exception:
                with self.lock:
                    try:
                        self.client.disconnect()
                    except Exception:
                        pass
                    self.connected = False
                time.sleep(backoff * (i + 1))
                self.connect()
        with self.lock:
            data = self.client.db_read(dbnum, start, size)
            self.last_op = time.monotonic()
            return data


# ======================= State =======================
class AppState:
    def __init__(self):
        self.cfg = self.load_cfg()
        self.pool: Dict[tuple, ClientEntry] = {}
        self.snapshot: Dict[str, Dict[str, Any]] = {}
        self.last_mtime = os.path.getmtime(CFG_PATH)
        self.lock = threading.Lock()

    def load_cfg(self):
        with open(CFG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    def get_ce(self, ip, rack, slot, min_interval_ms):
        key = (ip, rack, slot)
        ce = self.pool.get(key)
        if ce is None:
            ce = ClientEntry(ip, rack, slot, min_interval_ms=min_interval_ms)
            ce.connect()
            self.pool[key] = ce
        else:
            ce.connect()
        return ce


state = AppState()
app = FastAPI(title="S7 Monitor", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ws_clients: List[WebSocket] = []
ws_loop: asyncio.AbstractEventLoop | None = None
stop_event = threading.Event()


async def _broadcast(message: str):
    dead = []
    for ws in list(ws_clients):
        try:
            await ws.send_text(message)
        except Exception:
            dead.append(ws)
    for ws in dead:
        try:
            await ws.close()
        except Exception:
            pass
        if ws in ws_clients:
            ws_clients.remove(ws)


def queue_broadcast(payload: Dict[str, Any]):
    if ws_loop is None:
        return
    msg = json.dumps(payload, ensure_ascii=False)
    asyncio.run_coroutine_threadsafe(_broadcast(msg), ws_loop)


def poll_loop():
    while not stop_event.is_set():
        try:
            # reload config if changed
            try:
                mtime = os.path.getmtime(CFG_PATH)
                if mtime != state.last_mtime:
                    with state.lock:
                        state.cfg = state.load_cfg()
                        state.last_mtime = mtime
                    print("[CFG] clps.json recarregado.")
            except Exception:
                pass

            with state.lock:
                cfg = state.cfg

            base_period = float(cfg["mqtt"].get("period", 2.0))
            min_interval_ms = int(cfg.get("s7", {}).get("min_interval_ms", 10))
            retries = int(cfg.get("s7", {}).get("retries", 3))

            updates = {}
            ts_iso = datetime.utcnow().isoformat() + "Z"

            for clp in cfg["clps"]:
                ce = state.get_ce(clp["ip"], clp["rack"], clp["slot"], min_interval_ms)
                clp_slug = slug(clp["name"])
                clp_snapshot = {}

                for v in clp["vars"]:
                    vname = v["name"]
                    vdb = int(v.get("db", clp["db_default"]))
                    off = int(v["offset"])
                    size = int(v["size"])
                    typ = v["type"]
                    try:
                        if SIM_MODE:
                            val = simulate_value(clp_slug, slug(vname), typ, time.time())
                        else:
                            raw = ce.safe_db_read(vdb, off, size, retries=retries)
                            val = parse_value(raw, typ)
                    except Exception:
                        val = simulate_value(clp_slug, slug(vname), typ, time.time()) if SIM_MODE else None
                    clp_snapshot[slug(vname)] = val

                clp_snapshot["_ts"] = ts_iso
                updates[clp_slug] = clp_snapshot

            with state.lock:
                state.snapshot = updates

            queue_broadcast({"type": "update", "ts": ts_iso, "clps": updates})
            time.sleep(base_period)
        except Exception as e:
            print("[LOOP] erro:", repr(e))
            time.sleep(1.0)


@app.on_event("startup")
async def on_startup():
    global ws_loop
    ws_loop = asyncio.get_running_loop()
    threading.Thread(target=poll_loop, daemon=True).start()


@app.on_event("shutdown")
async def on_shutdown():
    stop_event.set()
    queue_broadcast({"type": "shutdown"})


@app.get("/api/clps")
async def list_clps():
    with state.lock:
        cfg = state.cfg
    items = []
    for clp in cfg["clps"]:
        items.append(
            {
                "name": clp["name"],
                "slug": slug(clp["name"]),
                "ip": clp["ip"],
                "vars": [{"name": v["name"], "slug": slug(v["name"])} for v in clp["vars"]],
            }
        )
    return JSONResponse(items)


@app.get("/api/snapshot")
async def snapshot():
    with state.lock:
        snap = state.snapshot
    return JSONResponse({"ts": datetime.utcnow().isoformat() + "Z", "clps": snap})


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    ws_clients.append(websocket)
    try:
        # snapshot inicial
        with state.lock:
            snap = state.snapshot
        await websocket.send_text(json.dumps({"type": "snapshot", "clps": snap}, ensure_ascii=False))
        while True:
            # Mantém a conexão viva; não esperamos mensagens do cliente
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        if websocket in ws_clients:
            ws_clients.remove(websocket)


# Servir frontend estático
if os.path.isdir("static"):
    app.mount("/", StaticFiles(directory="static", html=True), name="static")
