# ============================================================
#  s7_monitor.py — Leitura Snap7 + FastAPI + WebSocket
#  Loop fatiado: lê 1 CLP por vez para performance ótima
# ============================================================

### --- BLOCO 1: IMPORTS & CONFIG -------------------------------------------

import os
import json
import time
import struct
import unicodedata
import threading
from datetime import datetime
from typing import Dict, Any, List

import snap7
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import asyncio


CFG_PATH = os.getenv("CFG_PATH", "clps.json")


# ============================================================
# Util: slug (normaliza os nomes das variáveis)
# ============================================================

def slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    out = []
    for ch in s:
        if ch.isalnum():
            out.append(ch.lower())
        else:
            out.append("_")
    t = "_".join(filter(None, "".join(out).split("_")))
    return t.strip("_")


# ============================================================
# Util: parse de tipos Snap7
# ============================================================

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


# ============================================================
# Classe ClientEntry: gerencia conexão Snap7 com lock
# ============================================================

class ClientEntry:
    def __init__(self, ip, rack, slot, min_interval_ms):
        self.ip = ip
        self.rack = rack
        self.slot = slot
        self.client = snap7.client.Client()
        self.lock = threading.Lock()
        self.connected = False
        self.last_op = 0.0
        self.min_interval = min_interval_ms / 1000.0

    def connect(self):
        if not self.connected or not self.client.get_connected():
            try:
                self.client.connect(self.ip, self.rack, self.slot)
                self.connected = True
            except Exception:
                self.connected = False

    def safe_db_read(self, dbnum, start, size):
        with self.lock:
            now = time.monotonic()
            delta = now - self.last_op
            if delta < self.min_interval:
                time.sleep(self.min_interval - delta)

            try:
                data = self.client.db_read(dbnum, start, size)
                self.last_op = time.monotonic()
                return data
            except Exception:
                try:
                    self.client.disconnect()
                except Exception:
                    pass
                self.connected = False
                self.connect()
                return None


# ============================================================
# Classe AppState
# ============================================================

class AppState:
    def __init__(self):
        self.cfg = self.load_cfg()
        self.last_mtime = os.path.getmtime(CFG_PATH)
        self.lock = threading.Lock()
        self.pool: Dict[tuple, ClientEntry] = {}
        self.snapshot: Dict[str, Dict[str, Any]] = {}

    def load_cfg(self):
        with open(CFG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    def get_client(self, clp):
        key = (clp["ip"], clp["rack"], clp["slot"])
        min_interval_ms = int(self.cfg.get("s7", {}).get("min_interval_ms", 10))

        if key not in self.pool:
            ce = ClientEntry(clp["ip"], clp["rack"], clp["slot"], min_interval_ms)
            ce.connect()
            self.pool[key] = ce
        else:
            ce = self.pool[key]
            ce.connect()
        return ce


state = AppState()

# ============================================================
# FastAPI & WebSocket clients
# ============================================================

app = FastAPI(title="S7 Monitor", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

ws_clients: List[WebSocket] = []
ws_loop: asyncio.AbstractEventLoop | None = None
stop_event = threading.Event()


# ============================================================
# Envio WebSocket
# ============================================================

async def _broadcast(msg: str):
    dead = []
    for ws in list(ws_clients):
        try:
            await ws.send_text(msg)
        except Exception:
            dead.append(ws)

    for ws in dead:
        try:
            ws_clients.remove(ws)
            await ws.close()
        except Exception:
            pass


def queue_broadcast(obj: Dict[str, Any]):
    if ws_loop:
        asyncio.run_coroutine_threadsafe(
            _broadcast(json.dumps(obj, ensure_ascii=False)),
            ws_loop
        )


# ============================================================
# BLOCO 3: THREAD DE LEITURA — LOOP FATIADO POR CLP
# ============================================================

def poll_loop():
    idx = 0  # índice do CLP atual

    while not stop_event.is_set():
        try:
            # detecta mudanças no JSON
            try:
                mtime = os.path.getmtime(CFG_PATH)
                if mtime != state.last_mtime:
                    with state.lock:
                        state.cfg = state.load_cfg()
                        state.last_mtime = mtime
                    print("Config atualizada (clps.json recarregado).")
            except Exception:
                pass

            with state.lock:
                cfg = state.cfg
                clps = cfg.get("clps", [])

            if not clps:
                time.sleep(0.2)
                continue

            # seleciona 1 CLP por vez
            clp = clps[idx]
            idx = (idx + 1) % len(clps)

            ce = state.get_client(clp)
            clp_slug = slug(clp["name"])

            result = {}
            ts_iso = datetime.utcnow().isoformat() + "Z"

            # lê variáveis
            for v in clp["vars"]:
                vname = slug(v["name"])
                dbnum = int(v.get("db", clp["db_default"]))
                off = int(v["offset"])
                size = int(v["size"])
                typ = v["type"]

                raw = ce.safe_db_read(dbnum, off, size)
                val = parse_value(raw, typ) if raw else None
                result[vname] = val

            result["_ts"] = ts_iso

            with state.lock:
                state.snapshot[clp_slug] = result

            # envia update apenas deste CLP
            queue_broadcast({
                "type": "update",
                "ts": ts_iso,
                "clps": {clp_slug: result}
            })

        except Exception as e:
            print("[poll_loop] erro:", e)
            time.sleep(0.2)


# ============================================================
# BLOCO 4: API & WebSocket
# ============================================================

@app.get("/api/clps")
async def api_clps():
    with state.lock:
        cfg = state.cfg

    out = []
    for clp in cfg.get("clps", []):
        out.append({
            "name": clp["name"],
            "slug": slug(clp["name"]),
            "ip": clp["ip"],
            "vars": [{"name": v["name"], "slug": slug(v["name"])} for v in clp["vars"]]
        })
    return JSONResponse(out)


@app.get("/api/snapshot")
async def api_snapshot():
    with state.lock:
        snap = state.snapshot.copy()
    return JSONResponse({"ts": datetime.utcnow().isoformat() + "Z", "clps": snap})


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    ws_clients.append(ws)

    # envia snapshot inicial
    with state.lock:
        snap = state.snapshot.copy()

    await ws.send_text(json.dumps({
        "type": "snapshot",
        "clps": snap
    }, ensure_ascii=False))

    try:
        while True:
            await ws.receive_text()  # mantém conexão viva
    except WebSocketDisconnect:
        pass
    finally:
        if ws in ws_clients:
            ws_clients.remove(ws)


# ============================================================
# BLOCO 5: STARTUP & STATIC
# ============================================================

@app.on_event("startup")
async def on_startup():
    global ws_loop
    ws_loop = asyncio.get_running_loop()
    threading.Thread(target=poll_loop, daemon=True).start()
    print("Thread de leitura iniciada.")


@app.on_event("shutdown")
async def on_shutdown():
    stop_event.set()
    queue_broadcast({"type": "shutdown"})
    print("Servidor encerrando...")


# servir interface web
if os.path.isdir("static"):
    app.mount("/", StaticFiles(directory="static", html=True), name="static")
