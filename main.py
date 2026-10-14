import json
import math
import re
import secrets
import time
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()

# room_id -> {"clients": {client_id: client}, "state": {...}, "history": [chat messages]}
rooms: dict[str, dict] = {}

VIDEO_EVENTS = {"play", "pause", "seek", "load", "time"}
MAX_NAME = 24
MAX_CHAT = 500
HISTORY = 50
CHAT_BURST = 8        # max chat messages ...
CHAT_WINDOW = 5.0     # ... per this many seconds, per client

# Optional: serve local videos from ./media (supports Range requests, so seeking works).
_media = Path(__file__).with_name("media")
if _media.is_dir():
    app.mount("/media", StaticFiles(directory=_media), name="media")


@app.get("/")
async def index():
    return HTMLResponse(Path(__file__).with_name("index.html").read_text(encoding="utf-8"))


@app.get("/healthz")
async def healthz():
    return {"ok": True}


# --------------------------------------------------------------------------- helpers

def clean_name(raw: str | None) -> str:
    s = re.sub(r"[\x00-\x1f\x7f]", "", raw or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s[:MAX_NAME]


def current_position(state: dict) -> float:
    """Position now, accounting for time elapsed while playing."""
    if state["playing"]:
        return state["position"] + (time.time() - state["updated"])
    return state["position"]


def as_position(value) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if not math.isfinite(value) or value < 0:
        return None
    return float(value)


def as_url(value) -> str | None:
    if isinstance(value, str) and len(value) <= 2048 and re.match(r"^https?://", value):
        return value
    return None


async def send(client: dict, payload: dict) -> None:
    try:
        await client["ws"].send_json(payload)
    except Exception:
        pass  # a dead socket is cleaned up by its own receive loop


async def broadcast(room: dict, payload: dict, exclude: str | None = None) -> None:
    for cid, client in list(room["clients"].items()):
        if cid != exclude:
            await send(client, payload)


def member_info(cid: str, client: dict) -> dict:
    return {"id": cid, "name": client["name"], "mic": client["mic"], "cam": client["cam"]}


# --------------------------------------------------------------------------- message handlers

async def handle(room: dict, cid: str, client: dict, msg: dict) -> None:
    kind = msg.get("type")

    if kind in VIDEO_EVENTS:
        s = room["state"]
        pos = as_position(msg.get("position"))
        out = {"type": kind, "from": cid, "name": client["name"]}

        if kind == "load":
            url = as_url(msg.get("url"))
            if url is None:
                return
            s.update(url=url, playing=False, position=0.0, updated=time.time())
            out.update(url=url, position=0.0)
        else:
            if kind == "seek" and pos is None:
                return
            if kind == "play":
                s["position"] = pos if pos is not None else current_position(s)
                s["playing"] = True
            elif kind == "pause":
                s["position"] = pos if pos is not None else current_position(s)
                s["playing"] = False
            elif pos is not None:  # seek / time keep the current playing flag
                s["position"] = pos
            s["updated"] = time.time()
            out["position"] = s["position"]

        await broadcast(room, out, exclude=cid)

    elif kind == "chat":
        text = msg.get("text")
        if not isinstance(text, str):
            return
        text = re.sub(r"[\x00-\x08\x0b-\x1f\x7f]", "", text).strip()[:MAX_CHAT]
        if not text:
            return
        now = time.time()
        recent = [t for t in client["chat_times"] if now - t < CHAT_WINDOW]
        if len(recent) >= CHAT_BURST:
            client["chat_times"] = recent
            await send(client, {"type": "notice", "message": "You're sending messages too fast."})
            return
        recent.append(now)
        client["chat_times"] = recent
        payload = {"type": "chat", "id": cid, "name": client["name"], "text": text, "ts": int(now * 1000)}
        room["history"].append(payload)
        del room["history"][:-HISTORY]
        await broadcast(room, payload)  # includes the sender, so everyone sees one ordering

    elif kind == "signal":  # WebRTC offer / answer / ICE candidate, relayed to one peer
        target = room["clients"].get(msg.get("to")) if isinstance(msg.get("to"), str) else None
        data = msg.get("data")
        if target is None or target is client or not isinstance(data, dict):
            return
        if len(json.dumps(data)) > 16000:
            return
        await send(target, {"type": "signal", "from": cid, "data": data})

    elif kind == "media":  # who currently has mic / camera on
        client["mic"] = bool(msg.get("mic"))
        client["cam"] = bool(msg.get("cam"))
        await broadcast(room, {"type": "media", "id": cid, "mic": client["mic"], "cam": client["cam"]}, exclude=cid)


# --------------------------------------------------------------------------- websocket

@app.websocket("/ws/{room_id}")
async def ws_endpoint(websocket: WebSocket, room_id: str):
    await websocket.accept()

    name = clean_name(websocket.query_params.get("name"))
    if not name:
        await websocket.send_json({"type": "error", "code": "name_required", "message": "Enter your name to join."})
        await websocket.close(code=4001)
        return

    room_id = room_id[:64]
    room = rooms.setdefault(room_id, {
        "clients": {},
        "state": {"playing": False, "position": 0.0, "updated": time.time(), "url": None},
        "history": [],
    })
    cid = secrets.token_hex(4)
    client = {"ws": websocket, "name": name, "mic": False, "cam": False, "chat_times": []}
    room["clients"][cid] = client

    try:
        s = room["state"]
        await websocket.send_json({
            "type": "welcome", "id": cid, "name": name,
            "members": [member_info(i, c) for i, c in room["clients"].items() if i != cid],
            "history": room["history"][-HISTORY:],
        })
        await websocket.send_json({
            "type": "state", "playing": s["playing"], "position": current_position(s), "url": s["url"],
        })
        await broadcast(room, {"type": "join", "id": cid, "name": name}, exclude=cid)

        while True:
            raw = await websocket.receive_text()
            if len(raw) > 20000:
                continue
            try:
                msg = json.loads(raw)
            except ValueError:
                continue
            if isinstance(msg, dict):
                await handle(room, cid, client, msg)
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        room["clients"].pop(cid, None)
        if room["clients"]:
            await broadcast(room, {"type": "leave", "id": cid, "name": name})
        elif rooms.get(room_id) is room:
            rooms.pop(room_id, None)
