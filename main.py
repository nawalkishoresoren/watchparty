import time
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()
rooms: dict[str, dict] = {}  # room_id -> {"clients": set, "state": {...}}

# Optional: serve local videos from ./media (supports Range requests, so seeking works).
_media = Path(__file__).with_name("media")
if _media.is_dir():
    app.mount("/media", StaticFiles(directory=_media), name="media")


def current_position(state: dict) -> float:
    """Position now, accounting for time elapsed while playing."""
    if state["playing"]:
        return state["position"] + (time.time() - state["updated"])
    return state["position"]


@app.get("/")
async def index():
    return HTMLResponse(Path(__file__).with_name("index.html").read_text())


@app.websocket("/ws/{room_id}")
async def ws(websocket: WebSocket, room_id: str):
    await websocket.accept()
    room = rooms.setdefault(room_id, {
        "clients": set(),
        "state": {"playing": False, "position": 0.0, "updated": time.time(), "url": None},
    })
    room["clients"].add(websocket)

    # Late-joiner sync: send the room's state with position advanced to "now".
    s = room["state"]
    await websocket.send_json({
        "type": "state",
        "playing": s["playing"],
        "position": current_position(s),
        "url": s["url"],
    })

    try:
        while True:
            msg = await websocket.receive_json()
            t = msg.get("type")
            if t not in ("play", "pause", "seek", "load", "time"):
                continue

            s = room["state"]
            if t == "play":
                s["playing"] = True
            elif t == "pause":
                s["playing"] = False
            elif t == "load":
                s["url"] = msg["url"]
                s["playing"] = False
                msg.setdefault("position", 0.0)
            # seek keeps the current playing flag
            s["position"] = float(msg.get("position", current_position(s)))
            s["updated"] = time.time()

            for c in list(room["clients"]):
                if c is not websocket:
                    try:
                        await c.send_json(msg)
                    except Exception:
                        room["clients"].discard(c)
    except WebSocketDisconnect:
        pass
    finally:
        room["clients"].discard(websocket)
        if not room["clients"]:
            rooms.pop(room_id, None)
