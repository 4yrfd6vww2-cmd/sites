from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from contextlib import asynccontextmanager
import sqlite3, json, time, uuid, os

DB_PATH = os.path.join(os.path.dirname(__file__), "hits.db")
OPERATORS = set()

def init_db():
    c = sqlite3.connect(DB_PATH)
    c.execute("""
        CREATE TABLE IF NOT EXISTS hits(
            id TEXT PRIMARY KEY,
            ts REAL,
            ip TEXT,
            ua TEXT,
            data TEXT
        )
    """)
    c.commit()
    c.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield

app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

async def broadcast(payload: dict):
    dead = []
    for ws in OPERATORS:
        try:
            await ws.send_json(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        OPERATORS.discard(ws)

@app.websocket("/ws/operator")
async def operator_ws(ws: WebSocket):
    await ws.accept()
    OPERATORS.add(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        OPERATORS.discard(ws)

@app.post("/api/collect")
async def collect(req: Request):
    try:
        body = await req.json()
    except Exception:
        body = {}
    rec = {
        "id": str(uuid.uuid4()),
        "ts": time.time(),
        "ip": req.headers.get("x-forwarded-for", req.client.host).split(",")[0].strip(),
        "ua": req.headers.get("user-agent", ""),
        "data": body,
    }
    try:
        c = sqlite3.connect(DB_PATH)
        c.execute(
            "INSERT INTO hits VALUES(?,?,?,?,?)",
            (rec["id"], rec["ts"], rec["ip"], rec["ua"], json.dumps(body, default=str)),
        )
        c.commit()
        c.close()
    except Exception as e:
        print("DB ERROR:", e)
    
    await broadcast(rec)
    return {"ok": True}

@app.get("/api/hits")
def hits(limit: int = 200):
    try:
        c = sqlite3.connect(DB_PATH)
        rows = c.execute(
            "SELECT id, ts, ip, ua, data FROM hits ORDER BY ts DESC LIMIT ?", (limit,)
        ).fetchall()
        c.close()
        return [
            {"id": r[0], "ts": r[1], "ip": r[2], "ua": r[3], "data": json.loads(r[4])}
            for r in rows
        ]
    except Exception:
        return []

@app.get("/")
def site1():
    return FileResponse(os.path.join(os.path.dirname(__file__), "site1.html"))

@app.get("/panel")
def site2():
    return FileResponse(os.path.join(os.path.dirname(__file__), "site2.html"))
