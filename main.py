import asyncio
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

MATCHES: dict[str, "MatchEvent"] = {}
REMINDER_LOG: dict[str, bool] = {}
ws_connections: list[WebSocket] = []

scheduler = AsyncIOScheduler(timezone="UTC")

_broadcaster_task: Optional[asyncio.Task] = None


@asynccontextmanager
async def lifespan(application: FastAPI):
    global _broadcaster_task
    scheduler.add_job(check_reminders, "interval", seconds=5, id="reminder_check")
    scheduler.start()
    _broadcaster_task = asyncio.create_task(countdown_broadcaster())
    yield
    scheduler.shutdown(wait=False)
    if _broadcaster_task:
        _broadcaster_task.cancel()


app = FastAPI(title="赛事倒计时服务", version="1.0.0", lifespan=lifespan)


class MatchCreate(BaseModel):
    name: str = Field(..., description="赛事名称")
    start_time: datetime = Field(..., description="开赛时间 (ISO 8601)")
    reminder_minutes: int = Field(15, description="提前提醒分钟数", ge=1)
    webhook_url: Optional[str] = Field(None, description="Webhook 回调地址")


class MatchEvent(BaseModel):
    id: str
    name: str
    start_time: datetime
    reminder_minutes: int
    webhook_url: Optional[str] = None
    reminded: bool = False

    @property
    def countdown_seconds(self) -> int:
        delta = self.start_time - datetime.now(timezone.utc)
        return max(0, int(delta.total_seconds()))

    @property
    def countdown_text(self) -> str:
        secs = self.countdown_seconds
        hours, remainder = divmod(secs, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


class ReminderPayload(BaseModel):
    event: str = "match_reminder"
    match_id: str
    match_name: str
    start_time: str
    minutes_before: int
    countdown: str


async def send_webhook(url: str, payload: dict) -> None:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(url, json=payload)
    except Exception:
        pass


async def broadcast_ws(payload: dict) -> None:
    dead: list[WebSocket] = []
    for ws in ws_connections:
        try:
            await ws.send_json(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        ws_connections.remove(ws)


async def check_reminders() -> None:
    now = datetime.now(timezone.utc)
    for match_id, match in list(MATCHES.items()):
        if match.reminded:
            continue
        reminder_time = match.start_time - timedelta(minutes=match.reminder_minutes)
        if now >= reminder_time:
            match.reminded = True
            REMINDER_LOG[match_id] = True
            payload = ReminderPayload(
                match_id=match.id,
                match_name=match.name,
                start_time=match.start_time.isoformat(),
                minutes_before=match.reminder_minutes,
                countdown=match.countdown_text,
            ).model_dump()
            await broadcast_ws(payload)
            if match.webhook_url:
                await send_webhook(match.webhook_url, payload)


async def countdown_broadcaster() -> None:
    while True:
        if ws_connections:
            payload = {
                "event": "countdown_tick",
                "matches": [
                    {
                        "id": m.id,
                        "name": m.name,
                        "countdown": m.countdown_text,
                        "reminded": m.reminded,
                    }
                    for m in MATCHES.values()
                ],
            }
            await broadcast_ws(payload)
        await asyncio.sleep(1)


@app.post("/matches", response_model=MatchEvent, status_code=201)
async def create_match(body: MatchCreate):
    match_id = uuid.uuid4().hex[:12]
    match = MatchEvent(
        id=match_id,
        name=body.name,
        start_time=body.start_time,
        reminder_minutes=body.reminder_minutes,
        webhook_url=body.webhook_url,
    )
    MATCHES[match_id] = match
    REMINDER_LOG[match_id] = False
    return match


@app.get("/matches", response_model=list[MatchEvent])
async def list_matches():
    return list(MATCHES.values())


@app.get("/matches/{match_id}", response_model=MatchEvent)
async def get_match(match_id: str):
    match = MATCHES.get(match_id)
    if not match:
        raise HTTPException(status_code=404, detail="赛事不存在")
    return match


@app.delete("/matches/{match_id}", status_code=204)
async def delete_match(match_id: str):
    if match_id in MATCHES:
        del MATCHES[match_id]
        REMINDER_LOG.pop(match_id, None)


@app.get("/matches/{match_id}/countdown")
async def get_countdown(match_id: str):
    match = MATCHES.get(match_id)
    if not match:
        raise HTTPException(status_code=404, detail="赛事不存在")
    return {
        "match_id": match.id,
        "name": match.name,
        "start_time": match.start_time.isoformat(),
        "countdown_seconds": match.countdown_seconds,
        "countdown": match.countdown_text,
        "reminded": match.reminded,
    }


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    ws_connections.append(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        if ws in ws_connections:
            ws_connections.remove(ws)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
