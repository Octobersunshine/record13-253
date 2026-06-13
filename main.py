import asyncio
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field, field_validator

DEFAULT_TZ = "Asia/Shanghai"

MATCHES: dict[str, "MatchEvent"] = {}
REMINDER_LOG: dict[str, bool] = {}
ws_connections: list[WebSocket] = []

scheduler = AsyncIOScheduler(timezone="UTC")

_broadcaster_task: Optional[asyncio.Task] = None


class MatchStatus(str, Enum):
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    PAUSED = "paused"
    FINISHED = "finished"
    CANCELLED = "cancelled"


class ScoreEventType(str, Enum):
    GOAL = "goal"
    POINT = "point"
    THREE_POINTER = "three_pointer"
    TOUCHDOWN = "touchdown"
    FIELD_GOAL = "field_goal"
    SAFETY = "safety"
    WICKET = "wicket"
    TRY = "try"
    CONVERSION = "conversion"
    PENALTY = "penalty"
    DROP_GOAL = "drop_goal"
    OTHER = "other"


class MatchTeam(BaseModel):
    home: str = Field(..., description="主队名称")
    away: str = Field(..., description="客队名称")


class ScoreUpdate(BaseModel):
    team: str = Field(..., description="得分方: home 或 away")
    points: int = Field(1, description="得分点数，默认 1（足球进球）", ge=1)
    event_type: ScoreEventType = Field(ScoreEventType.GOAL, description="得分事件类型")
    player: Optional[str] = Field(None, description="得分球员")
    match_time: Optional[str] = Field(None, description="比赛时间，如 '45+2' 或 '第 3 节 08:25'")
    notes: Optional[str] = Field(None, description="备注信息")


class ScoreEvent(BaseModel):
    id: str
    team: str
    points: int
    event_type: ScoreEventType
    player: Optional[str]
    match_time: Optional[str]
    notes: Optional[str]
    timestamp: datetime
    score_home: int
    score_away: int


def parse_tz(tz_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(tz_name)
    except ZoneInfoNotFoundError as exc:
        raise HTTPException(status_code=400, detail=f"无效的时区: {tz_name}") from exc


def to_utc(dt: datetime, tz: ZoneInfo) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    return dt.astimezone(timezone.utc)


def in_tz(dt: datetime, tz: ZoneInfo) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(tz)


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
    teams: MatchTeam = Field(..., description="对阵双方")
    start_time: datetime = Field(..., description="开赛时间 (ISO 8601)")
    timezone: str = Field(DEFAULT_TZ, description="输入时间所属时区，默认 Asia/Shanghai (UTC+8)")
    reminder_minutes: int = Field(15, description="提前提醒分钟数", ge=1)
    webhook_url: Optional[str] = Field(None, description="Webhook 回调地址")

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, v: str) -> str:
        try:
            ZoneInfo(v)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"无效的时区: {v}") from exc
        return v


class MatchEvent(BaseModel):
    id: str
    name: str
    teams: MatchTeam
    start_time: datetime = Field(description="开赛时间 (UTC)")
    timezone: str = Field(description="赛事本地时区")
    reminder_minutes: int
    webhook_url: Optional[str] = None
    reminded: bool = False
    status: MatchStatus = MatchStatus.NOT_STARTED
    score_home: int = 0
    score_away: int = 0
    score_events: list[ScoreEvent] = Field(default_factory=list)
    status_updated_at: Optional[datetime] = None

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

    @property
    def score_text(self) -> str:
        return f"{self.teams.home} {self.score_home} - {self.score_away} {self.teams.away}"

    def start_time_in_tz(self, tz_name: Optional[str] = None) -> datetime:
        tz = ZoneInfo(tz_name) if tz_name else ZoneInfo(self.timezone)
        return in_tz(self.start_time, tz)

    def add_score(self, update: ScoreUpdate) -> ScoreEvent:
        if update.team == "home":
            self.score_home += update.points
        elif update.team == "away":
            self.score_away += update.points
        else:
            raise ValueError("team 必须是 'home' 或 'away'")
        event = ScoreEvent(
            id=uuid.uuid4().hex[:12],
            team=update.team,
            points=update.points,
            event_type=update.event_type,
            player=update.player,
            match_time=update.match_time,
            notes=update.notes,
            timestamp=datetime.now(timezone.utc),
            score_home=self.score_home,
            score_away=self.score_away,
        )
        self.score_events.append(event)
        return event

    def set_status(self, status: MatchStatus) -> None:
        self.status = status
        self.status_updated_at = datetime.now(timezone.utc)


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
            local_tz = ZoneInfo(match.timezone)
            payload = {
                "event": "match_reminder",
                "match_id": match.id,
                "match_name": match.name,
                "start_time_utc": match.start_time.isoformat(),
                "start_time_local": in_tz(match.start_time, local_tz).isoformat(),
                "timezone": match.timezone,
                "minutes_before": match.reminder_minutes,
                "countdown": match.countdown_text,
            }
            await broadcast_ws(payload)
            if match.webhook_url:
                await send_webhook(match.webhook_url, payload)


async def countdown_broadcaster() -> None:
    while True:
        if ws_connections:
            matches_data = []
            for m in MATCHES.values():
                local_tz = ZoneInfo(m.timezone)
                matches_data.append({
                    "id": m.id,
                    "name": m.name,
                    "teams": {"home": m.teams.home, "away": m.teams.away},
                    "start_time_utc": m.start_time.isoformat(),
                    "start_time_local": in_tz(m.start_time, local_tz).isoformat(),
                    "timezone": m.timezone,
                    "countdown": m.countdown_text,
                    "countdown_seconds": m.countdown_seconds,
                    "status": m.status.value,
                    "score_home": m.score_home,
                    "score_away": m.score_away,
                    "score_text": m.score_text,
                    "reminded": m.reminded,
                })
            payload = {
                "event": "countdown_tick",
                "matches": matches_data,
            }
            await broadcast_ws(payload)
        await asyncio.sleep(1)


@app.post("/matches", response_model=MatchEvent, status_code=201)
async def create_match(body: MatchCreate):
    match_id = uuid.uuid4().hex[:12]
    tz = ZoneInfo(body.timezone)
    start_time_utc = to_utc(body.start_time, tz)
    match = MatchEvent(
        id=match_id,
        name=body.name,
        teams=body.teams,
        start_time=start_time_utc,
        timezone=body.timezone,
        reminder_minutes=body.reminder_minutes,
        webhook_url=body.webhook_url,
    )
    MATCHES[match_id] = match
    REMINDER_LOG[match_id] = False
    return match


@app.get("/matches")
async def list_matches(tz: Optional[str] = Query(None, description="返回时间的时区，默认使用赛事本地时区")):
    if tz:
        target_tz = parse_tz(tz)
    else:
        target_tz = None
    result = []
    for m in MATCHES.values():
        data = m.model_dump()
        display_tz = target_tz if target_tz else ZoneInfo(m.timezone)
        data["start_time_local"] = in_tz(m.start_time, display_tz).isoformat()
        data["timezone_display"] = str(display_tz)
        result.append(data)
    return result


@app.get("/matches/{match_id}")
async def get_match(match_id: str, tz: Optional[str] = Query(None, description="返回时间的时区")):
    match = MATCHES.get(match_id)
    if not match:
        raise HTTPException(status_code=404, detail="赛事不存在")
    data = match.model_dump()
    display_tz = ZoneInfo(tz) if tz else ZoneInfo(match.timezone)
    data["start_time_local"] = in_tz(match.start_time, display_tz).isoformat()
    data["timezone_display"] = str(display_tz)
    return data


@app.delete("/matches/{match_id}", status_code=204)
async def delete_match(match_id: str):
    if match_id in MATCHES:
        del MATCHES[match_id]
        REMINDER_LOG.pop(match_id, None)


@app.get("/matches/{match_id}/countdown")
async def get_countdown(match_id: str, tz: Optional[str] = Query(None, description="返回时间的时区")):
    match = MATCHES.get(match_id)
    if not match:
        raise HTTPException(status_code=404, detail="赛事不存在")
    display_tz = ZoneInfo(tz) if tz else ZoneInfo(match.timezone)
    return {
        "match_id": match.id,
        "name": match.name,
        "start_time_utc": match.start_time.isoformat(),
        "start_time_local": in_tz(match.start_time, display_tz).isoformat(),
        "timezone": str(display_tz),
        "countdown_seconds": match.countdown_seconds,
        "countdown": match.countdown_text,
        "reminded": match.reminded,
    }


class StatusUpdate(BaseModel):
    status: MatchStatus = Field(..., description="新状态")


@app.patch("/matches/{match_id}/status")
async def update_match_status(match_id: str, body: StatusUpdate):
    match = MATCHES.get(match_id)
    if not match:
        raise HTTPException(status_code=404, detail="赛事不存在")
    new_status = body.status
    transitions = {
        MatchStatus.NOT_STARTED: [MatchStatus.IN_PROGRESS, MatchStatus.CANCELLED],
        MatchStatus.IN_PROGRESS: [MatchStatus.PAUSED, MatchStatus.FINISHED, MatchStatus.CANCELLED],
        MatchStatus.PAUSED: [MatchStatus.IN_PROGRESS, MatchStatus.CANCELLED],
        MatchStatus.FINISHED: [],
        MatchStatus.CANCELLED: [],
    }
    allowed = transitions.get(match.status, [])
    if new_status not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"不允许从 {match.status.value} 转换到 {new_status.value}",
        )
    match.set_status(new_status)
    local_tz = ZoneInfo(match.timezone)
    payload = {
        "event": "match_status_change",
        "match_id": match.id,
        "match_name": match.name,
        "teams": {"home": match.teams.home, "away": match.teams.away},
        "old_status": body.status.value if body.status != new_status else match.status.value,
        "new_status": new_status.value,
        "score_home": match.score_home,
        "score_away": match.score_away,
        "score_text": match.score_text,
        "start_time_local": in_tz(match.start_time, local_tz).isoformat(),
        "timezone": match.timezone,
    }
    await broadcast_ws(payload)
    if match.webhook_url:
        await send_webhook(match.webhook_url, payload)
    return match.model_dump()


@app.post("/matches/{match_id}/score")
async def update_score(match_id: str, body: ScoreUpdate):
    match = MATCHES.get(match_id)
    if not match:
        raise HTTPException(status_code=404, detail="赛事不存在")
    if match.status not in (MatchStatus.IN_PROGRESS, MatchStatus.PAUSED):
        raise HTTPException(status_code=400, detail="比赛未开始或已结束，无法更新比分")
    if body.team not in ("home", "away"):
        raise HTTPException(status_code=400, detail="team 必须是 'home' 或 'away'")
    try:
        score_event = match.add_score(body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    local_tz = ZoneInfo(match.timezone)
    team_name = match.teams.home if body.team == "home" else match.teams.away
    payload = {
        "event": "score_update",
        "match_id": match.id,
        "match_name": match.name,
        "teams": {"home": match.teams.home, "away": match.teams.away},
        "score_event_id": score_event.id,
        "team": body.team,
        "team_name": team_name,
        "points": body.points,
        "event_type": body.event_type.value,
        "player": body.player,
        "match_time": body.match_time,
        "notes": body.notes,
        "score_home": match.score_home,
        "score_away": match.score_away,
        "score_text": match.score_text,
        "timestamp": score_event.timestamp.isoformat(),
        "start_time_local": in_tz(match.start_time, local_tz).isoformat(),
        "timezone": match.timezone,
    }
    await broadcast_ws(payload)
    if match.webhook_url:
        await send_webhook(match.webhook_url, payload)
    return payload


@app.get("/matches/{match_id}/score")
async def get_score_events(match_id: str):
    match = MATCHES.get(match_id)
    if not match:
        raise HTTPException(status_code=404, detail="赛事不存在")
    return {
        "match_id": match.id,
        "match_name": match.name,
        "teams": {"home": match.teams.home, "away": match.teams.away},
        "status": match.status.value,
        "score_home": match.score_home,
        "score_away": match.score_away,
        "score_text": match.score_text,
        "events": [e.model_dump() for e in match.score_events],
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
