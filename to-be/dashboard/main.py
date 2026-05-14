import asyncio
import os
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI(title="To-Be 대시보드")

SUPPLY_URL = os.environ.get("SUPPLY_SERVICE_URL", "http://localhost:8100")
SEARCH_URL = os.environ.get("SEARCH_SERVICE_URL", "http://localhost:8101")
MOBILE_URL = os.environ.get("MOBILE_SERVICE_URL", "http://localhost:8102")
GRAPH_URL = os.environ.get("GRAPH_SERVICE_URL", "http://localhost:8103")
_INDEX = Path(__file__).resolve().parent / "index.html"


async def fetch(client: httpx.AsyncClient, url: str, method: str = "GET", **kwargs):
    try:
        if method == "POST":
            r = await client.post(url, **kwargs)
        else:
            r = await client.get(url, **kwargs)
        return r.json()
    except Exception:
        return None


@app.get("/api/all")
async def get_all():
    async with httpx.AsyncClient(timeout=3) as client:
        supply_health, supply_stats, search_health, mobile_health, graph_health, search_logs, mobile_logs, graph_logs = await asyncio.gather(
            fetch(client, f"{SUPPLY_URL}/"),
            fetch(client, f"{SUPPLY_URL}/stats"),
            fetch(client, f"{SEARCH_URL}/"),
            fetch(client, f"{MOBILE_URL}/"),
            fetch(client, f"{GRAPH_URL}/"),
            fetch(client, f"{SEARCH_URL}/logs"),
            fetch(client, f"{MOBILE_URL}/logs"),
            fetch(client, f"{GRAPH_URL}/logs"),
        )

    logs = []
    for raw in [search_logs, mobile_logs, graph_logs]:
        if raw:
            logs.extend(raw.get("logs", []))

    return {
        "supply": supply_health or {"status": "down"},
        "search": search_health or {"status": "down"},
        "mobile": mobile_health or {"status": "down"},
        "graph": graph_health or {"status": "down"},
        "supply_stats": supply_stats or {},
        "logs": logs,
    }


@app.post("/api/create-mail")
async def create_mail():
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(
            f"{SUPPLY_URL}/mails",
            json={
                "title": "테스트 메일",
                "body": "To-Be: Kafka ingest 흐름 시연입니다.",
                "sender": "test@company.com",
            },
        )
        return r.json()


@app.post("/api/reset")
async def reset():
    async with httpx.AsyncClient(timeout=3) as client:
        await asyncio.gather(
            fetch(client, f"{SUPPLY_URL}/stats/reset", method="POST"),
            fetch(client, f"{SEARCH_URL}/reset", method="POST"),
            fetch(client, f"{MOBILE_URL}/reset", method="POST"),
            fetch(client, f"{GRAPH_URL}/reset", method="POST"),
        )
    return {"message": "전체 초기화 완료"}


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return _INDEX.read_text(encoding="utf-8")
