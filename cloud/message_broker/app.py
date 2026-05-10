from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, Request, Response
from pydantic import BaseModel


class SubscribeResponse(BaseModel):
    consumer_id: str


class BrokerState:
    def __init__(self) -> None:
        self.messages: dict[str, list[bytes]] = defaultdict(list)
        self.consumers: dict[tuple[str, str], int] = {}
        self.conditions: dict[str, asyncio.Condition] = defaultdict(asyncio.Condition)


broker_state = BrokerState()
app = FastAPI(title="Message Broker")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/topics/{topic}/publish", status_code=202)
async def publish(topic: str, request: Request) -> dict[str, Any]:
    payload = await request.body()
    condition = broker_state.conditions[topic]
    async with condition:
        broker_state.messages[topic].append(payload)
        condition.notify_all()
    return {"topic": topic, "queued": len(broker_state.messages[topic])}


@app.post("/topics/{topic}/subscribe", response_model=SubscribeResponse)
async def subscribe(topic: str) -> SubscribeResponse:
    consumer_id = str(uuid4())
    broker_state.consumers[(topic, consumer_id)] = 0
    return SubscribeResponse(consumer_id=consumer_id)


@app.get("/topics/{topic}/consume/{consumer_id}")
async def consume(topic: str, consumer_id: str, timeout_seconds: float = 15.0) -> Response:
    if (topic, consumer_id) not in broker_state.consumers:
        return Response(status_code=404)

    condition = broker_state.conditions[topic]
    async with condition:
        cursor = broker_state.consumers[(topic, consumer_id)]
        if cursor >= len(broker_state.messages[topic]):
            try:
                await asyncio.wait_for(condition.wait(), timeout=timeout_seconds)
            except TimeoutError:
                return Response(status_code=204)
            cursor = broker_state.consumers[(topic, consumer_id)]
            if cursor >= len(broker_state.messages[topic]):
                return Response(status_code=204)

        message = broker_state.messages[topic][cursor]
        broker_state.consumers[(topic, consumer_id)] = cursor + 1
        return Response(content=message, media_type="application/octet-stream")
