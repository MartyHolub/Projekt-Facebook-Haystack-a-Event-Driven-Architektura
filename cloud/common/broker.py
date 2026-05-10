from __future__ import annotations

import abc
import asyncio
from collections import defaultdict
from dataclasses import dataclass
from typing import AsyncIterator
from uuid import uuid4

import httpx

TIMEOUT_BUFFER_SECONDS = 5.0


class BrokerClient(abc.ABC):
    @abc.abstractmethod
    async def publish(self, topic: str, payload: bytes) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def stream(self, topic: str) -> AsyncIterator[bytes]:
        raise NotImplementedError


class InMemoryBroker(BrokerClient):
    def __init__(self) -> None:
        self._messages: dict[str, list[bytes]] = defaultdict(list)
        self._consumers: dict[tuple[str, str], int] = {}
        self._conditions: dict[str, asyncio.Condition] = defaultdict(asyncio.Condition)

    async def publish(self, topic: str, payload: bytes) -> None:
        condition = self._conditions[topic]
        async with condition:
            self._messages[topic].append(payload)
            condition.notify_all()

    async def _subscribe(self, topic: str) -> str:
        consumer_id = str(uuid4())
        self._consumers[(topic, consumer_id)] = 0
        return consumer_id

    async def _consume(self, topic: str, consumer_id: str) -> bytes:
        condition = self._conditions[topic]
        async with condition:
            while self._consumers[(topic, consumer_id)] >= len(self._messages[topic]):
                await condition.wait()
            cursor = self._consumers[(topic, consumer_id)]
            message = self._messages[topic][cursor]
            self._consumers[(topic, consumer_id)] = cursor + 1
            return message

    async def stream(self, topic: str) -> AsyncIterator[bytes]:
        consumer_id = await self._subscribe(topic)
        while True:
            yield await self._consume(topic, consumer_id)


@dataclass(slots=True)
class HttpBrokerClient(BrokerClient):
    base_url: str
    timeout_seconds: float = 10.0
    poll_timeout_seconds: float = 15.0

    async def publish(self, topic: str, payload: bytes) -> None:
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.post(
                f"{self.base_url}/topics/{topic}/publish",
                content=payload,
                headers={"content-type": "application/octet-stream"},
            )
            response.raise_for_status()

    async def stream(self, topic: str) -> AsyncIterator[bytes]:
        async with httpx.AsyncClient(timeout=self.poll_timeout_seconds + TIMEOUT_BUFFER_SECONDS) as client:
            subscribe = await client.post(f"{self.base_url}/topics/{topic}/subscribe")
            subscribe.raise_for_status()
            consumer_id = subscribe.json()["consumer_id"]
            while True:
                response = await client.get(
                    f"{self.base_url}/topics/{topic}/consume/{consumer_id}",
                    params={"timeout_seconds": self.poll_timeout_seconds},
                )
                response.raise_for_status()
                if response.status_code == 204:
                    continue
                yield response.content
