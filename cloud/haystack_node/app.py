from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
import re

from fastapi import FastAPI, HTTPException, Response

from cloud.common.broker import BrokerClient, HttpBrokerClient
from cloud.common.config import HaystackSettings, HttpBrokerSettings
from cloud.common.contracts import (
    StorageAckMessage,
    decode_storage_write,
    encode_storage_ack,
)


_VOLUME_RE = re.compile(r"^volume_(\d+)\.dat$")
_ERROR_RETRY_DELAY_SECONDS = 1.0


@dataclass(slots=True)
class WriteResult:
    volume_id: int
    offset: int
    size: int


class VolumeManager:
    def __init__(self, volumes_dir: str, max_volume_size_bytes: int) -> None:
        self._volumes_dir = Path(volumes_dir)
        self._volumes_dir.mkdir(parents=True, exist_ok=True)
        self._max_volume_size_bytes = max_volume_size_bytes
        self._active_file = None
        self._active_volume_id = 1
        self._lock = asyncio.Lock()

    def start(self) -> None:
        highest = 0
        for item in self._volumes_dir.iterdir():
            if not item.is_file():
                continue
            match = _VOLUME_RE.match(item.name)
            if not match:
                continue
            highest = max(highest, int(match.group(1)))
        self._active_volume_id = highest if highest > 0 else 1
        self._open_active_file()

    def close(self) -> None:
        if self._active_file and not self._active_file.closed:
            self._active_file.close()

    def _volume_path(self, volume_id: int) -> Path:
        return self._volumes_dir / f"volume_{volume_id}.dat"

    def _open_active_file(self) -> None:
        self._active_file = self._volume_path(self._active_volume_id).open("ab+")
        self._active_file.seek(0, 2)

    async def append(self, payload: bytes) -> WriteResult:
        if not payload:
            raise ValueError("payload cannot be empty")

        async with self._lock:
            if self._active_file is None:
                raise RuntimeError("Active volume file is not initialized")
            self._active_file.seek(0, 2)
            current_size = self._active_file.tell()
            if current_size + len(payload) > self._max_volume_size_bytes:
                self._active_file.close()
                self._active_volume_id += 1
                self._open_active_file()
                current_size = self._active_file.tell()

            offset = current_size
            self._active_file.write(payload)
            self._active_file.flush()
            return WriteResult(
                volume_id=self._active_volume_id,
                offset=offset,
                size=len(payload),
            )

    def read(self, volume_id: int, offset: int, size: int) -> bytes:
        if offset < 0 or size <= 0:
            raise ValueError("offset must be >= 0 and size must be > 0")

        path = self._volume_path(volume_id)
        if not path.exists():
            raise FileNotFoundError(f"volume {volume_id} not found")

        with path.open("rb") as source:
            source.seek(0, 2)
            file_size = source.tell()
            end = offset + size
            if end > file_size:
                raise ValueError("requested range exceeds file bounds")
            source.seek(offset)
            return source.read(size)


def create_app(
    *,
    settings: HaystackSettings | None = None,
    broker: BrokerClient | None = None,
) -> FastAPI:
    cfg = settings or HaystackSettings()
    manager = VolumeManager(cfg.volumes_dir, cfg.max_volume_size_bytes)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.manager = manager
        app.state.broker = broker
        app.state.consumer_task = None
        manager.start()
        if app.state.broker is None:
            broker_cfg = HttpBrokerSettings()
            app.state.broker = HttpBrokerClient(
                base_url=broker_cfg.base_url,
                timeout_seconds=broker_cfg.timeout_seconds,
                poll_timeout_seconds=broker_cfg.poll_timeout_seconds,
            )
        app.state.consumer_task = asyncio.create_task(_consume_storage_write(app), name="storage-write-consumer")
        try:
            yield
        finally:
            task = app.state.consumer_task
            if task:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
            manager.close()

    app = FastAPI(title="Haystack Node", lifespan=lifespan)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/volume/{volume_id}/{offset}/{size}")
    async def read_volume(volume_id: int, offset: int, size: int) -> Response:
        try:
            payload = manager.read(volume_id=volume_id, offset=offset, size=size)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return Response(content=payload, media_type="application/octet-stream")

    return app


async def _consume_storage_write(app: FastAPI) -> None:
    broker: BrokerClient = app.state.broker
    manager: VolumeManager = app.state.manager
    while True:
        try:
            async for raw in broker.stream("storage.write"):
                write = decode_storage_write(raw)
                result = await manager.append(write.data)
                ack = StorageAckMessage(
                    object_id=write.object_id,
                    volume_id=result.volume_id,
                    offset=result.offset,
                    size=result.size,
                )
                await broker.publish("storage.ack", encode_storage_ack(ack))
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(_ERROR_RETRY_DELAY_SECONDS)


app = create_app()
