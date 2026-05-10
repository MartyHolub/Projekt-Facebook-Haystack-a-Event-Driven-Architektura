from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from contextlib import suppress
from datetime import datetime, timezone
import os
import sqlite3
from typing import Any, Awaitable, Callable
from uuid import uuid4

import httpx
from fastapi import FastAPI, File, HTTPException, Query, Response, UploadFile
from pydantic import BaseModel

from cloud.common.broker import BrokerClient, HttpBrokerClient
from cloud.common.config import GatewaySettings, HttpBrokerSettings
from cloud.common.contracts import (
    StorageWriteMessage,
    decode_storage_ack,
    encode_storage_write,
)

_ERROR_RETRY_DELAY_SECONDS = 1.0


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class GatewayDB:
    def __init__(self, path: str) -> None:
        self.path = path
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def close(self) -> None:
        self.conn.close()

    def _init_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS objects (
                object_id TEXT PRIMARY KEY,
                bucket TEXT NOT NULL,
                owner TEXT NOT NULL,
                status TEXT NOT NULL,
                volume_id INTEGER,
                offset INTEGER,
                size INTEGER,
                content_type TEXT,
                is_deleted INTEGER NOT NULL DEFAULT 0,
                billed INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        self.conn.commit()

    def create_uploading_object(self, *, object_id: str, bucket: str, owner: str, content_type: str | None) -> None:
        now = _utcnow()
        self.conn.execute(
            """
            INSERT INTO objects (object_id, bucket, owner, status, content_type, created_at, updated_at)
            VALUES (?, ?, ?, 'uploading', ?, ?, ?)
            """,
            (object_id, bucket, owner, content_type, now, now),
        )
        self.conn.commit()

    def mark_ready(self, *, object_id: str, volume_id: int, offset: int, size: int) -> bool:
        row = self.conn.execute(
            "SELECT status, billed FROM objects WHERE object_id = ?",
            (object_id,),
        ).fetchone()
        if row is None:
            return False

        now = _utcnow()
        if row["status"] == "ready":
            # idempotent ACK handling
            self.conn.execute(
                """
                UPDATE objects
                SET volume_id = ?, offset = ?, size = ?, updated_at = ?
                WHERE object_id = ?
                """,
                (volume_id, offset, size, now, object_id),
            )
            self.conn.commit()
            return True

        self.conn.execute(
            """
            UPDATE objects
            SET status = 'ready', volume_id = ?, offset = ?, size = ?, billed = 1, updated_at = ?
            WHERE object_id = ?
            """,
            (volume_id, offset, size, now, object_id),
        )
        self.conn.commit()
        return True

    def get_object(self, object_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM objects WHERE object_id = ?",
            (object_id,),
        ).fetchone()

    def soft_delete(self, object_id: str) -> bool:
        row = self.get_object(object_id)
        if row is None:
            return False
        self.conn.execute(
            "UPDATE objects SET is_deleted = 1, updated_at = ? WHERE object_id = ?",
            (_utcnow(), object_id),
        )
        self.conn.commit()
        return True

    def list_live_objects_for_volume(self, volume_id: int) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT object_id, volume_id, offset, size
            FROM objects
            WHERE status = 'ready' AND is_deleted = 0 AND volume_id = ?
            ORDER BY offset ASC
            """,
            (volume_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def update_location(self, *, object_id: str, volume_id: int, offset: int, size: int) -> bool:
        row = self.get_object(object_id)
        if row is None:
            return False
        self.conn.execute(
            """
            UPDATE objects
            SET volume_id = ?, offset = ?, size = ?, updated_at = ?
            WHERE object_id = ?
            """,
            (volume_id, offset, size, _utcnow(), object_id),
        )
        self.conn.commit()
        return True


class UploadAccepted(BaseModel):
    object_id: str
    status: str


class LocationPatch(BaseModel):
    volume_id: int
    offset: int
    size: int


def _default_haystack_fetcher(base_url: str) -> Callable[[int, int, int], Awaitable[bytes]]:
    async def fetch(volume_id: int, offset: int, size: int) -> bytes:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(f"{base_url}/volume/{volume_id}/{offset}/{size}")
            if response.status_code == 404:
                raise HTTPException(status_code=404, detail="Object data not found in Haystack")
            if response.status_code >= 400:
                raise HTTPException(status_code=502, detail="Haystack read failed")
            return response.content

    return fetch


def create_app(
    *,
    settings: GatewaySettings | None = None,
    broker: BrokerClient | None = None,
    haystack_fetcher: Callable[[int, int, int], Awaitable[bytes]] | None = None,
) -> FastAPI:
    cfg = settings or GatewaySettings()
    db = GatewayDB(cfg.db_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.db = db
        app.state.broker = broker
        app.state.ack_task = None
        app.state.haystack_fetcher = haystack_fetcher or _default_haystack_fetcher(cfg.haystack_base_url)
        if app.state.broker is None:
            broker_cfg = HttpBrokerSettings()
            app.state.broker = HttpBrokerClient(
                base_url=broker_cfg.base_url,
                timeout_seconds=broker_cfg.timeout_seconds,
                poll_timeout_seconds=broker_cfg.poll_timeout_seconds,
            )
        app.state.ack_task = asyncio.create_task(_consume_storage_ack(app), name="storage-ack-consumer")
        try:
            yield
        finally:
            task = app.state.ack_task
            if task:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
            db.close()

    app = FastAPI(title="S3 Gateway", lifespan=lifespan)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/upload", response_model=UploadAccepted, status_code=202)
    async def upload(
        file: UploadFile = File(...),
        bucket: str = Query("default"),
        owner: str = Query("anonymous"),
    ) -> UploadAccepted:
        payload = await file.read()
        if not payload:
            raise HTTPException(status_code=400, detail="Uploaded file is empty")

        object_id = str(uuid4())
        db.create_uploading_object(
            object_id=object_id,
            bucket=bucket,
            owner=owner,
            content_type=file.content_type,
        )

        write = StorageWriteMessage(object_id=object_id, data=payload)
        broker_client: BrokerClient = app.state.broker
        await broker_client.publish("storage.write", encode_storage_write(write))

        return UploadAccepted(object_id=object_id, status="uploading")

    @app.get("/download/{object_id}")
    async def download(object_id: str) -> Response:
        row = db.get_object(object_id)
        if row is None or row["is_deleted"] == 1:
            raise HTTPException(status_code=404, detail="Object not found")
        if row["status"] != "ready":
            raise HTTPException(status_code=409, detail="Object is not ready yet")

        fetcher: Callable[[int, int, int], Awaitable[bytes]] = app.state.haystack_fetcher
        content = await fetcher(int(row["volume_id"]), int(row["offset"]), int(row["size"]))
        return Response(content=content, media_type=row["content_type"] or "application/octet-stream")

    @app.delete("/download/{object_id}", status_code=204)
    async def delete_object(object_id: str) -> None:
        if not db.soft_delete(object_id):
            raise HTTPException(status_code=404, detail="Object not found")

    @app.get("/admin/volumes/{volume_id}/live-objects")
    async def list_live_objects(volume_id: int) -> dict[str, Any]:
        return {
            "volume_id": volume_id,
            "objects": db.list_live_objects_for_volume(volume_id),
        }

    @app.patch("/admin/objects/{object_id}/location", status_code=204)
    async def patch_location(object_id: str, payload: LocationPatch) -> None:
        updated = db.update_location(
            object_id=object_id,
            volume_id=payload.volume_id,
            offset=payload.offset,
            size=payload.size,
        )
        if not updated:
            raise HTTPException(status_code=404, detail="Object not found")

    return app


async def _consume_storage_ack(app: FastAPI) -> None:
    broker: BrokerClient = app.state.broker
    db: GatewayDB = app.state.db
    while True:
        try:
            async for raw in broker.stream("storage.ack"):
                ack = decode_storage_ack(raw)
                db.mark_ready(
                    object_id=ack.object_id,
                    volume_id=ack.volume_id,
                    offset=ack.offset,
                    size=ack.size,
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(_ERROR_RETRY_DELAY_SECONDS)


app = create_app()
