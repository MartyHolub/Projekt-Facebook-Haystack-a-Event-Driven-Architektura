from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import pytest

from cloud.common.broker import InMemoryBroker
from cloud.common.config import GatewaySettings, HaystackSettings
from cloud.haystack_node.app import VolumeManager, create_app as create_haystack_app
from cloud.s3_gateway.app import GatewayDB
from cloud.s3_gateway.app import create_app as create_gateway_app


@pytest.mark.asyncio
async def test_inmemory_broker_publish_and_stream() -> None:
    broker = InMemoryBroker()

    async def producer() -> None:
        await broker.publish("storage.write", b"hello")

    async def consumer() -> bytes:
        async for item in broker.stream("storage.write"):
            return item
        return b""

    consume_task = asyncio.create_task(consumer())
    await producer()
    result = await asyncio.wait_for(consume_task, timeout=1)
    assert result == b"hello"


@pytest.mark.asyncio
async def test_volume_manager_append_read_and_rotation(tmp_path: Path) -> None:
    manager = VolumeManager(str(tmp_path), max_volume_size_bytes=5)
    manager.start()

    first = await manager.append(b"abc")
    second = await manager.append(b"de")
    third = await manager.append(b"f")

    assert first.volume_id == 1
    assert second.volume_id == 1
    assert third.volume_id == 2

    assert manager.read(1, first.offset, first.size) == b"abc"
    assert manager.read(1, second.offset, second.size) == b"de"
    assert manager.read(2, third.offset, third.size) == b"f"

    manager.close()


def test_gateway_db_ack_idempotency_and_soft_delete(tmp_path: Path) -> None:
    db = GatewayDB(str(tmp_path / "gateway.db"))
    oid = "obj-1"

    db.create_uploading_object(object_id=oid, bucket="b", owner="u", content_type="image/jpeg")
    assert db.mark_ready(object_id=oid, volume_id=1, offset=10, size=3)
    assert db.mark_ready(object_id=oid, volume_id=1, offset=10, size=3)

    row = db.get_object(oid)
    assert row is not None
    assert row["status"] == "ready"
    assert row["billed"] == 1

    assert db.soft_delete(oid)
    row = db.get_object(oid)
    assert row is not None
    assert row["is_deleted"] == 1
    db.close()


@pytest.mark.asyncio
async def test_upload_ack_download_and_soft_delete_flow(tmp_path: Path) -> None:
    broker = InMemoryBroker()
    haystack_app = create_haystack_app(
        settings=HaystackSettings(
            volumes_dir=str(tmp_path / "volumes"),
            max_volume_size_bytes=1024,
        ),
        broker=broker,
    )

    async def fetch_from_haystack(volume_id: int, offset: int, size: int) -> bytes:
        return haystack_app.state.manager.read(volume_id, offset, size)

    gateway_app = create_gateway_app(
        settings=GatewaySettings(
            db_path=str(tmp_path / "gateway.db"),
            haystack_base_url="http://unused",
        ),
        broker=broker,
        haystack_fetcher=fetch_from_haystack,
    )

    haystack_transport = httpx.ASGITransport(app=haystack_app)
    gateway_transport = httpx.ASGITransport(app=gateway_app)

    async with haystack_app.router.lifespan_context(haystack_app):
        async with gateway_app.router.lifespan_context(gateway_app):
            async with httpx.AsyncClient(transport=haystack_transport, base_url="http://haystack"):
                async with httpx.AsyncClient(transport=gateway_transport, base_url="http://gateway") as gateway_client:
                    upload = await gateway_client.post(
                        "/upload?bucket=test&owner=alice",
                        files={"file": ("avatar.jpg", b"abc123", "image/jpeg")},
                    )
                    assert upload.status_code == 202
                    object_id = upload.json()["object_id"]

                    for _ in range(20):
                        row = gateway_app.state.db.get_object(object_id)
                        if row and row["status"] == "ready":
                            break
                        await asyncio.sleep(0.05)

                    row = gateway_app.state.db.get_object(object_id)
                    assert row is not None
                    assert row["status"] == "ready"
                    assert row["is_deleted"] == 0

                    download = await gateway_client.get(f"/download/{object_id}")
                    assert download.status_code == 200
                    assert download.content == b"abc123"
                    assert download.headers["content-type"].startswith("image/jpeg")

                    delete_response = await gateway_client.delete(f"/download/{object_id}")
                    assert delete_response.status_code == 204

                    missing_after_delete = await gateway_client.get(f"/download/{object_id}")
                    assert missing_after_delete.status_code == 404
