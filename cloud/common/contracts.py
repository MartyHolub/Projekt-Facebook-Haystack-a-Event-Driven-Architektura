from __future__ import annotations

from dataclasses import dataclass

import msgpack


@dataclass(slots=True)
class StorageWriteMessage:
    object_id: str
    data: bytes


@dataclass(slots=True)
class StorageAckMessage:
    object_id: str
    volume_id: int
    offset: int
    size: int


def encode_storage_write(message: StorageWriteMessage) -> bytes:
    return msgpack.packb(
        {
            "object_id": message.object_id,
            "data": message.data,
        },
        use_bin_type=True,
    )


def decode_storage_write(payload: bytes) -> StorageWriteMessage:
    body = msgpack.unpackb(payload, raw=False)
    return StorageWriteMessage(
        object_id=body["object_id"],
        data=body["data"],
    )


def encode_storage_ack(message: StorageAckMessage) -> bytes:
    return msgpack.packb(
        {
            "object_id": message.object_id,
            "volume_id": message.volume_id,
            "offset": message.offset,
            "size": message.size,
        },
        use_bin_type=True,
    )


def decode_storage_ack(payload: bytes) -> StorageAckMessage:
    body = msgpack.unpackb(payload, raw=False)
    return StorageAckMessage(
        object_id=body["object_id"],
        volume_id=int(body["volume_id"]),
        offset=int(body["offset"]),
        size=int(body["size"]),
    )
