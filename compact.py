from __future__ import annotations

import argparse
import os
from pathlib import Path

import httpx


def compact_volume(*, gateway_base_url: str, volumes_dir: str, volume_id: int) -> None:
    volumes_path = Path(volumes_dir)
    volumes_path.mkdir(parents=True, exist_ok=True)
    source = volumes_path / f"volume_{volume_id}.dat"
    if not source.exists():
        raise FileNotFoundError(f"Missing source volume: {source}")

    target = volumes_path / f"volume_{volume_id}_compacted.dat"
    lock_file = volumes_path / f"volume_{volume_id}.compact.lock"
    lock_fd = None
    backup = volumes_path / f"volume_{volume_id}.bak"

    try:
        lock_fd = os.open(lock_file, os.O_CREAT | os.O_EXCL | os.O_RDWR)
    except FileExistsError as exc:
        raise RuntimeError(f"Compaction for volume {volume_id} is already in progress") from exc

    if target.exists():
        target.unlink()

    response = httpx.get(f"{gateway_base_url}/admin/volumes/{volume_id}/live-objects", timeout=30)
    response.raise_for_status()
    objects = response.json().get("objects", [])

    try:
        current_offset = 0
        with source.open("rb") as old_file, target.open("wb") as new_file:
            for obj in objects:
                old_file.seek(int(obj["offset"]))
                chunk = old_file.read(int(obj["size"]))
                new_file.write(chunk)
                patch_payload = {
                    "volume_id": volume_id,
                    "offset": current_offset,
                    "size": int(obj["size"]),
                }
                patch = httpx.patch(
                    f"{gateway_base_url}/admin/objects/{obj['object_id']}/location",
                    json=patch_payload,
                    timeout=30,
                )
                patch.raise_for_status()
                current_offset += int(obj["size"])

        source.replace(backup)
        target.replace(source)
        backup.unlink(missing_ok=True)
    except Exception:
        if backup.exists() and not source.exists():
            backup.replace(source)
        target.unlink(missing_ok=True)
        raise
    finally:
        if lock_fd is not None:
            os.close(lock_fd)
        lock_file.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compact a Haystack volume")
    parser.add_argument("volume_id", type=int)
    parser.add_argument("--gateway-base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--volumes-dir", default="volumes")
    args = parser.parse_args()

    compact_volume(
        gateway_base_url=args.gateway_base_url,
        volumes_dir=args.volumes_dir,
        volume_id=args.volume_id,
    )


if __name__ == "__main__":
    main()
