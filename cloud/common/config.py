from __future__ import annotations

from dataclasses import dataclass
import os


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


@dataclass(slots=True)
class GatewaySettings:
    db_path: str = os.getenv("GATEWAY_DB_PATH", "gateway.db")
    haystack_base_url: str = os.getenv("HAYSTACK_BASE_URL", "http://127.0.0.1:8002")
    gateway_base_url: str = os.getenv("GATEWAY_BASE_URL", "http://127.0.0.1:8000")
    volumes_dir: str = os.getenv("HAYSTACK_VOLUMES_DIR", "volumes")


@dataclass(slots=True)
class HaystackSettings:
    volumes_dir: str = os.getenv("HAYSTACK_VOLUMES_DIR", "volumes")
    max_volume_size_bytes: int = _int("HAYSTACK_MAX_VOLUME_BYTES", 100 * 1024 * 1024)


@dataclass(slots=True)
class BrokerSettings:
    host: str = os.getenv("BROKER_HOST", "127.0.0.1")
    port: int = _int("BROKER_PORT", 8001)


@dataclass(slots=True)
class HttpBrokerSettings:
    base_url: str = os.getenv("BROKER_BASE_URL", "http://127.0.0.1:8001")
    timeout_seconds: float = float(os.getenv("BROKER_TIMEOUT_SECONDS", "10"))
    poll_timeout_seconds: float = float(os.getenv("BROKER_POLL_TIMEOUT_SECONDS", "15"))
