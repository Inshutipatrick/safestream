"""Environment configuration for containerized services."""
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    mqtt_host: str = os.getenv("MQTT_HOST", "localhost")
    mqtt_port: int = int(os.getenv("MQTT_PORT", "1883"))
    redis_host: str = os.getenv("REDIS_HOST", "localhost")
    redis_port: int = int(os.getenv("REDIS_PORT", "6379"))
    redis_stream: str = os.getenv("REDIS_STREAM", "safestream:telemetry")
    ingestion_client_id: str = os.getenv("MQTT_INGEST_CLIENT_ID", "safestream-ingestion-v1")


MQTT_TOPIC_FILTER = "safestream/v1/sites/+/proximity"


def site_topic(site_id: str) -> str:
    return f"safestream/v1/sites/{site_id}/proximity"
