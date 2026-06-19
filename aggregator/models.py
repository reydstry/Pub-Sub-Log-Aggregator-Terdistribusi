"""
models.py — Pydantic schema untuk validasi event JSON.

Desain: validator event_id cukup "non-empty string" (bukan strictly UUID v4)
agar fleksibel menerima ULID, UUID, atau format ID lain dari berbagai source.
Timestamp divalidasi sebagai ISO 8601 untuk konsistensi lintas timezone.
"""

from pydantic import BaseModel, Field, field_validator
from typing import Any, Dict, List
from datetime import datetime


class EventSchema(BaseModel):
    """Schema tunggal untuk satu event dalam sistem Pub-Sub."""

    topic: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Nama topik/channel event",
    )
    event_id: str = Field(
        ...,
        description="Identifier unik event (UUID, ULID, atau string unik lainnya)",
    )
    timestamp: str = Field(
        ...,
        description="Waktu event dalam format ISO 8601",
    )
    source: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Asal/sumber event (nama service, sensor, dsb.)",
    )
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="Data bebas yang dibawa event",
    )

    # --- Validator: event_id harus non-empty string ---
    @field_validator("event_id")
    @classmethod
    def validate_event_id(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("event_id tidak boleh kosong atau hanya whitespace")
        return v.strip()

    # --- Validator: timestamp harus bisa di-parse sebagai ISO 8601 ---
    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            raise ValueError(f"timestamp harus format ISO 8601, diterima: {v}")
        return v


class BatchEventSchema(BaseModel):
    """Schema untuk mengirim batch (kumpulan) event sekaligus."""

    events: List[EventSchema] = Field(
        ...,
        min_length=1,
        description="Daftar event yang akan dipublish secara batch",
    )


class PublishResponse(BaseModel):
    """Respons standar setelah publish event."""

    status: str
    accepted: int = 0
    detail: str = ""


class StatsResponse(BaseModel):
    """Respons endpoint /stats."""

    received: int
    unique_processed: int
    duplicate_dropped: int
    topics: List[str]
    uptime_seconds: float
