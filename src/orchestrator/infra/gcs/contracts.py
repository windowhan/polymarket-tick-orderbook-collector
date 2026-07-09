"""GCS object notification과 processed manifest 계약입니다."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ObjectNotification:
    """Collector가 업로드한 GCS object의 메타데이터입니다."""

    collector_id: str
    assignment_version: int
    bucket: str
    object_name: str
    generation: str
    line_count: int
    first_event_ts_ms: Optional[int]
    last_event_ts_ms: Optional[int]
    checksum_crc32c: Optional[str]

    def idempotency_key(self) -> str:
        """object 단위 멱등성 key를 반환합니다."""

        return f"{self.bucket}/{self.object_name}#{self.generation}"


@dataclass(frozen=True)
class ProcessedObject:
    """GCS object generation이 병합 완료되었음을 나타내는 영속 기록입니다."""

    bucket: str
    object_name: str
    generation: str
    processed_at_ms: int
    input_line_count: int
    valid_line_count: int
    skipped_line_count: int
    output_path: str

    def idempotency_key(self) -> str:
        """중복 skip 판단에 사용하는 object 단위 멱등성 key를 반환합니다."""

        return f"{self.bucket}/{self.object_name}#{self.generation}"
