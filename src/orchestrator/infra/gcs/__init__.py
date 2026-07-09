"""GCS object intake와 processed manifest 계약 패키지입니다."""

from .contracts import ObjectNotification, ProcessedObject

__all__ = ["ObjectNotification", "ProcessedObject"]
