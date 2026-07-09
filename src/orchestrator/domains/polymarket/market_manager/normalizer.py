"""Gamma market payload를 Orchestrator 계약 타입으로 정규화합니다.

이 모듈은 네트워크를 호출하지 않고 단일 raw dict만 변환합니다. 실제 lifecycle 포함/제외
판단은 ``lifecycle.py``가 담당할 수 있도록 원천 flag와 token id를 보존합니다.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping

from src.orchestrator.domains.polymarket.contracts import MarketInfo, MarketLifecycleState


@dataclass(frozen=True)
class RawMarketFlags:
    """Gamma payload에서 lifecycle 판단에 필요한 필드를 추출한 값입니다.

    인자:
        market_id: 정규화된 market 식별자입니다.
        slug: 사람이 읽을 수 있는 slug입니다.
        question: 마켓 질문 또는 제목입니다.
        active: Gamma active flag입니다.
        closed: Gamma closed flag입니다.
        archived: Gamma archived flag입니다.
        accepting_orders: 주문 접수 가능 여부입니다.
        enable_order_book: CLOB orderbook 활성 여부입니다.
        token_ids: CLOB token id 목록입니다.
    """

    market_id: str
    slug: str
    question: str
    active: bool
    closed: bool
    archived: bool
    accepting_orders: bool
    enable_order_book: bool
    token_ids: list[str]


def normalize_gamma_market(
    raw: Mapping[str, Any],
    lifecycle_state: MarketLifecycleState,
) -> MarketInfo | None:
    """Gamma raw market payload를 ``MarketInfo``로 변환합니다.

    인자:
        raw: Gamma API가 반환한 단일 market dict입니다.
        lifecycle_state: 이미 계산된 lifecycle state입니다.

    반환값:
        market id가 있으면 ``MarketInfo``를 반환하고, 식별자가 없으면 ``None``을 반환합니다.
    """

    flags = extract_raw_market_flags(raw)
    if flags is None:
        return None

    return MarketInfo(
        market_id=flags.market_id,
        slug=flags.slug,
        question=flags.question,
        active=flags.active,
        closed=flags.closed,
        archived=flags.archived,
        accepting_orders=flags.accepting_orders,
        enable_order_book=flags.enable_order_book,
        token_ids=flags.token_ids,
        lifecycle_state=lifecycle_state,
    )


def extract_raw_market_flags(raw: Mapping[str, Any]) -> RawMarketFlags | None:
    """Gamma raw payload에서 lifecycle 판단용 flag 묶음을 추출합니다.

    인자:
        raw: Gamma API가 반환한 단일 market dict입니다.

    반환값:
        market id가 있으면 ``RawMarketFlags``를 반환하고, 없으면 ``None``을 반환합니다.
    """

    market_id = extract_market_id(raw)
    if market_id is None:
        return None

    active = _coerce_bool(raw.get("active"), default=False)
    closed = _coerce_bool(raw.get("closed"), default=False)
    archived = _coerce_bool(raw.get("archived"), default=False)
    accepting_orders = _coerce_bool(
        _first_present(raw, ("accepting_orders", "acceptingOrders")),
        default=active and not closed,
    )

    return RawMarketFlags(
        market_id=market_id,
        slug=_coerce_string(raw.get("slug")) or "",
        question=_coerce_string(_first_present(raw, ("question", "title"))) or "",
        active=active,
        closed=closed,
        archived=archived,
        accepting_orders=accepting_orders,
        enable_order_book=_coerce_bool(
            _first_present(raw, ("enable_order_book", "enableOrderBook", "enableOrderbook")),
            default=False,
        ),
        token_ids=extract_token_ids(raw),
    )


def extract_market_id(raw: Mapping[str, Any]) -> str | None:
    """Gamma payload에서 안정적인 market id 후보를 찾아 문자열로 반환합니다.

    인자:
        raw: Gamma API가 반환한 단일 market dict입니다.

    반환값:
        `id`, `market_id`, `conditionId`, `condition_id` 중 첫 유효 값을 문자열로 반환합니다.
    """

    return _coerce_string(_first_present(raw, ("id", "market_id", "conditionId", "condition_id")))


def extract_token_ids(raw: Mapping[str, Any]) -> list[str]:
    """Gamma payload에서 CLOB token id 목록을 추출합니다.

    인자:
        raw: Gamma API가 반환한 단일 market dict입니다.

    반환값:
        `clobTokenIds`, `clob_token_ids`, `tokens[].token_id`, `tokens[].id` 후보에서 찾은
        token id 목록입니다. 중복과 빈 값은 제거하고 원래 순서는 유지합니다.
    """

    direct_tokens = _first_present(raw, ("clobTokenIds", "clob_token_ids"))
    token_ids = _coerce_string_list(direct_tokens)
    if not token_ids:
        token_ids = _extract_token_ids_from_token_objects(raw.get("tokens"))

    deduped: list[str] = []
    seen: set[str] = set()
    for token_id in token_ids:
        if token_id and token_id not in seen:
            seen.add(token_id)
            deduped.append(token_id)
    return deduped


def _extract_token_ids_from_token_objects(value: Any) -> list[str]:
    """`tokens` 객체 배열에서 token id 후보를 추출합니다."""

    if not isinstance(value, list):
        return []

    token_ids: list[str] = []
    for item in value:
        if isinstance(item, Mapping):
            token_id = _coerce_string(_first_present(item, ("token_id", "tokenId", "id")))
            if token_id:
                token_ids.append(token_id)
    return token_ids


def _coerce_string_list(value: Any) -> list[str]:
    """문자열, JSON 문자열, list 값을 문자열 list로 변환합니다."""

    if value is None:
        return []
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        if stripped.startswith("["):
            try:
                return _coerce_string_list(json.loads(stripped))
            except json.JSONDecodeError:
                return []
        if "," in stripped:
            return [_strip for part in stripped.split(",") if (_strip := part.strip())]
        return [stripped]
    if isinstance(value, (list, tuple)):
        return [text for item in value if (text := _coerce_string(item))]
    text = _coerce_string(value)
    return [text] if text else []


def _first_present(raw: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
    """여러 후보 key 중 값이 존재하는 첫 항목을 반환합니다."""

    for key in keys:
        if key in raw and raw[key] is not None:
            return raw[key]
    return None


def _coerce_string(value: Any) -> str | None:
    """값을 비어 있지 않은 문자열로 변환합니다."""

    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_bool(value: Any, *, default: bool) -> bool:
    """Gamma payload의 bool 후보 값을 Python bool로 변환합니다."""

    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            return True
        if normalized in {"false", "0", "no", "n"}:
            return False
    return default
