"""Market Universe 생명주기 판단에 쓰는 순수 정책 타입입니다.

이 모듈은 Gamma API 호출이나 파일 저장을 하지 않습니다. refresh 사이에 필요한
최소 상태만 입력/출력 값으로 다루어 테스트 가능한 lifecycle 로직의 기반을 제공합니다.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class LifecyclePolicy:
    """마켓 생명주기 계산에 적용할 정책 값입니다.

    인자:
        new_market_confirm_refreshes: 신규 마켓을 ACTIVE로 올리기 전에 확인할 refresh 횟수입니다.
        closed_market_drain_secs: CLOSED 전환 전 DRAINING으로 유지할 시간입니다.
        archived_remove_immediately: archived 마켓을 active universe에서 즉시 제거할지 여부입니다.
        exclude_missing_token_ids: token id가 없는 마켓을 제외할지 여부입니다.
        exclude_orderbook_disabled: orderbook 비활성 마켓을 제외할지 여부입니다.
    """

    new_market_confirm_refreshes: int = 1
    closed_market_drain_secs: int = 180
    archived_remove_immediately: bool = True
    exclude_missing_token_ids: bool = True
    exclude_orderbook_disabled: bool = True

    def __post_init__(self) -> None:
        """정책 숫자 값이 음수가 아닌지 검증합니다.

        반환값:
            정상 값이면 아무것도 반환하지 않습니다. 음수 값이면 ``ValueError``를 발생시킵니다.
        """

        if self.new_market_confirm_refreshes < 0:
            raise ValueError("new_market_confirm_refreshes는 0 이상이어야 합니다")
        if self.closed_market_drain_secs < 0:
            raise ValueError("closed_market_drain_secs는 0 이상이어야 합니다")


@dataclass(frozen=True)
class LifecycleMemory:
    """refresh 사이에 순수 함수 입력/출력으로 전달되는 lifecycle 보조 상태입니다.

    인자:
        seen_refresh_counts: market_id별 관측 refresh 횟수입니다.
        draining_since_ms: market_id별 DRAINING 시작 시각이며 Unix millisecond 단위입니다.
    """

    seen_refresh_counts: dict[str, int] = field(default_factory=dict)
    draining_since_ms: dict[str, int] = field(default_factory=dict)

    def with_seen_count(self, market_id: str, count: int) -> "LifecycleMemory":
        """특정 마켓의 관측 횟수를 바꾼 새 memory를 반환합니다.

        인자:
            market_id: 갱신할 마켓 식별자입니다.
            count: 저장할 refresh 관측 횟수입니다.

        반환값:
            기존 memory를 변경하지 않고 복사본에 새 값을 반영한 ``LifecycleMemory``입니다.
        """

        updated = dict(self.seen_refresh_counts)
        updated[market_id] = count
        return LifecycleMemory(updated, dict(self.draining_since_ms))

    def with_draining_since(self, market_id: str, started_at_ms: int) -> "LifecycleMemory":
        """특정 마켓의 DRAINING 시작 시각을 바꾼 새 memory를 반환합니다.

        인자:
            market_id: 갱신할 마켓 식별자입니다.
            started_at_ms: DRAINING 시작 시각이며 Unix millisecond 단위입니다.

        반환값:
            기존 memory를 변경하지 않고 복사본에 새 값을 반영한 ``LifecycleMemory``입니다.
        """

        updated = dict(self.draining_since_ms)
        updated[market_id] = started_at_ms
        return LifecycleMemory(dict(self.seen_refresh_counts), updated)
