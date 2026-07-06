//! 그린필드 v2 바이너리의 최소 진입점입니다.
//!
//! 런타임 명령은 아직 의도적으로 구현하지 않았습니다. 첫 빌드 마일스톤은
//! 아키텍처 22.1절의 계약과 타입을 검증하는 것입니다.

/// 그린필드 계약 빌드 표식을 출력합니다.
///
/// # 상세 설명
/// 이전 구현은 의도적으로 폐기되었습니다. 이 진입점은 새로운 Orchestrator/Collector
/// 계약을 작고 검증 가능한 단계로 도입하는 동안 `cargo build`, `cargo run`, CI 스모크
/// 체크가 계속 성공하도록 유지합니다.
///
/// # 인자
/// 아직 명령줄 인자를 받지 않습니다.
///
/// # 반환값
/// 짧은 표식을 출력한 뒤 프로세스가 성공 상태로 종료됩니다.
///
/// # 예시 — 입력 / 출력
/// ```text
/// $ cargo run --quiet
/// polymarket-collector v0.1.0 greenfield contracts
/// ```
///
/// # 관련
/// - 아키텍처 22.1절의 도메인 타입은 `src/common/contracts.rs`에 있습니다.
fn main() {
    println!(
        "polymarket-collector v{} greenfield contracts",
        polymarket_collector::library_version()
    );
}
