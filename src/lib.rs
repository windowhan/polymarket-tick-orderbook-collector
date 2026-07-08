//! Polymarket 오더북 Collector의 그린필드 v2 라이브러리 진입점입니다.
//!
//! 현재 구현은 Rust Collector와 Python Orchestrator가 함께 사용할 공유 계약 타입에서
//! 시작합니다. 런타임 모듈은 아키텍처 계약이 안정화된 뒤 단계적으로 추가합니다.

pub mod common;
pub mod node;

/// 바이너리에 컴파일된 크레이트 버전을 반환합니다.
///
/// # 상세 설명
/// 그린필드 v2 계약을 쌓아가는 동안 바이너리와 스모크 테스트가 사용할 수 있는
/// 작고 안정적인 API를 제공합니다. 반환값은 컴파일 시점의 Cargo 패키지 메타데이터에서
/// 읽기 때문에 테스트 중인 산출물의 버전과 항상 일치합니다.
///
/// # 인자
/// 이 함수는 인자를 받지 않습니다.
///
/// # 반환값
/// `Cargo.toml`에 정의된 시맨틱 버전 문자열을 반환합니다.
///
/// # 예시 — 입력 / 출력
/// ```rust
/// let version = polymarket_collector::library_version();
/// assert!(!version.is_empty());
/// ```
///
/// # 관련
/// - v2 아키텍처 계약 타입은 [`common::contracts`]에 정의되어 있습니다.
pub fn library_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
