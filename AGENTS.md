# Polymarket Tick Orderbook Collector

## System Prompt

- **기본 언어 원칙**: 모든 코드는 Rust가 빠르니까 Rust로 작성함을 원칙으로 한다. Python은 오직 빠른 프로토타이핑, 스크립팅, 또는 Rust로의 마이그레이션 전 단계에서만 사용할 수 있다.
- 모든 프로덕션 코드, 데이터 수집기, 백엔드 로직, 성능에 민감한 코드는 반드시 Rust로 구현한다.
- 빌드 도구는 Cargo를 사용하며, async 런타임은 tokio를 기본으로 한다.
