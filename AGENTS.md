# Polymarket Tick Orderbook Collector

## System Prompt

- **기본 언어 원칙**: 모든 코드는 Rust가 빠르니까 Rust로 작성함을 원칙으로 한다. Python은 오직 빠른 프로토타이핑, 스크립팅, 또는 Rust로의 마이그레이션 전 단계에서만 사용할 수 있다.
- 모든 프로덕션 코드, 데이터 수집기, 백엔드 로직, 성능에 민감한 코드는 반드시 Rust로 구현한다.
- 빌드 도구는 Cargo를 사용하며, async 런타임은 tokio를 기본으로 한다.

---

## Code Documentation Style Guide

Every function, method, and complex logic block **must** have detailed doc comments with the following format:

### Rust Function Documentation Template

```rust
/// One-line summary of what this function does.
///
/// # Detailed Description
/// Explain the business logic, why it exists, and any important caveats.
/// Mention edge cases, preconditions, and postconditions.
///
/// # Arguments
/// * `arg_name` — What this argument represents, its expected format/range.
/// * `another_arg` — Additional context if non-obvious.
///
/// # Returns
/// Description of the return value, including error conditions.
///
/// # Example — Input / Output
/// ```rust
/// // Example input values
/// let logs = vec![
///     json!({
///         "address": "0xe2222d279d744050d28e00520010520000310f59",
///         "topics": [
///             "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee",
///             "0xabc...",
///             "0x000...448861155279dbf833d041b963e3ac854599e319",
///             "0x000...6f3c1ddc97c9abfb38ff0f1302a56a1946d04c6f"
///         ],
///         "data": "0x0000...000066fc62..."
///     })
/// ];
///
/// // Function call
/// let result = find_order_filled(&logs);
///
/// // Example output
/// assert_eq!(result.unwrap().maker, "0x448861155279dbf833d041b963e3ac854599e319");
/// assert_eq!(result.unwrap().taker, "0x6f3c1ddc97c9abfb38ff0f1302a56a1946d04c6f");
/// ```
///
/// # Related
/// - Links to related functions or concepts.
fn my_function(arg_name: &str) -> Result<SomeType> { ... }
```

### Rules

1. **Every public function** must have a `///` doc comment.
2. **Every non-trivial private function** should have a `//` or `///` comment.
3. **Example input/output is mandatory** for any function that:
   - Parses or transforms data formats (JSON, hex, bytes, etc.)
   - Interacts with external APIs (RPC, WebSocket, REST)
   - Implements business logic with non-obvious behavior
4. **Inside function bodies**, add inline comments for:
   - Any `unwrap()`, `expect()`, or `unsafe` block — explain why it's safe
   - Numeric literals — explain what they represent (e.g., `// 50 MiB`)
   - Complex iterator chains — explain each step
   - Regex patterns or magic strings — explain what they match
5. **For JavaScript/HTML** in `viewer.html`, use JSDoc-style comments:
   ```javascript
   /**
    * Fetches on-chain trade details by transaction hash.
    *
    * @param {string} txHash - The Polygon transaction hash (0x...)
    * @returns {Promise<Object|null>} OnchainTrade object or null if not found
    *
    * Example input:  "0x5e5fe7c64a30b1d23366bf508ea288b994e3b3d8d5afd5facd991af8551dae02"
    * Example output: { maker: "0x4488...", taker: "0x6f3c...", block_number: 88231325 }
    */
   async function showTradeDetail(txHash) { ... }
   ```
6. **Hex / byte manipulation** must always explain:
   - What the hex value represents (event signature, address, amount, etc.)
   - Why specific slice ranges are used (e.g., `data[62..64]` = side byte at offset 31)
   - The byte width of each field (u8, u256, bytes32, etc.)
