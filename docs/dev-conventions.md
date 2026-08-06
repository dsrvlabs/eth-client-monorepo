# Development conventions

## Test-module Clippy allows (Architecture R-10)

`clippy --all-targets` lints test code as well as production code. Workspace Clippy policy sets
`unwrap_used` and `expect_used` to `warn` (promoted to error under CI `-D warnings`). In tests,
`.unwrap()` / `.expect()` on known-good fixtures is intentional.

**Convention:** every test module opens with an inner allow attribute:

```rust
#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    // …
}
```

Do not scatter `#[allow(clippy::unwrap_used)]` on individual lines in tests; keep the module-level
allow so production code stays under the workspace lint.
