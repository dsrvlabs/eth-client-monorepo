// S0a-B-02 F1: rustfmt-empty cfg(test) module must not swallow the next item.

#[cfg(test)]
mod tests {}

fn production() {
    let _ = std::env::var("CC_SHOULD_FAIL");
}
