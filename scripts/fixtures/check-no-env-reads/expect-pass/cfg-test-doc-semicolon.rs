// S0a-B-02 F2: a rustdoc semicolon must not end the skip before the item.

#[cfg(test)]
/// Returns the cached path; panics if unset.
fn cache() {
    let _ = std::env::var("CC_OK_IN_TEST");
}
