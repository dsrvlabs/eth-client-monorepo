// S0a-B-02 AC1: a cfg(test) module must not exempt a later production read.
// The old awk set skip=1 on the attribute and never reset.

#[cfg(test)]
mod tests {
    fn helper() {}
}

fn production() {
    let _ = std::env::var("CC_SHOULD_FAIL");
}
