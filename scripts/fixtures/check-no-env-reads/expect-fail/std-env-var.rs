// S0a-B-02 AC2: fully-qualified std env var is a production process-env read.

fn production() {
    let _ = std::env::var("CC_SHOULD_FAIL");
}
