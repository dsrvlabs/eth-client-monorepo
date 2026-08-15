// S0a-B-02 AC2: short-form var_os is a production process-env read.

use std::env;

fn production() {
    let _ = env::var_os("CC_SHOULD_FAIL");
}
