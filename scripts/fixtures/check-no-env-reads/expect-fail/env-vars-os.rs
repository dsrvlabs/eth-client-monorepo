// S0a-B-02 F3: short-form vars_os is a production process-env read.

use std::env;

fn production() {
    for (k, _) in env::vars_os() {
        let _ = k;
    }
}
