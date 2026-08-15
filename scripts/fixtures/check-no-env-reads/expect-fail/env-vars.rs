// S0a-B-02 AC2: short-form vars() is a production process-env read.

use std::env;

fn production() {
    for (k, _) in env::vars() {
        let _ = k;
    }
}
