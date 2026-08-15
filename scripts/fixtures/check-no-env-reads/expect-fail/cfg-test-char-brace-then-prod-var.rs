// S0a-B-02 F2: char/string/comment braces inside a cfg(test) item must not
// latch skip_depth to EOF (status.rs split-on-char-brace witness).

#[cfg(test)]
mod tests {
    fn inspect(src: &str) -> Option<&str> {
        let _ = "{";
        /* { */
        let _ = r#" { "#;
        src.split('{').next()
    }
}

fn production() {
    let _ = std::env::var("CC_SHOULD_FAIL");
}
