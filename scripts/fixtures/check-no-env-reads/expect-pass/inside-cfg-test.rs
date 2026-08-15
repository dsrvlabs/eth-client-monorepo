// S0a-B-02 AC3: a read genuinely inside #[cfg(test)] stays exempt.

fn production() {}

#[cfg(test)]
mod tests {
    fn helper() {
        let _ = std::env::var("CC_OK_IN_TEST");
    }
}
