# Displays this message
@help:
    just --list

# Formats the code
fmt:
    cargo fmt
    cargo fix --allow-dirty --allow-staged
    cargo c
    just --fmt --unstable

# Runs the tests
test *ARGS:
    cargo test --doc
    cargo nextest run {{ ARGS }}

watch_test *ARGS:
    cargo watch -s "just test {{ ARGS }}"
