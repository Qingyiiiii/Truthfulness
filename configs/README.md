# configs

Configuration templates live here.

Allowed here:

- Public example configuration files.
- Provider names and default local settings.
- Placeholder values that are safe to commit.
- Smoke-test training config examples that use a caller-provided, reviewed gold JSONL path.

Do not commit API keys, cookies, tokens, account identifiers, local absolute secret paths, or private model paths.

Current examples:

- `demo1.local.example.toml`: local Demo1 provider defaults.
- `train_baseline.smoke.example.toml`: gold JSONL data-loading and metrics smoke test only; not a formal training configuration. The public repository deliberately does not include a real gold batch.
