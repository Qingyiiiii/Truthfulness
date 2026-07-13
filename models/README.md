# models

Local model notes may live here, but model files must not be committed.

Do not commit:

- `.gguf`
- `.safetensors`
- `.bin`
- downloaded model directories
- private provider configuration

Use `configs/demo1.local.example.toml` for safe placeholder settings.

The current `train-baseline` command writes smoke-test artifacts under
`experiments/<exp_id>/` and does not produce model weights.
