# models

Local model notes may live here, but model files must not be committed.

Do not commit:

- `.gguf`
- `.safetensors`
- `.bin`
- downloaded model directories
- private provider configuration

Use `configs/versions/v01/demo1.local.example.toml` for frozen V01 placeholder settings.

The `v01-train-baseline` command is read-only by default. With explicit write
opt-in it writes smoke artifacts under `runtime/V01/reproduction-experiments/<exp_id>/`
unless another non-V02 path is supplied; it does not produce model weights.
