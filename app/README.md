# app

Streamlit entry points live here.

Current stage:

- Run the offline MVP from local transcript and evidence JSON.
- Provide a guarded single-download tab that attempts one platform download only when the user clicks it.
- Show run artifact locations without exposing ignored runtime files.

Do not store run outputs or uploaded private media in this directory. Runtime artifacts belong under `runs/<run_id>/`.
