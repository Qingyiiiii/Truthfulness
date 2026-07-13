# video_truthfulness

Core Python package for the Demo1 offline and future platform pipelines.

Current scope:

- Pydantic schemas for project data objects.
- Interface protocols for future providers and adapters.
- Offline transcript/evidence MVP.
- Guarded single-video media download wrapper.
- Local LLM provider abstractions.
- Report generation for Markdown and JSON outputs.

The package must keep platform access, browser automation, and real downloads behind explicit adapters so failures are isolated.
