# Agent demo source catalog

`sources.jsonl` contains synthetic entities, publishers, dates, URLs, and facts.
No row is a real-world claim or private project artifact.

Three rows deliberately contain prompt-injection strings. The workflow must
treat those strings as untrusted source data, remove them from generated text,
and still validate any factual citation against the indexed source.

Two Northstar rows deliberately disagree and exercise the human-review tool.
