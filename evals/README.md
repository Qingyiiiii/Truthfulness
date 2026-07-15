# Fixed agent evaluation

`agent_cases.jsonl` is a public, synthetic 20-case suite:

- citation correctness: 5
- no answer: 3
- prompt injection: 3
- unauthorized access: 3
- timeout: 3
- refusal: 3

The runner checks status, exact retrieved source IDs, forbidden prompt text,
and the invariant that every returned quote is anchored in its indexed source.
