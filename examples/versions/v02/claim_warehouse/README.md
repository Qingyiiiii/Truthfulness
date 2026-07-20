# Invented Claim-warehouse micro fixture

These three files show the frozen GDB1 wire shape with two invented rows only:
one synthetic source identity and one synthetic run identity. They contain no
downloaded media, real Claim, real Evidence, real Gold, private absolute path,
or host-specific storage path.

- `export_manifest.json` is the schema-valid export manifest.
- `rows.jsonl` contains its two canonical, sorted table-specific rows.
- `load_receipt.json` shows the immutable receipt produced after projecting the
  two rows to Parquet and DuckDB with the frozen dependency versions.

The manifest freezes the exact source Registry prefix count, head and
`prefix_hash`. The receipt carries the export idempotency key plus the logical
`repository` Registry reference. The explanatory fixture omits the surrounding
Registry JSONL and projection files. The former 501-source / 919-export /
10-load-batch acceptance is deferred until sufficient native V02 non-product
data exists and the user reauthorizes a newly designed scale run; this fixture
does not execute or approximate that scale.

The `youtube_SYN00009099` key is deliberately impossible to mistake for an
observed source in this project: it is an invented contract identifier and the
row also carries `synthetic: true`. This micro fixture is explanatory only. It
contains only the general label taxonomy identity and no product taxonomy,
table, relation, route, or compatibility placeholder. Current acceptance uses
small deterministic contract tests and reports their actual fixture counts.

JSON viewers may display a final editor LF. Hash fields bind the parsed models'
canonical JSON bytes; `rows.jsonl` itself is LF-terminated by contract.
