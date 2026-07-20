-- Rebuildable DuckDB projection for truthfulness_db_v02.1.0.
-- Immutable JSON/JSONL Artifacts and append-only Registry records remain authoritative.

CREATE TABLE IF NOT EXISTS warehouse_metadata (
    metadata_key VARCHAR PRIMARY KEY,
    metadata_value VARCHAR NOT NULL
);

INSERT INTO warehouse_metadata VALUES
    ('database_schema_version', 'truthfulness_db_v02.1.0'),
    ('warehouse_projection_version', 'claim_warehouse_projection_v1.0.0')
ON CONFLICT (metadata_key) DO NOTHING;

CREATE TABLE IF NOT EXISTS warehouse_export (
    export_id VARCHAR PRIMARY KEY,
    export_idempotency_key VARCHAR NOT NULL UNIQUE,
    run_id VARCHAR NOT NULL,
    manifest_hash VARCHAR NOT NULL,
    rows_hash VARCHAR NOT NULL,
    logical_hash VARCHAR NOT NULL,
    projection_status VARCHAR NOT NULL CHECK (projection_status IN ('pending', 'succeeded'))
);

CREATE TABLE IF NOT EXISTS export_publication_journal (
    publication_id VARCHAR NOT NULL,
    sequence_no INTEGER NOT NULL CHECK (sequence_no > 0),
    phase VARCHAR NOT NULL,
    expected_registry_head_hash VARCHAR,
    fact_hash VARCHAR NOT NULL,
    PRIMARY KEY (publication_id, sequence_no),
    UNIQUE (publication_id, phase)
);

CREATE TABLE IF NOT EXISTS warehouse_load_plan (
    load_plan_id VARCHAR PRIMARY KEY,
    plan_hash VARCHAR NOT NULL UNIQUE,
    export_count INTEGER NOT NULL CHECK (export_count BETWEEN 1 AND 100),
    created_at VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse_load_batch (
    load_batch_id VARCHAR PRIMARY KEY,
    load_plan_id VARCHAR NOT NULL REFERENCES warehouse_load_plan(load_plan_id),
    batch_hash VARCHAR NOT NULL,
    status VARCHAR NOT NULL CHECK (status IN ('succeeded')),
    started_at VARCHAR NOT NULL,
    completed_at VARCHAR NOT NULL,
    export_count INTEGER NOT NULL CHECK (export_count BETWEEN 1 AND 100),
    row_count BIGINT NOT NULL CHECK (row_count > 0),
    logical_hash VARCHAR NOT NULL,
    receipt_payload VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse_loaded_export (
    export_id VARCHAR PRIMARY KEY,
    export_idempotency_key VARCHAR NOT NULL UNIQUE,
    logical_hash VARCHAR NOT NULL,
    manifest_hash VARCHAR NOT NULL,
    rows_hash VARCHAR NOT NULL,
    load_batch_id VARCHAR NOT NULL REFERENCES warehouse_load_batch(load_batch_id),
    loaded_at VARCHAR NOT NULL,
    row_count BIGINT NOT NULL CHECK (row_count > 0)
);

CREATE TABLE IF NOT EXISTS warehouse_projection_attempt (
    attempt_id VARCHAR PRIMARY KEY,
    load_plan_id VARCHAR NOT NULL,
    attempt_hash VARCHAR NOT NULL,
    attempt_no INTEGER NOT NULL CHECK (attempt_no > 0),
    status VARCHAR NOT NULL CHECK (status IN ('succeeded', 'failed')),
    last_completed_stage VARCHAR NOT NULL,
    completed_at VARCHAR NOT NULL,
    error_code VARCHAR,
    UNIQUE (load_plan_id, attempt_no)
);

CREATE TABLE IF NOT EXISTS warehouse_parquet_file (
    relative_path VARCHAR PRIMARY KEY,
    export_id VARCHAR NOT NULL,
    logical_layer VARCHAR NOT NULL,
    table_code VARCHAR NOT NULL,
    file_hash VARCHAR NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes > 0),
    row_count BIGINT NOT NULL CHECK (row_count > 0),
    row_logical_hash VARCHAR NOT NULL,
    load_batch_id VARCHAR NOT NULL REFERENCES warehouse_load_batch(load_batch_id)
);

CREATE TABLE IF NOT EXISTS warehouse_rows (
    export_id VARCHAR NOT NULL,
    row_schema_version VARCHAR NOT NULL,
    logical_layer VARCHAR NOT NULL,
    table_code VARCHAR NOT NULL,
    canonical_primary_key VARCHAR NOT NULL,
    revision_no BIGINT NOT NULL CHECK (revision_no > 0),
    is_active BOOLEAN NOT NULL,
    effective_at VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    artifact_id VARCHAR NOT NULL,
    artifact_record_id VARCHAR NOT NULL,
    artifact_content_hash VARCHAR NOT NULL,
    created_at VARCHAR NOT NULL,
    writer_role VARCHAR NOT NULL,
    schema_versions_json VARCHAR NOT NULL,
    taxonomy_versions_json VARCHAR NOT NULL,
    data_json VARCHAR NOT NULL,
    row_hash VARCHAR NOT NULL,
    PRIMARY KEY (
        export_id,
        logical_layer,
        table_code,
        canonical_primary_key,
        revision_no
    ),
    UNIQUE (table_code, canonical_primary_key, revision_no)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_warehouse_rows_logical_revision
ON warehouse_rows (table_code, canonical_primary_key, revision_no);

CREATE TABLE IF NOT EXISTS warehouse_watermark (
    logical_layer VARCHAR PRIMARY KEY,
    latest_export_id VARCHAR NOT NULL,
    load_batch_id VARCHAR NOT NULL REFERENCES warehouse_load_batch(load_batch_id),
    updated_at VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse_load_receipt (
    receipt_id VARCHAR PRIMARY KEY,
    receipt_hash VARCHAR NOT NULL UNIQUE,
    load_batch_id VARCHAR NOT NULL UNIQUE REFERENCES warehouse_load_batch(load_batch_id),
    receipt_relative_path VARCHAR NOT NULL,
    committed_at VARCHAR NOT NULL
);

CREATE OR REPLACE VIEW v_warehouse_revision_history AS
SELECT *,
    CASE table_code
        WHEN 'parent_claim_revision' THEN json_extract_string(data_json, '$.revision.parent_claim_id')
        WHEN 'atomic_claim_revision' THEN json_extract_string(data_json, '$.revision.atomic_claim_id')
        WHEN 'claim_split_set_revision' THEN json_extract_string(data_json, '$.split_set.parent_claim_id')
        WHEN 'machine_claim_assessment' THEN concat_ws('|',
            json_extract_string(data_json, '$.assessment.atomic_revision_id'),
            json_extract_string(data_json, '$.phase'),
            json_extract_string(data_json, '$.label_namespace'))
        WHEN 'source_depth_assessment' THEN concat_ws('|',
            json_extract_string(data_json, '$.atomic_revision_id'),
            json_extract_string(data_json, '$.label_namespace'))
        WHEN 'evidence_revision' THEN json_extract_string(data_json, '$.revision.evidence_id')
        WHEN 'claim_evidence_link' THEN concat_ws('|',
            json_extract_string(data_json, '$.link.atomic_revision_id'),
            json_extract_string(data_json, '$.link.evidence_revision_id'))
        WHEN 'human_annotation' THEN json_extract_string(data_json, '$.annotation_task_id')
        WHEN 'gold_label' THEN concat_ws('|',
            json_extract_string(data_json, '$.gold.target_revision_id'),
            json_extract_string(data_json, '$.gold.annotation_scope'),
            json_extract_string(data_json, '$.label_namespace'))
        ELSE canonical_primary_key
    END AS stable_entity_key
FROM warehouse_rows;

CREATE OR REPLACE MACRO warehouse_rows_as_of(as_of_utc) AS TABLE
SELECT * EXCLUDE (warehouse_rank) FROM (
    SELECT *, row_number() OVER (
        PARTITION BY table_code, stable_entity_key
        ORDER BY revision_no DESC,
            CAST(effective_at AS TIMESTAMPTZ) DESC,
            CAST(created_at AS TIMESTAMPTZ) DESC,
            canonical_primary_key DESC
    ) AS warehouse_rank
    FROM v_warehouse_revision_history
    WHERE CAST(effective_at AS TIMESTAMPTZ) <= CAST(as_of_utc AS TIMESTAMPTZ)
      AND CAST(created_at AS TIMESTAMPTZ) <= CAST(as_of_utc AS TIMESTAMPTZ)
) ranked
WHERE warehouse_rank = 1 AND is_active;

CREATE OR REPLACE MACRO warehouse_rows_current() AS TABLE
SELECT * EXCLUDE (warehouse_rank) FROM (
    SELECT *, row_number() OVER (
        PARTITION BY table_code, stable_entity_key
        ORDER BY revision_no DESC,
            CAST(effective_at AS TIMESTAMPTZ) DESC,
            CAST(created_at AS TIMESTAMPTZ) DESC,
            canonical_primary_key DESC
    ) AS warehouse_rank
    FROM v_warehouse_revision_history
) ranked
WHERE warehouse_rank = 1 AND is_active;

CREATE OR REPLACE VIEW v_parent_claim_current AS
SELECT * EXCLUDE (stable_entity_key) FROM warehouse_rows_current()
WHERE table_code = 'parent_claim_revision';

CREATE OR REPLACE VIEW v_atomic_claim_current AS
SELECT * EXCLUDE (stable_entity_key) FROM warehouse_rows_current()
WHERE table_code = 'atomic_claim_revision';

CREATE OR REPLACE VIEW v_claim_split_current AS
SELECT * EXCLUDE (stable_entity_key) FROM warehouse_rows_current()
WHERE table_code = 'claim_split_set_revision';

CREATE OR REPLACE VIEW v_machine_verdict_current AS
SELECT * EXCLUDE (stable_entity_key) FROM warehouse_rows_current()
WHERE table_code = 'machine_claim_assessment';

CREATE OR REPLACE VIEW v_evidence_current AS
SELECT * EXCLUDE (stable_entity_key) FROM warehouse_rows_current()
WHERE table_code = 'evidence_revision';

CREATE OR REPLACE VIEW v_claim_evidence_current AS
SELECT * EXCLUDE (stable_entity_key) FROM warehouse_rows_current()
WHERE table_code = 'claim_evidence_link';

CREATE OR REPLACE VIEW v_gold_current AS
SELECT * EXCLUDE (stable_entity_key) FROM warehouse_rows_current()
WHERE table_code = 'gold_label';

CREATE OR REPLACE VIEW v_warehouse_projection_lag AS
SELECT logical_layer, latest_export_id, load_batch_id, updated_at
FROM warehouse_watermark;
