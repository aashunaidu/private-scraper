-- Layer 2 schema: documents, document_versions, chunks + claim_documents function

BEGIN;

CREATE TABLE IF NOT EXISTS documents (
    id BIGSERIAL PRIMARY KEY,
    url_id BIGINT NOT NULL UNIQUE REFERENCES urls(id) ON DELETE CASCADE,
    title TEXT,
    doc_type TEXT,
    language TEXT NOT NULL DEFAULT 'en',
    detected_mime TEXT,
    etag TEXT,
    source_last_modified TIMESTAMPTZ,
    scrape_status TEXT NOT NULL DEFAULT 'pending' CHECK (scrape_status IN ('pending', 'processing', 'success', 'failed', 'archived')),
    last_scraped_at TIMESTAMPTZ,
    next_scrape_at TIMESTAMPTZ,
    error_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    locked_at TIMESTAMPTZ,
    locked_by TEXT,
    latest_version_id BIGINT
);

-- Backfill missing columns for pre-existing databases where `documents` already existed.
ALTER TABLE documents ADD COLUMN IF NOT EXISTS title TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS doc_type TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS language TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS detected_mime TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS etag TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS source_last_modified TIMESTAMPTZ;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS scrape_status TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS last_scraped_at TIMESTAMPTZ;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS next_scrape_at TIMESTAMPTZ;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS error_count INTEGER;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS last_error TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS locked_by TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS latest_version_id BIGINT;

ALTER TABLE documents ALTER COLUMN language SET DEFAULT 'en';
UPDATE documents SET language = 'en' WHERE language IS NULL;
ALTER TABLE documents ALTER COLUMN language SET NOT NULL;

ALTER TABLE documents ALTER COLUMN scrape_status SET DEFAULT 'pending';
UPDATE documents SET scrape_status = 'pending' WHERE scrape_status IS NULL;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'documents_scrape_status_check'
          AND conrelid = 'documents'::regclass
    ) THEN
        ALTER TABLE documents
            ADD CONSTRAINT documents_scrape_status_check
            CHECK (scrape_status IN ('pending', 'processing', 'success', 'failed', 'archived'));
    END IF;
END $$;
ALTER TABLE documents ALTER COLUMN scrape_status SET NOT NULL;

ALTER TABLE documents ALTER COLUMN error_count SET DEFAULT 0;
UPDATE documents SET error_count = 0 WHERE error_count IS NULL;
ALTER TABLE documents ALTER COLUMN error_count SET NOT NULL;

CREATE TABLE IF NOT EXISTS document_versions (
    id BIGSERIAL PRIMARY KEY,
    document_id BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    version_number INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    markdown_content TEXT NOT NULL,
    parser_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_latest BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_document_versions_doc_version UNIQUE (document_id, version_number),
    CONSTRAINT uq_document_versions_doc_hash UNIQUE (document_id, content_hash)
);

-- Backfill missing columns for pre-existing `document_versions` table.
ALTER TABLE document_versions ADD COLUMN IF NOT EXISTS version_number INTEGER;
ALTER TABLE document_versions ADD COLUMN IF NOT EXISTS content_hash TEXT;
ALTER TABLE document_versions ADD COLUMN IF NOT EXISTS markdown_content TEXT;
ALTER TABLE document_versions ADD COLUMN IF NOT EXISTS parser_metadata JSONB;
ALTER TABLE document_versions ADD COLUMN IF NOT EXISTS is_latest BOOLEAN;
ALTER TABLE document_versions ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;

UPDATE document_versions SET parser_metadata = '{}'::jsonb WHERE parser_metadata IS NULL;
ALTER TABLE document_versions ALTER COLUMN parser_metadata SET DEFAULT '{}'::jsonb;
ALTER TABLE document_versions ALTER COLUMN parser_metadata SET NOT NULL;

UPDATE document_versions SET is_latest = TRUE WHERE is_latest IS NULL;
ALTER TABLE document_versions ALTER COLUMN is_latest SET DEFAULT TRUE;
ALTER TABLE document_versions ALTER COLUMN is_latest SET NOT NULL;

UPDATE document_versions SET created_at = NOW() WHERE created_at IS NULL;
ALTER TABLE document_versions ALTER COLUMN created_at SET DEFAULT NOW();
ALTER TABLE document_versions ALTER COLUMN created_at SET NOT NULL;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'document_versions'
          AND column_name = 'version_number'
    ) AND EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'document_versions'
          AND column_name = 'document_id'
    ) THEN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'uq_document_versions_doc_version'
              AND conrelid = 'document_versions'::regclass
        ) THEN
            ALTER TABLE document_versions
                ADD CONSTRAINT uq_document_versions_doc_version UNIQUE (document_id, version_number);
        END IF;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'document_versions'
          AND column_name = 'content_hash'
    ) AND EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'document_versions'
          AND column_name = 'document_id'
    ) THEN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'uq_document_versions_doc_hash'
              AND conrelid = 'document_versions'::regclass
        ) THEN
            ALTER TABLE document_versions
                ADD CONSTRAINT uq_document_versions_doc_hash UNIQUE (document_id, content_hash);
        END IF;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS chunks (
    id BIGSERIAL PRIMARY KEY,
    document_version_id BIGINT NOT NULL REFERENCES document_versions(id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    chunk_text TEXT NOT NULL,
    heading_path TEXT,
    token_count INTEGER NOT NULL,
    CONSTRAINT uq_chunks_version_index UNIQUE (document_version_id, chunk_index)
);

-- Backfill missing columns for pre-existing `chunks` table.
ALTER TABLE chunks ADD COLUMN IF NOT EXISTS chunk_index INTEGER;
ALTER TABLE chunks ADD COLUMN IF NOT EXISTS chunk_text TEXT;
ALTER TABLE chunks ADD COLUMN IF NOT EXISTS heading_path TEXT;
ALTER TABLE chunks ADD COLUMN IF NOT EXISTS token_count INTEGER;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_chunks_version_index'
          AND conrelid = 'chunks'::regclass
    ) THEN
        ALTER TABLE chunks
            ADD CONSTRAINT uq_chunks_version_index UNIQUE (document_version_id, chunk_index);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_documents_latest_version'
          AND conrelid = 'documents'::regclass
    ) THEN
        ALTER TABLE documents
            ADD CONSTRAINT fk_documents_latest_version
            FOREIGN KEY (latest_version_id)
            REFERENCES document_versions(id)
            ON DELETE SET NULL;
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_document_versions_one_latest
    ON document_versions(document_id)
    WHERE is_latest = TRUE;

CREATE INDEX IF NOT EXISTS idx_documents_next_scrape_status
    ON documents(next_scrape_at, scrape_status);

CREATE INDEX IF NOT EXISTS idx_documents_scrape_status
    ON documents(scrape_status);

CREATE INDEX IF NOT EXISTS idx_chunks_document_version_id
    ON chunks(document_version_id);

CREATE OR REPLACE FUNCTION claim_documents(batch_size INTEGER, worker_id TEXT)
RETURNS TABLE (
    document_id BIGINT,
    url_id BIGINT,
    url TEXT,
    etag TEXT,
    source_last_modified TIMESTAMPTZ,
    locked_at TIMESTAMPTZ,
    locked_by TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH candidates AS (
        SELECT d.id
        FROM documents d
        INNER JOIN urls u ON u.id = d.url_id
        WHERE u.relevant = TRUE
          AND u.status = 'visited'
          AND d.error_count < 10
          AND d.scrape_status IN ('pending', 'success', 'failed', 'processing')
          AND (d.next_scrape_at IS NULL OR d.next_scrape_at <= NOW())
          AND (
            d.scrape_status <> 'processing'
            OR d.locked_at IS NULL
            OR d.locked_at <= NOW() - INTERVAL '30 minutes'
          )
        ORDER BY COALESCE(d.next_scrape_at, to_timestamp(0)), d.id
        FOR UPDATE SKIP LOCKED
        LIMIT GREATEST(batch_size, 0)
    ),
    updated AS (
        UPDATE documents d
        SET scrape_status = 'processing',
            locked_at = NOW(),
            locked_by = worker_id,
            last_error = NULL
        FROM candidates c
        WHERE d.id = c.id
        RETURNING d.id, d.url_id, d.etag, d.source_last_modified, d.locked_at, d.locked_by
    )
    SELECT u2.id, u2.url_id, u.url, u2.etag, u2.source_last_modified, u2.locked_at, u2.locked_by
    FROM updated u2
    INNER JOIN urls u ON u.id = u2.url_id;
END;
$$;

COMMIT;
