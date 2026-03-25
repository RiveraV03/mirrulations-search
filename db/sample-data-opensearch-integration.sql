-- OpenSearch integration seed additions/overrides.
--
-- Intended usage:
--   1) setup_postgres.sh always loads db/sample-data.sql first
--   2) then, when EXTRA_SEED_SQL_FILE is set, it loads this file to apply
--      scenario-specific overrides used for verifying OpenSearch merge logic.

-- Make sure the "medicare" query matches docket titles in Postgres (title-branch results).
UPDATE dockets
SET docket_title = 'CY 2026 Medicare Changes to the End-Stage Renal Disease (ESRD) Prospective Payment System and Quality Incentive Program.  CMS-1830-P'
WHERE docket_id = 'CMS-2025-0240';

UPDATE dockets
SET docket_title = 'CY 2026 Medicare Payment Policies under the Physician Fee Schedule and Other Changes to Part B Payment and Coverage Policies (CMS-1832-P)'
WHERE docket_id = 'CMS-2025-0304';

-- Third docket: exists for OpenSearch hits but is NOT a title match (full-text-only append).
INSERT INTO dockets (
    docket_id,
    docket_api_link,
    agency_id,
    docket_type,
    modify_date,
    docket_title,
    docket_abstract
) VALUES (
    'CMS-2026-0001',
    'https://api.regulations.gov/v4/dockets/CMS-2026-0001',
    'CMS',
    'Rulemaking',
    '2025-12-01T12:00:00Z',
    'CY 2026 Rural Dialysis Coverage and Access Improvements',
    'Rule updates affecting Medicare payment and access in rural areas.'
)
ON CONFLICT (docket_id) DO NOTHING;

-- Minimal document row so DBLayer.get_dockets_by_ids can hydrate the OpenSearch-only docket.
INSERT INTO documents (
    document_id,
    docket_id,
    document_api_link,
    agency_id,
    document_type,
    modify_date,
    posted_date,
    document_title
) VALUES (
    'CMS-2026-0001-0001',
    'CMS-2026-0001',
    'https://api.regulations.gov/v4/documents/CMS-2026-0001-0001',
    'CMS',
    'Rule',
    '2025-12-01T12:00:00Z',
    '2025-12-01T12:00:00Z',
    'Rural access payment updates and operational standards'
)
ON CONFLICT (document_id) DO NOTHING;

-- Extra CMS-2025-0304 document to create non-whole document fraction (denominator > numerator).
INSERT INTO documents (
    document_id,
    docket_id,
    document_api_link,
    agency_id,
    document_type,
    modify_date,
    posted_date,
    document_title
) VALUES (
    'CMS-2025-0304-0003',
    'CMS-2025-0304',
    'https://api.regulations.gov/v4/documents/CMS-2025-0304-0003',
    'CMS',
    'Rule',
    '2025-05-10T12:00:00Z',
    '2025-05-05T12:00:00Z',
    'Updates to ESRD Treatment Choices Model for calendar year 2026'
)
ON CONFLICT (document_id) DO NOTHING;

