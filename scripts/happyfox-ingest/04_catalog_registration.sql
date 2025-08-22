-- ============================================================================
-- Catalog Registration for HappyFox Views
-- Purpose: Register all HappyFox views in MCP.CATALOG_VIEWS for discovery
-- Follows schema awareness patterns
-- ============================================================================

USE DATABASE CLAUDE_BI;
USE WAREHOUSE CLAUDE_WAREHOUSE;
USE SCHEMA MCP;

-- ----------------------------------------------------------------------------
-- CREATE CATALOG TABLE IF NOT EXISTS
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS MCP.CATALOG_VIEWS (
    VIEW_NAME STRING,
    VIEW_SCHEMA STRING DEFAULT 'MCP',
    TITLE STRING,
    DESCRIPTION STRING,
    TAGS ARRAY,
    METADATA VARIANT,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ----------------------------------------------------------------------------
-- REGISTER HAPPYFOX VIEWS
-- ----------------------------------------------------------------------------

-- Clear existing HappyFox view registrations
DELETE FROM MCP.CATALOG_VIEWS WHERE VIEW_NAME LIKE 'VW_HF_%';

-- Register VW_HF_TICKETS
INSERT INTO MCP.CATALOG_VIEWS (VIEW_NAME, TITLE, DESCRIPTION, TAGS, METADATA)
VALUES (
    'VW_HF_TICKETS',
    'HappyFox Tickets - Current State',
    'Latest version of each HappyFox support ticket with all core fields',
    ARRAY_CONSTRUCT('happyfox', 'tickets', 'support', 'current'),
    OBJECT_CONSTRUCT(
        'source', 'HappyFox API',
        'update_frequency', 'Real-time via Dynamic Table',
        'primary_key', 'ticket_id',
        'sample_queries', ARRAY_CONSTRUCT(
            'SELECT * FROM MCP.VW_HF_TICKETS WHERE status = ''New''',
            'SELECT * FROM MCP.VW_HF_TICKETS WHERE product_prefix = ''GZ'''
        )
    )
);

-- Register VW_HF_TICKET_AGING
INSERT INTO MCP.CATALOG_VIEWS (VIEW_NAME, TITLE, DESCRIPTION, TAGS, METADATA)
VALUES (
    'VW_HF_TICKET_AGING',
    'HappyFox Ticket Aging Analysis',
    'Ticket aging metrics with age buckets, lifecycle states, and SLA tracking',
    ARRAY_CONSTRUCT('happyfox', 'tickets', 'aging', 'sla', 'metrics'),
    OBJECT_CONSTRUCT(
        'source', 'HappyFox API',
        'metrics', ARRAY_CONSTRUCT('age_days', 'age_bucket', 'lifecycle_state', 'first_response_sla'),
        'sample_queries', ARRAY_CONSTRUCT(
            'SELECT age_bucket, COUNT(*) FROM MCP.VW_HF_TICKET_AGING GROUP BY age_bucket',
            'SELECT * FROM MCP.VW_HF_TICKET_AGING WHERE age_days > 30 AND lifecycle_state = ''Open'''
        )
    )
);

-- Register VW_HF_CUSTOM_FIELDS
INSERT INTO MCP.CATALOG_VIEWS (VIEW_NAME, TITLE, DESCRIPTION, TAGS, METADATA)
VALUES (
    'VW_HF_CUSTOM_FIELDS',
    'HappyFox Custom Fields',
    'Entity-Attribute-Value view of all custom fields on tickets',
    ARRAY_CONSTRUCT('happyfox', 'tickets', 'custom_fields', 'eav'),
    OBJECT_CONSTRUCT(
        'source', 'HappyFox API',
        'pattern', 'EAV (Entity-Attribute-Value)',
        'sample_queries', ARRAY_CONSTRUCT(
            'SELECT * FROM MCP.VW_HF_CUSTOM_FIELDS WHERE field_name = ''Component - CM/MZ''',
            'SELECT ticket_id, field_name, field_value FROM MCP.VW_HF_CUSTOM_FIELDS WHERE is_required = TRUE'
        )
    )
);

-- Register VW_HF_TICKET_TAGS
INSERT INTO MCP.CATALOG_VIEWS (VIEW_NAME, TITLE, DESCRIPTION, TAGS, METADATA)
VALUES (
    'VW_HF_TICKET_TAGS',
    'HappyFox Ticket Tags',
    'Many-to-many relationship of tickets and their tags',
    ARRAY_CONSTRUCT('happyfox', 'tickets', 'tags', 'classification'),
    OBJECT_CONSTRUCT(
        'source', 'HappyFox API',
        'relationship', 'Many-to-Many',
        'sample_queries', ARRAY_CONSTRUCT(
            'SELECT tag, COUNT(*) FROM MCP.VW_HF_TICKET_TAGS GROUP BY tag',
            'SELECT * FROM MCP.VW_HF_TICKET_TAGS WHERE tag IN (''billing'', ''bug'')'
        )
    )
);

-- Register VW_HF_TICKET_HISTORY
INSERT INTO MCP.CATALOG_VIEWS (VIEW_NAME, TITLE, DESCRIPTION, TAGS, METADATA)
VALUES (
    'VW_HF_TICKET_HISTORY',
    'HappyFox Ticket History',
    'Complete version history of all ticket changes with change detection',
    ARRAY_CONSTRUCT('happyfox', 'tickets', 'history', 'audit', 'changes'),
    OBJECT_CONSTRUCT(
        'source', 'HappyFox API',
        'features', ARRAY_CONSTRUCT('version tracking', 'change detection', 'status transitions'),
        'sample_queries', ARRAY_CONSTRUCT(
            'SELECT * FROM MCP.VW_HF_TICKET_HISTORY WHERE ticket_id = 12345',
            'SELECT * FROM MCP.VW_HF_TICKET_HISTORY WHERE change_type = ''Status Changed'''
        )
    )
);

-- Register VW_HF_PRODUCT_SUMMARY
INSERT INTO MCP.CATALOG_VIEWS (VIEW_NAME, TITLE, DESCRIPTION, TAGS, METADATA)
VALUES (
    'VW_HF_PRODUCT_SUMMARY',
    'HappyFox Product Analytics Summary',
    'Aggregated metrics by product prefix and lifecycle state',
    ARRAY_CONSTRUCT('happyfox', 'tickets', 'analytics', 'products', 'summary'),
    OBJECT_CONSTRUCT(
        'source', 'HappyFox API',
        'aggregation_level', 'Product and Lifecycle State',
        'metrics', ARRAY_CONSTRUCT('ticket_count', 'avg_age_days', 'status_breakdown', 'priority_breakdown'),
        'sample_queries', ARRAY_CONSTRUCT(
            'SELECT * FROM MCP.VW_HF_PRODUCT_SUMMARY WHERE product = ''GZ''',
            'SELECT * FROM MCP.VW_HF_PRODUCT_SUMMARY ORDER BY ticket_count DESC'
        )
    )
);

-- Register VW_HF_SLA_BREACHES
INSERT INTO MCP.CATALOG_VIEWS (VIEW_NAME, TITLE, DESCRIPTION, TAGS, METADATA)
VALUES (
    'VW_HF_SLA_BREACHES',
    'HappyFox SLA Breaches',
    'Tickets that have breached SLA requirements',
    ARRAY_CONSTRUCT('happyfox', 'tickets', 'sla', 'breaches', 'compliance'),
    OBJECT_CONSTRUCT(
        'source', 'HappyFox API',
        'focus', 'SLA violations and breach analysis',
        'sample_queries', ARRAY_CONSTRUCT(
            'SELECT * FROM MCP.VW_HF_SLA_BREACHES WHERE priority = ''Urgent''',
            'SELECT sla_name, COUNT(*) FROM MCP.VW_HF_SLA_BREACHES GROUP BY sla_name'
        )
    )
);

-- ----------------------------------------------------------------------------
-- CREATE DISCOVERY HELPER FUNCTION
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION MCP.DISCOVER_HAPPYFOX_VIEWS(search_term STRING DEFAULT NULL)
RETURNS TABLE (
    view_name STRING,
    title STRING,
    description STRING,
    tags ARRAY,
    sample_query STRING
)
AS
$$
    SELECT 
        VIEW_NAME,
        TITLE,
        DESCRIPTION,
        TAGS,
        METADATA:sample_queries[0]::STRING AS sample_query
    FROM MCP.CATALOG_VIEWS
    WHERE VIEW_NAME LIKE 'VW_HF_%'
      AND (
        search_term IS NULL 
        OR LOWER(TITLE) LIKE LOWER('%' || search_term || '%')
        OR LOWER(DESCRIPTION) LIKE LOWER('%' || search_term || '%')
        OR ARRAY_TO_STRING(TAGS, ',') LIKE LOWER('%' || search_term || '%')
      )
    ORDER BY VIEW_NAME
$$;

-- ----------------------------------------------------------------------------
-- CREATE QUICK ACCESS VIEWS
-- ----------------------------------------------------------------------------

-- Simple view for finding what HappyFox data is available
CREATE OR REPLACE VIEW MCP.VW_HAPPYFOX_CATALOG AS
SELECT 
    VIEW_NAME,
    TITLE,
    DESCRIPTION,
    TAGS,
    METADATA:sample_queries[0]::STRING AS example_query
FROM MCP.CATALOG_VIEWS
WHERE VIEW_NAME LIKE 'VW_HF_%'
ORDER BY VIEW_NAME;

-- ----------------------------------------------------------------------------
-- VERIFICATION
-- ----------------------------------------------------------------------------

-- Show all registered HappyFox views
SELECT 
    VIEW_NAME,
    TITLE,
    ARRAY_SIZE(TAGS) AS tag_count,
    CREATED_AT
FROM MCP.CATALOG_VIEWS
WHERE VIEW_NAME LIKE 'VW_HF_%'
ORDER BY VIEW_NAME;

-- Test discovery function
SELECT * FROM TABLE(MCP.DISCOVER_HAPPYFOX_VIEWS('aging'));
SELECT * FROM TABLE(MCP.DISCOVER_HAPPYFOX_VIEWS('ticket'));

-- Show catalog access
SELECT 
    'Use MCP.VW_HAPPYFOX_CATALOG to see all available HappyFox views' AS instruction,
    'Use MCP.DISCOVER_HAPPYFOX_VIEWS(''search_term'') to find specific views' AS discovery_tip;