-- HappyFox Analytics Views for ActivitySchema v2
-- These views provide business-ready analytics from the raw event stream

USE ROLE SYSADMIN;
USE DATABASE CLAUDE_BI;
USE WAREHOUSE CLAUDE_WAREHOUSE;

-- 1. Executive Summary View
CREATE OR REPLACE VIEW MCP.VW_SUPPORT_EXECUTIVE_SUMMARY AS
WITH ticket_metrics AS (
    SELECT 
        COUNT(DISTINCT object_id) as total_tickets,
        COUNT(DISTINCT CASE WHEN attributes:status::STRING NOT IN ('Closed', 'Trash') THEN object_id END) as open_tickets,
        COUNT(DISTINCT CASE WHEN attributes:status::STRING = 'Closed' THEN object_id END) as closed_tickets,
        COUNT(DISTINCT CASE WHEN attributes:priority::STRING = 'High' THEN object_id END) as high_priority,
        COUNT(DISTINCT CASE WHEN attributes:priority::STRING = 'Urgent' THEN object_id END) as urgent_tickets
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action LIKE 'ticket.%'
      AND source = 'HAPPYFOX'
),
response_times AS (
    SELECT
        AVG(TIMESTAMPDIFF(HOUR, 
            TRY_TO_TIMESTAMP_NTZ(attributes:created_at::STRING),
            TRY_TO_TIMESTAMP_NTZ(attributes:first_response_time::STRING)
        )) as avg_first_response_hours,
        MEDIAN(TIMESTAMPDIFF(HOUR,
            TRY_TO_TIMESTAMP_NTZ(attributes:created_at::STRING),
            TRY_TO_TIMESTAMP_NTZ(attributes:first_response_time::STRING)
        )) as median_first_response_hours
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action = 'ticket.created'
      AND source = 'HAPPYFOX'
      AND attributes:first_response_time IS NOT NULL
),
resolution_times AS (
    SELECT
        AVG(TIMESTAMPDIFF(DAY,
            TRY_TO_TIMESTAMP_NTZ(attributes:created_at::STRING),
            TRY_TO_TIMESTAMP_NTZ(attributes:closed_at::STRING)
        )) as avg_resolution_days,
        MEDIAN(TIMESTAMPDIFF(DAY,
            TRY_TO_TIMESTAMP_NTZ(attributes:created_at::STRING),
            TRY_TO_TIMESTAMP_NTZ(attributes:closed_at::STRING)
        )) as median_resolution_days
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action = 'ticket.created'
      AND source = 'HAPPYFOX'
      AND attributes:status::STRING = 'Closed'
      AND attributes:closed_at IS NOT NULL
)
SELECT 
    CURRENT_TIMESTAMP() as snapshot_time,
    tm.total_tickets,
    tm.open_tickets,
    tm.closed_tickets,
    ROUND(tm.closed_tickets * 100.0 / NULLIF(tm.total_tickets, 0), 2) as close_rate_percent,
    tm.high_priority,
    tm.urgent_tickets,
    rt.avg_first_response_hours,
    rt.median_first_response_hours,
    res.avg_resolution_days,
    res.median_resolution_days,
    OBJECT_CONSTRUCT(
        'metrics_summary', OBJECT_CONSTRUCT(
            'total_volume', tm.total_tickets,
            'current_backlog', tm.open_tickets,
            'urgency_level', CASE 
                WHEN tm.urgent_tickets > 10 THEN 'Critical'
                WHEN tm.high_priority > 20 THEN 'High'
                ELSE 'Normal'
            END
        ),
        'performance', OBJECT_CONSTRUCT(
            'response_sla_met', rt.avg_first_response_hours < 24,
            'resolution_sla_met', res.avg_resolution_days < 7
        )
    ) as executive_context
FROM ticket_metrics tm
CROSS JOIN response_times rt
CROSS JOIN resolution_times res;

-- 2. Ticket Aging Analysis View
CREATE OR REPLACE VIEW MCP.VW_SUPPORT_AGING_ANALYSIS AS
WITH latest_tickets AS (
    SELECT 
        object_id as ticket_id,
        attributes:display_id::NUMBER as display_id,
        attributes:subject::STRING as subject,
        attributes:status::STRING as status,
        attributes:priority::STRING as priority,
        attributes:product::STRING as product,
        actor_id as assignee,
        TRY_TO_TIMESTAMP_NTZ(attributes:created_at::STRING) as created_at,
        TRY_TO_TIMESTAMP_NTZ(attributes:updated_at::STRING) as updated_at,
        attributes:tags::ARRAY as tags,
        attributes:client_name::STRING as client_name,
        ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY occurred_at DESC) as rn
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action LIKE 'ticket.%'
      AND source = 'HAPPYFOX'
)
SELECT 
    ticket_id,
    display_id,
    subject,
    status,
    priority,
    product,
    assignee,
    client_name,
    created_at,
    updated_at,
    DATEDIFF(DAY, created_at, CURRENT_TIMESTAMP()) as age_days,
    CASE 
        WHEN DATEDIFF(DAY, created_at, CURRENT_TIMESTAMP()) <= 1 THEN '0-1 days'
        WHEN DATEDIFF(DAY, created_at, CURRENT_TIMESTAMP()) <= 3 THEN '2-3 days'
        WHEN DATEDIFF(DAY, created_at, CURRENT_TIMESTAMP()) <= 7 THEN '4-7 days'
        WHEN DATEDIFF(DAY, created_at, CURRENT_TIMESTAMP()) <= 14 THEN '8-14 days'
        WHEN DATEDIFF(DAY, created_at, CURRENT_TIMESTAMP()) <= 30 THEN '15-30 days'
        WHEN DATEDIFF(DAY, created_at, CURRENT_TIMESTAMP()) <= 60 THEN '31-60 days'
        WHEN DATEDIFF(DAY, created_at, CURRENT_TIMESTAMP()) <= 90 THEN '61-90 days'
        ELSE '90+ days'
    END as age_bucket,
    DATEDIFF(DAY, updated_at, CURRENT_TIMESTAMP()) as days_since_update,
    CASE
        WHEN status IN ('Closed', 'Trash') THEN 'Resolved'
        WHEN DATEDIFF(DAY, updated_at, CURRENT_TIMESTAMP()) > 7 THEN 'Stale'
        WHEN priority IN ('High', 'Urgent') THEN 'Priority'
        ELSE 'Active'
    END as aging_status,
    tags,
    OBJECT_CONSTRUCT(
        'escalation_needed', priority IN ('High', 'Urgent') AND age_days > 3,
        'sla_breach', CASE
            WHEN priority = 'Urgent' AND age_days > 1 THEN TRUE
            WHEN priority = 'High' AND age_days > 3 THEN TRUE
            WHEN age_days > 7 THEN TRUE
            ELSE FALSE
        END,
        'client_impact', client_name IS NOT NULL
    ) as risk_factors
FROM latest_tickets
WHERE rn = 1
  AND status NOT IN ('Closed', 'Trash')
ORDER BY 
    CASE priority
        WHEN 'Urgent' THEN 1
        WHEN 'High' THEN 2
        WHEN 'Medium' THEN 3
        ELSE 4
    END,
    age_days DESC;

-- 3. Agent Workload View
CREATE OR REPLACE VIEW MCP.VW_SUPPORT_AGENT_WORKLOAD AS
WITH agent_tickets AS (
    SELECT 
        actor_id as agent,
        object_id as ticket_id,
        attributes:status::STRING as status,
        attributes:priority::STRING as priority,
        attributes:product::STRING as product,
        TRY_TO_TIMESTAMP_NTZ(attributes:created_at::STRING) as created_at,
        TRY_TO_TIMESTAMP_NTZ(attributes:updated_at::STRING) as updated_at,
        ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY occurred_at DESC) as rn
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action IN ('ticket.assigned', 'ticket.status_changed', 'ticket.created')
      AND source = 'HAPPYFOX'
      AND actor_id IS NOT NULL
),
workload_summary AS (
    SELECT 
        agent,
        COUNT(DISTINCT CASE WHEN status NOT IN ('Closed', 'Trash') THEN ticket_id END) as open_tickets,
        COUNT(DISTINCT CASE WHEN status = 'In Progress' THEN ticket_id END) as in_progress,
        COUNT(DISTINCT CASE WHEN status = 'Waiting' THEN ticket_id END) as waiting,
        COUNT(DISTINCT CASE WHEN status = 'New' THEN ticket_id END) as new_tickets,
        COUNT(DISTINCT ticket_id) as total_assigned,
        COUNT(DISTINCT CASE WHEN priority = 'Urgent' THEN ticket_id END) as urgent_count,
        COUNT(DISTINCT CASE WHEN priority = 'High' THEN ticket_id END) as high_priority_count,
        AVG(DATEDIFF(DAY, created_at, CURRENT_TIMESTAMP())) as avg_ticket_age,
        MAX(DATEDIFF(DAY, created_at, CURRENT_TIMESTAMP())) as oldest_ticket_age,
        ARRAY_AGG(DISTINCT product) as products_handled
    FROM agent_tickets
    WHERE rn = 1
    GROUP BY agent
),
agent_performance AS (
    SELECT
        actor_id as agent,
        COUNT(DISTINCT CASE 
            WHEN action = 'ticket.status_changed' 
            AND attributes:status::STRING = 'Closed' 
            AND occurred_at >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
            THEN object_id 
        END) as tickets_closed_week,
        COUNT(DISTINCT CASE 
            WHEN action = 'ticket.status_changed' 
            AND attributes:status::STRING = 'Closed' 
            AND occurred_at >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
            THEN object_id 
        END) as tickets_closed_month
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action = 'ticket.status_changed'
      AND source = 'HAPPYFOX'
    GROUP BY actor_id
)
SELECT 
    ws.agent,
    ws.open_tickets,
    ws.in_progress,
    ws.waiting,
    ws.new_tickets,
    ws.total_assigned,
    ws.urgent_count,
    ws.high_priority_count,
    ROUND(ws.avg_ticket_age, 1) as avg_ticket_age_days,
    ws.oldest_ticket_age as oldest_ticket_days,
    ws.products_handled,
    COALESCE(ap.tickets_closed_week, 0) as closed_last_7_days,
    COALESCE(ap.tickets_closed_month, 0) as closed_last_30_days,
    CASE
        WHEN ws.open_tickets = 0 THEN 'No Load'
        WHEN ws.open_tickets <= 5 THEN 'Light'
        WHEN ws.open_tickets <= 10 THEN 'Moderate'
        WHEN ws.open_tickets <= 20 THEN 'Heavy'
        ELSE 'Overloaded'
    END as workload_level,
    OBJECT_CONSTRUCT(
        'utilization_score', LEAST(100, ws.open_tickets * 5),
        'priority_pressure', (ws.urgent_count * 3 + ws.high_priority_count * 2) / GREATEST(1, ws.open_tickets) * 100,
        'productivity_index', COALESCE(ap.tickets_closed_week, 0) / GREATEST(1, ws.open_tickets) * 100,
        'needs_help', ws.open_tickets > 15 OR ws.urgent_count > 2
    ) as performance_metrics
FROM workload_summary ws
LEFT JOIN agent_performance ap ON ws.agent = ap.agent
ORDER BY ws.open_tickets DESC, ws.urgent_count DESC;

-- 4. Product Support Metrics (Enhanced)
CREATE OR REPLACE VIEW MCP.VW_PRODUCT_SUPPORT_DETAILED AS
WITH product_tickets AS (
    SELECT 
        COALESCE(attributes:product::STRING, 'Unassigned') as product,
        object_id as ticket_id,
        attributes:status::STRING as status,
        attributes:priority::STRING as priority,
        attributes:client_name::STRING as client_name,
        TRY_TO_TIMESTAMP_NTZ(attributes:created_at::STRING) as created_at,
        TRY_TO_TIMESTAMP_NTZ(attributes:closed_at::STRING) as closed_at,
        actor_id as assignee,
        ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY occurred_at DESC) as rn
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action LIKE 'ticket.%'
      AND source = 'HAPPYFOX'
),
product_summary AS (
    SELECT 
        product,
        COUNT(DISTINCT ticket_id) as total_tickets,
        COUNT(DISTINCT CASE WHEN status NOT IN ('Closed', 'Trash') THEN ticket_id END) as open_tickets,
        COUNT(DISTINCT CASE WHEN status = 'Closed' THEN ticket_id END) as closed_tickets,
        COUNT(DISTINCT CASE WHEN priority = 'Urgent' THEN ticket_id END) as urgent_tickets,
        COUNT(DISTINCT CASE WHEN priority = 'High' THEN ticket_id END) as high_priority,
        COUNT(DISTINCT client_name) as unique_clients,
        COUNT(DISTINCT assignee) as agents_involved,
        AVG(CASE 
            WHEN closed_at IS NOT NULL 
            THEN TIMESTAMPDIFF(HOUR, created_at, closed_at)
        END) as avg_resolution_hours,
        MEDIAN(CASE 
            WHEN closed_at IS NOT NULL 
            THEN TIMESTAMPDIFF(HOUR, created_at, closed_at)
        END) as median_resolution_hours,
        AVG(DATEDIFF(DAY, created_at, CURRENT_TIMESTAMP())) as avg_age_days
    FROM product_tickets
    WHERE rn = 1
    GROUP BY product
),
trend_data AS (
    SELECT 
        attributes:product::STRING as product,
        DATE_TRUNC('WEEK', occurred_at) as week,
        COUNT(DISTINCT object_id) as weekly_tickets
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action = 'ticket.created'
      AND source = 'HAPPYFOX'
      AND occurred_at >= DATEADD(WEEK, -4, CURRENT_TIMESTAMP())
    GROUP BY 1, 2
)
SELECT 
    ps.product,
    ps.total_tickets,
    ps.open_tickets,
    ps.closed_tickets,
    ROUND(ps.closed_tickets * 100.0 / NULLIF(ps.total_tickets, 0), 2) as close_rate,
    ps.urgent_tickets,
    ps.high_priority,
    ps.unique_clients,
    ps.agents_involved,
    ROUND(ps.avg_resolution_hours, 1) as avg_resolution_hours,
    ROUND(ps.median_resolution_hours, 1) as median_resolution_hours,
    ROUND(ps.avg_age_days, 1) as avg_age_days,
    CASE
        WHEN ps.open_tickets = 0 THEN 'Healthy'
        WHEN ps.urgent_tickets > 0 OR ps.avg_age_days > 7 THEN 'Critical'
        WHEN ps.high_priority > 2 OR ps.avg_age_days > 3 THEN 'Warning'
        ELSE 'Normal'
    END as health_status,
    OBJECT_CONSTRUCT(
        'workload_score', ps.open_tickets * 2 + ps.urgent_tickets * 5 + ps.high_priority * 3,
        'complexity_score', ps.unique_clients * ps.agents_involved / GREATEST(1, ps.total_tickets),
        'recent_trend', (
            SELECT ARRAY_AGG(weekly_tickets) WITHIN GROUP (ORDER BY week)
            FROM trend_data td
            WHERE td.product = ps.product
        )
    ) as analytics
FROM product_summary ps
ORDER BY ps.open_tickets DESC, ps.urgent_tickets DESC;

-- 5. SLA Compliance View
CREATE OR REPLACE VIEW MCP.VW_SUPPORT_SLA_COMPLIANCE AS
WITH sla_targets AS (
    SELECT
        'Urgent' as priority,
        4 as response_hours,
        24 as resolution_hours
    UNION ALL
    SELECT 'High', 8, 48
    UNION ALL
    SELECT 'Medium', 24, 120
    UNION ALL
    SELECT 'Low', 48, 240
),
ticket_sla AS (
    SELECT 
        object_id as ticket_id,
        attributes:display_id::NUMBER as display_id,
        attributes:priority::STRING as priority,
        attributes:status::STRING as status,
        attributes:product::STRING as product,
        TRY_TO_TIMESTAMP_NTZ(attributes:created_at::STRING) as created_at,
        TRY_TO_TIMESTAMP_NTZ(attributes:first_response_time::STRING) as first_response_at,
        TRY_TO_TIMESTAMP_NTZ(attributes:closed_at::STRING) as closed_at,
        TIMESTAMPDIFF(HOUR, 
            TRY_TO_TIMESTAMP_NTZ(attributes:created_at::STRING),
            TRY_TO_TIMESTAMP_NTZ(attributes:first_response_time::STRING)
        ) as actual_response_hours,
        TIMESTAMPDIFF(HOUR,
            TRY_TO_TIMESTAMP_NTZ(attributes:created_at::STRING),
            TRY_TO_TIMESTAMP_NTZ(attributes:closed_at::STRING)
        ) as actual_resolution_hours
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action = 'ticket.created'
      AND source = 'HAPPYFOX'
)
SELECT 
    ts.ticket_id,
    ts.display_id,
    ts.priority,
    ts.status,
    ts.product,
    ts.created_at,
    ts.actual_response_hours,
    st.response_hours as target_response_hours,
    CASE 
        WHEN ts.first_response_at IS NULL AND ts.status NOT IN ('Closed', 'Trash') THEN 'Pending'
        WHEN ts.actual_response_hours <= st.response_hours THEN 'Met'
        ELSE 'Breached'
    END as response_sla_status,
    ts.actual_resolution_hours,
    st.resolution_hours as target_resolution_hours,
    CASE
        WHEN ts.status NOT IN ('Closed', 'Trash') THEN 'In Progress'
        WHEN ts.actual_resolution_hours <= st.resolution_hours THEN 'Met'
        ELSE 'Breached'
    END as resolution_sla_status,
    CASE
        WHEN ts.status NOT IN ('Closed', 'Trash') THEN
            TIMESTAMPDIFF(HOUR, ts.created_at, CURRENT_TIMESTAMP())
        ELSE ts.actual_resolution_hours
    END as current_age_hours,
    OBJECT_CONSTRUCT(
        'response_variance', ts.actual_response_hours - st.response_hours,
        'resolution_variance', ts.actual_resolution_hours - st.resolution_hours,
        'at_risk', ts.status NOT IN ('Closed', 'Trash') AND 
                   TIMESTAMPDIFF(HOUR, ts.created_at, CURRENT_TIMESTAMP()) > st.resolution_hours * 0.8
    ) as sla_metrics
FROM ticket_sla ts
LEFT JOIN sla_targets st ON COALESCE(ts.priority, 'Low') = st.priority
ORDER BY 
    CASE 
        WHEN ts.status NOT IN ('Closed', 'Trash') AND 
             TIMESTAMPDIFF(HOUR, ts.created_at, CURRENT_TIMESTAMP()) > st.resolution_hours 
        THEN 1
        ELSE 2
    END,
    ts.created_at DESC;

-- 6. Client Impact Analysis
CREATE OR REPLACE VIEW MCP.VW_CLIENT_SUPPORT_IMPACT AS
WITH client_tickets AS (
    SELECT 
        COALESCE(attributes:client_name::STRING, 'Internal') as client_name,
        object_id as ticket_id,
        attributes:status::STRING as status,
        attributes:priority::STRING as priority,
        attributes:product::STRING as product,
        TRY_TO_TIMESTAMP_NTZ(attributes:created_at::STRING) as created_at,
        TRY_TO_TIMESTAMP_NTZ(attributes:closed_at::STRING) as closed_at,
        attributes:tags::ARRAY as tags,
        ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY occurred_at DESC) as rn
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action LIKE 'ticket.%'
      AND source = 'HAPPYFOX'
)
SELECT 
    client_name,
    COUNT(DISTINCT ticket_id) as total_tickets,
    COUNT(DISTINCT CASE WHEN status NOT IN ('Closed', 'Trash') THEN ticket_id END) as open_tickets,
    COUNT(DISTINCT CASE WHEN priority IN ('Urgent', 'High') THEN ticket_id END) as priority_tickets,
    ARRAY_AGG(DISTINCT product) as products_affected,
    AVG(TIMESTAMPDIFF(DAY, created_at, COALESCE(closed_at, CURRENT_TIMESTAMP()))) as avg_age_days,
    MAX(TIMESTAMPDIFF(DAY, created_at, CURRENT_TIMESTAMP())) as oldest_open_ticket_days,
    COUNT(DISTINCT DATE_TRUNC('WEEK', created_at)) as active_weeks,
    CASE
        WHEN COUNT(DISTINCT CASE WHEN priority = 'Urgent' THEN ticket_id END) > 0 THEN 'Critical'
        WHEN COUNT(DISTINCT CASE WHEN status NOT IN ('Closed', 'Trash') THEN ticket_id END) > 5 THEN 'High'
        WHEN COUNT(DISTINCT CASE WHEN status NOT IN ('Closed', 'Trash') THEN ticket_id END) > 2 THEN 'Medium'
        ELSE 'Low'
    END as impact_level,
    OBJECT_CONSTRUCT(
        'urgency_score', 
            COUNT(DISTINCT CASE WHEN priority = 'Urgent' THEN ticket_id END) * 10 +
            COUNT(DISTINCT CASE WHEN priority = 'High' THEN ticket_id END) * 5,
        'volume_trend', COUNT(DISTINCT CASE 
            WHEN created_at >= DATEADD(DAY, -7, CURRENT_TIMESTAMP()) 
            THEN ticket_id 
        END),
        'requires_escalation', 
            COUNT(DISTINCT CASE WHEN priority = 'Urgent' THEN ticket_id END) > 0 OR
            MAX(TIMESTAMPDIFF(DAY, created_at, CURRENT_TIMESTAMP())) > 14
    ) as client_metrics
FROM client_tickets
WHERE rn = 1
GROUP BY client_name
HAVING COUNT(DISTINCT ticket_id) > 0
ORDER BY 
    CASE impact_level
        WHEN 'Critical' THEN 1
        WHEN 'High' THEN 2
        WHEN 'Medium' THEN 3
        ELSE 4
    END,
    open_tickets DESC;

-- Grant permissions
GRANT SELECT ON ALL VIEWS IN SCHEMA MCP TO ROLE PUBLIC;

-- Verification
SELECT 'Analytics views created successfully' as status;