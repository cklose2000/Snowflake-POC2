-- ============================================================================
-- 08_test_setup.sql
-- Insert test data and create test users
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;

-- ============================================================================
-- Insert test business events
-- ============================================================================

-- Order events
INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
SELECT 
  OBJECT_CONSTRUCT(
    'event_id', 'ord_' || UUID_STRING(),
    'action', 'order.placed',
    'occurred_at', DATEADD('hour', -SEQ4(), CURRENT_TIMESTAMP()),
    'actor_id', 'customer_' || (MOD(SEQ4(), 100) + 1),
    'source', 'ecommerce',
    'schema_version', '2.1.0',
    'object', OBJECT_CONSTRUCT(
      'type', 'order',
      'id', 'ORD_' || LPAD(SEQ4(), 8, '0')
    ),
    'attributes', OBJECT_CONSTRUCT(
      'amount', UNIFORM(10, 1000, RANDOM()),
      'items', UNIFORM(1, 10, RANDOM()),
      'currency', 'USD',
      'channel', CASE MOD(SEQ4(), 3) 
        WHEN 0 THEN 'web' 
        WHEN 1 THEN 'mobile' 
        ELSE 'api' 
      END
    )
  ),
  'TEST_DATA',
  CURRENT_TIMESTAMP()
FROM TABLE(GENERATOR(ROWCOUNT => 500));

-- User signup events
INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
SELECT 
  OBJECT_CONSTRUCT(
    'event_id', 'usr_' || UUID_STRING(),
    'action', 'user.signup',
    'occurred_at', DATEADD('hour', -SEQ4() * 2, CURRENT_TIMESTAMP()),
    'actor_id', 'user_' || SEQ4(),
    'source', 'auth',
    'schema_version', '2.1.0',
    'object', OBJECT_CONSTRUCT(
      'type', 'user',
      'id', 'USR_' || LPAD(SEQ4(), 8, '0')
    ),
    'attributes', OBJECT_CONSTRUCT(
      'email', 'user' || SEQ4() || '@example.com',
      'signup_channel', CASE MOD(SEQ4(), 3) 
        WHEN 0 THEN 'organic' 
        WHEN 1 THEN 'paid_search' 
        ELSE 'referral' 
      END,
      'country', CASE MOD(SEQ4(), 5)
        WHEN 0 THEN 'US'
        WHEN 1 THEN 'UK'
        WHEN 2 THEN 'CA'
        WHEN 3 THEN 'AU'
        ELSE 'DE'
      END
    )
  ),
  'TEST_DATA',
  CURRENT_TIMESTAMP()
FROM TABLE(GENERATOR(ROWCOUNT => 200));

-- User activation events
INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
SELECT 
  OBJECT_CONSTRUCT(
    'event_id', 'act_' || UUID_STRING(),
    'action', 'user.activated',
    'occurred_at', DATEADD('hour', -SEQ4() * 3, CURRENT_TIMESTAMP()),
    'actor_id', 'user_' || SEQ4(),
    'source', 'auth',
    'schema_version', '2.1.0',
    'object', OBJECT_CONSTRUCT(
      'type', 'user',
      'id', 'USR_' || LPAD(SEQ4(), 8, '0')
    ),
    'attributes', OBJECT_CONSTRUCT(
      'activation_method', CASE MOD(SEQ4(), 2) 
        WHEN 0 THEN 'email_verification' 
        ELSE 'auto_activated' 
      END,
      'time_to_activate_hours', UNIFORM(1, 48, RANDOM())
    )
  ),
  'TEST_DATA',
  CURRENT_TIMESTAMP()
FROM TABLE(GENERATOR(ROWCOUNT => 150));

-- Order shipped events
INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
SELECT 
  OBJECT_CONSTRUCT(
    'event_id', 'shp_' || UUID_STRING(),
    'action', 'order.shipped',
    'occurred_at', DATEADD('hour', -SEQ4() / 2, CURRENT_TIMESTAMP()),
    'actor_id', 'system',
    'source', 'fulfillment',
    'schema_version', '2.1.0',
    'object', OBJECT_CONSTRUCT(
      'type', 'order',
      'id', 'ORD_' || LPAD(SEQ4(), 8, '0')
    ),
    'attributes', OBJECT_CONSTRUCT(
      'carrier', CASE MOD(SEQ4(), 4)
        WHEN 0 THEN 'USPS'
        WHEN 1 THEN 'UPS'
        WHEN 2 THEN 'FedEx'
        ELSE 'DHL'
      END,
      'tracking_number', 'TRK' || LPAD(SEQ4(), 12, '0'),
      'estimated_delivery_days', UNIFORM(2, 7, RANDOM())
    ),
    'depends_on_event_id', 'ord_' || LPAD(SEQ4(), 8, '0')  -- Links to order
  ),
  'TEST_DATA',
  CURRENT_TIMESTAMP()
FROM TABLE(GENERATOR(ROWCOUNT => 300));

-- Payment events
INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
SELECT 
  OBJECT_CONSTRUCT(
    'event_id', 'pay_' || UUID_STRING(),
    'action', 'payment.processed',
    'occurred_at', DATEADD('minute', -SEQ4() * 10, CURRENT_TIMESTAMP()),
    'actor_id', 'customer_' || (MOD(SEQ4(), 100) + 1),
    'source', 'payments',
    'schema_version', '2.1.0',
    'object', OBJECT_CONSTRUCT(
      'type', 'payment',
      'id', 'PAY_' || LPAD(SEQ4(), 8, '0')
    ),
    'attributes', OBJECT_CONSTRUCT(
      'amount', UNIFORM(10, 500, RANDOM()),
      'currency', 'USD',
      'method', CASE MOD(SEQ4(), 4)
        WHEN 0 THEN 'credit_card'
        WHEN 1 THEN 'debit_card'
        WHEN 2 THEN 'paypal'
        ELSE 'apple_pay'
      END,
      'status', 'success'
    )
  ),
  'TEST_DATA',
  CURRENT_TIMESTAMP()
FROM TABLE(GENERATOR(ROWCOUNT => 400));

-- ============================================================================
-- Create test users with different permission levels
-- ============================================================================

-- Marketing user - limited access
CALL CLAUDE_BI.MCP.CREATE_MCP_USER(
  'sarah_marketing',
  'sarah@company.com',
  'Marketing',
  ARRAY_CONSTRUCT('order.placed', 'user.signup', 'user.activated'),
  10000,  -- max_rows
  60      -- 60 seconds daily runtime budget
);

-- Analytics user - broader access
CALL CLAUDE_BI.MCP.CREATE_MCP_USER(
  'john_analyst',
  'john@company.com',
  'Analytics',
  ARRAY_CONSTRUCT('order.placed', 'order.shipped', 'user.signup', 'user.activated', 'payment.processed'),
  50000,  -- max_rows
  300     -- 5 minutes daily runtime budget
);

-- Intern - very limited access
CALL CLAUDE_BI.MCP.CREATE_MCP_USER(
  'intern_viewer',
  'intern@company.com',
  'Marketing',
  ARRAY_CONSTRUCT('order.placed'),
  1000,   -- max_rows
  30      -- 30 seconds daily runtime budget
);

-- Executive - full business event access
CALL CLAUDE_BI.MCP.CREATE_MCP_USER(
  'exec_dashboard',
  'exec@company.com',
  'Executive',
  ARRAY_CONSTRUCT('order.placed', 'order.shipped', 'order.delivered', 'order.cancelled', 
                  'user.signup', 'user.activated', 'user.upgraded', 'user.churned',
                  'payment.processed', 'payment.failed', 'payment.refunded'),
  100000,  -- max_rows
  600      -- 10 minutes daily runtime budget
);

-- ============================================================================
-- Verify setup
-- ============================================================================

-- Check event counts
SELECT 
  action,
  COUNT(*) AS event_count
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE source IN ('ecommerce', 'auth', 'fulfillment', 'payments')
GROUP BY action
ORDER BY event_count DESC;

-- Check user permissions
SELECT * FROM CLAUDE_BI.MCP.CURRENT_USER_PERMISSIONS
WHERE status = 'ACTIVE';

-- Show test instructions
SELECT 'Test Setup Complete!' AS status,
       'Test users created with temporary password: TempPassword123!' AS message,
       'Users must change password on first login' AS note;