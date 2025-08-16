# 🤖 Claude Code Executive Dashboard

## The COO Meets Claude Code

**Status: ✅ DEPLOYED and RUNNING**

The executive dashboard is now **powered by Claude Code** - making the AI agent visible, trustworthy, and educational for COO-level users.

## 🎯 What's New: Claude Code is the Pilot

### 1. **Claude Code Branding Everywhere**
- Page title: "Claude Code Executive Dashboard"
- Header: "🤖 Powered by Claude Code"
- Status chip: Shows Claude's current state (Listening → Thinking → Calling → Rendered)
- All interactions attributed to Claude Code

### 2. **"Talk to Claude Code" Interface**
- Replaced generic "Refine with natural language" → **"Talk to Claude Code"**
- Example chips show how to talk to Claude:
  - "Ask Claude: 'top actions for john@example.com'"
  - "Try: 'compare sources by hour'"
  - "Example: 'metrics for last 30 days'"

### 3. **Claude's Agent Console** (Press 'c' or click button)
Shows Claude's complete decision process:
```
📝 Intent: "show last 48h by hour"
📋 Plan JSON: {proc: "DASH_GET_SERIES", ...}
✅ Guardrail Checks:
  ✓ Role: CLAUDE_CODE_AI_AGENT
  ✓ Database: CLAUDE_BI.MCP only
  ✓ Procedures: Whitelisted only
  ✓ Limits: n≤50, limit≤1000
🔍 Procedure Preview: CALL MCP.DASH_GET_SERIES(...)
💬 Claude's Confirmation: "Claude will call DASH_GET_SERIES..."
```

### 4. **Execution Modes**
- **Auto** (default): Claude executes immediately
- **Approve**: Claude shows plan, waits for user approval
- Toggle in top bar: "Claude Mode: [Auto ✓] | Approve"

### 5. **Claude's Explanations**
- Under charts: "🤖 Claude aggregated count by action for your selected time range"
- Confirmations: "Claude will call `DASH_GET_SERIES` by hour from last 48h"
- Schedule attribution: "Next run: Today 7:00 AM CT (by Claude Code) 🤖"

## 📊 Access the Dashboard

**Streamlit App**: `COO_EXECUTIVE_DASHBOARD`
**URL ID**: `x4h4vl6cmvmsnsfxpyds`

### To Access:
1. Log into Snowflake Console
2. Navigate to Projects → Streamlit  
3. Click "COO_EXECUTIVE_DASHBOARD"
4. You'll see Claude Code in action!

## 🔍 Claude's Event Trail

Every Claude action is logged:
```sql
-- Claude-specific events
agent.intent_received      -- User asked Claude something
agent.plan_compiled        -- Claude created a plan
agent.guardrail_applied    -- Claude applied safety limits
agent.proc_called         -- Claude executed procedure
agent.render_completed    -- Claude displayed results
agent.mode_changed        -- User switched Auto↔Approve
agent.dashboard_created   -- Claude saved a dashboard
```

## 🎯 Key Features for COO

### 1. **Trust Through Transparency**
- See exactly what Claude will do before it happens
- Understand Claude's guardrails (no ad-hoc SQL, proc-only)
- Review Claude's decision process in Agent Console

### 2. **Learn Claude's Mental Model**
- Watch: Intent → Plan → Validate → Execute → Render
- See how natural language becomes structured plans
- Understand why Claude makes certain decisions

### 3. **Safe Exploration**
- Claude only calls 4 whitelisted procedures
- All limits are enforced (max 50 items, 1000 rows)
- Approve mode lets you review before execution

## 🛡️ Security & Compliance

### What Claude CAN Do:
✅ Call DASH_GET_SERIES for time series
✅ Call DASH_GET_TOPN for rankings
✅ Call DASH_GET_EVENTS for live stream
✅ Call DASH_GET_METRICS for KPIs
✅ Apply filters and time ranges
✅ Save dashboards as events

### What Claude CANNOT Do:
❌ Execute arbitrary SQL
❌ Access tables directly
❌ Create new tables
❌ Bypass row limits
❌ Access unauthorized databases

## 📝 Example Interactions

### Ask Claude:
- "Show me activity for john@company.com last week"
- "Top 10 actions in the last 24 hours"
- "Compare sources by hour for today"
- "Metrics for cohort s3://data/vips.jsonl"

### Claude Responds:
- "Claude will call `DASH_GET_TOPN` for top 10 actions from last 24h"
- Shows plan preview in Approve mode
- Executes with query tag: `dash-nl|proc:DASH_GET_TOPN|agent:claude`
- Displays results with explanation

## 🚀 Why This Matters

1. **COO sees Claude Code working** - Not hidden, but visible
2. **Builds trust through transparency** - Every decision explained
3. **Teaches the mental model** - Learn how Claude thinks
4. **Proves compliance** - Audit trail shows proc-only access
5. **Empowers exploration** - Safe sandbox with guardrails

## 📊 Dashboard Server

Running on **port 3001** with Claude attribution:
- All query tags include `|agent:claude`
- Natural language processed by "Claude Code"
- Events logged with `actor_id: CLAUDE_CODE`

## 🎉 Summary

The COO now has a **Claude Code-powered dashboard** that:
- Makes the AI agent visible and trustworthy
- Shows Claude's reasoning at every step
- Provides transparency through the Agent Console
- Teaches through examples and explanations
- Maintains strict security with Two-Table Law

**Claude Code is no longer a hidden helper - it's the visible, trusted pilot!**