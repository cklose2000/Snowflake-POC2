# SnowflakePOC2

Claude Desktop-like UI powered by Claude Code with Activity Schema 2.0 compliance.

## Features

- 🎯 **Looks like Claude Desktop. Runs on Claude Code.**
- 🔍 **Every claim verified by an Audit Agent.**
- 📊 **Strict Activity Schema v2 logging. No drift.**
- 🛡️ **SafeSQL templates only. No raw SQL in v1.**
- 📈 **Full SQL result tables + interactive dashboards**
- 💾 **Pure Snowflake storage. No external dependencies.**

## Quick Start

```bash
git clone <this-repo>
cd SnowflakePOC2
npm run setup
# Edit .env with your Snowflake credentials
npm run dev
```

## Architecture

- **UI Shell**: Tauri + React (Claude Desktop clone)
- **Bridge**: Claude Code CLI wrapper with activity logging
- **Snowflake Agent**: SafeSQL execution only (mandatory for Snowflake access)
- **Audit Agent**: Auto-verification of success claims
- **Activity Schema**: All actions logged to analytics.activity.events

## Victory Audit Score: TBD% (NOT PRODUCTION READY)

All claims must be verified before production deployment.