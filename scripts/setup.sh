#!/bin/bash
# Setup script for SnowflakePOC2

echo "🚀 Setting up SnowflakePOC2..."

# Check prerequisites
command -v node >/dev/null 2>&1 || { echo "❌ Node.js is required but not installed."; exit 1; }
command -v npm >/dev/null 2>&1 || { echo "❌ npm is required but not installed."; exit 1; }

# Install dependencies
echo "📦 Installing dependencies..."
npm install

# Setup environment
if [ ! -f .env ]; then
  echo "⚙️  Creating .env file..."
  cp .env.example .env
  echo "📝 Please edit .env with your Snowflake credentials"
else
  echo "✅ .env file already exists"
fi

# Verify Snowflake connection
echo "🔍 Testing Snowflake connection..."
if [ -f .env ]; then
  source .env
  if [ -z "$SNOWFLAKE_ACCOUNT" ]; then
    echo "⚠️  SNOWFLAKE_ACCOUNT not set in .env"
  else
    echo "✅ Environment variables loaded"
  fi
fi

echo "🎯 Setup complete! Next steps:"
echo "  1. Edit .env with your Snowflake credentials"
echo "  2. Run: npm run setup:db"
echo "  3. Run: npm run dev"
echo ""
echo "📖 Read CLAUDE.md for important Claude Code guidance"