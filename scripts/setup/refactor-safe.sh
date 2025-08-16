#!/bin/bash
# Safe refactor script - creates clean structure with symlinks (zero breakage)
set -euo pipefail

echo "🚀 Starting safe repo refactor..."
echo ""

# Create directory structure
echo "📁 Creating clean directory structure..."
mkdir -p packages/{snowflake-cli,claude-code-auth,mcp-logger,activation-gateway}
mkdir -p scripts/{deploy,checks,sql,utils}
mkdir -p tests/{unit,integration,scripts}
mkdir -p apps/streamlit
mkdir -p docs
mkdir -p .github/workflows

# Store current state
echo "📝 Recording current state in manifest..."
cat > refactor-progress.json << 'EOF'
{
  "phase": 1,
  "directories_created": true,
  "files_copied": false,
  "symlinks_created": false,
  "keys_moved": false
}
EOF

# 1) COPY (don't move yet) critical files to new locations
echo ""
echo "📋 Copying files to new locations (preserving originals)..."

# Deploy scripts
echo "  Copying deploy scripts..."
for f in deploy-*.js; do
  if [[ -f "$f" ]]; then
    cp "$f" "scripts/deploy/" && echo "    $f → scripts/deploy/"
  fi
done

# Test scripts  
echo "  Copying test scripts..."
for f in test-*.js; do
  if [[ -f "$f" ]]; then
    cp "$f" "tests/scripts/" && echo "    $f → tests/scripts/"
  fi
done

# Check/verify scripts
echo "  Copying check scripts..."
for f in check-*.js verify-*.js; do
  if [[ -f "$f" ]]; then
    cp "$f" "scripts/checks/" && echo "    $f → scripts/checks/"
  fi
done

# SQL files
echo "  Copying SQL files..."
for f in scripts/dashboard-procs*.sql; do
  if [[ -f "$f" ]]; then
    basename_f=$(basename "$f")
    cp "$f" "scripts/sql/$basename_f" && echo "    $f → scripts/sql/$basename_f"
  fi
done

# Update progress
jq '.files_copied = true' refactor-progress.json > tmp.json && mv tmp.json refactor-progress.json

# 2) Rename auth variants for clarity (in new location)
echo ""
echo "🔐 Clarifying auth variants..."
if [[ -f "scripts/deploy/deploy-auth-simple.js" ]]; then
  cp "scripts/deploy/deploy-auth-simple.js" "scripts/deploy/deploy-auth-password.js"
  echo "    deploy-auth-simple.js → deploy-auth-password.js"
fi

if [[ -f "scripts/deploy/deploy-auth-fixed.js" ]]; then
  cp "scripts/deploy/deploy-auth-fixed.js" "scripts/deploy/deploy-auth-keypair.js"
  echo "    deploy-auth-fixed.js → deploy-auth-keypair.js"
fi

# 3) Create symlinks from old locations → new locations (zero breakage)
echo ""
echo "🔗 Creating symlinks to preserve existing paths..."

# Deploy script symlinks
for f in deploy-*.js; do
  if [[ -f "$f" && -f "scripts/deploy/$f" ]]; then
    rm "$f" && ln -s "scripts/deploy/$f" "$f"
    echo "    $f → scripts/deploy/$f"
    # Record in manifest
    jq --arg link "$f" --arg target "scripts/deploy/$f" '.symlinks_created += [{link: $link, target: $target}]' refactor-manifest.json > tmp.json && mv tmp.json refactor-manifest.json
  fi
done

# Test script symlinks
for f in test-*.js; do
  if [[ -f "$f" && -f "tests/scripts/$f" ]]; then
    rm "$f" && ln -s "tests/scripts/$f" "$f"
    echo "    $f → tests/scripts/$f"
    jq --arg link "$f" --arg target "tests/scripts/$f" '.symlinks_created += [{link: $link, target: $target}]' refactor-manifest.json > tmp.json && mv tmp.json refactor-manifest.json
  fi
done

# Check script symlinks
for f in check-*.js verify-*.js; do
  if [[ -f "$f" && -f "scripts/checks/$f" ]]; then
    rm "$f" && ln -s "scripts/checks/$f" "$f"
    echo "    $f → scripts/checks/$f"
    jq --arg link "$f" --arg target "scripts/checks/$f" '.symlinks_created += [{link: $link, target: $target}]' refactor-manifest.json > tmp.json && mv tmp.json refactor-manifest.json
  fi
done

jq '.symlinks_created = true' refactor-progress.json > tmp.json && mv tmp.json refactor-progress.json

# 4) Secure RSA keys (move to secure location with temp symlinks)
echo ""
echo "🔐 Securing RSA keys..."
mkdir -p ~/.snowflake-keys

for key in claude_code_rsa_key.p8 claude_code_rsa_key.pem claude_code_rsa_key.pub; do
  if [[ -f "$key" ]]; then
    echo "    Moving $key to ~/.snowflake-keys/"
    mv "$key" ~/.snowflake-keys/
    echo "    Creating temporary symlink for backward compatibility"
    ln -s ~/.snowflake-keys/"$key" "$key"
    jq --arg key "$key" --arg target "~/.snowflake-keys/$key" '.files_moved += [{from: $key, to: $target}]' refactor-manifest.json > tmp.json && mv tmp.json refactor-manifest.json
  fi
done

# 5) Update .gitignore for security
echo ""
echo "🛡️  Updating .gitignore for security..."
cp .gitignore .gitignore.backup

if ! grep -q "^\*.p8" .gitignore; then
  echo "*.p8" >> .gitignore
  echo "    Added *.p8"
fi

if ! grep -q "^\*.pem" .gitignore; then
  echo "*.pem" >> .gitignore
  echo "    Added *.pem"
fi

if ! grep -q "^\.env$" .gitignore; then
  echo ".env" >> .gitignore
  echo "    Added .env"
fi

# Remove .env from tracking if present
if [[ -f ".env" ]] && git ls-files --error-unmatch .env >/dev/null 2>&1; then
  echo "    WARNING: .env is tracked in git! Adding to .gitignore but not removing from git."
  echo "    Run manually: git rm --cached .env"
fi

jq '.keys_moved = true' refactor-progress.json > tmp.json && mv tmp.json refactor-progress.json

echo ""
echo "✅ Safe refactor structure created!"
echo ""
echo "🧪 Quick verification tests:"
echo "  1. Test SF connection:    SF_PK_PATH=./claude_code_rsa_key.p8 ~/bin/sf sql \"SELECT 'migration-test'\""
echo "  2. Test npm scripts:      npm run test"
echo "  3. Test symlinked files:  ls -la deploy-*.js test-*.js check-*.js"
echo ""
echo "📁 New structure preview:"
echo "  scripts/deploy/    - All deployment scripts"
echo "  scripts/checks/    - Security and validation scripts"
echo "  scripts/sql/       - SQL procedure files"
echo "  tests/scripts/     - All test scripts" 
echo "  packages/          - Sub-project workspaces"
echo ""
echo "🔄 If anything breaks, run: ./scripts/setup/rollback.sh"
echo ""
echo "Next steps:"
echo "  1. Test one workflow completely before proceeding"
echo "  2. Create the unified CLI dispatcher"
echo "  3. Create repo guards for security enforcement"