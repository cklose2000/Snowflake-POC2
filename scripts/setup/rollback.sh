#!/bin/bash
# Rollback script for repo refactor - instant recovery
set -euo pipefail

echo "🔄 Rolling back repo refactor..."

# Read the manifest
MANIFEST="refactor-manifest.json"
if [[ ! -f "$MANIFEST" ]]; then
    echo "❌ No refactor manifest found. Nothing to rollback."
    exit 1
fi

# Remove any symlinks we created
echo "Removing symlinks..."
if [[ -f "$MANIFEST" ]]; then
    # Remove symlinks from manifest
    jq -r '.symlinks_created[]?' "$MANIFEST" 2>/dev/null | while read -r link; do
        if [[ -L "$link" ]]; then
            echo "  Removing symlink: $link"
            rm "$link"
        fi
    done
fi

# Restore moved files
echo "Restoring moved files..."
if [[ -f "$MANIFEST" ]]; then
    jq -r '.files_moved[]?' "$MANIFEST" 2>/dev/null | while read -r entry; do
        from=$(echo "$entry" | jq -r '.from')
        to=$(echo "$entry" | jq -r '.to')
        if [[ -f "$to" && ! -f "$from" ]]; then
            echo "  Restoring: $to → $from"
            mv "$to" "$from"
        fi
    done
fi

# Restore RSA keys if moved
if [[ -d ~/.snowflake-keys ]]; then
    echo "Restoring RSA keys..."
    for key in ~/.snowflake-keys/claude_code_rsa_key.*; do
        if [[ -f "$key" ]]; then
            basename_key=$(basename "$key")
            if [[ ! -f "./$basename_key" ]]; then
                echo "  Restoring: $key → ./$basename_key"
                cp "$key" "./$basename_key"
            fi
        fi
    done
fi

# Remove created directories if empty
echo "Cleaning up created directories..."
for dir in packages scripts/deploy scripts/checks scripts/sql tests/unit tests/integration apps/streamlit docs; do
    if [[ -d "$dir" && -z "$(ls -A "$dir" 2>/dev/null)" ]]; then
        echo "  Removing empty directory: $dir"
        rmdir "$dir" 2>/dev/null || true
    fi
done

# Restore original .gitignore if we modified it
if [[ -f ".gitignore.backup" ]]; then
    echo "Restoring original .gitignore..."
    mv ".gitignore.backup" ".gitignore"
fi

echo "✅ Rollback complete!"
echo ""
echo "To verify everything is back to normal:"
echo "  git status"
echo "  make test-real"
echo "  ~/bin/sf sql \"SELECT 'connection-test'\""
echo ""
echo "You can delete the rollback files:"
echo "  rm $MANIFEST scripts/setup/rollback.sh"