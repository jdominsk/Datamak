#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXPORT_LINE="export DTWIN_ROOT=\"${ROOT_DIR}\""

echo "${EXPORT_LINE}"
echo ""
echo "Run this to apply in your current shell:"
echo "  ${EXPORT_LINE}"
echo ""
echo "To persist in bash/zsh:"
echo "  echo '${EXPORT_LINE}' >> ~/.zshrc   # or ~/.bashrc"
