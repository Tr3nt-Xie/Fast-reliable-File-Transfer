#!/bin/bash
# Export the git history for submission. The assignment requires the GitHub
# logs to be included in the PDF, not merely to exist in the repository.
#   ./docs/export-log.sh            full log
#   ./docs/export-log.sh --by-author  per-member summary (for group grading)
cd "$(dirname "$0")/.."

if [ "$1" = "--by-author" ]; then
  echo "# Contributions by author"
  echo
  git shortlog -sn --all | while read n a; do
    echo "## $a  ($n commits)"
    echo
    git log --all --author="$a" --date=short \
        --pretty=format:"- %ad  %h  %s" | sed 's/^/  /'
    echo; echo
  done
  exit 0
fi

echo "# Git log — $(git config --get remote.origin.url)"
echo
echo "Generated $(date -u '+%Y-%m-%d %H:%M UTC') · $(git rev-list --count HEAD) commits"
echo
git log --date=iso --pretty=format:'
## %h — %s
**Author:** %an <%ae>  ·  **Date:** %ad

%b' --stat
