#!/usr/bin/env bash
# Activates the scrappy_RA venv and runs the scraper (python -m scrappy_RA).
#
# The venv is a WSL/Linux venv, so this script must run under WSL, e.g.:
#   wsl -e bash -lc "/mnt/c/wd/scrappy_RA/run.sh"
# or, from inside a WSL shell already cd'd into the project:
#   ./run.sh

set -euo pipefail

# Resolve the project directory (this script's location) and its parent,
# since __main__.py is invoked as a package (`python -m scrappy_RA`) and
# uses paths like ./scrappy_RA/data_saved_locally/... relative to the parent.
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARENT_DIR="$(dirname "$PROJECT_DIR")"

VENV_ACTIVATE="$PROJECT_DIR/scrappy_RA_env/bin/activate"
if [[ ! -f "$VENV_ACTIVATE" ]]; then
    echo "Error: venv not found at $VENV_ACTIVATE" >&2
    echo "Create it with: python3 -m venv $PROJECT_DIR/scrappy_RA_env" >&2
    exit 1
fi

# shellcheck source=/dev/null
source "$VENV_ACTIVATE"

cd "$PARENT_DIR"
python -m scrappy_RA "$@"
