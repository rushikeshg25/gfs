#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=scripts/kind-common.sh
source "$(dirname "$0")/kind-common.sh"
client "$@"
