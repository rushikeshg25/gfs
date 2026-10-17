#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=scripts/kind-common.sh
source "$(dirname "$0")/kind-common.sh"
[[ -f "$GFS_REPO/.kind/owned-cluster" ]] || { echo 'No project-owned cluster marker found.' >&2; exit 1; }
kind delete cluster --name "$GFS_CLUSTER"
printf 'Cluster removed. All GFS data and the token remain under %s/.kind/.\n' "$GFS_REPO"
