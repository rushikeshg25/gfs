#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=scripts/kind-common.sh
source "$(dirname "$0")/kind-common.sh"
cd "$GFS_REPO"
[[ -f .kind/owned-cluster ]] || { echo 'Run scripts/kind-up.sh first.' >&2; exit 1; }
command -v jq >/dev/null
stopped_worker=''
restore_worker() { if [[ -n "$stopped_worker" ]]; then docker start "$stopped_worker" >/dev/null || true; fi; }
trap restore_worker EXIT
trap 'exit 130' INT TERM
wait_status() {
  local condition="$1" output="$2"
  for _ in $(seq 1 90); do
    if client status > "$output" 2>/dev/null && jq -e "$condition" "$output" >/dev/null; then return 0; fi
    sleep 2
  done
  echo "Timed out waiting for status: $condition" >&2
  cat "$output" >&2
  return 1
}
wait_status '.liveNodes == 4 and .unavailable == 0' .kind/smoke-before.json
remote="/smoke/$(date +%s)-$$/file.bin"
printf 'Creating and uploading a multichunk fixture: %s\n' "$remote"
k -n gfs exec deployment/client -- sh -c 'dd if=/dev/urandom of=/work/input.bin bs=1048576 count=3 2>/dev/null; printf "GFS chunk-boundary verification\n" >> /work/input.bin'
source_hash="$(k -n gfs exec deployment/client -- sha256sum /work/input.bin | awk '{print $1}')"
client put /work/input.bin "$remote" > .kind/smoke-upload.json
jq -e '.chunks | length > 1' .kind/smoke-upload.json >/dev/null
client get "$remote" /work/before.bin > /dev/null
k -n gfs exec deployment/client -- cmp /work/input.bin /work/before.bin
worker="$(k -n gfs get pod chunk-0 -o jsonpath='{.spec.nodeName}')"
case "$worker" in gfs-lab-worker|gfs-lab-worker2|gfs-lab-worker3|gfs-lab-worker4) ;; *) echo 'Unexpected worker; refusing failure injection.' >&2; exit 1;; esac
[[ "$(docker inspect --format '{{index .Config.Labels "io.x-k8s.kind.cluster"}}' "$worker")" == "$GFS_CLUSTER" ]] || exit 1
[[ "$(docker inspect --format '{{.State.Running}}' "$worker")" == true ]] || { echo 'Worker was already stopped; refusing to change it.' >&2; exit 1; }
printf 'Stopping only the GFS storage worker %s.\n' "$worker"
stopped_worker="$worker"
docker stop --time 10 "$worker" >/dev/null
client get "$remote" /work/during-failure.bin > /dev/null
k -n gfs exec deployment/client -- cmp /work/input.bin /work/during-failure.bin
wait_status '.liveNodes == 3 and .underReplicated == 0 and .unavailable == 0' .kind/smoke-repaired.json
printf 'Download matches; all published chunks have at least three live replicas while the worker is stopped.\n'
docker start "$worker" >/dev/null
stopped_worker=''
wait_status '.liveNodes == 4 and .unavailable == 0' .kind/smoke-returned.json
printf 'Restarting the GFS master pod and verifying its persisted namespace.\n'
k -n gfs delete pod master-0 --wait=true --timeout=90s
wait_status '.liveNodes == 4 and .unavailable == 0 and .underReplicated == 0' .kind/smoke-final.json
client get "$remote" /work/after-restart.bin > /dev/null
k -n gfs exec deployment/client -- cmp /work/input.bin /work/after-restart.bin
k -n gfs exec deployment/client -- sh -c ': > /work/empty.bin'
client put /work/empty.bin "$remote.empty" > /dev/null
client stat "$remote.empty" | jq -e '.size == 0 and (.chunks | length == 0)' >/dev/null
client rm "$remote.empty" > /dev/null
if client stat "$remote.empty" >/dev/null 2>&1; then echo 'Deleted file is still visible.' >&2; exit 1; fi
if k -n gfs exec deployment/client -- env GFS_TOKEN=invalid gfs status >/dev/null 2>&1; then echo 'Invalid credentials were accepted.' >&2; exit 1; fi
jq -n --arg date "$(date -u +%FT%TZ)" --arg path "$remote" --arg sha256 "$source_hash" --arg worker "$worker" --slurpfile repaired .kind/smoke-repaired.json --slurpfile final .kind/smoke-final.json '{passed:true,date:$date,path:$path,sha256:$sha256,stoppedWorker:$worker,checks:["multichunk round-trip","read with worker stopped","repair to three live replicas","worker rejoin","master restart persistence","empty file","conditional delete","invalid credentials rejected"],duringFailure:$repaired[0],final:$final[0]}' > .kind/smoke-result.json
printf 'PASS: real kind recovery smoke test. SHA-256: %s\nEvidence: .kind/smoke-result.json\nThe fixture remains at %s.\n' "$source_hash" "$remote"
