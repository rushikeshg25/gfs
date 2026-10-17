#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=scripts/kind-common.sh
source "$(dirname "$0")/kind-common.sh"
cd "$GFS_REPO"
for command in docker kind jq; do command -v "$command" >/dev/null || { echo "Missing command: $command" >&2; exit 1; }; done
mkdir -p .kind/data/master bin
for i in 0 1 2 3; do mkdir -p ".kind/data/chunk-$i"; done
if [[ ! -f .kind/token ]]; then (umask 077; head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' > .kind/token); fi
umask 077
cat > .kind/cluster.yaml <<YAML
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraMounts:
      - hostPath: $GFS_REPO/.kind/data/master
        containerPath: /gfs/data
YAML
for i in 0 1 2 3; do
cat >> .kind/cluster.yaml <<YAML
  - role: worker
    extraMounts:
      - hostPath: $GFS_REPO/.kind/data/chunk-$i
        containerPath: /gfs/data
YAML
done
if kind get clusters | grep -qx "$GFS_CLUSTER"; then
  [[ -f .kind/owned-cluster ]] || { echo 'A cluster named gfs-lab exists without this project ownership marker; refusing to modify it.' >&2; exit 1; }
else
  kind create cluster --name "$GFS_CLUSTER" --image 'kindest/node:v1.34.0@sha256:7416a61b42b1662ca6ca89f02028ac133a309a2a30ba309614e8ec94d976dc5a' --config .kind/cluster.yaml --kubeconfig "$KUBECONFIG" --wait 180s
  printf 'gfs-lab\n' > .kind/owned-cluster
fi
# Use the kubectl shipped in the node image so client/server versions match.
docker cp "$GFS_CLUSTER-control-plane:/usr/bin/kubectl" bin/kubectl
chmod u+x bin/kubectl
chmod 600 "$KUBECONFIG"
k get nodes
image="gfs-lab:$(git rev-parse --short HEAD)-$(date +%s)"
docker build -t "$image" .
kind load docker-image "$image" --name "$GFS_CLUSTER"
k apply -f k8s/storage.yaml
k -n gfs create secret generic gfs-auth --from-file=GFS_TOKEN=.kind/token --dry-run=client -o yaml | k apply -f -
sed "s|gfs-lab:local|$image|g" k8s/workloads.yaml > .kind/workloads.yaml
k apply -f .kind/workloads.yaml
k -n gfs rollout status statefulset/master --timeout=180s
k -n gfs rollout status statefulset/chunk --timeout=240s
k -n gfs rollout status deployment/client --timeout=180s
for _ in $(seq 1 60); do
  if client status > .kind/status.json 2>/dev/null && jq -e '.liveNodes == 4' .kind/status.json >/dev/null; then
    printf 'GFS kind cluster is ready. Use scripts/kind-client.sh status or scripts/kind-smoke.sh.\n'
    exit 0
  fi
  sleep 2
done
echo 'Timed out waiting for four chunkserver heartbeats.' >&2
k -n gfs get pods -o wide
exit 1
