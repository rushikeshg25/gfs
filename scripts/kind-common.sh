#!/usr/bin/env bash
# Source this file. Every kubectl call is restricted to this project's kubeconfig and context.
GFS_REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export KUBECONFIG="$GFS_REPO/.kind/kubeconfig"
GFS_CLUSTER=gfs-lab
GFS_KUBECTL="${GFS_KUBECTL:-$GFS_REPO/bin/kubectl}"
k() { "$GFS_KUBECTL" --context "kind-$GFS_CLUSTER" "$@"; }
client() { k -n gfs exec deployment/client -- gfs "$@"; }
