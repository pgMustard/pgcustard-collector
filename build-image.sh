#!/usr/bin/env bash
set -euo pipefail

builder=$(docker buildx create --driver-opt image=moby/buildkit:v0.35.1@sha256:baf6138eb8c37644800c7c913fba051e840ebb9ba13eaf92d92ef9a1712d0b4c)
metadata=$(mktemp)
trap 'docker buildx rm "$builder" > /dev/null; rm "$metadata"' EXIT

docker buildx build "$(dirname "$0")" \
  --builder "$builder" \
  --platform linux/amd64,linux/arm64 \
  --provenance=false \
  --build-arg SOURCE_DATE_EPOCH=946684800 \
  --output type=image,oci-mediatypes=true,rewrite-timestamp=true \
  --metadata-file "$metadata" \
  "$@"

grep -o '"containerimage.digest": "[^"]*"' "$metadata" | grep -o 'sha256:[0-9a-f]*'
