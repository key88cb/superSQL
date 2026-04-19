#!/bin/bash
# generate-thrift.sh
# Generates Java stubs from supersql.thrift using Docker (no local thrift install needed).
# Run this script from the repo root after modifying supersql.thrift.
#
# Usage:
#   ./generate-thrift.sh
#
# Output: rpc-proto/src/main/java/edu/zju/supersql/rpc/*.java (committed to git)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "==> Generating Thrift Java stubs via Docker..."

docker run --rm \
    -v "$(pwd)/rpc-proto:/work" \
    -w /work \
    thrift \
    thrift \
        --gen "java:beans,private-members" \
        -out src/main/java \
        supersql.thrift

echo "==> Done. Files written to rpc-proto/src/main/java/edu/zju/supersql/rpc/"
echo "==> Commit the generated files so team members can build without thrift on PATH."
