#!/bin/bash
#
# QueryMesh Content Reader Wrapper
# Handles port-forwarding to MinIO automatically
#
# Usage:
#   ./read.sh list-raw              # List raw content
#   ./read.sh list-parsed           # List parsed content  
#   ./read.sh raw <key>             # Read raw by key
#   ./read.sh parsed <key>          # Read parsed by key
#   ./read.sh url "https://..."     # Search by URL
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="${NAMESPACE:-querymesh}"
LOCAL_PORT="${LOCAL_PORT:-9000}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if kubectl is available
if ! command -v kubectl &> /dev/null; then
    echo -e "${RED}Error: kubectl not found${NC}"
    exit 1
fi

# Check if python3 is available
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: python3 not found${NC}"
    exit 1
fi

# Check if minio package is installed
if ! python3 -c "import minio" 2>/dev/null; then
    echo -e "${YELLOW}Installing minio package...${NC}"
    pip3 install minio
fi

# Check if port is already forwarded
if ! nc -z localhost $LOCAL_PORT 2>/dev/null; then
    echo -e "${GREEN}Starting port-forward to MinIO...${NC}"
    kubectl port-forward -n $NAMESPACE svc/s3 $LOCAL_PORT:9000 &
    PF_PID=$!
    
    # Wait for port-forward to be ready
    sleep 2
    
    # Cleanup on exit
    trap "kill $PF_PID 2>/dev/null" EXIT
fi

# Set environment variables
export S3_ENDPOINT="localhost:$LOCAL_PORT"
export S3_ACCESS_KEY="${S3_ACCESS_KEY:-minioadmin}"
export S3_SECRET_KEY="${S3_SECRET_KEY:-minioadmin}"

# Run the Python script
python3 "$SCRIPT_DIR/read_content.py" "$@"
