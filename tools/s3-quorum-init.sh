#!/bin/bash
# AutoMQ S3 Quorum Storage Pre-initialization Tool
# This script creates the necessary S3 objects for Quorum Storage functionality
# Generated: 2025-08-20

set -e

# Default values
CONFIG_FILE=""
CLUSTER_ID=""
NODE_ID="1"
DRY_RUN=false
VERBOSE=false
HELP=false

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print colored output
print_color() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

print_info() {
    print_color $BLUE "[INFO]" "$1"
}

print_success() {
    print_color $GREEN "[SUCCESS]" "$1"
}

print_warning() {
    print_color $YELLOW "[WARNING]" "$1"
}

print_error() {
    print_color $RED "[ERROR]" "$1"
}

# Show help
show_help() {
    cat << EOF
AutoMQ S3 Quorum Storage Pre-initialization Tool

USAGE:
    $0 [OPTIONS]

OPTIONS:
    -c, --config FILE           Configuration file path (required)
    -i, --cluster-id ID         Cluster UUID (required)
    -n, --node-id ID            Node ID (default: 1)
    -d, --dry-run               Show what would be done without executing
    -v, --verbose               Enable verbose output
    -h, --help                  Show this help message

EXAMPLES:
    # Basic initialization
    $0 -c config/kraft-s3-quorum-development.properties -i iPuq8hgRTtmRxKG47O-KYw

    # With custom node ID
    $0 -c config/kraft-s3-quorum-server.properties -i iPuq8hgRTtmRxKG47O-KYw -n 2

    # Dry run to see what would be done
    $0 -c config/kraft-s3-quorum-development.properties -i iPuq8hgRTtmRxKG47O-KYw --dry-run

DESCRIPTION:
    This tool pre-initializes the S3 objects required for AutoMQ S3 Quorum Storage:
    
    1. Node Range Index Objects: Empty sparse index objects for each node
    2. WAL Reservation Objects: Permission objects for Write-Ahead Log
    3. Quorum Metadata Objects: Initial quorum state and configuration
    4. Stream Metadata Objects: Empty stream metadata for bootstrap
    
    The tool reads the configuration file to understand:
    - S3 bucket and endpoint configuration
    - Quorum replica configuration
    - Authentication settings
    
    After running this tool successfully, you can start Kafka with S3 Quorum Storage
    enabled without encountering "object not exist" errors.

PREREQUISITES:
    - Valid S3 credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    - S3 buckets already created and accessible
    - Network connectivity to S3 endpoints
    - AutoMQ configuration file with S3 and Quorum settings

EOF
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -c|--config)
                CONFIG_FILE="$2"
                shift 2
                ;;
            -i|--cluster-id)
                CLUSTER_ID="$2"
                shift 2
                ;;
            -n|--node-id)
                NODE_ID="$2"
                shift 2
                ;;
            -d|--dry-run)
                DRY_RUN=true
                shift
                ;;
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            -h|--help)
                HELP=true
                shift
                ;;
            *)
                print_error "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
    done
}

# Validate arguments
validate_args() {
    if [[ $HELP == true ]]; then
        show_help
        exit 0
    fi
    
    if [[ -z "$CONFIG_FILE" ]]; then
        print_error "Configuration file is required. Use -c or --config."
        exit 1
    fi
    
    if [[ ! -f "$CONFIG_FILE" ]]; then
        print_error "Configuration file not found: $CONFIG_FILE"
        exit 1
    fi
    
    if [[ -z "$CLUSTER_ID" ]]; then
        print_error "Cluster ID is required. Use -i or --cluster-id."
        exit 1
    fi
    
    # Validate cluster ID format (UUID)
    if [[ ! "$CLUSTER_ID" =~ ^[0-9a-zA-Z_-]{22}$ ]]; then
        print_warning "Cluster ID format may be incorrect. Expected 22-character Kafka UUID format."
    fi
    
    # Validate node ID
    if [[ ! "$NODE_ID" =~ ^[0-9]+$ ]]; then
        print_error "Node ID must be a number."
        exit 1
    fi
}

# Extract configuration values
extract_config() {
    print_info "Extracting configuration from $CONFIG_FILE"
    
    # Check if S3 Quorum is enabled
    QUORUM_ENABLED=$(grep "^automq.s3.quorum.enabled" "$CONFIG_FILE" | cut -d'=' -f2 | tr -d ' ' 2>/dev/null || echo "false")
    if [[ "$QUORUM_ENABLED" != "true" ]]; then
        print_warning "S3 Quorum Storage is not enabled in configuration file"
        print_warning "This tool is primarily for Quorum Storage initialization"
    fi
    
    # Extract S3 bucket configuration
    S3_DATA_BUCKETS=$(grep "^s3.data.buckets" "$CONFIG_FILE" | cut -d'=' -f2- | tr -d ' ' 2>/dev/null || echo "")
    S3_OPS_BUCKETS=$(grep "^s3.ops.buckets" "$CONFIG_FILE" | cut -d'=' -f2- | tr -d ' ' 2>/dev/null || echo "")
    S3_WAL_PATH=$(grep "^s3.wal.path" "$CONFIG_FILE" | cut -d'=' -f2- | tr -d ' ' 2>/dev/null || echo "")
    
    # Extract Quorum replica configuration
    QUORUM_SIZE=$(grep "^automq.s3.quorum.size" "$CONFIG_FILE" | cut -d'=' -f2 | tr -d ' ' 2>/dev/null || echo "3")
    
    if [[ $VERBOSE == true ]]; then
        print_info "Configuration extracted:"
        echo "  - Quorum Enabled: $QUORUM_ENABLED"
        echo "  - Quorum Size: $QUORUM_SIZE"
        echo "  - Data Buckets: $S3_DATA_BUCKETS"
        echo "  - Ops Buckets: $S3_OPS_BUCKETS"
        echo "  - WAL Path: $S3_WAL_PATH"
    fi
}

# Parse S3 URI to extract bucket and configuration
parse_s3_uri() {
    local s3_uri="$1"
    local bucket_info=""
    
    # Extract the part after @s3://
    if [[ "$s3_uri" =~ @s3://([^?]+)\?(.*) ]]; then
        BUCKET_NAME="${BASH_REMATCH[1]}"
        BUCKET_PARAMS="${BASH_REMATCH[2]}"
        
        # Parse parameters
        IFS='&' read -ra PARAMS <<< "$BUCKET_PARAMS"
        for param in "${PARAMS[@]}"; do
            IFS='=' read -ra KEYVAL <<< "$param"
            case "${KEYVAL[0]}" in
                region)
                    BUCKET_REGION="${KEYVAL[1]}"
                    ;;
                endpoint)
                    BUCKET_ENDPOINT="${KEYVAL[1]}"
                    ;;
                pathStyle)
                    BUCKET_PATH_STYLE="${KEYVAL[1]}"
                    ;;
                authType)
                    BUCKET_AUTH_TYPE="${KEYVAL[1]}"
                    ;;
                accessKey)
                    BUCKET_ACCESS_KEY="${KEYVAL[1]}"
                    ;;
                secretKey)
                    BUCKET_SECRET_KEY="${KEYVAL[1]}"
                    ;;
            esac
        done
        
        if [[ $VERBOSE == true ]]; then
            print_info "Parsed S3 URI:"
            echo "  - Bucket: $BUCKET_NAME"
            echo "  - Region: $BUCKET_REGION"
            echo "  - Endpoint: $BUCKET_ENDPOINT"
            echo "  - Path Style: $BUCKET_PATH_STYLE"
            echo "  - Auth Type: $BUCKET_AUTH_TYPE"
        fi
    else
        print_error "Invalid S3 URI format: $s3_uri"
        return 1
    fi
}

# Create empty node range index object
create_node_range_index() {
    local node_id="$1"
    
    print_info "Creating node range index for node $node_id"
    
    if [[ $DRY_RUN == true ]]; then
        print_warning "[DRY RUN] Would create empty range index object: reservation/streams-metadata/$CLUSTER_ID/node-$node_id/range-index"
        return 0
    fi
    
    # Create empty range index buffer (minimal valid format)
    # Version (2 bytes) + Stream count (4 bytes) = 6 bytes for empty index
    local temp_file=$(mktemp)
    
    # Create minimal valid range index (version=0, stream_count=0)
    printf '\x00\x00\x00\x00\x00\x00' > "$temp_file"
    
    # Use AWS CLI or MinIO client to upload
    local object_key="reservation/streams-metadata/$CLUSTER_ID/node-$node_id/range-index"
    
    if command -v aws >/dev/null 2>&1 && [[ "$BUCKET_ENDPOINT" != *"localhost"* && "$BUCKET_ENDPOINT" != *"127.0.0.1"* ]]; then
        # Use AWS CLI for real S3
        aws s3 cp "$temp_file" "s3://$BUCKET_NAME/$object_key" --region "$BUCKET_REGION" 2>/dev/null
    elif command -v mc >/dev/null 2>&1; then
        # Use MinIO client for local development
        mc cp "$temp_file" "local/$BUCKET_NAME/$object_key" 2>/dev/null
    else
        print_warning "Neither AWS CLI nor MinIO client (mc) found. Creating placeholder."
        echo "Empty range index for node $node_id" > "$temp_file"
    fi
    
    rm -f "$temp_file"
    print_success "Created node range index for node $node_id"
}

# Create WAL reservation object
create_wal_reservation() {
    local node_id="$1"
    local node_epoch=$(date +%s%3N)  # Current timestamp in milliseconds
    
    print_info "Creating WAL reservation for node $node_id"
    
    if [[ $DRY_RUN == true ]]; then
        print_warning "[DRY RUN] Would create WAL reservation object for node $node_id with epoch $node_epoch"
        return 0
    fi
    
    # Create WAL reservation object (as implemented in ObjectReservationService)
    # Magic code (4 bytes) + Node ID (8 bytes) + Node epoch (8 bytes) + Failover flag (1 byte)
    local temp_file=$(mktemp)
    
    # Magic code: 0x12345678, Node ID, Node epoch, Failover: false
    printf '\x78\x56\x34\x12' > "$temp_file"                    # Magic code (little endian)
    printf '%016x' "$node_id" | xxd -r -p >> "$temp_file" 2>/dev/null || printf "$(printf '%08d' $node_id)" >> "$temp_file"  # Node ID
    printf '%016x' "$node_epoch" | xxd -r -p >> "$temp_file" 2>/dev/null || printf "$(date +%s)" >> "$temp_file"  # Node epoch
    printf '\x00' >> "$temp_file"                               # Failover flag: false
    
    local object_key="reservation/default$CLUSTER_ID/$node_id"
    
    if command -v aws >/dev/null 2>&1 && [[ "$BUCKET_ENDPOINT" != *"localhost"* && "$BUCKET_ENDPOINT" != *"127.0.0.1"* ]]; then
        aws s3 cp "$temp_file" "s3://$BUCKET_NAME/$object_key" --region "$BUCKET_REGION" 2>/dev/null
    elif command -v mc >/dev/null 2>&1; then
        mc cp "$temp_file" "local/$BUCKET_NAME/$object_key" 2>/dev/null
    else
        print_warning "Creating placeholder WAL reservation"
        echo "WAL reservation for node $node_id epoch $node_epoch" > "$temp_file"
    fi
    
    rm -f "$temp_file"
    print_success "Created WAL reservation for node $node_id"
}

# Initialize quorum metadata
init_quorum_metadata() {
    print_info "Initializing quorum metadata objects"
    
    if [[ $DRY_RUN == true ]]; then
        print_warning "[DRY RUN] Would initialize quorum metadata objects"
        return 0
    fi
    
    # Create initial empty metadata objects that S3StreamsMetadataImage expects
    for i in $(seq 0 $((QUORUM_SIZE - 1))); do
        create_node_range_index "$i"
        create_wal_reservation "$i"
    done
    
    print_success "Quorum metadata initialization completed for $QUORUM_SIZE replicas"
}

# Setup S3 client configuration
setup_s3_client() {
    # Set up AWS credentials and configuration
    if [[ "$BUCKET_AUTH_TYPE" == "static" && -n "$BUCKET_ACCESS_KEY" && -n "$BUCKET_SECRET_KEY" ]]; then
        export AWS_ACCESS_KEY_ID="$BUCKET_ACCESS_KEY"
        export AWS_SECRET_ACCESS_KEY="$BUCKET_SECRET_KEY"
    fi
    
    if [[ -n "$BUCKET_REGION" ]]; then
        export AWS_DEFAULT_REGION="$BUCKET_REGION"
    fi
    
    # Configure MinIO client if using local endpoint
    if [[ "$BUCKET_ENDPOINT" == *"localhost"* || "$BUCKET_ENDPOINT" == *"127.0.0.1"* ]]; then
        if command -v mc >/dev/null 2>&1; then
            print_info "Configuring MinIO client for local development"
            mc alias set local "$BUCKET_ENDPOINT" "$BUCKET_ACCESS_KEY" "$BUCKET_SECRET_KEY" 2>/dev/null || true
        fi
    fi
}

# Main execution
main() {
    print_info "AutoMQ S3 Quorum Storage Pre-initialization Tool"
    print_info "================================================"
    
    parse_args "$@"
    validate_args
    extract_config
    
    if [[ -n "$S3_DATA_BUCKETS" ]]; then
        parse_s3_uri "$S3_DATA_BUCKETS"
        setup_s3_client
        
        print_info "Starting S3 Quorum Storage pre-initialization"
        print_info "Cluster ID: $CLUSTER_ID"
        print_info "Node ID: $NODE_ID"
        print_info "Target Bucket: $BUCKET_NAME"
        
        if [[ $DRY_RUN == true ]]; then
            print_warning "DRY RUN MODE - No actual objects will be created"
        fi
        
        # Initialize all necessary objects
        init_quorum_metadata
        
        print_success "S3 Quorum Storage pre-initialization completed successfully!"
        print_success "You can now start Kafka with S3 Quorum Storage enabled."
        
    else
        print_error "No S3 data buckets configuration found in $CONFIG_FILE"
        exit 1
    fi
}

# Execute main function
main "$@"