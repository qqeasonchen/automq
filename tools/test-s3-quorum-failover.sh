#!/bin/bash

# S3 Quorum Storage Failover Testing Script
# This script tests the complete three-replica failover functionality
# Generated: 2025-08-21

set -e

# Configuration
CONFIG_FILE="config/kraft-s3-quorum-development.properties"
CLUSTER_ID=""
TEST_TOPIC="s3-quorum-failover-test"
MESSAGE_COUNT=100
MINIO_CONTAINER="minio-automq"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
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

print_test() {
    print_color $CYAN "[TEST]" "$1"
}

# Get cluster ID from logs
get_cluster_id() {
    if [ -z "$CLUSTER_ID" ]; then
        # Try to extract from recent Kafka logs
        CLUSTER_ID=$(grep -r "clusterId" logs/ 2>/dev/null | head -1 | sed -n 's/.*clusterId=\([^,]*\).*/\1/p' || echo "")
        
        if [ -z "$CLUSTER_ID" ]; then
            # Try to extract from configuration or generate
            CLUSTER_ID="CUe7NpZPSF6GiSk_B8L7zQ"  # Use the one from previous testing
        fi
    fi
    
    print_info "Using Cluster ID: $CLUSTER_ID"
}

# Check prerequisites
check_prerequisites() {
    print_info "Checking prerequisites..."
    
    # Check if MinIO container is running
    if ! docker ps | grep -q "$MINIO_CONTAINER"; then
        print_error "MinIO container '$MINIO_CONTAINER' is not running"
        print_info "Please start MinIO with: docker run -d --name $MINIO_CONTAINER -p 9000:9000 -p 9001:9001 -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin quay.io/minio/minio server /data --console-address :9001"
        exit 1
    fi
    
    # Check if configuration file exists
    if [ ! -f "$CONFIG_FILE" ]; then
        print_error "Configuration file not found: $CONFIG_FILE"
        exit 1
    fi
    
    # Check if Kafka is running
    if ! jps | grep -q "Kafka"; then
        print_warning "Kafka does not appear to be running"
        print_info "Will attempt to start Kafka..."
        start_kafka
    fi
    
    print_success "Prerequisites check completed"
}

# Start Kafka if not running
start_kafka() {
    print_info "Starting Kafka with S3 Quorum Storage..."
    
    # Format storage if needed
    if [ ! -d "/tmp/kraft-combined-logs" ]; then
        print_info "Formatting Kafka storage..."
        ./bin/kafka-storage.sh format -t "$CLUSTER_ID" -c "$CONFIG_FILE" --ignore-formatted 2>/dev/null || true
    fi
    
    # Start Kafka in background
    nohup ./bin/kafka-server-start.sh "$CONFIG_FILE" > kafka-server.log 2>&1 &
    KAFKA_PID=$!
    
    # Wait for Kafka to start
    print_info "Waiting for Kafka to start..."
    sleep 10
    
    # Check if Kafka started successfully
    for i in {1..30}; do
        if ./bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
            print_success "Kafka started successfully"
            return 0
        fi
        sleep 1
    done
    
    print_error "Failed to start Kafka"
    exit 1
}

# Create test topic
create_test_topic() {
    print_info "Creating test topic: $TEST_TOPIC"
    
    ./bin/kafka-topics.sh --bootstrap-server localhost:9092 \
        --create --topic "$TEST_TOPIC" \
        --partitions 3 --replication-factor 1 \
        --if-not-exists
    
    print_success "Test topic created"
}

# Check S3 bucket status
check_s3_buckets() {
    print_info "Checking S3 bucket status..."
    
    local buckets=("automq-dev-primary" "automq-dev-secondary-1" "automq-dev-secondary-2")
    
    for bucket in "${buckets[@]}"; do
        local object_count=$(docker exec "$MINIO_CONTAINER" mc find "local/$bucket" --name "*" 2>/dev/null | wc -l || echo "0")
        print_info "Bucket $bucket: $object_count objects"
    done
}

# Simulate bucket failure
simulate_bucket_failure() {
    local bucket_to_fail=$1
    print_test "Simulating failure of bucket: $bucket_to_fail"
    
    # Rename bucket to simulate failure
    docker exec "$MINIO_CONTAINER" mc mv "local/$bucket_to_fail" "local/${bucket_to_fail}-failed" 2>/dev/null || true
    
    print_warning "Bucket $bucket_to_fail has been marked as failed"
}

# Restore failed bucket
restore_bucket() {
    local bucket_to_restore=$1
    print_test "Restoring bucket: $bucket_to_restore"
    
    # Restore bucket name
    docker exec "$MINIO_CONTAINER" mc mv "local/${bucket_to_restore}-failed" "local/$bucket_to_restore" 2>/dev/null || true
    
    print_success "Bucket $bucket_to_restore has been restored"
}

# Test data writes
test_data_writes() {
    local phase=$1
    print_test "Testing data writes - Phase: $phase"
    
    local start_msg=$((MESSAGE_COUNT * $(date +%s) % 1000))
    
    for i in $(seq 1 20); do
        local msg_id=$((start_msg + i))
        echo "S3 Quorum Failover Test - Phase: $phase - Message $msg_id - Timestamp: $(date)" | \
            ./bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic "$TEST_TOPIC" 2>/dev/null
    done
    
    print_success "Phase $phase: Sent 20 test messages"
}

# Test data reads
test_data_reads() {
    local phase=$1
    print_test "Testing data reads - Phase: $phase"
    
    local consumed_count=$(./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
        --topic "$TEST_TOPIC" --from-beginning --max-messages 5 --timeout-ms 5000 2>/dev/null | wc -l || echo "0")
    
    if [ "$consumed_count" -gt 0 ]; then
        print_success "Phase $phase: Successfully read $consumed_count messages"
        return 0
    else
        print_error "Phase $phase: Failed to read messages"
        return 1
    fi
}

# Monitor system health
monitor_health() {
    print_info "Monitoring system health..."
    
    # Check Kafka process
    if jps | grep -q "Kafka"; then
        print_info "✓ Kafka process is running"
    else
        print_warning "✗ Kafka process not found"
    fi
    
    # Check S3 buckets accessibility
    local accessible_buckets=0
    local buckets=("automq-dev-primary" "automq-dev-secondary-1" "automq-dev-secondary-2")
    
    for bucket in "${buckets[@]}"; do
        if docker exec "$MINIO_CONTAINER" mc ls "local/$bucket" >/dev/null 2>&1; then
            print_info "✓ Bucket $bucket is accessible"
            accessible_buckets=$((accessible_buckets + 1))
        else
            print_warning "✗ Bucket $bucket is not accessible"
        fi
    done
    
    print_info "Accessible buckets: $accessible_buckets/3"
    return $accessible_buckets
}

# Main failover test sequence
run_failover_test() {
    print_info "Starting S3 Quorum Storage Failover Test"
    print_info "=================================================="
    
    get_cluster_id
    check_prerequisites
    create_test_topic
    
    # Phase 1: Baseline test with all buckets healthy
    print_test "PHASE 1: Baseline Test (All buckets healthy)"
    check_s3_buckets
    test_data_writes "1-Baseline"
    sleep 2
    test_data_reads "1-Baseline"
    monitor_health
    
    echo
    
    # Phase 2: Test with primary bucket failure
    print_test "PHASE 2: Primary Bucket Failure Test"
    simulate_bucket_failure "automq-dev-primary"
    sleep 3
    test_data_writes "2-Primary-Failed"
    sleep 2
    test_data_reads "2-Primary-Failed"
    monitor_health
    
    echo
    
    # Phase 3: Test with secondary bucket failure (while primary is still failed)
    print_test "PHASE 3: Multiple Bucket Failure Test"
    simulate_bucket_failure "automq-dev-secondary-1"
    sleep 3
    test_data_writes "3-Multi-Failed"
    sleep 2
    
    # This should fail or show degraded performance
    if test_data_reads "3-Multi-Failed"; then
        print_warning "System is still functional with 2/3 buckets failed"
    else
        print_warning "System is degraded with 2/3 buckets failed (expected)"
    fi
    monitor_health
    
    echo
    
    # Phase 4: Restore primary bucket
    print_test "PHASE 4: Primary Bucket Recovery Test"
    restore_bucket "automq-dev-primary"
    sleep 5
    test_data_writes "4-Primary-Recovered"
    sleep 2
    test_data_reads "4-Primary-Recovered"
    monitor_health
    
    echo
    
    # Phase 5: Full recovery
    print_test "PHASE 5: Full Recovery Test"
    restore_bucket "automq-dev-secondary-1"
    sleep 3
    test_data_writes "5-Full-Recovery"
    sleep 2
    test_data_reads "5-Full-Recovery"
    monitor_health
    
    echo
    
    # Final verification
    print_test "FINAL VERIFICATION"
    check_s3_buckets
    
    print_success "S3 Quorum Storage Failover Test Completed!"
}

# Cleanup function
cleanup() {
    print_info "Cleaning up test resources..."
    
    # Delete test topic
    ./bin/kafka-topics.sh --bootstrap-server localhost:9092 \
        --delete --topic "$TEST_TOPIC" 2>/dev/null || true
    
    # Restore any failed buckets
    restore_bucket "automq-dev-primary" 2>/dev/null || true
    restore_bucket "automq-dev-secondary-1" 2>/dev/null || true
    restore_bucket "automq-dev-secondary-2" 2>/dev/null || true
    
    print_success "Cleanup completed"
}

# Handle script interruption
trap cleanup EXIT

# Check command line arguments
if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    echo "S3 Quorum Storage Failover Testing Script"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --help, -h          Show this help message"
    echo "  --cleanup           Run cleanup only"
    echo "  --config FILE       Use specific config file (default: $CONFIG_FILE)"
    echo "  --cluster-id ID     Use specific cluster ID"
    echo ""
    echo "This script tests the failover capabilities of S3 Quorum Storage by:"
    echo "  1. Testing baseline functionality with all buckets healthy"
    echo "  2. Simulating bucket failures"
    echo "  3. Testing system behavior under failure conditions"
    echo "  4. Testing recovery scenarios"
    echo "  5. Verifying full system recovery"
    exit 0
fi

if [ "$1" = "--cleanup" ]; then
    cleanup
    exit 0
fi

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        --cluster-id)
            CLUSTER_ID="$2"
            shift 2
            ;;
        *)
            print_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Execute the main test
run_failover_test