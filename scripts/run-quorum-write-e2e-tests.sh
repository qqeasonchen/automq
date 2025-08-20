#!/bin/bash

# S3 Quorum Storage Write E2E Test Runner
# This script runs comprehensive end-to-end tests for S3 Quorum Storage write operations

set -e

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Test configuration
TEST_MODE="${1:-mock}"  # mock, real, or all
GRADLE_ARGS="${2:---info}"

log_info "S3 Quorum Storage Write E2E Test Runner"
log_info "Project root: $PROJECT_ROOT"
log_info "Test mode: $TEST_MODE"

# Function to run mock tests
run_mock_tests() {
    log_info "Running Mock S3 Quorum Write E2E Tests..."
    
    cd "$PROJECT_ROOT"
    
    # Run the mock E2E tests
    if ./gradlew :s3stream:test --tests "*S3QuorumWriteE2ETest*" $GRADLE_ARGS; then
        log_success "Mock S3 Quorum Write E2E tests passed"
        return 0
    else
        log_error "Mock S3 Quorum Write E2E tests failed"
        return 1
    fi
}

# Function to run real S3 tests
run_real_s3_tests() {
    log_info "Running Real S3 Quorum Write E2E Tests..."
    
    # Check if required environment variables are set
    if [[ -z "$AWS_ACCESS_KEY_ID" || -z "$AWS_SECRET_ACCESS_KEY" ]]; then
        log_warning "AWS credentials not found. Setting test environment variables..."
        export AWS_ACCESS_KEY_ID="test-access-key"
        export AWS_SECRET_ACCESS_KEY="test-secret-key"
    fi
    
    # Set S3 endpoint for local testing (MinIO)
    export S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:9000}"
    export REAL_S3_TEST="true"
    
    log_info "Using S3 endpoint: $S3_ENDPOINT"
    
    cd "$PROJECT_ROOT"
    
    # Run the real S3 E2E tests
    if ./gradlew :s3stream:test --tests "*S3QuorumRealWriteE2ETest*" $GRADLE_ARGS; then
        log_success "Real S3 Quorum Write E2E tests passed"
        return 0
    else
        log_error "Real S3 Quorum Write E2E tests failed"
        return 1
    fi
}

# Function to check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check if Java is available
    if ! command -v java &> /dev/null; then
        log_error "Java is not installed or not in PATH"
        return 1
    fi
    
    # Check Java version
    JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2)
    log_info "Java version: $JAVA_VERSION"
    
    # Check if Gradle wrapper exists
    if [[ ! -f "$PROJECT_ROOT/gradlew" ]]; then
        log_error "Gradle wrapper not found in $PROJECT_ROOT"
        return 1
    fi
    
    log_success "Prerequisites check passed"
    return 0
}

# Function to run configuration tests
run_configuration_tests() {
    log_info "Running Configuration Loading Tests..."
    
    cd "$PROJECT_ROOT"
    
    # Run configuration loading tests first
    if ./gradlew :s3stream:test --tests "*QuorumConfigLoaderTest*" --tests "*S3QuorumStorageFactoryTest*" $GRADLE_ARGS; then
        log_success "Configuration tests passed"
        return 0
    else
        log_error "Configuration tests failed"
        return 1
    fi
}

# Function to create test report summary
create_test_summary() {
    log_info "Creating test summary..."
    
    REPORT_DIR="$PROJECT_ROOT/build/test-reports"
    SUMMARY_FILE="$REPORT_DIR/quorum-e2e-summary.md"
    
    mkdir -p "$REPORT_DIR"
    
    cat > "$SUMMARY_FILE" << EOF
# S3 Quorum Storage Write E2E Test Summary

**Generated:** $(date)
**Test Mode:** $TEST_MODE
**Project:** AutoMQ S3 Quorum Storage

## Test Categories

### 1. Configuration Loading Tests
- QuorumConfigLoaderTest: Configuration file parsing and validation
- S3QuorumStorageFactoryTest: Factory integration with configuration

### 2. Mock E2E Tests (S3QuorumWriteE2ETest)
- Configuration loading to quorum write flow
- Write operation with replica failure simulation
- Concurrent write operations

### 3. Real S3 E2E Tests (S3QuorumRealWriteE2ETest)
- Real S3 quorum write with stream data
- Large payload write testing
- Quorum consistency verification

## Environment
- AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID:-"Not Set"}
- S3_ENDPOINT: ${S3_ENDPOINT:-"Not Set"}
- REAL_S3_TEST: ${REAL_S3_TEST:-"false"}

## Test Results
EOF

    log_success "Test summary created: $SUMMARY_FILE"
}

# Main execution function
main() {
    log_info "Starting S3 Quorum Write E2E Test Suite"
    
    # Check prerequisites
    if ! check_prerequisites; then
        exit 1
    fi
    
    # Create test summary
    create_test_summary
    
    # Initialize counters
    PASSED_TESTS=0
    FAILED_TESTS=0
    
    # Run configuration tests first
    log_info "=== Running Configuration Tests ==="
    if run_configuration_tests; then
        ((PASSED_TESTS++))
    else
        ((FAILED_TESTS++))
    fi
    
    # Run tests based on mode
    case "$TEST_MODE" in
        "mock")
            log_info "=== Running Mock E2E Tests ==="
            if run_mock_tests; then
                ((PASSED_TESTS++))
            else
                ((FAILED_TESTS++))
            fi
            ;;
        "real")
            log_info "=== Running Real S3 E2E Tests ==="
            if run_real_s3_tests; then
                ((PASSED_TESTS++))
            else
                ((FAILED_TESTS++))
            fi
            ;;
        "all")
            log_info "=== Running Mock E2E Tests ==="
            if run_mock_tests; then
                ((PASSED_TESTS++))
            else
                ((FAILED_TESTS++))
            fi
            
            log_info "=== Running Real S3 E2E Tests ==="
            if run_real_s3_tests; then
                ((PASSED_TESTS++))
            else
                ((FAILED_TESTS++))
            fi
            ;;
        *)
            log_error "Invalid test mode: $TEST_MODE. Use 'mock', 'real', or 'all'"
            exit 1
            ;;
    esac
    
    # Summary
    echo
    log_info "=== Test Execution Summary ==="
    log_info "Passed test suites: $PASSED_TESTS"
    log_info "Failed test suites: $FAILED_TESTS"
    
    if [[ $FAILED_TESTS -eq 0 ]]; then
        log_success "All S3 Quorum Write E2E tests passed!"
        exit 0
    else
        log_error "Some S3 Quorum Write E2E tests failed!"
        exit 1
    fi
}

# Show usage
show_usage() {
    echo "Usage: $0 [test_mode] [gradle_args]"
    echo
    echo "Test modes:"
    echo "  mock  - Run mock S3 tests (default)"
    echo "  real  - Run real S3 tests (requires S3 setup)"
    echo "  all   - Run both mock and real tests"
    echo
    echo "Examples:"
    echo "  $0                    # Run mock tests"
    echo "  $0 real               # Run real S3 tests"
    echo "  $0 all --debug        # Run all tests with debug output"
    echo
    echo "Environment variables for real S3 tests:"
    echo "  AWS_ACCESS_KEY_ID     - AWS access key"
    echo "  AWS_SECRET_ACCESS_KEY - AWS secret key"
    echo "  S3_ENDPOINT          - S3 endpoint (default: http://localhost:9000)"
}

# Handle command line arguments
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    show_usage
    exit 0
fi

# Run main function
main "$@"