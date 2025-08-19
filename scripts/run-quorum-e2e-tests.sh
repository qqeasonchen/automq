#!/bin/bash

#
# Copyright 2025, AutoMQ HK Limited.
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# S3 Quorum Storage End-to-End Test Runner
# This script runs all E2E tests for the S3 Quorum Storage implementation

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
JAVA_OPTS="-Xmx4g -Xms2g"
GRADLE_OPTS="--no-daemon --parallel"
TEST_RESULTS_DIR="$PROJECT_ROOT/build/test-results/quorum-e2e"
REPORTS_DIR="$PROJECT_ROOT/build/reports/quorum-e2e"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}S3 Quorum Storage E2E Test Runner${NC}"
echo -e "${BLUE}========================================${NC}"

# Function to print status
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Check prerequisites
print_status "Checking prerequisites..."

# Check Java version
if ! java -version 2>&1 | grep -q "version.*1\.[89]\|version.*[1-9][0-9]"; then
    print_error "Java 8+ is required"
    exit 1
fi

# Check if we're in the right directory
if [ ! -f "$PROJECT_ROOT/build.gradle" ]; then
    print_error "Must be run from AutoMQ project root directory"
    exit 1
fi

# Create output directories
mkdir -p "$TEST_RESULTS_DIR"
mkdir -p "$REPORTS_DIR"

# Set environment variables
export JAVA_OPTS="$JAVA_OPTS"

print_status "Starting S3 Quorum Storage E2E tests..."
print_status "Test results will be saved to: $TEST_RESULTS_DIR"
print_status "Test reports will be saved to: $REPORTS_DIR"

# Function to run a specific test class
run_test_class() {
    local test_class="$1"
    local test_name="$2"
    
    print_status "Running $test_name..."
    
    if ./gradlew :s3stream:test --tests "$test_class" $GRADLE_OPTS \
        --info \
        -Dtest.single="$test_class" \
        -Djunit.jupiter.execution.timeout.default=300s \
        -Djunit.jupiter.execution.timeout.testable.method.default=300s; then
        print_success "$test_name completed successfully"
        return 0
    else
        print_error "$test_name failed"
        return 1
    fi
}

# Function to run all tests in the suite
run_full_suite() {
    print_status "Running full S3QuorumStorage E2E test suite..."
    
    if ./gradlew :s3stream:test --tests "com.automq.stream.s3.quorum.S3QuorumStorageE2ETestSuite" $GRADLE_OPTS \
        --info \
        -Djunit.jupiter.execution.timeout.default=600s \
        -Djunit.jupiter.execution.timeout.testable.method.default=600s; then
        print_success "Full test suite completed successfully"
        return 0
    else
        print_error "Test suite failed"
        return 1
    fi
}

# Parse command line arguments
TEST_MODE="full"
SPECIFIC_TEST=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --basic)
            TEST_MODE="basic"
            SPECIFIC_TEST="com.automq.stream.s3.quorum.S3QuorumStorageE2ETest"
            shift
            ;;
        --integration)
            TEST_MODE="integration"
            SPECIFIC_TEST="com.automq.stream.s3.quorum.QuorumWriteIntegrationTest"
            shift
            ;;
        --failure-recovery)
            TEST_MODE="failure"
            SPECIFIC_TEST="com.automq.stream.s3.quorum.S3QuorumStorageFailureRecoveryE2ETest"
            shift
            ;;
        --performance)
            TEST_MODE="performance"
            SPECIFIC_TEST="com.automq.stream.s3.quorum.S3QuorumStoragePerformanceE2ETest"
            shift
            ;;
        --configuration)
            TEST_MODE="config"
            SPECIFIC_TEST="com.automq.stream.s3.quorum.S3QuorumStorageConfigurationE2ETest"
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --basic              Run basic functionality tests only"
            echo "  --integration        Run integration tests only"
            echo "  --failure-recovery   Run failure recovery tests only"
            echo "  --performance        Run performance tests only"
            echo "  --configuration      Run configuration validation tests only"
            echo "  --help, -h          Show this help message"
            echo ""
            echo "Default: Run full test suite"
            exit 0
            ;;
        *)
            print_warning "Unknown option: $1"
            shift
            ;;
    esac
done

# Change to project root
cd "$PROJECT_ROOT"

# Run tests based on mode
case $TEST_MODE in
    basic)
        run_test_class "$SPECIFIC_TEST" "Basic Functionality Tests"
        ;;
    integration)
        run_test_class "$SPECIFIC_TEST" "Integration Tests"
        ;;
    failure)
        run_test_class "$SPECIFIC_TEST" "Failure Recovery Tests"
        ;;
    performance)
        run_test_class "$SPECIFIC_TEST" "Performance Tests"
        ;;
    config)
        run_test_class "$SPECIFIC_TEST" "Configuration Tests"
        ;;
    full)
        # Run individual test categories for better reporting
        total_failures=0
        
        print_status "Running all test categories individually..."
        
        if ! run_test_class "com.automq.stream.s3.quorum.S3QuorumStorageE2ETest" "Basic Functionality Tests"; then
            ((total_failures++))
        fi
        
        if ! run_test_class "com.automq.stream.s3.quorum.QuorumWriteIntegrationTest" "Integration Tests"; then
            ((total_failures++))
        fi
        
        if ! run_test_class "com.automq.stream.s3.quorum.S3QuorumStorageFailureRecoveryE2ETest" "Failure Recovery Tests"; then
            ((total_failures++))
        fi
        
        if ! run_test_class "com.automq.stream.s3.quorum.S3QuorumStoragePerformanceE2ETest" "Performance Tests"; then
            ((total_failures++))
        fi
        
        if ! run_test_class "com.automq.stream.s3.quorum.S3QuorumStorageConfigurationE2ETest" "Configuration Tests"; then
            ((total_failures++))
        fi
        
        if [ $total_failures -eq 0 ]; then
            print_success "All test categories passed!"
        else
            print_error "$total_failures test categories failed"
            exit 1
        fi
        ;;
esac

# Generate summary report
print_status "Generating test summary report..."

SUMMARY_FILE="$REPORTS_DIR/test-summary.txt"
cat > "$SUMMARY_FILE" << EOF
S3 Quorum Storage E2E Test Summary
==================================

Test execution completed at: $(date)
Test mode: $TEST_MODE
Java version: $(java -version 2>&1 | head -n 1)
Project root: $PROJECT_ROOT

Test Categories:
- Basic Functionality: Core read/write operations with multiple replicas
- Integration: Mock-based testing with various scenarios  
- Failure Recovery: Fault tolerance and recovery scenarios
- Performance: Throughput, latency, and stress testing
- Configuration: Validation of quorum configuration options

For detailed test results, check:
- Test results: $TEST_RESULTS_DIR
- Test reports: $REPORTS_DIR
- Gradle test reports: $PROJECT_ROOT/build/reports/tests/test

EOF

print_success "Test summary saved to: $SUMMARY_FILE"

print_status "================================================"
print_success "S3 Quorum Storage E2E tests completed!"
print_status "Check the reports directory for detailed results:"
print_status "  $REPORTS_DIR"
print_status "================================================"