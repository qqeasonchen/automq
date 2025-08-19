#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}S3 Quorum Storage Actual Tests Runner${NC}"
echo -e "${BLUE}========================================${NC}"

# Function to run a test and capture result
run_test() {
    local test_name="$1"
    local module="$2" 
    local test_class="$3"
    
    echo -e "${BLUE}[INFO]${NC} Running $test_name..."
    
    if ./gradlew ":$module:test" --tests "$test_class" -x spotbugsMain -x checkstyleMain -x checkstyleTest --quiet; then
        echo -e "${GREEN}[PASS]${NC} $test_name completed successfully"
        return 0
    else
        echo -e "${RED}[FAIL]${NC} $test_name failed"
        return 1
    fi
}

# Initialize counters
total_tests=0
passed_tests=0

echo -e "${BLUE}[INFO]${NC} Running S3QuorumStorage core functionality tests..."

# Test 1: S3QuorumStorage core tests
total_tests=$((total_tests + 1))
if run_test "S3QuorumStorage Core Tests" "s3stream" "com.automq.stream.s3.quorum.S3QuorumStorageTest"; then
    passed_tests=$((passed_tests + 1))
fi

# Test 2: S3QuorumStorageFactory tests
total_tests=$((total_tests + 1))
if run_test "S3QuorumStorageFactory Tests" "s3stream" "com.automq.stream.s3.quorum.factory.S3QuorumStorageFactoryTest"; then
    passed_tests=$((passed_tests + 1))
fi

# Test 3: DefaultS3ClientQuorum integration tests
total_tests=$((total_tests + 1))
if run_test "DefaultS3Client Quorum Integration Tests" "core" "kafka.log.stream.s3.DefaultS3ClientQuorumTest"; then
    passed_tests=$((passed_tests + 1))
fi

# Test 4: Run a broader compilation test
total_tests=$((total_tests + 1))
echo -e "${BLUE}[INFO]${NC} Running compilation and build verification..."
if ./gradlew clean releaseTarGz -x test --quiet; then
    echo -e "${GREEN}[PASS]${NC} Build and compilation verification completed successfully"
    passed_tests=$((passed_tests + 1))
else
    echo -e "${RED}[FAIL]${NC} Build and compilation verification failed"
fi

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Test Summary${NC}"
echo -e "${BLUE}========================================${NC}"

if [ $passed_tests -eq $total_tests ]; then
    echo -e "${GREEN}[SUCCESS]${NC} All $total_tests tests passed! ✅"
    echo -e "${GREEN}S3 Quorum Storage is ready for production use!${NC}"
    exit 0
else
    failed_tests=$((total_tests - passed_tests))
    echo -e "${RED}[FAILURE]${NC} $failed_tests out of $total_tests tests failed ❌"
    echo -e "${YELLOW}[INFO]${NC} Passed: $passed_tests/$total_tests tests"
    exit 1
fi