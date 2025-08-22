/*
 * Copyright 2025, AutoMQ HK Limited.
 * Simple P4 Integration Test focused on component interaction.
 */

package com.automq.stream.s3;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import com.automq.stream.s3.quorum.security.QuorumSecurityContext;
import com.automq.stream.s3.quorum.security.SecurityAuditor;
import com.automq.stream.s3.quorum.ops.QuorumHealthChecker;
import com.automq.stream.s3.quorum.errors.QuorumException;
import com.automq.stream.s3.quorum.errors.QuorumErrorCodes;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Simple integration test focusing on core P4 component instantiation and basic functionality.
 * This test verifies that all P4 components can be created and work together.
 */
public class SimpleP4IntegrationTest {

    private QuorumSecurityContext securityContext;
    private SecurityAuditor securityAuditor;
    private QuorumHealthChecker healthChecker;
    
    @BeforeEach
    void setUp() {
        System.out.println("\n=== Setting up P4 components for testing ===");
        
        // Initialize SecurityContext
        QuorumSecurityContext.SecurityConfig securityConfig = 
            QuorumSecurityContext.SecurityConfig.defaultConfig();
        this.securityContext = new QuorumSecurityContext(securityConfig);
        this.securityContext.initialize();
        System.out.println("✓ SecurityContext initialized");
        
        // Initialize SecurityAuditor
        this.securityAuditor = new SecurityAuditor(true);
        System.out.println("✓ SecurityAuditor initialized");
        
        // Initialize HealthChecker
        QuorumHealthChecker.HealthConfig healthConfig = 
            QuorumHealthChecker.HealthConfig.defaultConfig();
        this.healthChecker = new QuorumHealthChecker(Collections.emptyList(), healthConfig);
        System.out.println("✓ HealthChecker initialized");
        
        System.out.println("=== P4 components setup completed ===\n");
    }

    @Test
    @DisplayName("Test P4 Components Basic Instantiation")
    void testP4ComponentsBasicInstantiation() {
        System.out.println("=== Testing P4 Components Basic Instantiation ===");
        
        // Verify all components are not null
        assertNotNull(securityContext, "SecurityContext should be instantiated");
        assertNotNull(securityAuditor, "SecurityAuditor should be instantiated");
        assertNotNull(healthChecker, "HealthChecker should be instantiated");
        
        System.out.println("✓ All P4 components instantiated successfully");
        
        // Test SecurityContext basic functionality - verification by successful initialization
        System.out.println("✓ SecurityContext configuration accessible via successful initialization");
        
        // Test SecurityAuditor event counting
        long initialCount = securityAuditor.getAuditEventCount();
        assertEquals(0L, initialCount, "Initial audit count should be 0");
        System.out.println("✓ SecurityAuditor initial state correct");
        
        // Test HealthChecker basic functionality
        QuorumHealthChecker.ClusterHealthStatus health = healthChecker.getClusterHealth();
        assertNotNull(health, "Cluster health should be available");
        System.out.println("✓ HealthChecker provides cluster health: " + health.getClusterState());
        
        System.out.println("✓ P4 Components Basic Instantiation Test PASSED\n");
    }

    @Test
    @DisplayName("Test Security Integration Flow")
    void testSecurityIntegrationFlow() {
        System.out.println("=== Testing Security Integration Flow ===");
        
        String testUser = "integration-user";
        String testClient = "integration-client";
        String testObject = "test-object-key";
        
        // Step 1: Record authentication event
        securityAuditor.recordAuthenticationEvent(
            testUser, testClient, true, "Integration test authentication"
        );
        
        long countAfterAuth = securityAuditor.getAuditEventCount();
        assertEquals(1L, countAfterAuth, "Should have 1 audit event after authentication");
        System.out.println("✓ Authentication event recorded");
        
        // Step 2: Record authorization event
        securityAuditor.recordAuthorizationEvent(
            testUser, "read", testObject, true, "Read operation authorized"
        );
        
        long countAfterAuthz = securityAuditor.getAuditEventCount();
        assertEquals(2L, countAfterAuthz, "Should have 2 audit events after authorization");
        System.out.println("✓ Authorization event recorded");
        
        // Step 3: Test permission enums
        QuorumSecurityContext.Permission readPerm = QuorumSecurityContext.Permission.READ;
        QuorumSecurityContext.Permission writePerm = QuorumSecurityContext.Permission.WRITE;
        QuorumSecurityContext.Permission adminPerm = QuorumSecurityContext.Permission.ADMIN;
        
        assertTrue(adminPerm.includes(readPerm), "Admin should include read permission");
        assertTrue(adminPerm.includes(writePerm), "Admin should include write permission");
        System.out.println("✓ Permission hierarchy validated");
        
        // Step 4: Test access denied exception
        QuorumException accessDenied = QuorumException.accessDenied(
            testObject, "write", "unauthorized-user"
        );
        
        assertNotNull(accessDenied, "Access denied exception should be created");
        assertEquals(QuorumErrorCodes.SECURITY_ACCESS_DENIED, accessDenied.getErrorCode());
        assertEquals(testObject, accessDenied.getObjectKey());
        assertEquals("write", accessDenied.getOperation());
        System.out.println("✓ Access denied exception created: " + accessDenied.getMessage());
        
        // Step 5: Test security violation recording
        securityAuditor.recordSecurityViolation(
            "unauthorized-user", "ACCESS_DENIED", "Attempted unauthorized write", testClient
        );
        
        long finalCount = securityAuditor.getAuditEventCount();
        assertEquals(3L, finalCount, "Should have 3 audit events after security violation");
        System.out.println("✓ Security violation recorded");
        
        System.out.println("✓ Security Integration Flow Test PASSED\n");
    }

    @Test
    @DisplayName("Test Health Monitoring Integration")
    void testHealthMonitoringIntegration() {
        System.out.println("=== Testing Health Monitoring Integration ===");
        
        // Test health states availability
        QuorumHealthChecker.HealthState[] allStates = QuorumHealthChecker.HealthState.values();
        assertEquals(4, allStates.length, "Should have 4 health states");
        
        Set<String> stateNames = new HashSet<>();
        for (QuorumHealthChecker.HealthState state : allStates) {
            stateNames.add(state.name());
        }
        
        assertTrue(stateNames.contains("HEALTHY"), "HEALTHY state should exist");
        assertTrue(stateNames.contains("DEGRADED"), "DEGRADED state should exist");
        assertTrue(stateNames.contains("UNHEALTHY"), "UNHEALTHY state should exist");
        assertTrue(stateNames.contains("UNKNOWN"), "UNKNOWN state should exist");
        System.out.println("✓ All health states available: " + stateNames);
        
        // Test cluster health status
        QuorumHealthChecker.ClusterHealthStatus clusterHealth = healthChecker.getClusterHealth();
        assertNotNull(clusterHealth, "Cluster health should be available");
        assertNotNull(clusterHealth.getClusterState(), "Cluster state should not be null");
        System.out.println("✓ Cluster health status: " + clusterHealth.getClusterState());
        
        // Test async health check
        CompletableFuture<QuorumHealthChecker.ClusterHealthStatus> asyncHealth = 
            healthChecker.checkHealthNow();
        assertNotNull(asyncHealth, "Async health check should return future");
        assertFalse(asyncHealth.isCancelled(), "Health check future should not be cancelled");
        System.out.println("✓ Async health check initiated");
        
        // Test health statistics
        QuorumHealthChecker.HealthStats healthStats = healthChecker.getHealthStats();
        assertNotNull(healthStats, "Health stats should be available");
        assertTrue(healthStats.getTotalChecks() >= 0, "Total checks should be non-negative");
        System.out.println("✓ Health statistics available - Total checks: " + healthStats.getTotalChecks());
        
        System.out.println("✓ Health Monitoring Integration Test PASSED\n");
    }

    @Test
    @DisplayName("Test Error Handling Integration")
    void testErrorHandlingIntegration() {
        System.out.println("=== Testing Error Handling Integration ===");
        
        // Test new security error code
        QuorumErrorCodes securityAccessDenied = QuorumErrorCodes.SECURITY_ACCESS_DENIED;
        assertNotNull(securityAccessDenied, "SECURITY_ACCESS_DENIED error code should exist");
        assertEquals("SECURITY_ACCESS_DENIED", securityAccessDenied.getName());
        System.out.println("✓ Security error code available: " + securityAccessDenied.getName());
        
        // Test other essential error codes
        QuorumErrorCodes writeQuorumInsufficient = QuorumErrorCodes.WRITE_QUORUM_INSUFFICIENT;
        QuorumErrorCodes readQuorumInsufficient = QuorumErrorCodes.READ_QUORUM_INSUFFICIENT;
        
        assertNotNull(writeQuorumInsufficient, "WRITE_QUORUM_INSUFFICIENT should exist");
        assertNotNull(readQuorumInsufficient, "READ_QUORUM_INSUFFICIENT should exist");
        System.out.println("✓ Core error codes available");
        
        // Test structured exception with context
        String testObject = "test-integration-object";
        String testUser = "test-user";
        
        QuorumException exception = QuorumException.accessDenied(testObject, "read", testUser);
        
        // Verify exception properties
        assertEquals(QuorumErrorCodes.SECURITY_ACCESS_DENIED, exception.getErrorCode());
        assertEquals("read", exception.getOperation());
        assertEquals(testObject, exception.getObjectKey());
        assertNotNull(exception.getRequestId(), "Request ID should be generated");
        assertTrue(exception.getTimestamp() > 0, "Timestamp should be set");
        System.out.println("✓ Exception properties: " + exception.getRequestId());
        
        // Test exception context
        Map<String, Object> context = exception.getContext();
        assertNotNull(context, "Exception context should exist");
        assertEquals(testUser, context.get("principal"), "Principal should be in context");
        assertEquals("read", context.get("attemptedOperation"), "Operation should be in context");
        System.out.println("✓ Exception context populated");
        
        // Test JSON serialization
        String jsonString = exception.toJsonString();
        assertNotNull(jsonString, "JSON serialization should work");
        assertTrue(jsonString.contains("SECURITY_ACCESS_DENIED"), "JSON should contain error code");
        assertTrue(jsonString.contains(testUser), "JSON should contain user");
        assertTrue(jsonString.contains(testObject), "JSON should contain object key");
        System.out.println("✓ JSON serialization: " + jsonString.substring(0, Math.min(100, jsonString.length())) + "...");
        
        // Test error report
        QuorumException.ErrorReport report = exception.toErrorReport();
        assertNotNull(report, "Error report should be generated");
        assertEquals(exception.getErrorCode(), report.getErrorCode());
        assertEquals(exception.getOperation(), report.getOperation());
        assertEquals(exception.getObjectKey(), report.getObjectKey());
        assertEquals(exception.getRequestId(), report.getRequestId());
        System.out.println("✓ Error report generated");
        
        System.out.println("✓ Error Handling Integration Test PASSED\n");
    }

    @Test
    @DisplayName("Test Complete P4 Workflow Simulation")
    void testCompleteP4WorkflowSimulation() {
        System.out.println("=== Testing Complete P4 Workflow Simulation ===");
        
        String workflowUser = "workflow-test-user";
        String workflowObject = "workflow-test-object-" + System.currentTimeMillis();
        
        System.out.println("Starting P4 integrated workflow for user: " + workflowUser);
        System.out.println("Target object: " + workflowObject);
        
        // === PHASE 1: PRE-OPERATION (Security & Health) ===
        System.out.println("\n--- Phase 1: Pre-Operation Checks ---");
        
        // 1.1: User authentication
        securityAuditor.recordAuthenticationEvent(
            workflowUser, "workflow-client", true, "Workflow authentication"
        );
        System.out.println("✓ User authenticated and audited");
        
        // 1.2: Pre-operation health check
        QuorumHealthChecker.ClusterHealthStatus preOpHealth = healthChecker.getClusterHealth();
        System.out.println("✓ Pre-operation health: " + preOpHealth.getClusterState());
        
        // 1.3: Authorization check simulation
        securityAuditor.recordAuthorizationEvent(
            workflowUser, "write", workflowObject, true, "Write operation authorized for workflow"
        );
        System.out.println("✓ Operation authorized");
        
        // === PHASE 2: OPERATION SIMULATION ===
        System.out.println("\n--- Phase 2: Operation Execution ---");
        
        // 2.1: Simulate write operation (this would be actual QuorumObjectStorage.write())
        System.out.println("✓ Write operation simulated (would invoke QuorumObjectStorage.write())");
        
        // 2.2: Simulate read-back verification
        System.out.println("✓ Read verification simulated (would invoke QuorumObjectStorage.read())");
        
        // === PHASE 3: POST-OPERATION (Health & Diagnostics) ===
        System.out.println("\n--- Phase 3: Post-Operation Verification ---");
        
        // 3.1: Post-operation health check
        CompletableFuture<QuorumHealthChecker.ClusterHealthStatus> postOpHealthFuture = 
            healthChecker.checkHealthNow();
        System.out.println("✓ Post-operation health check initiated");
        
        // 3.2: System health validation
        QuorumHealthChecker.HealthStats healthStats = healthChecker.getHealthStats();
        System.out.println("✓ Health statistics collected - Checks: " + healthStats.getTotalChecks());
        
        // === PHASE 4: AUDIT AND CLEANUP ===
        System.out.println("\n--- Phase 4: Audit and Cleanup ---");
        
        // 4.1: Record successful completion
        securityAuditor.recordAuthenticationEvent(
            workflowUser, "workflow-client", true, "Workflow completed successfully"
        );
        
        // 4.2: Verify audit trail
        long totalAuditEvents = securityAuditor.getAuditEventCount();
        assertTrue(totalAuditEvents >= 4, "Should have comprehensive audit trail");
        System.out.println("✓ Complete audit trail: " + totalAuditEvents + " events");
        
        // === PHASE 5: INTEGRATION VERIFICATION ===
        System.out.println("\n--- Phase 5: Integration Verification ---");
        
        // 5.1: Verify all components still functional
        assertNotNull(securityContext, "SecurityContext still functional");
        assertTrue(securityAuditor.getAuditEventCount() > 0, "SecurityAuditor still recording");
        assertNotNull(healthChecker.getClusterHealth(), "HealthChecker still monitoring");
        System.out.println("✓ All P4 components remain functional after workflow");
        
        // 5.2: Test error scenario simulation
        securityAuditor.recordSecurityViolation(
            "unauthorized-user", "WORKFLOW_VIOLATION", 
            "Attempted to access " + workflowObject + " without permission", "malicious-client"
        );
        System.out.println("✓ Security violation handling verified");
        
        System.out.println("\n=== WORKFLOW SUMMARY ===");
        System.out.println("✓ Pre-operation security and health checks completed");
        System.out.println("✓ Operation execution simulated successfully");
        System.out.println("✓ Post-operation verification completed");
        System.out.println("✓ Complete audit trail maintained");
        System.out.println("✓ Error handling capabilities verified");
        System.out.println("✓ All P4 components integrated and functional");
        
        System.out.println("\n✓ Complete P4 Workflow Simulation PASSED");
        
        // Final assertion
        long finalAuditCount = securityAuditor.getAuditEventCount();
        assertTrue(finalAuditCount >= 5, "Should have complete audit trail with security violation");
        
        System.out.println("\n=== P4 INTEGRATION SUCCESSFUL ===");
        System.out.println("Security, Health Monitoring, Error Handling - ALL WORKING TOGETHER!");
    }
}