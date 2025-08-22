/*
 * Copyright 2025, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.automq.stream.s3.quorum.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * QuorumSecurityContext provides security and access control for S3 Quorum operations.
 * Handles authentication, authorization, and security auditing.
 */
public class QuorumSecurityContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuorumSecurityContext.class);
    
    private final SecurityConfig config;
    private final Map<String, Principal> principals;
    private final Map<String, AccessToken> activeSessions;
    private final SecurityAuditor auditor;
    private final AtomicLong sessionCounter;
    private final SecureRandom secureRandom;
    
    public QuorumSecurityContext(SecurityConfig config) {
        this.config = config;
        this.principals = new ConcurrentHashMap<>();
        this.activeSessions = new ConcurrentHashMap<>();
        this.auditor = new SecurityAuditor(config.isAuditingEnabled());
        this.sessionCounter = new AtomicLong(0);
        this.secureRandom = new SecureRandom();
        
        LOGGER.info("QuorumSecurityContext initialized with {} authentication, auditing: {}", 
                   config.getAuthenticationMode(), config.isAuditingEnabled());
    }
    
    /**
     * Initialize the security context (call this after construction)
     */
    public void initialize() {
        initializeDefaultPrincipals();
    }
    
    /**
     * Authenticate a user and create an access token
     */
    public AuthenticationResult authenticate(String username, String password, String clientId) {
        long startTime = System.currentTimeMillis();
        
        try {
            if (!config.isAuthenticationEnabled()) {
                // Create anonymous session for non-authenticated mode
                AccessToken token = createAccessToken("anonymous", Permission.ALL, clientId);
                auditor.recordAuthenticationEvent("anonymous", clientId, true, "Authentication disabled");
                return new AuthenticationResult(true, token, "Authentication disabled");
            }
            
            Principal principal = principals.get(username);
            if (principal == null) {
                auditor.recordAuthenticationEvent(username, clientId, false, "User not found");
                return new AuthenticationResult(false, null, "Invalid credentials");
            }
            
            if (!verifyPassword(password, principal.getPasswordHash())) {
                auditor.recordAuthenticationEvent(username, clientId, false, "Invalid password");
                return new AuthenticationResult(false, null, "Invalid credentials");
            }
            
            if (!principal.isActive()) {
                auditor.recordAuthenticationEvent(username, clientId, false, "Account disabled");
                return new AuthenticationResult(false, null, "Account disabled");
            }
            
            // Create access token
            AccessToken token = createAccessToken(username, principal.getPermissions(), clientId);
            auditor.recordAuthenticationEvent(username, clientId, true, "Authentication successful");
            
            LOGGER.debug("Authentication successful for user: {} from client: {} (duration: {}ms)", 
                        username, clientId, System.currentTimeMillis() - startTime);
            
            return new AuthenticationResult(true, token, "Authentication successful");
            
        } catch (Exception e) {
            auditor.recordAuthenticationEvent(username, clientId, false, "Authentication error: " + e.getMessage());
            LOGGER.error("Authentication error for user: {} from client: {}", username, clientId, e);
            return new AuthenticationResult(false, null, "Authentication failed");
        }
    }
    
    /**
     * Authorize an operation for a given access token
     */
    public AuthorizationResult authorize(String tokenString, Permission requiredPermission, String resource) {
        long startTime = System.currentTimeMillis();
        
        try {
            if (!config.isAuthorizationEnabled()) {
                auditor.recordAuthorizationEvent("anonymous", requiredPermission.name(), resource, true, "Authorization disabled");
                return new AuthorizationResult(true, "Authorization disabled");
            }
            
            AccessToken token = activeSessions.get(tokenString);
            if (token == null) {
                auditor.recordAuthorizationEvent("unknown", requiredPermission.name(), resource, false, "Invalid token");
                return new AuthorizationResult(false, "Invalid or expired token");
            }
            
            if (token.isExpired()) {
                activeSessions.remove(tokenString);
                auditor.recordAuthorizationEvent(token.getUsername(), requiredPermission.name(), resource, false, "Token expired");
                return new AuthorizationResult(false, "Token expired");
            }
            
            boolean authorized = token.hasPermission(requiredPermission);
            auditor.recordAuthorizationEvent(token.getUsername(), requiredPermission.name(), resource, authorized, 
                                           authorized ? "Access granted" : "Insufficient permissions");
            
            if (!authorized) {
                LOGGER.warn("Authorization denied for user: {} permission: {} resource: {}", 
                           token.getUsername(), requiredPermission, resource);
            }
            
            LOGGER.debug("Authorization {} for user: {} permission: {} resource: {} (duration: {}ms)", 
                        authorized ? "granted" : "denied", token.getUsername(), requiredPermission, resource,
                        System.currentTimeMillis() - startTime);
            
            return new AuthorizationResult(authorized, authorized ? "Access granted" : "Insufficient permissions");
            
        } catch (Exception e) {
            auditor.recordAuthorizationEvent("error", requiredPermission.name(), resource, false, "Authorization error");
            LOGGER.error("Authorization error for permission: {} resource: {}", requiredPermission, resource, e);
            return new AuthorizationResult(false, "Authorization failed");
        }
    }
    
    /**
     * Validate and refresh an access token
     */
    public TokenValidationResult validateToken(String tokenString) {
        AccessToken token = activeSessions.get(tokenString);
        if (token == null) {
            return new TokenValidationResult(false, null, "Token not found");
        }
        
        if (token.isExpired()) {
            activeSessions.remove(tokenString);
            return new TokenValidationResult(false, null, "Token expired");
        }
        
        // Refresh token if it's close to expiry
        if (token.shouldRefresh()) {
            token.refreshExpiration();
            LOGGER.debug("Token refreshed for user: {}", token.getUsername());
        }
        
        return new TokenValidationResult(true, token, "Token valid");
    }
    
    /**
     * Revoke an access token
     */
    public void revokeToken(String tokenString, String reason) {
        AccessToken token = activeSessions.remove(tokenString);
        if (token != null) {
            auditor.recordTokenEvent(token.getUsername(), "REVOKE", reason);
            LOGGER.info("Token revoked for user: {} reason: {}", token.getUsername(), reason);
        }
    }
    
    /**
     * Add or update a principal (user account)
     */
    public void addPrincipal(String username, String password, Permission permissions, boolean active) {
        String passwordHash = hashPassword(password);
        Principal principal = new Principal(username, passwordHash, permissions, active, System.currentTimeMillis());
        principals.put(username, principal);
        
        auditor.recordPrincipalEvent(username, "CREATE", "Principal created with permissions: " + permissions);
        LOGGER.info("Principal added: {} with permissions: {}", username, permissions);
    }
    
    /**
     * Update principal permissions
     */
    public void updatePrincipalPermissions(String username, Permission newPermissions) {
        Principal principal = principals.get(username);
        if (principal != null) {
            Principal updatedPrincipal = new Principal(username, principal.getPasswordHash(), newPermissions, 
                                                     principal.isActive(), principal.getCreatedTime());
            principals.put(username, updatedPrincipal);
            
            auditor.recordPrincipalEvent(username, "UPDATE", "Permissions updated to: " + newPermissions);
            LOGGER.info("Principal permissions updated: {} -> {}", username, newPermissions);
        }
    }
    
    /**
     * Disable/enable a principal
     */
    public void setPrincipalActive(String username, boolean active) {
        Principal principal = principals.get(username);
        if (principal != null) {
            Principal updatedPrincipal = new Principal(username, principal.getPasswordHash(), 
                                                     principal.getPermissions(), active, principal.getCreatedTime());
            principals.put(username, updatedPrincipal);
            
            auditor.recordPrincipalEvent(username, active ? "ENABLE" : "DISABLE", 
                                       "Principal " + (active ? "enabled" : "disabled"));
            LOGGER.info("Principal {}: {}", active ? "enabled" : "disabled", username);
        }
    }
    
    /**
     * Get security statistics
     */
    public SecurityStats getSecurityStats() {
        return new SecurityStats(
            principals.size(),
            activeSessions.size(),
            auditor.getAuditEventCount(),
            auditor.getFailedAuthenticationCount(),
            auditor.getFailedAuthorizationCount()
        );
    }
    
    /**
     * Get audit log entries
     */
    public SecurityAuditor.AuditLog getAuditLog(int maxEntries) {
        return auditor.getAuditLog(maxEntries);
    }
    
    private void initializeDefaultPrincipals() {
        if (config.getAuthenticationMode() == SecurityConfig.AuthenticationMode.BASIC) {
            // Create default admin user
            addPrincipal("admin", "admin123", Permission.ALL, true);
            // Create default read-only user  
            addPrincipal("readonly", "readonly123", Permission.READ, true);
            // Create default write user
            addPrincipal("writer", "writer123", Permission.WRITE, true);
        }
    }
    
    private AccessToken createAccessToken(String username, Permission permissions, String clientId) {
        String tokenId = generateTokenId();
        long expirationTime = System.currentTimeMillis() + config.getTokenExpirationMs();
        
        AccessToken token = new AccessToken(tokenId, username, permissions, clientId, expirationTime);
        activeSessions.put(tokenId, token);
        
        auditor.recordTokenEvent(username, "CREATE", "Token created for client: " + clientId);
        return token;
    }
    
    private String generateTokenId() {
        byte[] tokenBytes = new byte[32];
        secureRandom.nextBytes(tokenBytes);
        return "qrm-token-" + sessionCounter.incrementAndGet() + "-" + Base64.getUrlEncoder().withoutPadding().encodeToString(tokenBytes);
    }
    
    private String hashPassword(String password) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] salt = generateSalt();
            digest.update(salt);
            byte[] hashedBytes = digest.digest(password.getBytes());
            
            // Combine salt and hash for storage
            byte[] combined = new byte[salt.length + hashedBytes.length];
            System.arraycopy(salt, 0, combined, 0, salt.length);
            System.arraycopy(hashedBytes, 0, combined, salt.length, hashedBytes.length);
            
            return Base64.getEncoder().encodeToString(combined);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }
    
    private boolean verifyPassword(String password, String storedHash) {
        try {
            byte[] combined = Base64.getDecoder().decode(storedHash);
            byte[] salt = new byte[16];
            byte[] hash = new byte[combined.length - 16];
            
            System.arraycopy(combined, 0, salt, 0, 16);
            System.arraycopy(combined, 16, hash, 0, hash.length);
            
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(salt);
            byte[] testHash = digest.digest(password.getBytes());
            
            return MessageDigest.isEqual(hash, testHash);
        } catch (Exception e) {
            LOGGER.error("Password verification error", e);
            return false;
        }
    }
    
    private byte[] generateSalt() {
        byte[] salt = new byte[16];
        secureRandom.nextBytes(salt);
        return salt;
    }
    
    // Data classes
    public static class SecurityConfig {
        public enum AuthenticationMode {
            DISABLED, BASIC, CERTIFICATE
        }
        
        private final AuthenticationMode authenticationMode;
        private final boolean authorizationEnabled;
        private final boolean auditingEnabled;
        private final long tokenExpirationMs;
        private final int maxActiveSessions;
        
        public SecurityConfig(AuthenticationMode authenticationMode, boolean authorizationEnabled,
                            boolean auditingEnabled, long tokenExpirationMs, int maxActiveSessions) {
            this.authenticationMode = authenticationMode;
            this.authorizationEnabled = authorizationEnabled;
            this.auditingEnabled = auditingEnabled;
            this.tokenExpirationMs = tokenExpirationMs;
            this.maxActiveSessions = maxActiveSessions;
        }
        
        public static SecurityConfig defaultConfig() {
            return new SecurityConfig(AuthenticationMode.BASIC, true, true, 3600000, 1000); // 1 hour, 1000 sessions
        }
        
        public static SecurityConfig disabledConfig() {
            return new SecurityConfig(AuthenticationMode.DISABLED, false, true, 3600000, 1000);
        }
        
        public AuthenticationMode getAuthenticationMode() { return authenticationMode; }
        public boolean isAuthenticationEnabled() { return authenticationMode != AuthenticationMode.DISABLED; }
        public boolean isAuthorizationEnabled() { return authorizationEnabled; }
        public boolean isAuditingEnabled() { return auditingEnabled; }
        public long getTokenExpirationMs() { return tokenExpirationMs; }
        public int getMaxActiveSessions() { return maxActiveSessions; }
    }
    
    public enum Permission {
        READ(1),
        WRITE(2),
        DELETE(4), 
        ADMIN(8),
        READ_WRITE(READ.value | WRITE.value),
        ALL(READ.value | WRITE.value | DELETE.value | ADMIN.value);
        
        private final int value;
        
        Permission(int value) {
            this.value = value;
        }
        
        public boolean includes(Permission other) {
            return (this.value & other.value) == other.value;
        }
        
        public int getValue() { return value; }
    }
    
    public static class Principal {
        private final String username;
        private final String passwordHash;
        private final Permission permissions;
        private final boolean active;
        private final long createdTime;
        
        public Principal(String username, String passwordHash, Permission permissions, boolean active, long createdTime) {
            this.username = username;
            this.passwordHash = passwordHash;
            this.permissions = permissions;
            this.active = active;
            this.createdTime = createdTime;
        }
        
        public String getUsername() { return username; }
        public String getPasswordHash() { return passwordHash; }
        public Permission getPermissions() { return permissions; }
        public boolean isActive() { return active; }
        public long getCreatedTime() { return createdTime; }
    }
    
    public static class AccessToken {
        private final String tokenId;
        private final String username;
        private final Permission permissions;
        private final String clientId;
        private volatile long expirationTime;
        private final long createdTime;
        
        public AccessToken(String tokenId, String username, Permission permissions, String clientId, long expirationTime) {
            this.tokenId = tokenId;
            this.username = username;
            this.permissions = permissions;
            this.clientId = clientId;
            this.expirationTime = expirationTime;
            this.createdTime = System.currentTimeMillis();
        }
        
        public boolean hasPermission(Permission requiredPermission) {
            return permissions.includes(requiredPermission);
        }
        
        public boolean isExpired() {
            return System.currentTimeMillis() > expirationTime;
        }
        
        public boolean shouldRefresh() {
            long timeLeft = expirationTime - System.currentTimeMillis();
            return timeLeft < (300000); // Refresh if less than 5 minutes left
        }
        
        public void refreshExpiration() {
            this.expirationTime = System.currentTimeMillis() + 3600000; // Extend by 1 hour
        }
        
        public String getTokenId() { return tokenId; }
        public String getUsername() { return username; }
        public Permission getPermissions() { return permissions; }
        public String getClientId() { return clientId; }
        public long getExpirationTime() { return expirationTime; }
        public long getCreatedTime() { return createdTime; }
    }
    
    public static class AuthenticationResult {
        private final boolean success;
        private final AccessToken token;
        private final String message;
        
        public AuthenticationResult(boolean success, AccessToken token, String message) {
            this.success = success;
            this.token = token;
            this.message = message;
        }
        
        public boolean isSuccess() { return success; }
        public AccessToken getToken() { return token; }
        public String getMessage() { return message; }
    }
    
    public static class AuthorizationResult {
        private final boolean authorized;
        private final String message;
        
        public AuthorizationResult(boolean authorized, String message) {
            this.authorized = authorized;
            this.message = message;
        }
        
        public boolean isAuthorized() { return authorized; }
        public String getMessage() { return message; }
    }
    
    public static class TokenValidationResult {
        private final boolean valid;
        private final AccessToken token;
        private final String message;
        
        public TokenValidationResult(boolean valid, AccessToken token, String message) {
            this.valid = valid;
            this.token = token;
            this.message = message;
        }
        
        public boolean isValid() { return valid; }
        public AccessToken getToken() { return token; }
        public String getMessage() { return message; }
    }
    
    public static class SecurityStats {
        private final int totalPrincipals;
        private final int activeSessions;
        private final long auditEventCount;
        private final long failedAuthenticationCount;
        private final long failedAuthorizationCount;
        
        public SecurityStats(int totalPrincipals, int activeSessions, long auditEventCount,
                           long failedAuthenticationCount, long failedAuthorizationCount) {
            this.totalPrincipals = totalPrincipals;
            this.activeSessions = activeSessions;
            this.auditEventCount = auditEventCount;
            this.failedAuthenticationCount = failedAuthenticationCount;
            this.failedAuthorizationCount = failedAuthorizationCount;
        }
        
        public int getTotalPrincipals() { return totalPrincipals; }
        public int getActiveSessions() { return activeSessions; }
        public long getAuditEventCount() { return auditEventCount; }
        public long getFailedAuthenticationCount() { return failedAuthenticationCount; }
        public long getFailedAuthorizationCount() { return failedAuthorizationCount; }
        public double getAuthenticationSuccessRate() { 
            long total = auditEventCount;
            return total > 0 ? 1.0 - (double) failedAuthenticationCount / total : 1.0;
        }
        public double getAuthorizationSuccessRate() {
            long total = auditEventCount;
            return total > 0 ? 1.0 - (double) failedAuthorizationCount / total : 1.0;
        }
    }
}