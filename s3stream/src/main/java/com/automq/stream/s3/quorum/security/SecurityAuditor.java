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
import org.slf4j.MDC;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SecurityAuditor provides comprehensive security event logging and auditing
 * for S3 Quorum Storage operations.
 */
public class SecurityAuditor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SecurityAuditor.class);
    private static final Logger AUDIT_LOGGER = LoggerFactory.getLogger("QUORUM_SECURITY_AUDIT");
    
    private final boolean enabled;
    private final ConcurrentLinkedQueue<AuditEvent> auditEvents;
    private final AtomicLong eventCounter;
    private final AtomicLong failedAuthenticationCounter;
    private final AtomicLong failedAuthorizationCounter;
    private final int maxAuditEvents;
    
    public SecurityAuditor(boolean enabled) {
        this.enabled = enabled;
        this.auditEvents = new ConcurrentLinkedQueue<>();
        this.eventCounter = new AtomicLong(0);
        this.failedAuthenticationCounter = new AtomicLong(0);
        this.failedAuthorizationCounter = new AtomicLong(0);
        this.maxAuditEvents = 10000; // Keep last 10k events in memory
        
        LOGGER.info("SecurityAuditor initialized with auditing {}", enabled ? "enabled" : "disabled");
    }
    
    /**
     * Record an authentication event
     */
    public void recordAuthenticationEvent(String username, String clientId, boolean success, String details) {
        if (!enabled) return;
        
        AuditEvent event = new AuditEvent(
            generateEventId(),
            AuditEvent.EventType.AUTHENTICATION,
            username,
            success ? AuditEvent.EventResult.SUCCESS : AuditEvent.EventResult.FAILURE,
            clientId,
            null,
            details,
            System.currentTimeMillis()
        );
        
        recordEvent(event);
        
        if (!success) {
            failedAuthenticationCounter.incrementAndGet();
        }
        
        // Log to audit logger with structured format
        try {
            MDC.put("audit.type", "AUTHENTICATION");
            MDC.put("audit.user", username);
            MDC.put("audit.client", clientId);
            MDC.put("audit.result", success ? "SUCCESS" : "FAILURE");
            
            if (success) {
                AUDIT_LOGGER.info("Authentication successful: user={} client={} details={}", username, clientId, details);
            } else {
                AUDIT_LOGGER.warn("Authentication failed: user={} client={} details={}", username, clientId, details);
            }
        } finally {
            MDC.clear();
        }
    }
    
    /**
     * Record an authorization event
     */
    public void recordAuthorizationEvent(String username, String permission, String resource, boolean success, String details) {
        if (!enabled) return;
        
        AuditEvent event = new AuditEvent(
            generateEventId(),
            AuditEvent.EventType.AUTHORIZATION,
            username,
            success ? AuditEvent.EventResult.SUCCESS : AuditEvent.EventResult.FAILURE,
            null,
            resource,
            details + " (permission: " + permission + ")",
            System.currentTimeMillis()
        );
        
        recordEvent(event);
        
        if (!success) {
            failedAuthorizationCounter.incrementAndGet();
        }
        
        // Log to audit logger
        try {
            MDC.put("audit.type", "AUTHORIZATION");
            MDC.put("audit.user", username);
            MDC.put("audit.permission", permission);
            MDC.put("audit.resource", resource);
            MDC.put("audit.result", success ? "SUCCESS" : "FAILURE");
            
            if (success) {
                AUDIT_LOGGER.info("Authorization granted: user={} permission={} resource={} details={}", 
                                 username, permission, resource, details);
            } else {
                AUDIT_LOGGER.warn("Authorization denied: user={} permission={} resource={} details={}", 
                                 username, permission, resource, details);
            }
        } finally {
            MDC.clear();
        }
    }
    
    /**
     * Record a token lifecycle event
     */
    public void recordTokenEvent(String username, String action, String details) {
        if (!enabled) return;
        
        AuditEvent event = new AuditEvent(
            generateEventId(),
            AuditEvent.EventType.TOKEN_LIFECYCLE,
            username,
            AuditEvent.EventResult.SUCCESS,
            null,
            null,
            action + ": " + details,
            System.currentTimeMillis()
        );
        
        recordEvent(event);
        
        // Log to audit logger
        try {
            MDC.put("audit.type", "TOKEN_LIFECYCLE");
            MDC.put("audit.user", username);
            MDC.put("audit.action", action);
            
            AUDIT_LOGGER.info("Token event: user={} action={} details={}", username, action, details);
        } finally {
            MDC.clear();
        }
    }
    
    /**
     * Record a principal management event
     */
    public void recordPrincipalEvent(String username, String action, String details) {
        if (!enabled) return;
        
        AuditEvent event = new AuditEvent(
            generateEventId(),
            AuditEvent.EventType.PRINCIPAL_MANAGEMENT,
            username,
            AuditEvent.EventResult.SUCCESS,
            null,
            null,
            action + ": " + details,
            System.currentTimeMillis()
        );
        
        recordEvent(event);
        
        // Log to audit logger
        try {
            MDC.put("audit.type", "PRINCIPAL_MANAGEMENT");
            MDC.put("audit.user", username);
            MDC.put("audit.action", action);
            
            AUDIT_LOGGER.info("Principal event: user={} action={} details={}", username, action, details);
        } finally {
            MDC.clear();
        }
    }
    
    /**
     * Record a security violation event
     */
    public void recordSecurityViolation(String username, String violationType, String details, String clientId) {
        AuditEvent event = new AuditEvent(
            generateEventId(),
            AuditEvent.EventType.SECURITY_VIOLATION,
            username,
            AuditEvent.EventResult.FAILURE,
            clientId,
            null,
            violationType + ": " + details,
            System.currentTimeMillis()
        );
        
        recordEvent(event);
        
        // Always log security violations regardless of audit enabled status
        try {
            MDC.put("audit.type", "SECURITY_VIOLATION");
            MDC.put("audit.user", username);
            MDC.put("audit.violation", violationType);
            MDC.put("audit.client", clientId);
            
            AUDIT_LOGGER.error("SECURITY VIOLATION: user={} violation={} client={} details={}", 
                              username, violationType, clientId, details);
        } finally {
            MDC.clear();
        }
    }
    
    /**
     * Record a configuration change event
     */
    public void recordConfigurationEvent(String username, String setting, String oldValue, String newValue) {
        if (!enabled) return;
        
        String details = String.format("Changed %s from '%s' to '%s'", setting, oldValue, newValue);
        
        AuditEvent event = new AuditEvent(
            generateEventId(),
            AuditEvent.EventType.CONFIGURATION_CHANGE,
            username,
            AuditEvent.EventResult.SUCCESS,
            null,
            setting,
            details,
            System.currentTimeMillis()
        );
        
        recordEvent(event);
        
        // Log to audit logger
        try {
            MDC.put("audit.type", "CONFIGURATION_CHANGE");
            MDC.put("audit.user", username);
            MDC.put("audit.setting", setting);
            
            AUDIT_LOGGER.info("Configuration changed: user={} setting={} oldValue={} newValue={}", 
                             username, setting, oldValue, newValue);
        } finally {
            MDC.clear();
        }
    }
    
    /**
     * Get audit statistics
     */
    public long getAuditEventCount() {
        return eventCounter.get();
    }
    
    public long getFailedAuthenticationCount() {
        return failedAuthenticationCounter.get();
    }
    
    public long getFailedAuthorizationCount() {
        return failedAuthorizationCounter.get();
    }
    
    /**
     * Get recent audit events
     */
    public AuditLog getAuditLog(int maxEntries) {
        List<AuditEvent> events = new ArrayList<>();
        int count = 0;
        
        for (AuditEvent event : auditEvents) {
            if (count >= maxEntries) break;
            events.add(event);
            count++;
        }
        
        Collections.reverse(events); // Show most recent first
        return new AuditLog(events, eventCounter.get());
    }
    
    /**
     * Get audit events by type
     */
    public List<AuditEvent> getAuditEventsByType(AuditEvent.EventType eventType, int maxEntries) {
        List<AuditEvent> filteredEvents = new ArrayList<>();
        int count = 0;
        
        for (AuditEvent event : auditEvents) {
            if (count >= maxEntries) break;
            if (event.getEventType() == eventType) {
                filteredEvents.add(event);
                count++;
            }
        }
        
        Collections.reverse(filteredEvents);
        return filteredEvents;
    }
    
    /**
     * Get audit events by user
     */
    public List<AuditEvent> getAuditEventsByUser(String username, int maxEntries) {
        List<AuditEvent> userEvents = new ArrayList<>();
        int count = 0;
        
        for (AuditEvent event : auditEvents) {
            if (count >= maxEntries) break;
            if (username.equals(event.getUsername())) {
                userEvents.add(event);
                count++;
            }
        }
        
        Collections.reverse(userEvents);
        return userEvents;
    }
    
    private void recordEvent(AuditEvent event) {
        auditEvents.offer(event);
        eventCounter.incrementAndGet();
        
        // Remove old events if we exceed the maximum
        while (auditEvents.size() > maxAuditEvents) {
            auditEvents.poll();
        }
    }
    
    private String generateEventId() {
        return "audit-" + System.currentTimeMillis() + "-" + eventCounter.get();
    }
    
    // Data classes
    public static class AuditEvent {
        public enum EventType {
            AUTHENTICATION,
            AUTHORIZATION, 
            TOKEN_LIFECYCLE,
            PRINCIPAL_MANAGEMENT,
            SECURITY_VIOLATION,
            CONFIGURATION_CHANGE
        }
        
        public enum EventResult {
            SUCCESS,
            FAILURE
        }
        
        private final String eventId;
        private final EventType eventType;
        private final String username;
        private final EventResult result;
        private final String clientId;
        private final String resource;
        private final String details;
        private final long timestamp;
        
        public AuditEvent(String eventId, EventType eventType, String username, EventResult result,
                         String clientId, String resource, String details, long timestamp) {
            this.eventId = eventId;
            this.eventType = eventType;
            this.username = username;
            this.result = result;
            this.clientId = clientId;
            this.resource = resource;
            this.details = details;
            this.timestamp = timestamp;
        }
        
        public String getEventId() { return eventId; }
        public EventType getEventType() { return eventType; }
        public String getUsername() { return username; }
        public EventResult getResult() { return result; }
        public String getClientId() { return clientId; }
        public String getResource() { return resource; }
        public String getDetails() { return details; }
        public long getTimestamp() { return timestamp; }
        
        public String getFormattedTimestamp() {
            return DateTimeFormatter.ISO_LOCAL_DATE_TIME
                .withZone(ZoneId.systemDefault())
                .format(Instant.ofEpochMilli(timestamp));
        }
        
        @Override
        public String toString() {
            return String.format("[%s] %s %s %s: %s (user=%s, client=%s, resource=%s)", 
                               getFormattedTimestamp(),
                               eventType,
                               result,
                               eventId,
                               details,
                               username,
                               clientId,
                               resource);
        }
        
        public String toJsonString() {
            StringBuilder json = new StringBuilder();
            json.append("{");
            json.append("\"eventId\":\"").append(eventId).append("\",");
            json.append("\"eventType\":\"").append(eventType).append("\",");
            json.append("\"username\":\"").append(username != null ? username : "").append("\",");
            json.append("\"result\":\"").append(result).append("\",");
            json.append("\"clientId\":\"").append(clientId != null ? clientId : "").append("\",");
            json.append("\"resource\":\"").append(resource != null ? resource : "").append("\",");
            json.append("\"details\":\"").append(escapeJsonString(details)).append("\",");
            json.append("\"timestamp\":").append(timestamp);
            json.append("}");
            return json.toString();
        }
        
        private String escapeJsonString(String str) {
            if (str == null) return "";
            return str.replace("\"", "\\\"").replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r");
        }
    }
    
    public static class AuditLog {
        private final List<AuditEvent> events;
        private final long totalEventCount;
        private final long generatedTime;
        
        public AuditLog(List<AuditEvent> events, long totalEventCount) {
            this.events = new ArrayList<>(events);
            this.totalEventCount = totalEventCount;
            this.generatedTime = System.currentTimeMillis();
        }
        
        public List<AuditEvent> getEvents() { return events; }
        public long getTotalEventCount() { return totalEventCount; }
        public long getGeneratedTime() { return generatedTime; }
        public int getReturnedEventCount() { return events.size(); }
        
        public String toJsonString() {
            StringBuilder json = new StringBuilder();
            json.append("{");
            json.append("\"totalEventCount\":").append(totalEventCount).append(",");
            json.append("\"returnedEventCount\":").append(events.size()).append(",");
            json.append("\"generatedTime\":").append(generatedTime).append(",");
            json.append("\"events\":[");
            
            for (int i = 0; i < events.size(); i++) {
                if (i > 0) json.append(",");
                json.append(events.get(i).toJsonString());
            }
            
            json.append("]}");
            return json.toString();
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("=== Security Audit Log ===\n");
            sb.append("Generated: ").append(Instant.ofEpochMilli(generatedTime)).append("\n");
            sb.append("Total Events: ").append(totalEventCount).append("\n");
            sb.append("Returned Events: ").append(events.size()).append("\n");
            sb.append("\n");
            
            for (AuditEvent event : events) {
                sb.append(event.toString()).append("\n");
            }
            
            return sb.toString();
        }
    }
}