package com.example.workflow;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record PracticeEvent(UUID eventId, UUID practiceId, int sequence, String type,
                            Instant occurredAt, Map<String, String> data) { }
