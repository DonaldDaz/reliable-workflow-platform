package com.example.workflow;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.mapping.Document;

@Document("practice_views")
public class ProjectionState {
    @Id public String id;
    @Version public Long revision;
    public String title = "Waiting for creation event";
    public String status = "PENDING";
    public int lastSequence = 0;
    public List<String> documents = new ArrayList<>();
    public List<EventSummary> events = new ArrayList<>();
    public List<PracticeEvent> pending = new ArrayList<>();
    public Instant updatedAt;

    public ProjectionState() { }

    public ProjectionState(String id) {
        this.id = id;
    }

    public record EventSummary(String eventId, int sequence, String type, Instant occurredAt, String description) { }
}
