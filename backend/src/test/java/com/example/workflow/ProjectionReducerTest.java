package com.example.workflow;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ProjectionReducerTest {
    private final UUID practiceId = UUID.randomUUID();

    private PracticeEvent event(int sequence, String type, Map<String, String> data) {
        return new PracticeEvent(UUID.randomUUID(), practiceId, sequence, type, Instant.parse("2026-01-01T12:00:00Z"), data);
    }

    @Test
    void duplicateDeliveryDoesNotDuplicateHistoryOrDocuments() {
        ProjectionState view = new ProjectionState(practiceId.toString());
        PracticeEvent created = event(1, "PracticeCreated", Map.of("title", "Lab access"));
        PracticeEvent uploaded = event(2, "DocumentUploaded", Map.of("filename", "request.txt"));
        assertTrue(ProjectionReducer.accept(view, created));
        assertTrue(ProjectionReducer.accept(view, uploaded));
        assertFalse(ProjectionReducer.accept(view, uploaded));
        assertEquals(2, view.events.size());
        assertEquals(1, view.documents.size());
        assertEquals("DOCUMENTED", view.status);
    }

    @Test
    void outOfOrderEventsWaitForTheMissingSequence() {
        ProjectionState view = new ProjectionState(practiceId.toString());
        PracticeEvent approved = event(3, "PracticeApproved", Map.of());
        ProjectionReducer.accept(view, approved);
        assertEquals(0, view.lastSequence);
        assertEquals(1, view.pending.size());
        ProjectionReducer.accept(view, event(1, "PracticeCreated", Map.of("title", "Lab access")));
        assertEquals(1, view.lastSequence);
        ProjectionReducer.accept(view, event(2, "DocumentUploaded", Map.of("filename", "request.txt")));
        assertEquals(3, view.lastSequence);
        assertEquals("APPROVED", view.status);
        assertTrue(view.pending.isEmpty());
    }

    @Test
    void conflictingSequenceIsRejected() {
        ProjectionState view = new ProjectionState(practiceId.toString());
        ProjectionReducer.accept(view, event(2, "DocumentUploaded", Map.of("filename", "a.txt")));
        assertThrows(IllegalStateException.class,
                () -> ProjectionReducer.accept(view, event(2, "DocumentUploaded", Map.of("filename", "b.txt"))));
    }
}
