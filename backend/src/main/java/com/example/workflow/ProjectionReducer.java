package com.example.workflow;

import java.util.Comparator;
import java.util.Objects;

public final class ProjectionReducer {
    private ProjectionReducer() { }

    public static boolean accept(ProjectionState state, PracticeEvent event) {
        if (!state.id.equals(event.practiceId().toString())) {
            throw new IllegalArgumentException("Event belongs to another practice");
        }
        if (event.sequence() <= state.lastSequence) return false;
        for (PracticeEvent queued : state.pending) {
            if (queued.sequence() == event.sequence()) {
                if (!queued.eventId().equals(event.eventId())) {
                    throw new IllegalStateException("Conflicting event at sequence " + event.sequence());
                }
                return false;
            }
        }
        state.pending.add(event);
        state.pending.sort(Comparator.comparingInt(PracticeEvent::sequence));
        while (!state.pending.isEmpty() && state.pending.getFirst().sequence() == state.lastSequence + 1) {
            apply(state, state.pending.removeFirst());
        }
        return true;
    }

    private static void apply(ProjectionState state, PracticeEvent event) {
        String description;
        switch (event.type()) {
            case "PracticeCreated" -> {
                state.title = Objects.requireNonNull(event.data().get("title"));
                state.status = "CREATED";
                description = "Practice created";
            }
            case "DocumentUploaded" -> {
                String filename = Objects.requireNonNull(event.data().get("filename"));
                state.documents.add(filename);
                state.status = "DOCUMENTED";
                description = "Document uploaded: " + filename;
            }
            case "PracticeApproved" -> {
                state.status = "APPROVED";
                description = "Practice approved";
            }
            default -> throw new IllegalArgumentException("Unknown event type " + event.type());
        }
        state.lastSequence = event.sequence();
        state.updatedAt = event.occurredAt();
        state.events.add(new ProjectionState.EventSummary(event.eventId().toString(), event.sequence(),
                event.type(), event.occurredAt(), description));
    }
}
