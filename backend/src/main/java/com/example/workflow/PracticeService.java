package com.example.workflow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.trace.Span;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PracticeService {
    private final JdbcTemplate db;
    private final ObjectMapper json;

    public PracticeService(JdbcTemplate db, ObjectMapper json) {
        this.db = db;
        this.json = json;
    }

    @Transactional
    public UUID create(String title) {
        if (title == null || title.isBlank() || title.length() > 160) {
            throw new IllegalArgumentException("Title must contain 1 to 160 characters");
        }
        UUID id = UUID.randomUUID();
        Span.current().setAttribute("practice.id", id.toString());
        db.update("INSERT INTO practices (id, title, status, version) VALUES (?, ?, 'CREATED', 1)", id, title.trim());
        append(new PracticeEvent(UUID.randomUUID(), id, 1, "PracticeCreated", Instant.now(),
                Map.of("title", title.trim(), "status", "CREATED")));
        return id;
    }

    @Transactional
    public UUID upload(UUID practiceId, String filename, String contentType, byte[] content) {
        State state = lock(practiceId);
        if (!state.status.equals("CREATED")) {
            throw new IllegalStateException("Documents can be uploaded only to a created practice");
        }
        if (content.length == 0 || content.length > 1_048_576) {
            throw new IllegalArgumentException("Document must contain 1 byte to 1 MB");
        }
        if (filename == null || filename.isBlank() || filename.length() > 200) {
            throw new IllegalArgumentException("Filename must contain 1 to 200 characters");
        }
        UUID documentId = UUID.randomUUID();
        String digest;
        try {
            digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content));
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is unavailable", error);
        }
        Span.current().setAttribute("practice.id", practiceId.toString());
        db.update("INSERT INTO documents (id, practice_id, filename, content_type, content, sha256, size_bytes) VALUES (?, ?, ?, ?, ?, ?, ?)",
                documentId, practiceId, filename, contentType == null ? "application/octet-stream" : contentType,
                content, digest, content.length);
        int next = state.version + 1;
        db.update("UPDATE practices SET status = 'DOCUMENTED', version = ?, updated_at = now() WHERE id = ?", next, practiceId);
        append(new PracticeEvent(UUID.randomUUID(), practiceId, next, "DocumentUploaded", Instant.now(),
                Map.of("documentId", documentId.toString(), "filename", filename,
                        "sha256", digest, "status", "DOCUMENTED")));
        return documentId;
    }

    @Transactional
    public void approve(UUID practiceId) {
        State state = lock(practiceId);
        if (!state.status.equals("DOCUMENTED")) {
            throw new IllegalStateException("Only a documented practice can be approved");
        }
        Span.current().setAttribute("practice.id", practiceId.toString());
        int next = state.version + 1;
        db.update("UPDATE practices SET status = 'APPROVED', version = ?, updated_at = now() WHERE id = ?", next, practiceId);
        append(new PracticeEvent(UUID.randomUUID(), practiceId, next, "PracticeApproved", Instant.now(),
                Map.of("status", "APPROVED")));
    }

    private State lock(UUID id) {
        List<State> rows = db.query("SELECT status, version FROM practices WHERE id = ? FOR UPDATE",
                (rs, row) -> new State(rs.getString("status"), rs.getInt("version")), id);
        if (rows.isEmpty()) throw new IllegalArgumentException("Practice not found");
        return rows.getFirst();
    }

    private void append(PracticeEvent event) {
        String payload;
        String eventJson;
        try {
            payload = json.writeValueAsString(event.data());
            eventJson = json.writeValueAsString(event);
        } catch (JsonProcessingException error) {
            throw new IllegalStateException("Could not serialize event", error);
        }
        db.update("INSERT INTO practice_events (event_id, practice_id, sequence, event_type, payload, occurred_at) VALUES (?, ?, ?, ?, ?::jsonb, ?)",
                event.eventId(), event.practiceId(), event.sequence(), event.type(), payload, Timestamp.from(event.occurredAt()));
        var context = Span.current().getSpanContext();
        String traceparent = context.isValid()
                ? "00-" + context.getTraceId() + "-" + context.getSpanId() + "-" + context.getTraceFlags().asHex()
                : null;
        db.update("INSERT INTO outbox (event_id, practice_id, sequence, event_json, traceparent) VALUES (?, ?, ?, ?::jsonb, ?)",
                event.eventId(), event.practiceId(), event.sequence(), eventJson, traceparent);
    }

    public List<Map<String, Object>> list() {
        return db.queryForList("SELECT id, title, status, version, created_at, updated_at FROM practices ORDER BY created_at DESC");
    }

    public Map<String, Object> detail(UUID id) {
        List<Map<String, Object>> rows = db.queryForList("SELECT id, title, status, version, created_at, updated_at FROM practices WHERE id = ?", id);
        if (rows.isEmpty()) throw new IllegalArgumentException("Practice not found");
        Map<String, Object> result = new java.util.LinkedHashMap<>(rows.getFirst());
        result.put("documents", db.queryForList("SELECT id, filename, content_type, sha256, size_bytes, created_at FROM documents WHERE practice_id = ? ORDER BY created_at", id));
        result.put("events", db.queryForList("SELECT event_id, sequence, event_type, occurred_at FROM practice_events WHERE practice_id = ? ORDER BY sequence", id));
        return result;
    }

    public long pendingOutbox() {
        return db.queryForObject("SELECT count(*) FROM outbox WHERE published_at IS NULL", Long.class);
    }

    private record State(String status, int version) { }
}
