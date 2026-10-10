package com.example.workflow;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class OutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private final JdbcTemplate db;
    private final KafkaTemplate<String, String> kafka;
    private final String topic;

    public OutboxPublisher(JdbcTemplate db, KafkaTemplate<String, String> kafka,
                           @Value("${workflow.topic}") String topic) {
        this.db = db;
        this.kafka = kafka;
        this.topic = topic;
    }

    @Scheduled(fixedDelayString = "${workflow.outbox-delay-ms}")
    @Transactional
    public void publish() {
        List<Map<String, Object>> rows = db.queryForList("""
                SELECT event_id, practice_id, sequence, event_json::text AS event_json, traceparent
                FROM outbox candidate
                WHERE published_at IS NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM outbox earlier
                    WHERE earlier.practice_id = candidate.practice_id
                      AND earlier.sequence < candidate.sequence
                      AND earlier.published_at IS NULL)
                ORDER BY created_at, sequence LIMIT 20 FOR UPDATE SKIP LOCKED
                """);
        for (Map<String, Object> row : rows) {
            UUID eventId = (UUID) row.get("event_id");
            String practiceId = row.get("practice_id").toString();
            String traceparent = (String) row.get("traceparent");
            Context parent = Context.current();
            if (traceparent != null) {
                String[] parts = traceparent.split("-");
                if (parts.length == 4) {
                    var remote = SpanContext.createFromRemoteParent(parts[1], parts[2],
                            TraceFlags.fromHex(parts[3], 0), TraceState.getDefault());
                    parent = parent.with(Span.wrap(remote));
                }
            }
            Span span = GlobalOpenTelemetry.getTracer("workflow-outbox").spanBuilder("outbox.publish")
                    .setParent(parent).startSpan();
            try (Scope scope = span.makeCurrent()) {
                span.setAttribute("practice.id", practiceId);
                span.setAttribute("event.id", eventId.toString());
                kafka.send(topic, practiceId, (String) row.get("event_json")).get(10, TimeUnit.SECONDS);
                db.update("UPDATE outbox SET published_at = now() WHERE event_id = ?", eventId);
            } catch (Exception error) {
                span.recordException(error);
                log.warn("Outbox publication deferred eventId={} reason={}", eventId, error.getClass().getSimpleName());
                throw new IllegalStateException("Kafka publication failed; the outbox row remains pending", error);
            } finally {
                span.end();
            }
        }
    }
}
