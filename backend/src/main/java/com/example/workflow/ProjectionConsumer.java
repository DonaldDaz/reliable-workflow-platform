package com.example.workflow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.trace.Span;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ProjectionConsumer {
    private static final Logger log = LoggerFactory.getLogger(ProjectionConsumer.class);
    private final MongoTemplate mongo;
    private final ObjectMapper json;
    private final AtomicBoolean failNext = new AtomicBoolean();
    private final AtomicInteger injectedFailures = new AtomicInteger();
    private final AtomicInteger duplicateDeliveries = new AtomicInteger();

    public ProjectionConsumer(MongoTemplate mongo, ObjectMapper json) {
        this.mongo = mongo;
        this.json = json;
    }

    @KafkaListener(id = "projection", topics = "${workflow.topic}")
    public void consume(String value) throws JsonProcessingException {
        PracticeEvent event = json.readValue(value, PracticeEvent.class);
        Span.current().setAttribute("practice.id", event.practiceId().toString());
        if (failNext.compareAndSet(true, false)) {
            injectedFailures.incrementAndGet();
            log.warn("Injected transient projection failure eventId={}", event.eventId());
            throw new IllegalStateException("Injected transient projection failure");
        }
        apply(event);
    }

    public void apply(PracticeEvent event) {
        for (int attempt = 0; attempt < 8; attempt++) {
            ProjectionState state = mongo.findById(event.practiceId().toString(), ProjectionState.class);
            boolean exists = state != null;
            if (!exists) state = new ProjectionState(event.practiceId().toString());
            if (!ProjectionReducer.accept(state, event)) {
                duplicateDeliveries.incrementAndGet();
                return;
            }
            try {
                if (exists) mongo.save(state);
                else mongo.insert(state);
                log.info("Projected eventId={} practiceId={} sequence={}", event.eventId(), event.practiceId(), event.sequence());
                return;
            } catch (DuplicateKeyException | OptimisticLockingFailureException collision) {
                // Another delivery won the write; reload and reduce again.
            }
        }
        throw new IllegalStateException("Projection write contention exceeded retry limit");
    }

    public void failNext() {
        failNext.set(true);
    }

    public int injectedFailures() {
        return injectedFailures.get();
    }

    public int duplicateDeliveries() {
        return duplicateDeliveries.get();
    }
}
