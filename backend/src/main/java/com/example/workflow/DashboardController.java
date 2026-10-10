package com.example.workflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/dashboard")
@CrossOrigin(origins = "${CONSOLE_ORIGIN:http://localhost:4200}")
public class DashboardController {
    private final MongoTemplate mongo;
    private final PracticeService practices;
    private final ProjectionConsumer projection;
    private final KafkaListenerEndpointRegistry listeners;
    private final ObjectMapper json;
    private final String bootstrap;
    private final String topic;
    private final JdbcTemplate db;
    private final KafkaTemplate<String, String> kafka;

    public DashboardController(MongoTemplate mongo, PracticeService practices, ProjectionConsumer projection,
                               KafkaListenerEndpointRegistry listeners, ObjectMapper json,
                               JdbcTemplate db, KafkaTemplate<String, String> kafka,
                               @Value("${spring.kafka.bootstrap-servers}") String bootstrap,
                               @Value("${workflow.topic}") String topic) {
        this.mongo = mongo;
        this.practices = practices;
        this.projection = projection;
        this.listeners = listeners;
        this.json = json;
        this.db = db;
        this.kafka = kafka;
        this.bootstrap = bootstrap;
        this.topic = topic;
    }

    @GetMapping
    public List<ProjectionState> list() {
        return mongo.findAll(ProjectionState.class);
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        MessageListenerContainer container = listeners.getListenerContainer("projection");
        return Map.of("consumerPaused", container.isContainerPaused(), "pendingOutbox", practices.pendingOutbox(),
                "injectedFailures", projection.injectedFailures(), "duplicateDeliveries", projection.duplicateDeliveries());
    }

    @PostMapping("/pause")
    public Map<String, String> pause() {
        listeners.getListenerContainer("projection").pause();
        return Map.of("consumer", "paused");
    }

    @PostMapping("/resume")
    public Map<String, String> resume() {
        listeners.getListenerContainer("projection").resume();
        return Map.of("consumer", "running");
    }

    @PostMapping("/fail-next")
    public Map<String, String> failNext() {
        projection.failNext();
        return Map.of("failure", "armed");
    }

    @PostMapping("/duplicate-last/{id}")
    public Map<String, String> duplicateLast(@PathVariable UUID id) throws Exception {
        List<String> rows = db.queryForList("SELECT event_json::text FROM outbox WHERE practice_id = ? ORDER BY sequence DESC LIMIT 1", String.class, id);
        if (rows.isEmpty()) throw new IllegalArgumentException("Practice not found");
        kafka.send(topic, id.toString(), rows.getFirst()).get(10, TimeUnit.SECONDS);
        return Map.of("duplicate", "sent");
    }

    @PostMapping("/rebuild")
    public synchronized Map<String, Object> rebuild() {
        MessageListenerContainer listener = listeners.getListenerContainer("projection");
        boolean wasPaused = listener.isPauseRequested();
        listener.stop();
        int replayed = 0;
        try {
            mongo.dropCollection(ProjectionState.class);
            Properties props = new Properties();
            props.put("bootstrap.servers", bootstrap);
            props.put("group.id", "workflow-rebuild-" + UUID.randomUUID());
            props.put("key.deserializer", StringDeserializer.class.getName());
            props.put("value.deserializer", StringDeserializer.class.getName());
            props.put("enable.auto.commit", "false");
            props.put("auto.offset.reset", "earliest");
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                TopicPartition partition = new TopicPartition(topic, 0);
                consumer.assign(List.of(partition));
                consumer.seekToBeginning(List.of(partition));
                long end = consumer.endOffsets(List.of(partition)).get(partition);
                while (consumer.position(partition) < end) {
                    var records = consumer.poll(Duration.ofSeconds(1));
                    for (var record : records) {
                        projection.apply(json.readValue(record.value(), PracticeEvent.class));
                        replayed++;
                    }
                }
            }
            return Map.of("replayedEvents", replayed, "views", mongo.count(new org.springframework.data.mongodb.core.query.Query(), ProjectionState.class));
        } catch (Exception error) {
            throw new IllegalStateException("Projection replay failed: " + error.getClass().getSimpleName(), error);
        } finally {
            listener.start();
            if (wasPaused) listener.pause();
        }
    }
}
