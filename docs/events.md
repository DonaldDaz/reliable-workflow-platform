# Event flow

```mermaid
sequenceDiagram
    actor User
    participant API as Spring Boot API
    participant PG as PostgreSQL
    participant Publisher as Outbox publisher
    participant Kafka
    participant Consumer as Projection consumer
    participant MongoDB
    participant Console as Angular console

    User->>API: Create practice
    API->>PG: BEGIN; practice + PracticeCreated + outbox; COMMIT
    API-->>User: Practice ID and authoritative status
    Publisher->>PG: Lock pending outbox row
    Publisher->>Kafka: Publish PracticeCreated (key = practice ID)
    Kafka-->>Publisher: Acknowledged
    Publisher->>PG: Mark outbox published; COMMIT
    Kafka->>Consumer: Deliver event (at least once)
    Consumer->>MongoDB: Apply next sequence with optimistic version
    Consumer-->>Kafka: Commit consumer offset
    Console->>API: Read PostgreSQL practice and MongoDB view
    API-->>Console: Authoritative version + projected version

    User->>API: Upload document
    API->>PG: Document bytes + DOCUMENTED + DocumentUploaded + outbox
    User->>API: Approve practice
    API->>PG: APPROVED + PracticeApproved + outbox
```

The same pattern applies to all three events. A Kafka acknowledgement can precede a PostgreSQL failure when marking an outbox row. The row will be sent again; the consumer checks the per-practice sequence and event ID so the read model remains unchanged. An event with a higher sequence waits inside the MongoDB view until the missing sequence arrives. A conflicting event ID for the same pending sequence is rejected and retried, making the conflict visible rather than silently choosing a winner.

One Kafka partition keeps the event stream in order for this small deployment. The sequence check still protects the projection during manual replay and duplicate delivery. Scaling to several partitions would keep `practiceId` as the Kafka key and require the outbox claim query to preserve per-practice order across publisher instances.
