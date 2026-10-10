package com.example.workflow;

import java.io.IOException;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class DemoSeed implements ApplicationRunner {
    private final PracticeService service;
    private final JdbcTemplate db;

    public DemoSeed(PracticeService service, JdbcTemplate db) {
        this.service = service;
        this.db = db;
    }

    @Override
    public void run(ApplicationArguments args) throws IOException {
        Integer count = db.queryForObject("SELECT count(*) FROM practices WHERE title = ?", Integer.class,
                "Northstar Lab access renewal");
        if (count != null && count > 0) return;
        var id = service.create("Northstar Lab access renewal");
        byte[] content;
        try (var stream = new ClassPathResource("demo/access-request.txt").getInputStream()) {
            content = stream.readAllBytes();
        }
        service.upload(id, "access-request.txt", "text/plain", content);
        service.approve(id);
    }
}
