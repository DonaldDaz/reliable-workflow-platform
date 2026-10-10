package com.example.workflow;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/api/practices")
@CrossOrigin(origins = "${CONSOLE_ORIGIN:http://localhost:4200}")
public class PracticeController {
    private final PracticeService service;

    public PracticeController(PracticeService service) {
        this.service = service;
    }

    @PostMapping
    public Map<String, UUID> create(@RequestBody Map<String, String> input) {
        return Map.of("id", service.create(input.get("title")));
    }

    @GetMapping
    public List<Map<String, Object>> list() {
        return service.list();
    }

    @GetMapping("/{id}")
    public Map<String, Object> detail(@PathVariable UUID id) {
        return service.detail(id);
    }

    @PostMapping(value = "/{id}/documents", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Map<String, UUID> upload(@PathVariable UUID id, @RequestPart("file") MultipartFile file) throws IOException {
        return Map.of("id", service.upload(id, file.getOriginalFilename(), file.getContentType(), file.getBytes()));
    }

    @PostMapping("/{id}/approve")
    public Map<String, String> approve(@PathVariable UUID id) {
        service.approve(id);
        return Map.of("status", "APPROVED");
    }
}
