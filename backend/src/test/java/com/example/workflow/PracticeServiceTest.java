package com.example.workflow;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

class PracticeServiceTest {
    @Test
    void invalidCreationNeverTouchesDatabase() {
        JdbcTemplate db = mock(JdbcTemplate.class);
        PracticeService service = new PracticeService(db, new ObjectMapper());
        assertThrows(IllegalArgumentException.class, () -> service.create(" "));
        verifyNoInteractions(db);
    }

    @Test
    void approvalRequiresDocumentedState() {
        JdbcTemplate db = mock(JdbcTemplate.class);
        when(db.query(anyString(), any(org.springframework.jdbc.core.RowMapper.class), any(UUID.class)))
                .thenReturn(List.of());
        PracticeService service = new PracticeService(db, new ObjectMapper());
        assertThrows(IllegalArgumentException.class, () -> service.approve(UUID.randomUUID()));
    }
}
