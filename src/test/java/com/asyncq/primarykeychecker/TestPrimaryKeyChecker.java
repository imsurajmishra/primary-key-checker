package com.asyncq.primarykeychecker;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import com.asyncq.primarykeychecker.jobs.MissingPrimaryKeyChecker;

@ExtendWith(OutputCaptureExtension.class)
@SpringBootTest
public class TestPrimaryKeyChecker {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private MissingPrimaryKeyChecker job;

    @BeforeEach
    public void setup() throws SQLException {
        createTestTable();
    }

    @AfterEach
    public void tearDown() throws SQLException {
        deleteTestTable();
    }

    @Test
    @DisplayName("logs error log for missing primary key tables")
    public void testCheck(CapturedOutput output) {
        job.check();
        assertTrue(output.getAll().contains("Found table/s without primary key. table list :"));
    }

    @Test
    @DisplayName("returns the tables that are missing primary key")
    public void testGetTablesWithoutPrimaryKey(){
        List<String> tablesWithoutPrimaryKey = job.getTablesWithoutPrimaryKey();
        System.out.println(tablesWithoutPrimaryKey);
        assertTrue(tablesWithoutPrimaryKey.contains("sample_table".toUpperCase()));
        assertFalse(tablesWithoutPrimaryKey.contains("sample_table_1".toUpperCase()));
    }

    private void createTestTable() {
        jdbcTemplate.execute(
                """
                            CREATE TABLE IF NOT EXISTS public.sample_table (
                               employee_id SERIAL,
                               first_name VARCHAR(50),
                               last_name VARCHAR(50),
                               email VARCHAR(100)
                            )
                        """
        );

        jdbcTemplate.execute(
                """
                            CREATE TABLE IF NOT EXISTS public.sample_table_1 (
                               employee_id SERIAL PRIMARY KEY,
                               first_name VARCHAR(50),
                               last_name VARCHAR(50),
                               email VARCHAR(100)
                            )
                        """
        );
    }

    private void deleteTestTable() {
        Stream.of("sample_table", "sample_table_1").forEach( t ->
            jdbcTemplate.execute(
                    """
                                  DROP TABLE public.%s
                         """.formatted(t)
            )
        );
    }
}