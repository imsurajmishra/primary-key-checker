package com.asyncq.primarykeychecker.jobs;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class MissingPrimaryKeyChecker {

    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public MissingPrimaryKeyChecker(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    Logger logger = LoggerFactory.getLogger(this.getClass());

    @Scheduled(cron="0 * * * * *")
    public void check(){
        logger.info("checking tables with missing primary key");
        List<String> tablesWithoutPrimaryKey = getTablesWithoutPrimaryKey();
        System.out.println(tablesWithoutPrimaryKey);
        if (!tablesWithoutPrimaryKey.isEmpty()) {
            logger.error("Found table/s without primary key. table list : [" +
                    String.join(",", tablesWithoutPrimaryKey) + "]"
            );
        }
    }

    public List<String> getTablesWithoutPrimaryKey() {
        String sql =  """
                SELECT table_name
                FROM information_schema.tables
                WHERE lower(table_schema) = 'public'
                AND table_name NOT IN (
                    SELECT table_name
                    FROM information_schema.table_constraints
                    WHERE constraint_type = 'PRIMARY KEY'
                    AND lower(table_schema) = 'public'
                )
                """;
        return jdbcTemplate.queryForList(sql, String.class);
    }
}
