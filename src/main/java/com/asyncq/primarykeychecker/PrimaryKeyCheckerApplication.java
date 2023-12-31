package com.asyncq.primarykeychecker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class PrimaryKeyCheckerApplication {

	public static void main(String[] args) {
		SpringApplication.run(PrimaryKeyCheckerApplication.class, args);
	}

}
