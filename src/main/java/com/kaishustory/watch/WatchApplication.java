package com.kaishustory.watch;

import lombok.SneakyThrows;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class WatchApplication {

	@SneakyThrows
	public static void main(String[] args) {

		SpringApplication.run(WatchApplication.class, args);

	}
}
