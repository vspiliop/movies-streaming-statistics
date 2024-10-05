package com.ergotechis.streaming.statistics;

import org.springframework.boot.SpringApplication;

public class TestStreamingStatisticsApplication {

  public static void main(String[] args) {
    SpringApplication.from(StreamingStatisticsApplication::main)
        .with(TestcontainersConfiguration.class)
        .run(args);
  }
}
