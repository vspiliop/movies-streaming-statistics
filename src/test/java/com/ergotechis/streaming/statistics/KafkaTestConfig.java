package com.ergotechis.streaming.statistics;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@Profile("test")
public class KafkaTestConfig {

  @Value(value = "${title.vote}")
  private String titleVoteTopic;

  @Value(value = "${title.rating}")
  private String titleRatingTopic;

  @Value(value = "${title.ranking}")
  private String rankingTopic;

  @Bean
  NewTopic voteTopic() {
    return TopicBuilder.name(titleVoteTopic).partitions(1).replicas(1).build();
  }

  @Bean
  NewTopic ratingTopic() {
    return TopicBuilder.name(titleRatingTopic).partitions(1).replicas(1).build();
  }

  @Bean
  NewTopic rankingTopic() {
    return TopicBuilder.name(rankingTopic).partitions(1).replicas(1).build();
  }
}
