package com.ergotechis.streaming.statistics.config;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;

import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaConfig {

  @Value(value = "${spring.kafka.bootstrap-servers}")
  private String bootstrapAddress;

  @Value(value = "${spring.kafka.streams.state.dir}")
  private String stateStoreLocation;

  @Value(value = "${title.vote}")
  private String titleVoteTopic;

  @Value(value = "${title.rating}")
  private String titleRatingTopic;

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  KafkaStreamsConfiguration kStreamsConfig() {

    return new KafkaStreamsConfiguration(
        Map.of(
            APPLICATION_ID_CONFIG,
            "imdb-streaming-app",
            BOOTSTRAP_SERVERS_CONFIG,
            bootstrapAddress,
            STATE_DIR_CONFIG,
            stateStoreLocation));
  }

  @Bean
  NewTopic voteTopic() {
    return TopicBuilder.name(titleVoteTopic).partitions(1).replicas(1).build();
  }

  @Bean
  NewTopic ratingTopic() {
    return TopicBuilder.name(titleRatingTopic).partitions(1).replicas(1).build();
  }
}
