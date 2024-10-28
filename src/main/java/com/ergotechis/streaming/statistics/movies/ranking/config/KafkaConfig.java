package com.ergotechis.streaming.statistics.movies.ranking.config;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;

import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaConfig {

  @Value(value = "${spring.kafka.bootstrap-servers}")
  private String bootstrapAddress;

  @Value(value = "${spring.kafka.streams.state.dir}")
  private String stateStoreLocation;

  @Value(value = "${spring.kafka.streams.state.cache.size:10 * 1024 * 1024L}")
  private int stateStoreCacheSizeInBytes;

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  KafkaStreamsConfiguration kStreamsConfig() {
    return new KafkaStreamsConfiguration(
        Map.of(
            APPLICATION_ID_CONFIG,
            "imdb-streaming-app",
            BOOTSTRAP_SERVERS_CONFIG,
            bootstrapAddress,
            STATE_DIR_CONFIG,
            stateStoreLocation,
            STATESTORE_CACHE_MAX_BYTES_CONFIG,
            stateStoreCacheSizeInBytes));
  }
}
