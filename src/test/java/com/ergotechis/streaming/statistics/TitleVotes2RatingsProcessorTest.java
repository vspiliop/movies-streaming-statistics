package com.ergotechis.streaming.statistics;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

@Slf4j
public class TitleVotes2RatingsProcessorTest {

  private TitleVotes2RatingsProcessor processor;

  private final JsonSerde<Vote> voteSerde = new JsonSerde<>(Vote.class);
  private final JsonSerde<RatingAverageVoteCount> ratingAverageVoteCountSerde =
      new JsonSerde<>(RatingAverageVoteCount.class);

  @BeforeEach
  void setUp() {
    processor = new TitleVotes2RatingsProcessor("vote", "titlerating");
  }

  @Test
  void calculateRunningAverageOfVotes() {

    // given
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    processor.buildPipeline(streamsBuilder);
    Topology topology = streamsBuilder.build();

    try (TopologyTestDriver topologyTestDriver =
        new TopologyTestDriver(topology, new Properties())) {

      TestInputTopic<String, Vote> inputTopic =
          topologyTestDriver.createInputTopic(
              "vote", new StringSerializer(), voteSerde.serializer());

      TestOutputTopic<String, RatingAverageVoteCount> outputTopic =
          topologyTestDriver.createOutputTopic(
              "titlerating", new StringDeserializer(), ratingAverageVoteCountSerde.deserializer());

      // when
      inputTopic.pipeInput(Vote.builder().titleId("tt0000001").rating(100).build());
      inputTopic.pipeInput(Vote.builder().titleId("tt0000002").rating(2).build());
      inputTopic.pipeInput(Vote.builder().titleId("tt0000001").rating(50).build());
      inputTopic.pipeInput(Vote.builder().titleId("tt0000002").rating(1).build());
      inputTopic.pipeInput(Vote.builder().titleId("tt0000002").rating(10).build());

      // then
      List<KeyValue<String, RatingAverageVoteCount>> streamOfAverageCounts =
          outputTopic.readKeyValuesToList();
      log.info("v={}", streamOfAverageCounts);

      assertThat(streamOfAverageCounts)
          .containsAll(
              List.of(
                  KeyValue.pair(
                      "tt0000001",
                      RatingAverageVoteCount.builder()
                          .titleId("tt0000001")
                          .voteCount(2)
                          .ratingAverage(75)
                          .currentTotalVotesCounter(3)
                          .build()),
                  KeyValue.pair(
                      "tt0000002",
                      RatingAverageVoteCount.builder()
                          .titleId("tt0000002")
                          .voteCount(3)
                          .ratingAverage(4.3333335f)
                          .currentTotalVotesCounter(5)
                          .build())));
    }
  }
}
