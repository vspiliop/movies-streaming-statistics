package com.ergotechis.streaming.statistics;

import static org.assertj.core.api.Assertions.assertThat;

import com.ergotechis.streaming.statistics.movies.ranking.model.RankingAggregate;
import com.ergotechis.streaming.statistics.movies.ranking.model.RatingAverageVoteCount;
import com.ergotechis.streaming.statistics.movies.ranking.model.Top10RatedMovies;
import com.ergotechis.streaming.statistics.movies.ranking.processor.RankingProcessor;
import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

@Slf4j
public class RankingProcessorTest {

  private RankingProcessor processor;

  private final JsonSerde<Top10RatedMovies> top10MoviesSerde =
      new JsonSerde<>(Top10RatedMovies.class);
  private final JsonSerde<RatingAverageVoteCount> ratingAverageVoteCountSerde =
      new JsonSerde<>(RatingAverageVoteCount.class);

  @BeforeEach
  void setUp() {
    processor = new RankingProcessor("titlerating", "ranking", 1);
  }

  @Test
  void calculateTop2RatedMoviesInRankingDescendingOrder() {

    // given
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    processor.buildPipeline(streamsBuilder);
    Topology topology = streamsBuilder.build();

    try (TopologyTestDriver topologyTestDriver =
        new TopologyTestDriver(topology, new Properties())) {

      TestInputTopic<String, RatingAverageVoteCount> inputTopic =
          topologyTestDriver.createInputTopic(
              "titlerating", new StringSerializer(), ratingAverageVoteCountSerde.serializer());

      TestOutputTopic<String, Top10RatedMovies> outputTopic =
          topologyTestDriver.createOutputTopic(
              "ranking", new StringDeserializer(), top10MoviesSerde.deserializer());

      // when
      inputTopic.pipeInput(
          "tt0000001",
          RatingAverageVoteCount.builder()
              .titleId("tt0000001")
              .voteCount(2) // 2 source vote events aggregated in this RatingAverageVoteCount event
              .ratingAverage(75)
              .currentTotalVotesCounter(2)
              .build());

      // emitted RankingAggregate should be:
      // RankingAggregate(ratingAveragePerTitle=75.0, voteCountPerTitle=2, voteCountTotal=2,
      // titleId=tt0000001, ranking=50.0, titleIdCount=1)
      // averageNumberOfVotesPerTitle = voteCountTotal:2 / titleIdCount:1 = 2
      // rating = (voteCountPerTitle:2/ averageNumberOfVotesPerTitle:2) * ratingAveragePerTitle:75.0
      // = 75

      inputTopic.pipeInput(
          "tt0000002",
          RatingAverageVoteCount.builder()
              .titleId("tt0000002")
              .voteCount(3) // 3 source vote events aggregated in this RatingAverageVoteCount event
              .ratingAverage(90)
              .currentTotalVotesCounter(5) // 5 = 2 + 3 (3 votes emitted for tt0000002)
              .build());

      // emitted RankingAggregate should be:
      // RankingAggregate(ratingAveragePerTitle=75.0, voteCountPerTitle=2, voteCountTotal=3,
      // titleId=tt0000001, ranking=50.0, titleIdCount=1) - Same as above

      // RankingAggregate(ratingAveragePerTitle=90.0, voteCountPerTitle=3, voteCountTotal=5,
      // titleId=tt0000002, ranking=75.0, titleIdCount=2)
      // averageNumberOfVotesPerTitle = voteCountTotal:5 / titleIdCount:2 = 2.5
      // rating = (voteCountPerTitle:3/ averageNumberOfVotesPerTitle:2.5) *
      // ratingAveragePerTitle:90.0
      // = 108

      inputTopic.pipeInput(
          "tt0000001",
          RatingAverageVoteCount.builder()
              .titleId("tt0000001")
              .voteCount(3)
              .ratingAverage(79)
              .currentTotalVotesCounter(6) // = 5 + 1 (one more vote for tt0000001)
              .build());

      // emitted RankingAggregate should be:
      // RankingAggregate(ratingAveragePerTitle=90.0, voteCountPerTitle=3, voteCountTotal=5,
      // titleId=tt0000002, ranking=75.0, titleIdCount=2) - Same as above

      // RankingAggregate(ratingAveragePerTitle=79.0, voteCountPerTitle=3, voteCountTotal=6,
      // titleId=tt0000001, ranking=79.0, titleIdCount=2)
      // averageNumberOfVotesPerTitle = voteCountTotal:6 / titleIdCount:2 = 3
      // rating = (voteCountPerTitle:3/ averageNumberOfVotesPerTitle:3) *
      // ratingAveragePerTitle:79.0
      // = 79

      inputTopic.pipeInput(
          "tt0000002",
          RatingAverageVoteCount.builder()
              .titleId("tt0000002")
              .voteCount(4) // = 3 + 1 (one more vote emitted for tt0000002)
              .ratingAverage(95)
              .currentTotalVotesCounter(7)
              .build());

      // emitted RankingAggregate should be:
      // RankingAggregate(ratingAveragePerTitle=79.0, voteCountPerTitle=3, voteCountTotal=6,
      // titleId=tt0000001, ranking=79.0, titleIdCount=2)

      // RankingAggregate(ratingAveragePerTitle=95.0, voteCountPerTitle=4, voteCountTotal=7,
      // titleId=tt0000002, ranking=108.571429, titleIdCount=2)
      // averageNumberOfVotesPerTitle = voteCountTotal:7 / titleIdCount:2 = 3.5
      // rating = (voteCountPerTitle:4/ averageNumberOfVotesPerTitle:3.5) *
      // ratingAveragePerTitle:95.0
      // = 1.14285714 * 95
      // = 108.571429

      // then
      List<Top10RatedMovies> rankings = outputTopic.readValuesToList();
      log.info("rankings={}", rankings);

      var ranking1 = new Top10RatedMovies();
      RankingAggregate rankingAggregate1 =
          RankingAggregate.builder()
              .ratingAveragePerTitle(75)
              .voteCountPerTitle(2)
              .voteCountTotal(2)
              .titleId("tt0000001")
              .ranking(75)
              .titleIdCount(1)
              .build();
      ranking1.add(rankingAggregate1);

      var ranking2 = new Top10RatedMovies();
      RankingAggregate rankingAggregate2 =
          RankingAggregate.builder()
              .ratingAveragePerTitle(90)
              .voteCountPerTitle(3)
              .voteCountTotal(5)
              .titleId("tt0000002")
              .ranking(108.00001f)
              .titleIdCount(2)
              .build();
      ranking2.add(rankingAggregate1);
      ranking2.add(rankingAggregate2);

      var ranking3 = new Top10RatedMovies();
      RankingAggregate rankingAggregate3 =
          RankingAggregate.builder()
              .ratingAveragePerTitle(79)
              .voteCountPerTitle(3)
              .voteCountTotal(6)
              .titleId("tt0000001")
              .ranking(79)
              .titleIdCount(2)
              .build();
      ranking3.add(rankingAggregate2);
      ranking3.add(rankingAggregate3);

      var ranking4 = new Top10RatedMovies();
      RankingAggregate rankingAggregate4 =
          RankingAggregate.builder()
              .ratingAveragePerTitle(95)
              .voteCountPerTitle(4)
              .voteCountTotal(7)
              .titleId("tt0000002")
              .ranking(108.571434F)
              .titleIdCount(2)
              .build();

      ranking4.add(rankingAggregate4);
      ranking4.add(rankingAggregate3);

      assertThat(rankings).containsAll(List.of(ranking1, ranking2, ranking3, ranking4));
    }
  }
}
