package com.ergotechis.streaming.statistics;

import static org.assertj.core.api.Assertions.assertThat;

import com.ergotechis.streaming.statistics.movies.ranking.model.RatingAverageVoteCount;
import com.ergotechis.streaming.statistics.movies.ranking.model.Top10RatedMovies;
import com.ergotechis.streaming.statistics.movies.ranking.model.Vote;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
@Slf4j
class End2EndTests {

  @Container
  private static final KafkaContainer KAFKA =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));

  @TempDir private static File tempDir;

  @Autowired private KafkaTemplate<String, Vote> voteKafkaTemplate;

  @Value(value = "${title.vote}")
  private String titleVoteTopic;

  private List<Top10RatedMovies> rankings;

  @DynamicPropertySource
  static void registerKafkaProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    registry.add("spring.kafka.streams.state.dir", tempDir::getAbsolutePath);
  }

  @BeforeEach
  public void setUp() {
    rankings = new ArrayList<>();
  }

  @Test
  @DisplayName("Given ratings calculate the top 10 currently rated movies in descending order")
  void happyPath() {
    // given
    var votes =
        List.of(
            Vote.builder().titleId("tt0000001").rating(100).build(),
            Vote.builder().titleId("tt0000002").rating(25).build(),
            Vote.builder().titleId("tt0000002").rating(75).build(),
            Vote.builder().titleId("tt0000001").rating(50).build(),
            Vote.builder().titleId("tt0000001").rating(25).build(),
            Vote.builder().titleId("tt0000001").rating(50).build());

    // when
    votes.forEach(
        vote -> {
          voteKafkaTemplate.send(titleVoteTopic, vote);
          log.info("Produced={}", vote);
        });

    // then
    Awaitility.await().untilAsserted(() -> assertThat(rankings).hasSize(4));
    Awaitility.await()
        .untilAsserted(
            () ->
                assertThat(rankings)
                    .last()
                    .extracting(Top10RatedMovies::getTop10RatedMoviesSorted)
                    .isEqualTo(
                        """
            [{"ratingAveragePerTitle":56.25,"voteCountPerTitle":4,"voteCountTotal":6,"titleId":"tt0000001","ranking":75.0,"titleIdCount":2},\
            {"ratingAveragePerTitle":50.0,"voteCountPerTitle":2,"voteCountTotal":3,"titleId":"tt0000002","ranking":66.66667,"titleIdCount":2}]\
            """));
  }

  @KafkaListener(id = "testRankingsListener", topics = "${title.ranking}")
  public void rankingsListener(Top10RatedMovies top10RatedMovies) {
    log.info("Top10RatedMovies={}", top10RatedMovies);
    rankings.add(top10RatedMovies);
  }

  @KafkaListener(id = "testRatingsListener", topics = "${title.rating}")
  public void ratingsListen(RatingAverageVoteCount ratingSumVoteCount) {
    log.info("RatingAverageVoteCount={}", ratingSumVoteCount);
  }
}
