package com.ergotechis.streaming.statistics.movies.ranking.model;

import static org.assertj.core.api.Assertions.assertThat;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class Top10RatedMoviesTest {

  @Test
  void removeOverflowMoviesWithLowestRanking() {

    // given
    var top10RatedMovies = new Top10RatedMovies();

    // when
    top10RatedMovies.add(RankingAggregate.builder().titleId("1").ranking(1).build());
    top10RatedMovies.add(RankingAggregate.builder().titleId("2").ranking(2).build());
    top10RatedMovies.add(RankingAggregate.builder().titleId("3").ranking(3).build());
    top10RatedMovies.add(RankingAggregate.builder().titleId("4").ranking(4).build());
    top10RatedMovies.add(RankingAggregate.builder().titleId("5").ranking(5).build());
    top10RatedMovies.add(RankingAggregate.builder().titleId("6").ranking(6).build());
    top10RatedMovies.add(RankingAggregate.builder().titleId("7").ranking(7).build());
    top10RatedMovies.add(RankingAggregate.builder().titleId("8").ranking(8).build());
    top10RatedMovies.add(RankingAggregate.builder().titleId("9").ranking(9).build());
    top10RatedMovies.add(RankingAggregate.builder().titleId("10").ranking(10).build());
    top10RatedMovies.add(RankingAggregate.builder().titleId("11").ranking(11).build());
    top10RatedMovies.add(RankingAggregate.builder().titleId("12").ranking(12).build());

    // then
    log.info("Top10RatedMovies={}", top10RatedMovies.getTop10RatedMoviesSorted());
    assertThat(top10RatedMovies.getTop10RatedMoviesSorted())
        .doesNotContain("""
                "ranking":1.0""")
        .doesNotContain("""
                "ranking":2.0""")
        .contains("""
                "ranking":12.0""")
        .contains("""
                "ranking":11.0""")
        .contains("""
                "ranking":10.0""")
        .contains("""
                "ranking":9.0""")
        .contains("""
                "ranking":8.0""")
        .contains("""
                "ranking":7.0""")
        .contains("""
                "ranking":6.0""")
        .contains("""
                "ranking":5.0""")
        .contains("""
                "ranking":4.0""")
        .contains("""
                "ranking":3.0""");
  }

  @Test
  void canHoldMultipleMoviesWithSameRating() {

    // given
    var top10RatedMovies = new Top10RatedMovies();

    // when
    top10RatedMovies.add(RankingAggregate.builder().titleId("1").ranking(1).build());
    top10RatedMovies.add(RankingAggregate.builder().titleId("2").ranking(2).build());
    top10RatedMovies.add(RankingAggregate.builder().titleId("3").ranking(12).build());
    top10RatedMovies.add(RankingAggregate.builder().titleId("4").ranking(12).build());
    top10RatedMovies.add(RankingAggregate.builder().titleId("5").ranking(12).build());
    top10RatedMovies.add(RankingAggregate.builder().titleId("6").ranking(12).build());
    top10RatedMovies.add(RankingAggregate.builder().titleId("7").ranking(12).build());
    top10RatedMovies.add(RankingAggregate.builder().titleId("8").ranking(12).build());
    top10RatedMovies.add(RankingAggregate.builder().titleId("9").ranking(12).build());
    top10RatedMovies.add(RankingAggregate.builder().titleId("10").ranking(12).build());
    top10RatedMovies.add(RankingAggregate.builder().titleId("11").ranking(12).build());
    top10RatedMovies.add(RankingAggregate.builder().titleId("12").ranking(12).build());

    // then
    log.info("Top10RatedMovies={}", top10RatedMovies.getTop10RatedMoviesSorted());
    assertThat(top10RatedMovies.getTop10RatedMoviesSorted())
        .doesNotContain("""
                "ranking":1.0""")
        .doesNotContain("""
                "ranking":2.0""")
        .contains("""
                "titleId":"3","ranking":12.0""")
        .contains("""
                "titleId":"4","ranking":12.0""")
        .contains("""
                "titleId":"5","ranking":12.0""")
        .contains("""
                "titleId":"6","ranking":12.0""")
        .contains("""
                "titleId":"7","ranking":12.0""")
        .contains("""
                "titleId":"8","ranking":12.0""")
        .contains("""
                "titleId":"9","ranking":12.0""")
        .contains("""
                "titleId":"10","ranking":12.0""")
        .contains("""
                "titleId":"11","ranking":12.0""")
        .contains("""
                "titleId":"12","ranking":12.0""");
  }
}
