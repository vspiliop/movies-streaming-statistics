package com.ergotechis.streaming.statistics;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.TreeSet;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;

@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Top10RatedMovies {

  private final ObjectMapper objectMapper = new ObjectMapper();

  @EqualsAndHashCode.Include
  private final TreeSet<RankingAggregate> top10RatedMoviesSorted =
      new TreeSet<>((o1, o2) -> (int) (o2.getRanking() - o1.getRanking()));

  public void add(RankingAggregate newValue) {
    top10RatedMoviesSorted.add(newValue);
    if (top10RatedMoviesSorted.size() > 10) {
      top10RatedMoviesSorted.remove(top10RatedMoviesSorted.last());
    }
  }

  public void remove(RankingAggregate oldValue) {
    top10RatedMoviesSorted.remove(oldValue);
  }

  @SneakyThrows
  @JsonProperty("top10RatedMoviesSorted")
  public String getTop10RatedMoviesSorted() {
    return objectMapper.writeValueAsString(top10RatedMoviesSorted);
  }

  @SneakyThrows
  @JsonProperty("top10RatedMoviesSorted")
  public void setTop10RatedMoviesSorted(String top10RatedMoviesSorted) {
    RankingAggregate[] top10RatedMovies =
        objectMapper.readValue(top10RatedMoviesSorted, RankingAggregate[].class);
    for (RankingAggregate i : top10RatedMovies) {
      add(i);
    }
  }

  @Override
  public String toString() {
    return "Top10RatedMovies{" + top10RatedMoviesSorted + '}';
  }
}
