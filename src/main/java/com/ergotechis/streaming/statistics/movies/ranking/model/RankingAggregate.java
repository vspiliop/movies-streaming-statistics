package com.ergotechis.streaming.statistics.movies.ranking.model;

import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Builder(toBuilder = true)
@Data
@Jacksonized
public class RankingAggregate {
  float ratingAveragePerTitle;
  long voteCountPerTitle;
  long voteCountTotal;
  String titleId;
  float ranking;
  long titleIdCount;
}
