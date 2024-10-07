package com.ergotechis.streaming.statistics;

import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Builder(toBuilder = true)
@Data
@Jacksonized
public class RatingAverageVoteCount {
  float ratingAverage;
  long voteCount;
  String titleId;
  long titleIdCount;
  long currentTotalVotesCounter;
}
