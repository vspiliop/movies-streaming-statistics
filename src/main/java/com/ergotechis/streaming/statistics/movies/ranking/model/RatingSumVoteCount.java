package com.ergotechis.streaming.statistics.movies.ranking.model;

import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Builder(toBuilder = true)
@Data
@Jacksonized
public class RatingSumVoteCount {
  long ratingSum;
  long voteCount;
  String titleId;
  long currentTotalNumberOfVotes;
}
