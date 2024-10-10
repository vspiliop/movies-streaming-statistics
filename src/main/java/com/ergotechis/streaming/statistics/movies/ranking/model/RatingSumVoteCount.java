package com.ergotechis.streaming.statistics.movies.ranking.model;

import lombok.Builder;

@Builder(toBuilder = true)
public record RatingSumVoteCount(
    long ratingSum, long voteCount, String titleId, long currentTotalNumberOfVotes) {}
