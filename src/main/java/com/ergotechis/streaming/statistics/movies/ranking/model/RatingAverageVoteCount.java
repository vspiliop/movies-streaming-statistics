package com.ergotechis.streaming.statistics.movies.ranking.model;

import lombok.Builder;

@Builder(toBuilder = true)
public record RatingAverageVoteCount(
    float ratingAverage,
    long voteCount,
    String titleId,
    long titleIdCount,
    long currentTotalVotesCounter) {}
