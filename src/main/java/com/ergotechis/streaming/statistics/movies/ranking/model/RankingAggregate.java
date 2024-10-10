package com.ergotechis.streaming.statistics.movies.ranking.model;

import lombok.Builder;

@Builder(toBuilder = true)
public record RankingAggregate(
    float ratingAveragePerTitle,
    long voteCountPerTitle,
    long voteCountTotal,
    String titleId,
    float ranking,
    long titleIdCount) {}
