package com.ergotechis.streaming.statistics.movies.ranking.model;

import lombok.Builder;

@Builder(toBuilder = true)
public record Vote(long rating, String titleId, long currentTotalVotesCounter) {}
