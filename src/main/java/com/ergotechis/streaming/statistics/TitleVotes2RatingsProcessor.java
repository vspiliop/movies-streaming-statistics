package com.ergotechis.streaming.statistics;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class TitleVotes2RatingsProcessor {

  private final String titleVoteTopic;

  private final String titleRatingTopic;

  private final JsonSerde<Vote> voteSerde = new JsonSerde<>(Vote.class);

  private final JsonSerde<RatingSumVoteCount> ratingSumVoteCountJsonSerde =
      new JsonSerde<>(RatingSumVoteCount.class);

  private final JsonSerde<RatingAverageVoteCount> ratingAverageVoteCountSerde =
      new JsonSerde<>(RatingAverageVoteCount.class);

  public TitleVotes2RatingsProcessor(
      @Value(value = "${title.vote}") String titleVoteTopic,
      @Value(value = "${title.rating}") String titleRatingTopic) {
    this.titleVoteTopic = titleVoteTopic;
    this.titleRatingTopic = titleRatingTopic;
  }

  @Autowired
  void buildPipeline(StreamsBuilder streamsBuilder) {
    KStream<String, Vote> messageStream =
        streamsBuilder.stream(titleVoteTopic, Consumed.with(Serdes.String(), voteSerde));

    messageStream
        .selectKey((__, vote) -> vote.titleId)
        .repartition(Repartitioned.with(Serdes.String(), voteSerde))
        .peek((key, vote) -> log.info("Vote={}, with key={}", vote, key))
        .groupByKey(Grouped.keySerde(Serdes.String()))
        .aggregate(
            () -> RatingSumVoteCount.builder().build(),
            (__, vote, ratingSumVoteCount) ->
                ratingSumVoteCount.toBuilder()
                    .ratingSum(ratingSumVoteCount.ratingSum + vote.rating)
                    .voteCount(ratingSumVoteCount.voteCount + 1)
                    .titleId(vote.titleId)
                    .build(),
            Materialized.with(Serdes.String(), ratingSumVoteCountJsonSerde))
        .toStream()
        .mapValues(
            ratingSumVoteCount ->
                RatingAverageVoteCount.builder()
                    .titleId(ratingSumVoteCount.titleId)
                    .ratingAverage(
                        (float) ratingSumVoteCount.ratingSum / ratingSumVoteCount.voteCount)
                    .voteCount(ratingSumVoteCount.voteCount)
                    .build())
        .to(titleRatingTopic, Produced.with(Serdes.String(), ratingAverageVoteCountSerde));
  }
}
