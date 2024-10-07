package com.ergotechis.streaming.statistics.movies.ranking.processor;

import com.ergotechis.streaming.statistics.movies.ranking.model.RatingAverageVoteCount;
import com.ergotechis.streaming.statistics.movies.ranking.model.RatingSumVoteCount;
import com.ergotechis.streaming.statistics.movies.ranking.model.Vote;
import com.ergotechis.streaming.statistics.movies.ranking.transformer.TotalVotesCounter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class TitleVotes2RatingsProcessor {

  public static final String TOTAL_VOTES_COUNTER_STORE = "totalVotesCounterStore";
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

  @SuppressWarnings("deprecation")
  @Autowired
  public void buildPipeline(StreamsBuilder streamsBuilder) {

    StoreBuilder<KeyValueStore<String, Long>> totalVotesCounterStoreBuilder =
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(TOTAL_VOTES_COUNTER_STORE),
            Serdes.String(),
            Serdes.Long());

    streamsBuilder.addStateStore(totalVotesCounterStoreBuilder);

    KStream<String, Vote> messageStream =
        streamsBuilder.stream(titleVoteTopic, Consumed.with(Serdes.String(), voteSerde));

    messageStream
        .selectKey((__, vote) -> vote.getTitleId())
        .repartition(Repartitioned.with(Serdes.String(), voteSerde))
        .peek((key, vote) -> log.info("Vote={}, with key={}", vote, key))
        .transformValues(
            () -> new TotalVotesCounter(TOTAL_VOTES_COUNTER_STORE), TOTAL_VOTES_COUNTER_STORE)
        .groupByKey(Grouped.keySerde(Serdes.String()))
        .aggregate(
            () -> RatingSumVoteCount.builder().build(),
            (__, vote, ratingSumVoteCount) ->
                ratingSumVoteCount.toBuilder()
                    .ratingSum(ratingSumVoteCount.getRatingSum() + vote.getRating())
                    .voteCount(ratingSumVoteCount.getVoteCount() + 1)
                    .titleId(vote.getTitleId())
                    .currentTotalNumberOfVotes(vote.getCurrentTotalVotesCounter())
                    .build(),
            Materialized.with(Serdes.String(), ratingSumVoteCountJsonSerde))
        .toStream()
        .mapValues(
            ratingSumVoteCount ->
                RatingAverageVoteCount.builder()
                    .titleId(ratingSumVoteCount.getTitleId())
                    .currentTotalVotesCounter(ratingSumVoteCount.getCurrentTotalNumberOfVotes())
                    .ratingAverage(
                        (float) ratingSumVoteCount.getRatingSum() / ratingSumVoteCount.getVoteCount())
                    .voteCount(ratingSumVoteCount.getVoteCount())
                    .build())
        .to(titleRatingTopic, Produced.with(Serdes.String(), ratingAverageVoteCountSerde));
  }
}
