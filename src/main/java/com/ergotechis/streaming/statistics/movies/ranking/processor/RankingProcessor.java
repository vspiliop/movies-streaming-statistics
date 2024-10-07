package com.ergotechis.streaming.statistics.movies.ranking.processor;

import com.ergotechis.streaming.statistics.movies.ranking.model.RankingAggregate;
import com.ergotechis.streaming.statistics.movies.ranking.model.RatingAverageVoteCount;
import com.ergotechis.streaming.statistics.movies.ranking.model.Top10RatedMovies;
import com.ergotechis.streaming.statistics.movies.ranking.transformer.CountUniqueTitles;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class RankingProcessor {

  public static final String TITLE_STORE = "titleStore";
  private static final String COUNTER_STORE = "counterStore";
  private final String titleRatingTopic;
  private final String titleRankingTopic;
  private long minimumVotesRequired;

  private final JsonSerde<RatingAverageVoteCount> ratingAverageVoteCountSerde =
      new JsonSerde<>(RatingAverageVoteCount.class);

  private final JsonSerde<RankingAggregate> rankingAggregateSerde =
      new JsonSerde<>(RankingAggregate.class);

  private final JsonSerde<Top10RatedMovies> top10MoviesSerde =
      new JsonSerde<>(Top10RatedMovies.class);

  public RankingProcessor(
      @Value(value = "${title.rating}") String titleRatingTopic,
      @Value(value = "${title.ranking}") String titleRankingTopic,
      @Value(value = "${ranking.votes.min.threshold:500}") long minimumVotesRequired) {
    this.titleRatingTopic = titleRatingTopic;
    this.titleRankingTopic = titleRankingTopic;
    this.minimumVotesRequired = minimumVotesRequired;
  }

  @SuppressWarnings("deprecation")
  @Autowired
  public void buildPipeline(StreamsBuilder streamsBuilder) {

    StoreBuilder<KeyValueStore<String, String>> titleStoreBuilder =
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(TITLE_STORE), Serdes.String(), Serdes.String());

    StoreBuilder<KeyValueStore<String, Long>> titleCountBuilder =
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(COUNTER_STORE), Serdes.String(), Serdes.Long());

    streamsBuilder.addStateStore(titleStoreBuilder);
    streamsBuilder.addStateStore(titleCountBuilder);

    KStream<String, RatingAverageVoteCount> messageStream =
        streamsBuilder.stream(
            titleRatingTopic, Consumed.with(Serdes.String(), ratingAverageVoteCountSerde));

    messageStream
        .transformValues(
            () -> new CountUniqueTitles(TITLE_STORE, COUNTER_STORE), TITLE_STORE, COUNTER_STORE)
        .peek(
            (titleId, ratingAverageVoteCount) ->
                log.info("titleId={}, ratingAverageVoteCount={}", titleId, ratingAverageVoteCount))
        .toTable(Materialized.with(Serdes.String(), ratingAverageVoteCountSerde))
        .groupBy(KeyValue::pair, Grouped.with(Serdes.String(), ratingAverageVoteCountSerde))
        // keeps the latest ratings statistics per titleId
        .aggregate(
            () -> RankingAggregate.builder().build(),
            (key, newRatingAverageVoteCount, rankingAggregate) ->
                rankingAggregate.toBuilder()
                    .titleId(newRatingAverageVoteCount.getTitleId())
                    .ratingAveragePerTitle(newRatingAverageVoteCount.getRatingAverage())
                    .voteCountPerTitle(newRatingAverageVoteCount.getVoteCount())
                    .titleIdCount(newRatingAverageVoteCount.getTitleIdCount())
                    .voteCountTotal(newRatingAverageVoteCount.getCurrentTotalVotesCounter())
                    .build(),
            (key, oldRatingAverageVoteCount, rankingAggregate) -> rankingAggregate,
            Materialized.with(Serdes.String(), rankingAggregateSerde))
        // calculates rankings for each title
        .mapValues(
            rankingAggregate -> {
              float averageNumberOfVotesPerTitle =
                  (float) rankingAggregate.getVoteCountTotal() / rankingAggregate.getTitleIdCount();
              return rankingAggregate.toBuilder()
                  .ranking(
                      ((float) rankingAggregate.getVoteCountPerTitle()
                              / averageNumberOfVotesPerTitle)
                          * rankingAggregate.getRatingAveragePerTitle())
                  .build();
            },
            Named.as("RANKING_TABLE"),
            Materialized.with(Serdes.String(), rankingAggregateSerde))
        .filter(
            (__, rankingAggregate) -> {
              log.info(
                  "Is rankingAggregate.voteCountTotal={} > minimumVotesRequired={}",
                  rankingAggregate.getVoteCountPerTitle(),
                  minimumVotesRequired);
              log.info("rankingAggregate={}", rankingAggregate);
              return rankingAggregate.getVoteCountPerTitle() > minimumVotesRequired;
            },
            Named.as("MORE_THAN_THRESHOLD_VOTES_RANKING_TABLE"),
            Materialized.with(Serdes.String(), rankingAggregateSerde))
        .groupBy(
            (__, rankingAggregate) -> KeyValue.pair("COMMON-STATIC-KEY", rankingAggregate),
            Grouped.with(Serdes.String(), rankingAggregateSerde))
        .aggregate(
            Top10RatedMovies::new,
            (key, newRankingAggregate, top10RatedMoviesAggregate) -> {
              top10RatedMoviesAggregate.add(newRankingAggregate);
              return top10RatedMoviesAggregate;
            },
            (key, oldRankingAggregate, top10RatedMoviesAggregate) -> {
              top10RatedMoviesAggregate.remove(oldRankingAggregate);
              return top10RatedMoviesAggregate;
            },
            Materialized.with(Serdes.String(), top10MoviesSerde))
        .toStream()
        .to(titleRankingTopic, Produced.with(Serdes.String(), top10MoviesSerde));
  }
}
