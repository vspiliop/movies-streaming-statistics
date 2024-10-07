package com.ergotechis.streaming.statistics.movies.ranking.transformer;

import com.ergotechis.streaming.statistics.movies.ranking.model.RatingAverageVoteCount;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

@RequiredArgsConstructor
public class CountUniqueTitles
    implements ValueTransformer<RatingAverageVoteCount, RatingAverageVoteCount> {

  private KeyValueStore<String, String> titleStore;
  private KeyValueStore<String, Long> counterStore;

  private final String titleStoreName;
  private final String counterStoreName;

  private static final String COUNTER_KEY = "counter";

  @Override
  public void init(ProcessorContext context) {
    this.titleStore = context.getStateStore(titleStoreName);
    this.counterStore = context.getStateStore(counterStoreName);
  }

  @Override
  public RatingAverageVoteCount transform(RatingAverageVoteCount ratingAverageVoteCount) {
    String exists = this.titleStore.get(ratingAverageVoteCount.getTitleId());
    Long newValue = this.counterStore.get(COUNTER_KEY);

    if (exists == null) {
      newValue =
          this.counterStore.get(COUNTER_KEY) == null ? 1 : this.counterStore.get(COUNTER_KEY) + 1;
      this.titleStore.put(ratingAverageVoteCount.getTitleId(), "");
      this.counterStore.put(COUNTER_KEY, newValue);
    }

    return ratingAverageVoteCount.toBuilder().titleIdCount(newValue).build();
  }

  @Override
  public void close() {}
}
