package com.ergotechis.streaming.statistics;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

@RequiredArgsConstructor
public class TotalVotesCounter implements ValueTransformer<Vote, Vote> {

  private KeyValueStore<String, Long> store;

  private final String voteCounterStore;

  private static final String COUNTER_KEY = "counter";

  @Override
  public void init(ProcessorContext context) {
    this.store = context.getStateStore(voteCounterStore);
  }

  @Override
  public Vote transform(Vote vote) {

    Long currentCount = this.store.get(COUNTER_KEY);
    long newValue;
    if (currentCount == null) {
      newValue = 1L;
    } else {
      newValue = currentCount + 1;
    }

    this.store.put(COUNTER_KEY, newValue);
    return vote.toBuilder().currentTotalVotesCounter(newValue).build();
  }

  @Override
  public void close() {}
}
