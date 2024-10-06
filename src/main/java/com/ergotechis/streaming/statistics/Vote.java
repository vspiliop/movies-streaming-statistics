package com.ergotechis.streaming.statistics;

import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Builder
@Data
@Jacksonized
public class Vote {
  long rating;
  String titleId;
}
