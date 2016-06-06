package org.apache.flume.instrumentation;

import org.apache.commons.lang.ArrayUtils;

public class SourceCounterExt extends MonitoredCounterGroup implements
    SourceCounterMBeanExt {

  private static final String COUNTER_EVENTS_RECEIVED =
      "src.events.received";
  private static final String COUNTER_EVENTS_ACCEPTED =
      "src.events.accepted";

  private static final String COUNTER_APPEND_RECEIVED =
      "src.append.received";
  private static final String COUNTER_APPEND_ACCEPTED =
      "src.append.accepted";

  private static final String COUNTER_APPEND_BATCH_RECEIVED =
      "src.append-batch.received";
  private static final String COUNTER_APPEND_BATCH_ACCEPTED =
      "src.append-batch.accepted";

  private static final String COUNTER_OPEN_CONNECTION_COUNT =
      "src.open-connection.count";

  private static final String COUNTER_BYTES_ACCEPTED =
      "src.bytes.accepted";

  private static final String[] ATTRIBUTES =
      {
          COUNTER_EVENTS_RECEIVED, COUNTER_EVENTS_ACCEPTED,
          COUNTER_APPEND_RECEIVED, COUNTER_APPEND_ACCEPTED,
          COUNTER_APPEND_BATCH_RECEIVED, COUNTER_APPEND_BATCH_ACCEPTED,
          COUNTER_OPEN_CONNECTION_COUNT, COUNTER_BYTES_ACCEPTED
      };

  public SourceCounterExt(String name) {
    super(MonitoredCounterGroup.Type.SOURCE, name, ATTRIBUTES);
  }

  public SourceCounterExt(String name, String[] attributes) {
    super(Type.SOURCE, name,
        (String[]) ArrayUtils.addAll(attributes, ATTRIBUTES));
  }

  @Override
  public long getEventReceivedCount() {
    return get(COUNTER_EVENTS_RECEIVED);
  }

  public long incrementEventReceivedCount() {
    return increment(COUNTER_EVENTS_RECEIVED);
  }

  public long addToEventReceivedCount(long delta) {
    return addAndGet(COUNTER_EVENTS_RECEIVED, delta);
  }

  @Override
  public long getEventAcceptedCount() {
    return get(COUNTER_EVENTS_ACCEPTED);
  }

  public long incrementEventAcceptedCount() {
    return increment(COUNTER_EVENTS_ACCEPTED);
  }

  public long addToEventAcceptedCount(long delta) {
    return addAndGet(COUNTER_EVENTS_ACCEPTED, delta);
  }

  @Override
  public long getAppendReceivedCount() {
    return get(COUNTER_APPEND_RECEIVED);
  }

  public long incrementAppendReceivedCount() {
    return increment(COUNTER_APPEND_RECEIVED);
  }

  @Override
  public long getAppendAcceptedCount() {
    return get(COUNTER_APPEND_ACCEPTED);
  }

  public long incrementAppendAcceptedCount() {
    return increment(COUNTER_APPEND_ACCEPTED);
  }

  @Override
  public long getAppendBatchReceivedCount() {
    return get(COUNTER_APPEND_BATCH_RECEIVED);
  }

  public long incrementAppendBatchReceivedCount() {
    return increment(COUNTER_APPEND_BATCH_RECEIVED);
  }

  @Override
  public long getAppendBatchAcceptedCount() {
    return get(COUNTER_APPEND_BATCH_ACCEPTED);
  }

  public long incrementAppendBatchAcceptedCount() {
    return increment(COUNTER_APPEND_BATCH_ACCEPTED);
  }

  public long getOpenConnectionCount() {
    return get(COUNTER_OPEN_CONNECTION_COUNT);
  }

  public void setOpenConnectionCount(long openConnectionCount) {
    set(COUNTER_OPEN_CONNECTION_COUNT, openConnectionCount);
  }

  @Override
  public long getByteAcceptedCount() {
    return get(COUNTER_BYTES_ACCEPTED);
  }

  public long addToByteAcceptedCount(long delta) {
    return addAndGet(COUNTER_BYTES_ACCEPTED, delta);
  }


}