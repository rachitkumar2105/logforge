package dev.logforge.client;

/**
 * Consumer settings.
 *
 * @param groupId consumer group for {@link Consumer#subscribe}; null for manual assignment only
 * @param assignor {@code range} or {@code roundrobin}
 * @param autoOffsetReset where to start when there is no committed offset
 */
public record ConsumerConfig(
    String bootstrapServers,
    String clientId,
    String groupId,
    String assignor,
    OffsetReset autoOffsetReset,
    int maxPollRecords,
    int fetchMaxBytes,
    int partitionMaxBytes,
    int fetchMaxWaitMs,
    int sessionTimeoutMs,
    int rebalanceTimeoutMs,
    int heartbeatIntervalMs,
    int requestTimeoutMs) {

  public enum OffsetReset {
    EARLIEST,
    LATEST
  }

  public static Builder builder(String bootstrapServers) {
    return new Builder(bootstrapServers);
  }

  public static final class Builder {
    private final String bootstrap;
    private String clientId = "logforge-consumer";
    private String groupId;
    private String assignor = "range";
    private OffsetReset reset = OffsetReset.EARLIEST;
    private int maxPollRecords = 500;
    private int fetchMaxBytes = 8 * 1024 * 1024;
    private int partitionMaxBytes = 1024 * 1024;
    private int fetchMaxWaitMs = 500;
    private int sessionTimeoutMs = 6_000;
    private int rebalanceTimeoutMs = 10_000;
    private int heartbeatIntervalMs = 1_000;
    private int requestTimeoutMs = 30_000;

    private Builder(String bootstrap) {
      this.bootstrap = bootstrap;
    }

    public Builder clientId(String v) {
      clientId = v;
      return this;
    }

    public Builder groupId(String v) {
      groupId = v;
      return this;
    }

    public Builder assignor(String v) {
      assignor = v;
      return this;
    }

    public Builder autoOffsetReset(OffsetReset v) {
      reset = v;
      return this;
    }

    public Builder maxPollRecords(int v) {
      maxPollRecords = v;
      return this;
    }

    public Builder fetchMaxBytes(int v) {
      fetchMaxBytes = v;
      return this;
    }

    public Builder partitionMaxBytes(int v) {
      partitionMaxBytes = v;
      return this;
    }

    public Builder fetchMaxWaitMs(int v) {
      fetchMaxWaitMs = v;
      return this;
    }

    public Builder sessionTimeoutMs(int v) {
      sessionTimeoutMs = v;
      return this;
    }

    public Builder rebalanceTimeoutMs(int v) {
      rebalanceTimeoutMs = v;
      return this;
    }

    public Builder heartbeatIntervalMs(int v) {
      heartbeatIntervalMs = v;
      return this;
    }

    public Builder requestTimeoutMs(int v) {
      requestTimeoutMs = v;
      return this;
    }

    public ConsumerConfig build() {
      if (!assignor.equals("range") && !assignor.equals("roundrobin")) {
        throw new IllegalArgumentException("assignor must be range or roundrobin");
      }
      return new ConsumerConfig(
          bootstrap,
          clientId,
          groupId,
          assignor,
          reset,
          maxPollRecords,
          fetchMaxBytes,
          partitionMaxBytes,
          fetchMaxWaitMs,
          sessionTimeoutMs,
          rebalanceTimeoutMs,
          heartbeatIntervalMs,
          requestTimeoutMs);
    }
  }
}
