package dev.logforge.client;

/**
 * Producer settings.
 *
 * @param batchSize bytes per partition batch before it is sent without waiting for linger
 * @param lingerMs how long a non-full batch waits for more records (trades latency for throughput)
 * @param retries attempts after a retriable failure
 * @param deliveryTimeoutMs upper bound on the time from send() to success or failure
 * @param idempotent stamp producer id and sequences so retries are deduplicated by the broker
 * @param bufferMemory bytes of unsent records the producer may hold before send() blocks
 */
public record ProducerConfig(
    String bootstrapServers,
    String clientId,
    Acks acks,
    int batchSize,
    int lingerMs,
    int retries,
    int retryBackoffMs,
    int requestTimeoutMs,
    int deliveryTimeoutMs,
    boolean idempotent,
    long bufferMemory) {

  public static Builder builder(String bootstrapServers) {
    return new Builder(bootstrapServers);
  }

  public static final class Builder {
    private final String bootstrap;
    private String clientId = "logforge-producer";
    private Acks acks = Acks.ALL;
    private int batchSize = 16 * 1024;
    private int lingerMs = 5;
    private int retries = 10;
    private int retryBackoffMs = 100;
    private int requestTimeoutMs = 30_000;
    private int deliveryTimeoutMs = 120_000;
    private Boolean idempotent;
    private long bufferMemory = 32L * 1024 * 1024;

    private Builder(String bootstrap) {
      this.bootstrap = bootstrap;
    }

    public Builder clientId(String v) {
      clientId = v;
      return this;
    }

    public Builder acks(Acks v) {
      acks = v;
      return this;
    }

    public Builder batchSize(int v) {
      batchSize = v;
      return this;
    }

    public Builder lingerMs(int v) {
      lingerMs = v;
      return this;
    }

    public Builder retries(int v) {
      retries = v;
      return this;
    }

    public Builder retryBackoffMs(int v) {
      retryBackoffMs = v;
      return this;
    }

    public Builder requestTimeoutMs(int v) {
      requestTimeoutMs = v;
      return this;
    }

    public Builder deliveryTimeoutMs(int v) {
      deliveryTimeoutMs = v;
      return this;
    }

    public Builder idempotent(boolean v) {
      idempotent = v;
      return this;
    }

    public Builder bufferMemory(long v) {
      bufferMemory = v;
      return this;
    }

    public ProducerConfig build() {
      // Idempotence is the default, except with acks=0 where there is no response to retry on.
      boolean idem = idempotent != null ? idempotent : acks != Acks.NONE;
      if (idem && acks == Acks.NONE) {
        throw new IllegalArgumentException("idempotence needs acks=1 or acks=all");
      }
      if (batchSize < 1024) {
        throw new IllegalArgumentException("batchSize must be >= 1024");
      }
      return new ProducerConfig(
          bootstrap,
          clientId,
          acks,
          batchSize,
          lingerMs,
          retries,
          retryBackoffMs,
          requestTimeoutMs,
          deliveryTimeoutMs,
          idem,
          bufferMemory);
    }
  }
}
