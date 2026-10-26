package dev.logforge.admin;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Token buckets keyed by client (IP address, or API token). Each key refills at {@code
 * ratePerSecond} up to a burst of twice that.
 */
public final class RateLimiter {
  private static final class Bucket {
    double tokens;
    long lastNanos;

    Bucket(double tokens, long now) {
      this.tokens = tokens;
      this.lastNanos = now;
    }
  }

  private final double ratePerSecond;
  private final double burst;
  private final LongSupplier nanoClock;
  private final Map<String, Bucket> buckets = new ConcurrentHashMap<>();

  public RateLimiter(double ratePerSecond, LongSupplier nanoClock) {
    this.ratePerSecond = ratePerSecond;
    this.burst = Math.max(1, ratePerSecond * 2);
    this.nanoClock = nanoClock;
  }

  /**
   * Takes one token for {@code key}.
   *
   * @return 0 if allowed, otherwise the whole seconds to wait before retrying (at least 1)
   */
  public long acquire(String key) {
    long now = nanoClock.getAsLong();
    if (buckets.size() > 100_000) {
      // Bounded memory under a flood of distinct keys: drop only buckets idle long enough to be
      // full again, so a flood cannot reset the limit of a client that is still sending.
      long idleNanos = (long) (burst / ratePerSecond * 1e9);
      buckets.values().removeIf(b -> now - b.lastNanos > idleNanos);
    }
    Bucket b = buckets.computeIfAbsent(key, k -> new Bucket(burst, now));
    synchronized (b) {
      double elapsed = (now - b.lastNanos) / 1e9;
      b.tokens = Math.min(burst, b.tokens + elapsed * ratePerSecond);
      b.lastNanos = now;
      if (b.tokens >= 1) {
        b.tokens -= 1;
        return 0;
      }
      return Math.max(1, (long) Math.ceil((1 - b.tokens) / ratePerSecond));
    }
  }
}
