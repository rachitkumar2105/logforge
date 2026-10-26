package dev.logforge.util;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Structured JSON logging: one JSON object per line on stderr.
 *
 * <pre>{"ts":"2026-10-26T07:00:00.123Z","level":"INFO","logger":"Controller","node":1,
 *  "msg":"elected leader","corr":"c-42","topic":"orders","partition":0,"leader":2}</pre>
 *
 * <p>A correlation id ties together lines produced while serving one request: the network layer
 * sets it per request ({@code <connection>-<correlationId>}) and background tasks set their own
 * ({@code fetcher-2}, {@code controller-tick}).
 *
 * <p>Rule (SECURITY.md S19): never pass record keys, record values or tokens as fields. Only ids,
 * offsets, sizes and error names belong in logs.
 */
public final class JsonLog {
  public enum Level {
    DEBUG,
    INFO,
    WARN,
    ERROR
  }

  private static final ThreadLocal<String> CORRELATION = new ThreadLocal<>();
  private static volatile int nodeId = -1;
  private static volatile Level threshold = Level.INFO;
  private static final PrintStream OUT = new PrintStream(System.err, true, StandardCharsets.UTF_8);

  private final String name;

  private JsonLog(String name) {
    this.name = name;
  }

  public static JsonLog get(Class<?> cls) {
    return new JsonLog(cls.getSimpleName());
  }

  public static void setNodeId(int id) {
    nodeId = id;
  }

  public static void setLevel(Level level) {
    threshold = level;
  }

  /** Sets the correlation id for the current thread; returns the previous one. */
  public static String setCorrelationId(String id) {
    String prev = CORRELATION.get();
    if (id == null) {
      CORRELATION.remove();
    } else {
      CORRELATION.set(id);
    }
    return prev;
  }

  public static String correlationId() {
    return CORRELATION.get();
  }

  public void debug(String msg, Object... kv) {
    log(Level.DEBUG, msg, null, kv);
  }

  public void info(String msg, Object... kv) {
    log(Level.INFO, msg, null, kv);
  }

  public void warn(String msg, Object... kv) {
    log(Level.WARN, msg, null, kv);
  }

  public void error(String msg, Throwable t, Object... kv) {
    log(Level.ERROR, msg, t, kv);
  }

  public boolean isDebugEnabled() {
    return threshold.compareTo(Level.DEBUG) <= 0;
  }

  private void log(Level level, String msg, Throwable t, Object... kv) {
    if (level.compareTo(threshold) < 0) {
      return;
    }
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("ts", Instant.now().toString());
    m.put("level", level.name());
    m.put("logger", name);
    if (nodeId >= 0) {
      m.put("node", nodeId);
    }
    m.put("msg", msg);
    String corr = CORRELATION.get();
    if (corr != null) {
      m.put("corr", corr);
    }
    for (int i = 0; i + 1 < kv.length; i += 2) {
      m.put(String.valueOf(kv[i]), kv[i + 1]);
    }
    if (t != null) {
      // Exception class and message only; no stack traces in production logs.
      m.put("error", t.getClass().getSimpleName());
      if (t.getMessage() != null) {
        m.put("errorMessage", t.getMessage());
      }
    }
    OUT.println(Json.write(m));
  }
}
