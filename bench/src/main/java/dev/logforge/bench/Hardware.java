package dev.logforge.bench;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/** Hardware and software the numbers were measured on, attached to every result. */
final class Hardware {
  private Hardware() {}

  private static String grep(Path file, String prefix) {
    try {
      for (String line : Files.readAllLines(file, UTF_8)) {
        if (line.startsWith(prefix)) {
          return line.substring(line.indexOf(':') + 1).trim();
        }
      }
    } catch (Exception e) {
      // not Linux, or not readable
    }
    return "unknown";
  }

  static Map<String, Object> describe() {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("cpuModel", grep(Path.of("/proc/cpuinfo"), "model name"));
    m.put("cpusVisible", Runtime.getRuntime().availableProcessors());
    m.put("memTotal", grep(Path.of("/proc/meminfo"), "MemTotal"));
    m.put("os", System.getProperty("os.name") + " " + System.getProperty("os.version"));
    m.put("java", System.getProperty("java.vendor") + " " + System.getProperty("java.version"));
    m.put("host", System.getenv().getOrDefault("BENCH_HOST", "unknown"));
    m.put("environment", System.getenv().getOrDefault("BENCH_ENV", "unknown"));
    return m;
  }
}
