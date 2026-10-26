package dev.logforge.client;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A real cluster of separate JVM processes started from {@code dev.logforge.Main}: node 0 is the
 * controller, nodes 1..N are brokers. {@link #kill} uses {@link Process#destroyForcibly()}: SIGKILL
 * on Linux and macOS, TerminateProcess on Windows. No shutdown hook runs and nothing is flushed.
 */
final class ProcessCluster implements AutoCloseable {
  private final Path dir;
  private final int controllerPort;
  private final int[] ports;
  private final Process[] processes;
  private final Map<String, String> extraEnv;

  ProcessCluster(Path dir, int brokers, Map<String, String> extraEnv) throws IOException {
    this.dir = dir;
    this.extraEnv = extraEnv;
    this.ports = new int[brokers + 1];
    this.processes = new Process[brokers + 1];
    this.controllerPort = freePort();
    processes[0] = launch(0, controllerPort, "controller");
    for (int i = 1; i <= brokers; i++) {
      ports[i] = freePort();
      processes[i] = launch(i, ports[i], "broker");
    }
  }

  static int freePort() throws IOException {
    try (ServerSocket s = new ServerSocket(0)) {
      return s.getLocalPort();
    }
  }

  private Process launch(int id, int port, String roles) throws IOException {
    String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    ProcessBuilder pb =
        new ProcessBuilder(
            java,
            "-Xmx384m",
            "-XX:+UseSerialGC",
            "-cp",
            System.getProperty("java.class.path"),
            "dev.logforge.Main");
    Map<String, String> env = pb.environment();
    env.put("LOGFORGE_NODE_ID", Integer.toString(id));
    env.put("LOGFORGE_ROLES", roles);
    env.put("LOGFORGE_LISTEN", "127.0.0.1:" + port);
    env.put("LOGFORGE_ADVERTISED", "127.0.0.1:" + port);
    env.put("LOGFORGE_CONTROLLER", "127.0.0.1:" + controllerPort);
    env.put("LOGFORGE_DATA_DIR", dir.resolve("node-" + id).toString());
    env.put("LOGFORGE_ADMIN_LISTEN", "off");
    env.put("LOGFORGE_FSYNC", "never");
    env.put("LOGFORGE_HEARTBEAT_MS", "100");
    env.put("LOGFORGE_SESSION_TIMEOUT_MS", "2000");
    env.put("LOGFORGE_REPLICA_LAG_MS", "3000");
    env.putAll(extraEnv);
    pb.redirectErrorStream(true);
    pb.redirectOutput(dir.resolve("node-" + id + ".out").toFile());
    return pb.start();
  }

  String bootstrap() {
    List<String> hosts = new ArrayList<>();
    for (int i = 1; i < ports.length; i++) {
      hosts.add("127.0.0.1:" + ports[i]);
    }
    hosts.add("127.0.0.1:" + controllerPort);
    return String.join(",", hosts);
  }

  void kill(int id) throws InterruptedException {
    processes[id].destroyForcibly();
    processes[id].waitFor(30, TimeUnit.SECONDS);
  }

  void restart(int id) throws IOException {
    processes[id] = launch(id, ports[id], "broker");
  }

  boolean isAlive(int id) {
    return processes[id] != null && processes[id].isAlive();
  }

  File logFile(int id) {
    return dir.resolve("node-" + id + ".out").toFile();
  }

  Map<Integer, Long> pids() {
    Map<Integer, Long> out = new HashMap<>();
    for (int i = 0; i < processes.length; i++) {
      out.put(i, processes[i].pid());
    }
    return out;
  }

  @Override
  public void close() throws InterruptedException {
    for (Process p : processes) {
      if (p != null) {
        p.destroyForcibly();
      }
    }
    for (Process p : processes) {
      if (p != null) {
        p.waitFor(30, TimeUnit.SECONDS);
      }
    }
  }
}
