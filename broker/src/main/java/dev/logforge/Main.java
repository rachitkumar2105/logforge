package dev.logforge;

import dev.logforge.util.JsonLog;
import java.util.concurrent.CountDownLatch;

/** Entry point. Reads configuration from the environment and runs until terminated. */
public final class Main {
  private static final JsonLog LOG = JsonLog.get(Main.class);

  private Main() {}

  public static void main(String[] args) throws Exception {
    NodeConfig config;
    try {
      config = NodeConfig.fromEnv(System.getenv());
    } catch (IllegalArgumentException e) {
      LOG.error("invalid configuration", e);
      System.exit(2);
      return;
    }
    Node node = Node.start(config);
    if (config.seed != null && config.isBroker() && config.isController()) {
      Seeder.Spec spec = config.seed;
      Thread seeder =
          new Thread(
              () -> {
                try {
                  Seeder.seed("127.0.0.1", node.port(), spec);
                } catch (Exception e) {
                  LOG.error("seeding failed", e);
                }
              },
              "logforge-seeder");
      seeder.setDaemon(true);
      seeder.start();
    }
    CountDownLatch stopped = new CountDownLatch(1);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    node.close();
                  } catch (Exception e) {
                    LOG.error("shutdown failed", e);
                  } finally {
                    stopped.countDown();
                  }
                },
                "logforge-shutdown"));
    stopped.await();
  }
}
