package dev.logforge.network;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Executor;

/**
 * Runs tasks one at a time, in submission order, on a shared pool. One instance per connection:
 * requests on a connection are processed in order, while different connections run in parallel.
 */
final class SerialExecutor implements Executor {
  private final Queue<Runnable> tasks = new ArrayDeque<>();
  private final Executor pool;
  private boolean running;

  SerialExecutor(Executor pool) {
    this.pool = pool;
  }

  @Override
  public synchronized void execute(Runnable task) {
    tasks.add(task);
    if (!running) {
      running = true;
      pool.execute(this::drain);
    }
  }

  private void drain() {
    while (true) {
      Runnable next;
      synchronized (this) {
        next = tasks.poll();
        if (next == null) {
          running = false;
          return;
        }
      }
      try {
        next.run();
      } catch (Throwable t) {
        // Tasks report their own failures through their futures. Never let anything, even an
        // Error, end this loop with running=true: the connection would hang forever.
      }
    }
  }
}
