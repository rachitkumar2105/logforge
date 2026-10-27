package dev.logforge.network;

/** Per-connection state the request handler may keep: whether it presented the cluster token. */
public final class Session {
  private volatile boolean clusterMember;
  private int authFailures; // requests on one connection run one at a time

  public boolean isClusterMember() {
    return clusterMember;
  }

  public void markClusterMember() {
    clusterMember = true;
  }

  /** Counts a refused Authenticate on this connection and returns the total so far. */
  public synchronized int recordAuthFailure() {
    return ++authFailures;
  }
}
