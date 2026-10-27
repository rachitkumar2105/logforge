package dev.logforge.network;

/** Per-connection state the request handler may keep: whether it presented the cluster token. */
public final class Session {
  private volatile boolean clusterMember;

  public boolean isClusterMember() {
    return clusterMember;
  }

  public void markClusterMember() {
    clusterMember = true;
  }
}
