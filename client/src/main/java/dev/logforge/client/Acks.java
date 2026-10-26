package dev.logforge.client;

/** How many replicas must have a batch before the producer counts it as sent. */
public enum Acks {
  /** Fire and forget: no response. A lost batch is never noticed. */
  NONE((short) 0),
  /** The leader wrote it. Lost if the leader dies before any follower copied it. */
  LEADER((short) 1),
  /** Every in-sync replica has it (the high watermark passed it). Survives any single failure. */
  ALL((short) -1);

  private final short wire;

  Acks(short wire) {
    this.wire = wire;
  }

  public short wire() {
    return wire;
  }
}
