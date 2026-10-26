package dev.logforge.network.protocol;

/** Error codes from PROTOCOL.md section 4. */
public enum Errors {
  NONE(0, false),
  OFFSET_OUT_OF_RANGE(1, false),
  CORRUPT_MESSAGE(2, false),
  UNKNOWN_TOPIC_OR_PARTITION(3, true),
  INVALID_REQUEST(4, false),
  LEADER_NOT_AVAILABLE(5, true),
  NOT_LEADER_OR_FOLLOWER(6, true),
  REQUEST_TIMED_OUT(7, true),
  MESSAGE_TOO_LARGE(8, false),
  NOT_ENOUGH_REPLICAS(9, true),
  NOT_CONTROLLER(10, true),
  TOPIC_ALREADY_EXISTS(11, false),
  INVALID_REPLICATION_FACTOR(12, false),
  INVALID_PARTITIONS(13, false),
  INVALID_TOPIC(14, false),
  COORDINATOR_NOT_AVAILABLE(15, true),
  NOT_COORDINATOR(16, true),
  ILLEGAL_GENERATION(17, false),
  UNKNOWN_MEMBER_ID(18, false),
  REBALANCE_IN_PROGRESS(19, false),
  INCONSISTENT_ASSIGNOR(20, false),
  OUT_OF_ORDER_SEQUENCE(21, false),
  INVALID_PRODUCER_EPOCH(22, false),
  STORAGE_ERROR(23, true),
  UNSUPPORTED_VERSION(24, false),
  FENCED_LEADER_EPOCH(25, true),
  UNKNOWN_LEADER_EPOCH(26, true),
  BROKER_NOT_AVAILABLE(27, true),
  UNKNOWN_SERVER_ERROR(28, false);

  private final short code;
  private final boolean retriable;

  Errors(int code, boolean retriable) {
    this.code = (short) code;
    this.retriable = retriable;
  }

  public short code() {
    return code;
  }

  public boolean retriable() {
    return retriable;
  }

  private static final Errors[] BY_CODE = new Errors[values().length];

  static {
    for (Errors e : values()) {
      BY_CODE[e.code] = e;
    }
  }

  public static Errors forCode(short code) {
    if (code < 0 || code >= BY_CODE.length || BY_CODE[code] == null) {
      return UNKNOWN_SERVER_ERROR;
    }
    return BY_CODE[code];
  }
}
