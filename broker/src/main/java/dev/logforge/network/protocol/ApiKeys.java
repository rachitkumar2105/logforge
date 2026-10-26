package dev.logforge.network.protocol;

/** API keys from PROTOCOL.md section 5. */
public final class ApiKeys {
  private ApiKeys() {}

  public static final short PRODUCE = 0;
  public static final short FETCH = 1;
  public static final short LIST_OFFSETS = 2;
  public static final short METADATA = 3;
  public static final short CREATE_TOPICS = 4;
  public static final short DELETE_TOPICS = 5;
  public static final short FIND_COORDINATOR = 6;
  public static final short JOIN_GROUP = 7;
  public static final short HEARTBEAT = 8;
  public static final short LEAVE_GROUP = 9;
  public static final short OFFSET_COMMIT = 10;
  public static final short OFFSET_FETCH = 11;
  public static final short INIT_PRODUCER_ID = 12;
  public static final short OFFSET_FOR_LEADER_EPOCH = 13;
  public static final short BROKER_HEARTBEAT = 100;
  public static final short ALTER_ISR = 101;

  public static String name(short key) {
    return switch (key) {
      case PRODUCE -> "Produce";
      case FETCH -> "Fetch";
      case LIST_OFFSETS -> "ListOffsets";
      case METADATA -> "Metadata";
      case CREATE_TOPICS -> "CreateTopics";
      case DELETE_TOPICS -> "DeleteTopics";
      case FIND_COORDINATOR -> "FindCoordinator";
      case JOIN_GROUP -> "JoinGroup";
      case HEARTBEAT -> "Heartbeat";
      case LEAVE_GROUP -> "LeaveGroup";
      case OFFSET_COMMIT -> "OffsetCommit";
      case OFFSET_FETCH -> "OffsetFetch";
      case INIT_PRODUCER_ID -> "InitProducerId";
      case OFFSET_FOR_LEADER_EPOCH -> "OffsetForLeaderEpoch";
      case BROKER_HEARTBEAT -> "BrokerHeartbeat";
      case ALTER_ISR -> "AlterIsr";
      default -> "Unknown(" + key + ")";
    };
  }
}
