package dev.logforge.log;

/**
 * Answer to "where does epoch E end in your log?" (OffsetForLeaderEpoch).
 *
 * @param epoch the largest epoch at or below the one asked about, or -1 if there is none
 * @param endOffset the first offset after that epoch, or -1 if undefined
 */
public record EpochEndOffset(int epoch, long endOffset) {
  public static final EpochEndOffset UNDEFINED = new EpochEndOffset(-1, -1L);

  public boolean isUndefined() {
    return epoch < 0;
  }
}
