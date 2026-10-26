package dev.logforge.log;

/**
 * Result of an append.
 *
 * @param firstOffset offset of the first record (for a duplicate: of the original append)
 * @param lastOffset offset of the last record
 * @param records number of records in the batch
 * @param bytes batch size in bytes (0 for a duplicate, which writes nothing)
 * @param duplicate true if an idempotent retry was recognised and nothing was appended
 */
public record LogAppendInfo(
    long firstOffset, long lastOffset, int records, int bytes, boolean duplicate) {}
