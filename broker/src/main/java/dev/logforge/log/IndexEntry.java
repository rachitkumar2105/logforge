package dev.logforge.log;

/**
 * An absolute offset and the byte position in the segment's .log file of the batch starting there.
 */
public record IndexEntry(long offset, int position) {}
