package dev.logforge.log;

/** A read-only description of one segment, for the admin API and the site's segment explorer. */
public record LogSegmentInfo(
    long baseOffset,
    long nextOffset,
    int sizeBytes,
    int indexEntries,
    long maxTimestamp,
    boolean active) {}
