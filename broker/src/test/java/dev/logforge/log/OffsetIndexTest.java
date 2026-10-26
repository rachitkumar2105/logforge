package dev.logforge.log;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class OffsetIndexTest {

  @TempDir Path dir;

  @Test
  void floorLookupByBinarySearch() throws Exception {
    try (OffsetIndex index = new OffsetIndex(dir.resolve("0.index"), 1000L, 1024)) {
      index.append(1000L, 0);
      index.append(1010L, 4100);
      index.append(1025L, 8300);
      index.append(1040L, 12500);

      assertThat(index.entryCount()).isEqualTo(4);
      assertThat(index.lookup(999L)).isEqualTo(new IndexEntry(1000L, 0));
      assertThat(index.lookup(1000L)).isEqualTo(new IndexEntry(1000L, 0));
      assertThat(index.lookup(1009L)).isEqualTo(new IndexEntry(1000L, 0));
      assertThat(index.lookup(1010L)).isEqualTo(new IndexEntry(1010L, 4100));
      assertThat(index.lookup(1030L)).isEqualTo(new IndexEntry(1025L, 8300));
      assertThat(index.lookup(5000L)).isEqualTo(new IndexEntry(1040L, 12500));
    }
  }

  @Test
  void emptyIndexPointsAtSegmentStart() throws Exception {
    try (OffsetIndex index = new OffsetIndex(dir.resolve("e.index"), 50L, 1024)) {
      assertThat(index.lookup(77L)).isEqualTo(new IndexEntry(50L, 0));
    }
  }

  @Test
  void rejectsNonMonotonicEntriesAndFullIndex() throws Exception {
    try (OffsetIndex index = new OffsetIndex(dir.resolve("m.index"), 0L, 16)) {
      index.append(5L, 100);
      assertThatThrownBy(() -> index.append(5L, 200)).isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> index.append(6L, 50)).isInstanceOf(IllegalArgumentException.class);
      index.append(6L, 200);
      assertThat(index.isFull()).isTrue();
      assertThatThrownBy(() -> index.append(7L, 300)).isInstanceOf(IllegalStateException.class);
    }
  }

  @Test
  void survivesCloseAndReopenAndTrimsPreallocation() throws Exception {
    Path file = dir.resolve("r.index");
    try (OffsetIndex index = new OffsetIndex(file, 0L, 8 * 1024)) {
      index.append(0L, 0);
      index.append(30L, 4096);
      index.append(61L, 8200);
    }
    assertThat(Files.size(file)).isEqualTo(3 * 8L);
    try (OffsetIndex reopened = new OffsetIndex(file, 0L, 8 * 1024)) {
      assertThat(reopened.entryCount()).isEqualTo(3);
      assertThat(reopened.lookup(45L)).isEqualTo(new IndexEntry(30L, 4096));
      reopened.append(90L, 12300);
      assertThat(reopened.entryCount()).isEqualTo(4);
    }
  }

  @Test
  void truncatesEntriesAtOrAboveAnOffset() throws Exception {
    try (OffsetIndex index = new OffsetIndex(dir.resolve("t.index"), 0L, 1024)) {
      index.append(0L, 0);
      index.append(10L, 100);
      index.append(20L, 200);
      index.truncateTo(10L);
      assertThat(index.entryCount()).isEqualTo(1);
      assertThat(index.lookup(25L)).isEqualTo(new IndexEntry(0L, 0));
    }
  }

  @Test
  void sanityCheckFailsWhenLastEntryPointsPastTheLog() throws Exception {
    try (OffsetIndex index = new OffsetIndex(dir.resolve("s.index"), 0L, 1024)) {
      index.append(0L, 0);
      index.append(10L, 5000);
      assertThat(index.isConsistentWith(6000L)).isTrue();
      assertThat(index.isConsistentWith(4000L)).isFalse();
    }
  }
}
