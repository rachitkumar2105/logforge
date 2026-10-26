package dev.logforge.log;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * Explicit unmapping of memory-mapped buffers.
 *
 * <p>Java unmaps a {@code MappedByteBuffer} only when it is garbage collected. On Windows a file
 * with a live mapping cannot be truncated or deleted, so segment roll, retention and truncation
 * would fail until a GC happened. {@code sun.misc.Unsafe.invokeCleaner} releases the mapping now.
 * It is in the {@code jdk.unsupported} module, which is exported on purpose for this use.
 */
final class MappedBuffers {
  private static final Object UNSAFE;
  private static final Method INVOKE_CLEANER;

  static {
    Object unsafe = null;
    Method cleaner = null;
    try {
      Class<?> cls = Class.forName("sun.misc.Unsafe");
      Field f = cls.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      unsafe = f.get(null);
      cleaner = cls.getMethod("invokeCleaner", ByteBuffer.class);
    } catch (ReflectiveOperationException | RuntimeException e) {
      // Fall back to GC-driven unmapping. Correct on Linux; on Windows deletes may be delayed.
      unsafe = null;
      cleaner = null;
    }
    UNSAFE = unsafe;
    INVOKE_CLEANER = cleaner;
  }

  private MappedBuffers() {}

  static void unmap(ByteBuffer buffer) {
    if (buffer == null || !buffer.isDirect() || INVOKE_CLEANER == null) {
      return;
    }
    try {
      INVOKE_CLEANER.invoke(UNSAFE, buffer);
    } catch (ReflectiveOperationException e) {
      // Leave it to the GC.
    }
  }
}
