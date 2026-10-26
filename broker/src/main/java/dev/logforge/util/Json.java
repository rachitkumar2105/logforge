package dev.logforge.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A minimal JSON writer and parser, enough for the admin API, structured logs, heartbeat status and
 * the controller's metadata file. Hand-written to keep the broker free of runtime dependencies.
 *
 * <p>Values map to Java as: object to {@code Map<String,Object>} (insertion-ordered), array to
 * {@code List<Object>}, string to {@code String}, number to {@code Long} or {@code Double}, plus
 * {@code Boolean} and {@code null}.
 */
public final class Json {
  private Json() {}

  /** Thrown for malformed JSON input. */
  public static final class ParseException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    ParseException(String message) {
      super(message);
    }
  }

  // ---------------------------------------------------------------- writing

  public static String write(Object value) {
    StringBuilder sb = new StringBuilder(128);
    write(sb, value);
    return sb.toString();
  }

  @SuppressWarnings("unchecked")
  public static void write(StringBuilder sb, Object v) {
    if (v == null) {
      sb.append("null");
    } else if (v instanceof String s) {
      quote(sb, s);
    } else if (v instanceof Boolean b) {
      sb.append(b.booleanValue());
    } else if (v instanceof Double d) {
      if (d.isNaN() || d.isInfinite()) {
        sb.append("null");
      } else {
        sb.append(d.doubleValue());
      }
    } else if (v instanceof Float f) {
      write(sb, f.doubleValue());
    } else if (v instanceof Number n) {
      sb.append(n.longValue());
    } else if (v instanceof Map<?, ?> m) {
      sb.append('{');
      boolean first = true;
      for (Map.Entry<?, ?> e : m.entrySet()) {
        if (!first) {
          sb.append(',');
        }
        first = false;
        quote(sb, String.valueOf(e.getKey()));
        sb.append(':');
        write(sb, e.getValue());
      }
      sb.append('}');
    } else if (v instanceof Collection<?> c) {
      sb.append('[');
      boolean first = true;
      for (Object o : c) {
        if (!first) {
          sb.append(',');
        }
        first = false;
        write(sb, o);
      }
      sb.append(']');
    } else if (v instanceof int[] arr) {
      List<Object> l = new ArrayList<>();
      for (int i : arr) {
        l.add(i);
      }
      write(sb, l);
    } else {
      quote(sb, v.toString());
    }
  }

  /**
   * Writes a JSON string literal. Escapes quotes, backslashes, control characters and the
   * HTML-sensitive {@code <}, {@code >}, {@code &}.
   */
  public static void quote(StringBuilder sb, String s) {
    sb.append('"');
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"' -> sb.append("\\\"");
        case '\\' -> sb.append("\\\\");
        case '\n' -> sb.append("\\n");
        case '\r' -> sb.append("\\r");
        case '\t' -> sb.append("\\t");
        case '<' -> sb.append("\\u003c");
        case '>' -> sb.append("\\u003e");
        case '&' -> sb.append("\\u0026");
        default -> {
          if (c < 0x20 || c == 0x2028 || c == 0x2029) {
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
        }
      }
    }
    sb.append('"');
  }

  /** Ordered map builder: {@code Json.obj("a", 1, "b", "x")}. */
  public static Map<String, Object> obj(Object... kv) {
    if (kv.length % 2 != 0) {
      throw new IllegalArgumentException("odd number of arguments");
    }
    Map<String, Object> m = new LinkedHashMap<>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put((String) kv[i], kv[i + 1]);
    }
    return m;
  }

  // ---------------------------------------------------------------- parsing

  /** Parses a JSON document. Nesting is limited to 64 levels. */
  public static Object parse(String text) {
    Parser p = new Parser(text);
    p.skipWs();
    Object v = p.value(0);
    p.skipWs();
    if (p.pos != text.length()) {
      throw new ParseException("trailing characters at " + p.pos);
    }
    return v;
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> parseObject(String text) {
    Object v = parse(text);
    if (!(v instanceof Map)) {
      throw new ParseException("expected a JSON object");
    }
    return (Map<String, Object>) v;
  }

  private static final class Parser {
    private final String s;
    private int pos;

    Parser(String s) {
      this.s = s;
    }

    void skipWs() {
      while (pos < s.length()) {
        char c = s.charAt(pos);
        if (c == ' ' || c == '\n' || c == '\r' || c == '\t') {
          pos++;
        } else {
          break;
        }
      }
    }

    char peek() {
      if (pos >= s.length()) {
        throw new ParseException("unexpected end");
      }
      return s.charAt(pos);
    }

    Object value(int depth) {
      if (depth > 64) {
        throw new ParseException("nesting too deep");
      }
      char c = peek();
      return switch (c) {
        case '{' -> object(depth);
        case '[' -> array(depth);
        case '"' -> string();
        case 't' -> literal("true", Boolean.TRUE);
        case 'f' -> literal("false", Boolean.FALSE);
        case 'n' -> literal("null", null);
        default -> number();
      };
    }

    Object literal(String word, Object v) {
      if (!s.startsWith(word, pos)) {
        throw new ParseException("bad literal at " + pos);
      }
      pos += word.length();
      return v;
    }

    Map<String, Object> object(int depth) {
      Map<String, Object> m = new LinkedHashMap<>();
      pos++;
      skipWs();
      if (peek() == '}') {
        pos++;
        return m;
      }
      while (true) {
        skipWs();
        if (peek() != '"') {
          throw new ParseException("expected key at " + pos);
        }
        String k = string();
        skipWs();
        if (peek() != ':') {
          throw new ParseException("expected ':' at " + pos);
        }
        pos++;
        skipWs();
        m.put(k, value(depth + 1));
        skipWs();
        char c = peek();
        pos++;
        if (c == '}') {
          return m;
        }
        if (c != ',') {
          throw new ParseException("expected ',' or '}' at " + (pos - 1));
        }
      }
    }

    List<Object> array(int depth) {
      List<Object> l = new ArrayList<>();
      pos++;
      skipWs();
      if (peek() == ']') {
        pos++;
        return l;
      }
      while (true) {
        skipWs();
        l.add(value(depth + 1));
        skipWs();
        char c = peek();
        pos++;
        if (c == ']') {
          return l;
        }
        if (c != ',') {
          throw new ParseException("expected ',' or ']' at " + (pos - 1));
        }
      }
    }

    String string() {
      pos++;
      StringBuilder sb = new StringBuilder();
      while (true) {
        char c = peek();
        pos++;
        if (c == '"') {
          return sb.toString();
        }
        if (c == '\\') {
          char e = peek();
          pos++;
          switch (e) {
            case '"' -> sb.append('"');
            case '\\' -> sb.append('\\');
            case '/' -> sb.append('/');
            case 'b' -> sb.append('\b');
            case 'f' -> sb.append('\f');
            case 'n' -> sb.append('\n');
            case 'r' -> sb.append('\r');
            case 't' -> sb.append('\t');
            case 'u' -> {
              if (pos + 4 > s.length()) {
                throw new ParseException("bad unicode escape");
              }
              try {
                sb.append((char) Integer.parseInt(s.substring(pos, pos + 4), 16));
              } catch (NumberFormatException ex) {
                throw new ParseException("bad unicode escape");
              }
              pos += 4;
            }
            default -> throw new ParseException("bad escape at " + pos);
          }
        } else if (c < 0x20) {
          throw new ParseException("control character in string");
        } else {
          sb.append(c);
        }
      }
    }

    Object number() {
      int start = pos;
      boolean isDouble = false;
      while (pos < s.length()) {
        char c = s.charAt(pos);
        if ((c >= '0' && c <= '9') || c == '-' || c == '+') {
          pos++;
        } else if (c == '.' || c == 'e' || c == 'E') {
          isDouble = true;
          pos++;
        } else {
          break;
        }
      }
      String n = s.substring(start, pos);
      if (n.isEmpty()) {
        throw new ParseException("unexpected character at " + start);
      }
      try {
        return isDouble ? (Object) Double.parseDouble(n) : (Object) Long.parseLong(n);
      } catch (NumberFormatException e) {
        throw new ParseException("bad number " + n);
      }
    }
  }
}
