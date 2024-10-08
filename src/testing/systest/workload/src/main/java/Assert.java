public class Assert {
  public static void that(boolean condition, String message, Object... args) {
    if (!condition) {
      throw new AssertionError(message.formatted(args));
    }
  }

  public static <T> void equal(T a, T b) {
    that(a != null && b != null && a.equals(b), "%s != %s", a.toString(), b.toString());
  }
}
