import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Helpers for generating random values.
 */
public class Arbitrary {
  /**
   * Selects a random value in {@code elements}.
   */
  static <T> T element(Random random, Collection<T> elements) {
    var list = new ArrayList<>(elements);
    return list.get(random.nextInt(0, list.size()));
  }

  /**
   * Selects a random subset of values in {@code elements}.
   */
  static <T> Set<T> subset(Random random, Collection<T> elements) {
    var copy = new ArrayList<T>(elements);
    Collections.shuffle(copy);
    var count = random.nextInt(0, elements.size());
    return new HashSet<T>(copy.subList(0, count));
  }

  /**
   * Selects a random combination (bitwise OR) of the provided flags.
   */
  static Integer flags(Random random, Collection<Integer> flags) {
    return subset(random, flags).stream().reduce(0, (a, b) -> a | b);
  }

  static <T> Supplier<T> odds(Random random,
      ArrayList<? extends WithOdds<? extends Supplier<? extends T>>> arbs) {
    final int oddsTotal = arbs.stream().mapToInt(a -> a.odds()).sum();
    return (() -> {
      var pick = random.nextInt(0, oddsTotal) + 1;
      var pos = 0;
      for (var arb : arbs) {
        pos += arb.odds();
        if (pos >= pick) {
          return arb.value().get();
        }
      }
      throw new IllegalStateException("couldn't pick an arbitrary (shouldn't happen)");
    });
  }
}


record WithOdds<T>(int odds, T value) {
  static <T> WithOdds<T> of(int odds, T value) {
    return new WithOdds<T>(odds, value);
  }
}
