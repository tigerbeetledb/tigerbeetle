import java.security.SecureRandom;
import java.text.NumberFormat;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import com.tigerbeetle.Client;
import com.tigerbeetle.UInt128;

public final class Main {
  public static void main(String[] args) throws Exception {
    Map<String, String> env = System.getenv();
    String replicaAddressesArg = Objects.requireNonNull(env.get("REPLICAS"),
        "REPLICAS environment variable must be set (comma-separated list)");

    String[] replicaAddresses = replicaAddressesArg.split(",");
    if (replicaAddresses.length == 0) {
      throw new IllegalArgumentException(
          "REPLICAS must list at least one address (comma-separated)");
    }

    int workloadCount = Integer.parseUnsignedInt(env.getOrDefault("WORKLOAD_COUNT", "5"));
    if (replicaAddresses.length == 0) {
      throw new IllegalArgumentException(
          "REPLICAS must list at least one address (comma-separated)");
    }

    Random random = new SecureRandom();
    byte[] clusterID = UInt128.asBytes(Long.parseLong(env.getOrDefault("CLUSTER", "1")));

    try (var client = new Client(clusterID, replicaAddresses)) {
      System.err.println("starting %d workload(s)".formatted(workloadCount));

      var executor = Executors.newFixedThreadPool(workloadCount);
      var completionService = new ExecutorCompletionService<Void>(executor);
      var statistics = new Statistics();
      var logger = new Thread(() -> logStatistics(statistics));
      logger.setDaemon(true);
      logger.start();

      for (int i = 0; i < workloadCount; i++) {
        final var ledger = i + 1;
        completionService.submit(new Workload(random, client, ledger, statistics));
      }

      try {
        for (int i = 0; i < workloadCount; i++) {
          var result = completionService.take();
          result.get();
        }
      } finally {
        executor.shutdownNow();
      }
    }
  }


  static void logStatistics(Statistics statistics) {
    while (true) {
      try {
        Thread.sleep(Duration.ofSeconds(10));

        var eventsSuccessful = statistics.successful();
        var eventsFailed = statistics.failed();
        var eventsTotal = eventsSuccessful + eventsFailed;
        var eventSuccessRate = eventsTotal > 0 
          ? ((double) eventsSuccessful / eventsTotal) 
          : 0.0;

        System.err.println(
            "%d events in total, %s successful".formatted(
              eventsTotal,
              NumberFormat.getPercentInstance().format(eventSuccessRate)));
      } catch (InterruptedException e) {
        break;
      }
    }
  }
}
