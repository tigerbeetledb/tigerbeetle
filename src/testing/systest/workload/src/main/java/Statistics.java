/**
 * Collects basic statistics about requests completed by the workload.
 * This class is thread-safe.
 */
class Statistics {
  private long startTimeMs;
  private long successful;
  private long failed;

  public Statistics(long startTimeMs) {
    this.startTimeMs = startTimeMs;
    this.successful = 0;
    this.failed = 0;
  }

  public synchronized long eventsPerSecond() {
    long nowMs = System.currentTimeMillis();
    long elapsedSeconds = (nowMs - startTimeMs) / 1000;
    return (successful / elapsedSeconds) + (failed / elapsedSeconds);
  }

  public synchronized long successful() {
      return this.successful;
  }

  public synchronized long failed() {
      return this.failed;
  }

  public void addRequests(long successful, long failed) {
    synchronized (this) {
      this.successful += successful;
      this.failed += failed;
    }
  }

}
