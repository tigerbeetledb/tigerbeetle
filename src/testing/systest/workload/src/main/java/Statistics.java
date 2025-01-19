/**
 * Collects basic statistics about requests completed by the workload.
 * This class is thread-safe.
 */
class Statistics {
  private long successful;
  private long failed;

  public Statistics() {
    this.successful = 0;
    this.failed = 0;
  }

  public synchronized long successful() {
      return this.successful;
  }

  public synchronized long failed() {
      return this.failed;
  }

  public void addEvents(long successful, long failed) {
    synchronized (this) {
      this.successful += successful;
      this.failed += failed;
    }
  }

}
