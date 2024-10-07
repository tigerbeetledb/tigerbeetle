import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Tracks information about the accounts created by test (e.g. their ids). The model should only be
 * used for a single ledger.
 */
public class Model {
  /**
   * Accounts by id.
   */
  HashMap<Long, CreatedAccount> accounts = new HashMap<>();
  public HashSet<Long> pendingTransfers = new HashSet<>();
  public final int ledger;

  public Model(int ledger) {
    this.ledger = ledger;
  }

  ArrayList<CreatedAccount> allAccounts() {
    // We use a sorted collection of accounts for stable index-based lookups.
    var allAccounts =
        accounts.values().stream().sorted(Comparator.comparing(account -> account.id())).toList();
    return new ArrayList<CreatedAccount>(allAccounts);
  }
}


/**
 * Tracks basic information about an account created by our test.
 */
record CreatedAccount(long id, int ledger, int code, int flags) {
}

