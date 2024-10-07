import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import com.tigerbeetle.AccountFlags;
import com.tigerbeetle.Client;
import com.tigerbeetle.TransferFlags;

/**
 * This workload runs an infinite loop, generating and executing operations on a cluster.
 *
 * Any sucessful operations are reconciled with a model, tracking what accounts exist. Future
 * operations are generated based on this model.
 *
 * After every operation, all accounts are queried, and basic invariants are checked.
 */
public class Workload implements Callable<Void> {
  static int ACCOUNTS_COUNT_MAX = 100;
  static int BATCH_SIZE_MAX = 8190;

  final Model model;
  final Random random;
  final Client client;
  final int ledger;
  final Statistics statistics;

  public Workload(Random random, Client client, int ledger, Statistics statistics) {
    this.random = random;
    this.client = client;
    this.model = new Model(ledger);
    this.ledger = ledger;
    this.statistics = statistics;
  }

  @Override
  public Void call() {
    while (true) {
      var command = randomCommand();
      try {
        var result = command.execute(client);
        result.reconcile(model);

        switch (result) {
          case CreateAccountsResult(var entries) -> {
            recordResultEntries(entries);
          }
          case CreateTransfersResult(var entries) -> {
            recordResultEntries(entries);
          }
          default -> {
          }
        }

        lookupAllAccounts().ifPresent(query -> {
          var response = (LookupAccountsResult) query.execute(client);
          statistics.addRequests(response.accountsFound().size(), 0);
          response.reconcile(model);
        });
      } catch (AssertionError e) {
        System.err.println("ledger %d: Assertion failed after executing command: %s".formatted(
              ledger, 
              command));
        throw e;
      }
    }
  }

  <T> void recordResultEntries(ArrayList<ResultEntry<T>> entries) {
    for (var entry : entries) {
      if (entry.successful()) {
        statistics.addRequests(1, 0);
      } else {
        statistics.addRequests(0, 1);
      }
    }
  }

  Command<?> randomCommand() {
    // Commands are `Supplier`s of values. They are intially wrapped in `Optional`, to represent if
    // they are enabled. Further, they are wrapped in `WithOdds`, increasing the likelyhood of
    // certain commands being chosen.
    var commandsAll = List.of(WithOdds.of(1, createAccounts()), WithOdds.of(5, createTransfers()));

    // Here we select all commands that are currently enabled.
    var commandsEnabled = new ArrayList<WithOdds<Supplier<? extends Command<?>>>>();
    for (var command : commandsAll) {
      command.value().ifPresent(supplier -> {
        commandsEnabled.add(WithOdds.of(command.odds(), supplier));
      });
    }

    // There should always be at least one enabled command.
    assert !commandsEnabled.isEmpty() : "no commands are enabled";

    // Select and realize a single command based on the odds.
    return Arbitrary.odds(random, commandsEnabled).get();
  }

  Optional<Supplier<? extends Command<?>>> createAccounts() {
    // Note: IMPORTED accounts are not yet generated.

    int accountsCreatedCount = model.accounts.size();

    if (accountsCreatedCount < ACCOUNTS_COUNT_MAX) {
      return Optional.of(() -> {
        var newAccountsCount = random.nextInt(1,
            Math.min(ACCOUNTS_COUNT_MAX - accountsCreatedCount + 1, BATCH_SIZE_MAX));
        var newAccounts = new ArrayList<NewAccount>(newAccountsCount);

        for (int i = 0; i < newAccountsCount; i++) {
          var id = random.nextLong(0, Long.MAX_VALUE);
          var code = random.nextInt(1, 100);
          var flags = Arbitrary.flags(random,
              List.of(AccountFlags.LINKED,
                  AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS,
                  AccountFlags.CREDITS_MUST_NOT_EXCEED_DEBITS, 
                  AccountFlags.HISTORY));
          
          if (random.nextDouble(1.0) > 0.99) {
            flags |= AccountFlags.CLOSED;
          }
          newAccounts.add(new NewAccount(id, ledger, code, flags));
        }

        return new CreateAccounts(newAccounts);
      });
    } else {
      return Optional.empty();
    }

  }

  Optional<Supplier<? extends Command<?>>> createTransfers() {
    // Note: IMPORTED transfers are not yet generated.

    var accounts = model.allAccounts();

    // We can only transfer when there are at least two accounts.
    if (accounts.size() < 2) {
      return Optional.empty();
    }

    return Optional.of(() -> {
      var transfersCount = random.nextInt(1, BATCH_SIZE_MAX);
      var newTransfers = new ArrayList<NewTransfer>(transfersCount);

      for (int i = 0; i < transfersCount; i++) {
        var id = random.nextLong(1, Long.MAX_VALUE);
        var code = random.nextInt(1, 100);
        var amount = BigInteger.valueOf(random.nextLong(0, Long.MAX_VALUE));
        long pendingId = 0;
        var flags = TransferFlags.NONE;

        if (random.nextDouble(1.0) > 0.9 && !model.pendingTransfers.isEmpty()) {
          flags = Arbitrary.element(random, List.of(
                TransferFlags.POST_PENDING_TRANSFER, 
                TransferFlags.VOID_PENDING_TRANSFER));
        } else if (random.nextDouble(1.0) > 0.99999) {
          flags = Arbitrary.flags(random, List.of(
                TransferFlags.CLOSING_DEBIT, 
                TransferFlags.CLOSING_CREDIT)) | TransferFlags.PENDING;
        } else {
          flags = Arbitrary.flags(random, List.of(
                TransferFlags.PENDING,
                TransferFlags.BALANCING_DEBIT, 
                TransferFlags.BALANCING_CREDIT));
        }

        // The last transfer in a batch can't be linked.
        if (i < (transfersCount - 1) && random.nextDouble(1.0) > 0.9) {
          flags |= TransferFlags.LINKED;
        }

        int debitAccountIndex = random.nextInt(0, accounts.size());
        int creditAccountIndex = random.ints(0, accounts.size())
            .filter((index) -> index != debitAccountIndex).findFirst().orElseThrow();
        var debitAccountId = accounts.get(debitAccountIndex).id();
        var creditAccountId = accounts.get(creditAccountIndex).id();

        if (TransferFlags.hasPostPendingTransfer(flags) || TransferFlags.hasVoidPendingTransfer(flags)) {
          pendingId = Arbitrary.element(random, model.pendingTransfers);
          code = 0;
          debitAccountId = 0;
          creditAccountId = 0;
        }

        newTransfers.add(
            new NewTransfer(id, debitAccountId, creditAccountId, ledger, code, amount, pendingId, flags));
      }

      return new CreateTransfers(newTransfers);
    });
  }

  Optional<LookupAccounts> lookupAllAccounts() {
    var accounts = model.allAccounts();
    if (accounts.size() >= 1) {
      var ids = new long[accounts.size()];
      for (int i = 0; i < accounts.size(); i++) {
        ids[i] = accounts.get(i).id();
      }

      return Optional.of(new LookupAccounts(ids));
    }

    return Optional.empty();
  }
}

