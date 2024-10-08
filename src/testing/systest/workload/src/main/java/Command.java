import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;
import com.tigerbeetle.AccountBatch;
import com.tigerbeetle.AccountFlags;
import com.tigerbeetle.Client;
import com.tigerbeetle.CreateAccountResult;
import com.tigerbeetle.CreateAccountResultBatch;
import com.tigerbeetle.CreateTransferResult;
import com.tigerbeetle.IdBatch;
import com.tigerbeetle.TransferBatch;
import com.tigerbeetle.TransferFlags;
import com.tigerbeetle.UInt128;

interface Renderable {
  default void render(StringBuilder builder, boolean onNewLine, String indent) {
    if (onNewLine) {
      builder.append(indent);
    }
    builder.append(this);
  };
}

/**
 * A command (creation or query) that can be executed with a client. Every such command has an
 * associated result type.
 */
interface Command extends Renderable {
  Result execute(Client client);
}


/**
 * The result of executing some command, which can be reconciled with the model. For creations, this
 * might mean tracking more information about successfully created entities. For queries, it's a
 * hook to check consistency properties.
 */
interface Result extends Renderable {
  void reconcile(Model model);
  void assertIdempotent(Result other);
}

record ResultEntry<R, T extends Renderable>(R result, T value) {
  void render(StringBuilder builder, boolean onNewLine, String indent) {
    if (onNewLine) {
      builder.append(indent);
    }
    builder.append("(").append(result).append(") ");
    value.render(builder, false, indent);
  }
}


record NewAccount(long id, int ledger, int code, int flags) implements Renderable {
}


record CreateAccounts(ArrayList<NewAccount> accounts) implements Command {
  @Override
  public CreateAccountsResult execute(Client client) {
    AccountBatch batch = new AccountBatch(accounts.size());
    for (NewAccount account : accounts) {
      batch.add();
      batch.setId(account.id());
      batch.setLedger(account.ledger());
      batch.setCode(account.code());
      batch.setFlags(account.flags());
    }

    CreateAccountResultBatch accountErrors = client.createAccounts(batch);

    // Collect all results in a pre-filled list.
    var results = new ArrayList<CreateAccountResult>(
        Collections.nCopies(accounts.size(), CreateAccountResult.Ok));
    while (accountErrors.next()) {
      var index = accountErrors.getIndex();
      var result = accountErrors.getResult();
      results.set(index, result);
    }

    var entries = new ArrayList<ResultEntry<CreateAccountResult, NewAccount>>();

    int i = 0;
    for (NewAccount account : accounts) {
      entries.add(new ResultEntry<>(results.get(i), account));
      i++;
    }

    return new CreateAccountsResult(entries);
  }

  @Override 
  public void render(StringBuilder builder, boolean onNewLine, String indent) {
    builder
      .append(indent).append("create accounts:\n");

    for (var account : accounts) {
      account.render(builder, true, indent + "  ");
      builder.append(AccountFlags.hasLinked(account.flags()) ? " ⇩" : "");
      builder.append("\n");
    }
  }
}


record CreateAccountsResult(ArrayList<ResultEntry<CreateAccountResult, NewAccount>> entries)
    implements Result {

  @Override
  public void reconcile(Model model) {
    Optional<ResultEntry<CreateAccountResult, NewAccount>> previousEntry = Optional.empty();

    for (var current : entries) {
      var newAccount = current.value();

      // Check that linked accounts succeed or fail together.
      previousEntry.ifPresent(previous -> {
        if (AccountFlags.hasLinked(previous.value().flags())) {
          Assert.equal(
              previous.result() == CreateAccountResult.Ok, 
              current.result() == CreateAccountResult.Ok
          );
        }
      });
      previousEntry = Optional.of(current);

      Assert.equal(newAccount.ledger(), model.ledger);

      if (current.result() == CreateAccountResult.Ok) {
        Assert.that(
            !model.accounts.containsKey(newAccount.id()),
            "account already exists in model: %d",
            newAccount.id());

        var account = new CreatedAccount(newAccount.id(), newAccount.ledger(), newAccount.code(),
            newAccount.flags());

        model.accounts.put(account.id(), account);
      }
    }
  }

  @Override
  public void assertIdempotent(Result other) {
    switch (other) {
      case CreateAccountsResult(var entries) -> {
        Assert.that(this.entries.size() == entries.size(), "different number of entries in results");

        CreateAccountResult linkFailureThis = null;
        CreateAccountResult linkFailureOther = null;

        for (int i = 0; i < entries.size(); i++) {
          ResultEntry<CreateAccountResult, NewAccount> entryThis = this.entries.get(i);
          ResultEntry<CreateAccountResult, NewAccount> entryOther = entries.get(i);

          Assert.equal(entryThis.value(), entryOther.value());

          // Set failures for this chain if not already set by a prior event in the chain, or
          // if it's a linked failure.
          if (linkFailureThis == null || linkFailureThis == CreateAccountResult.LinkedEventFailed) {
            linkFailureThis = entryThis.result();
          }
          if (linkFailureOther == null || linkFailureOther == CreateAccountResult.LinkedEventFailed) {
            linkFailureOther = entryOther.result();
          }

          if (!AccountFlags.hasLinked(entryThis.value().flags())) {
            // Check consistency at end of chain, when we've decided which event's result to use.
            Assert.that(
                linkFailureThis == linkFailureOther
                  || linkFailureOther == CreateAccountResult.Exists,
                "entries at index %d do not have idempotency-consistent results: %s / %s",
                i,
                linkFailureThis,
                linkFailureOther);

            // Reset failures at end of chain.
            linkFailureThis = null;
            linkFailureOther = null;
          }
        }
      }
      default -> throw new AssertionError("%s != %s".formatted(getClass().getSimpleName(), other.getClass().getSimpleName()));
    }
  }

  @Override 
  public void render(StringBuilder builder, boolean onNewLine, String indent) {
    for (var entry : entries) {
      entry.render(builder, onNewLine, indent + "  ");
      builder.append(AccountFlags.hasLinked(entry.value().flags()) ? " ⇩" : "");
      builder.append("\n");
      onNewLine = true;
    }
  }

}

record NewTransfer(
    long id, 
    long debitAccountId, 
    long creditAccountId, 
    int ledger, 
    int code,
    BigInteger amount, 
    long pendingId, 
    int flags) implements Renderable {
}


record CreateTransfers(ArrayList<NewTransfer> transfers) implements Command {
  @Override
  public CreateTransfersResult execute(Client client) {
    TransferBatch batch = new TransferBatch(this.transfers.size());
    for (NewTransfer transfer : this.transfers) {
      batch.add();
      batch.setId(transfer.id());
      batch.setDebitAccountId(transfer.debitAccountId());
      batch.setCreditAccountId(transfer.creditAccountId());
      batch.setLedger(transfer.ledger());
      batch.setCode(transfer.code());
      batch.setAmount(transfer.amount());
      batch.setFlags(transfer.flags());
    }

    var transferErrors = client.createTransfers(batch);

    // Collect all results in a pre-filled list.
    var results = new ArrayList<CreateTransferResult>(
        Collections.nCopies(transfers.size(), CreateTransferResult.Ok));
    while (transferErrors.next()) {
      var index = transferErrors.getIndex();
      var result = transferErrors.getResult();
      results.set(index, result);
    }

    var entries = new ArrayList<ResultEntry<CreateTransferResult, NewTransfer>>();

    int i = 0;
    for (NewTransfer transfer : this.transfers) {
      entries.add(new ResultEntry<>(results.get(i), transfer));
      i++;
    }

    return new CreateTransfersResult(entries);
  }

  @Override 
  public void render(StringBuilder builder, boolean onNewLine, String indent) {
    builder
      .append(indent).append("create transfers:\n");

    for (var transfer : transfers) {
      transfer.render(builder, true, indent + "  ");
      builder.append(TransferFlags.hasLinked(transfer.flags()) ? " ⇩" : "");
      builder.append("\n");
    }
  }
}


record CreateTransfersResult(ArrayList<ResultEntry<CreateTransferResult, NewTransfer>> entries)
    implements Result {

  @Override
  public void reconcile(Model model) {
    Optional<ResultEntry<CreateTransferResult, NewTransfer>> previousEntry = Optional.empty();

    for (var current : entries) {
      var transfer = current.value();

      // Check that linked transfers succeed or fail together.
      previousEntry.ifPresent(previous -> {
        if (TransferFlags.hasLinked(previous.value().flags())) {
          Assert.equal(
              previous.result() == CreateTransferResult.Ok, 
              current.result() == CreateTransferResult.Ok);
        }
      });
      previousEntry = Optional.of(current);

      // No further validation needed for failed tranfers.
      if (current.result() != CreateTransferResult.Ok) {
        continue;
      }

      if (TransferFlags.hasPending(transfer.flags())) {
        Assert.that(
            model.pendingTransfers.add(transfer.id()),
            "pending transfers already contained %d",
            transfer.id());
      }
      if (TransferFlags.hasVoidPendingTransfer(transfer.flags()) 
          || TransferFlags.hasPostPendingTransfer(transfer.flags())) {
        Assert.that(
            model.pendingTransfers.remove(transfer.pendingId()),
            "pending transfers did not contain %d".formatted(transfer.pendingId()));
      } else {
        var debitAccount = model.accounts.get(transfer.debitAccountId());
        var creditAccount = model.accounts.get(transfer.creditAccountId());
        Assert.that(debitAccount != null, "debit account does not exist in model");
        Assert.that(creditAccount != null, "credit account does not exist in model");
        Assert.equal(debitAccount.ledger(), creditAccount.ledger());
      }
    }
  }

  @Override
  public void assertIdempotent(Result other) {
    switch (other) {
      case CreateTransfersResult(var entries) -> {
        Assert.that(this.entries.size() == entries.size(), "different number of entries in results");

        CreateTransferResult linkFailureThis = null;
        CreateTransferResult linkFailureOther = null;

        for (int i = 0; i < entries.size(); i++) {
          ResultEntry<CreateTransferResult, NewTransfer> entryThis = this.entries.get(i);
          ResultEntry<CreateTransferResult, NewTransfer> entryOther = entries.get(i);

          // Make sure the events do not change across retries.
          Assert.equal(entryThis.value(), entryOther.value());

          // Set failures for this chain if not already set by a prior event in the chain, or
          // if it's a linked failure.
          if ((linkFailureThis == null 
                || linkFailureThis == CreateTransferResult.LinkedEventFailed)
              && entryThis.result() != CreateTransferResult.Ok) {
            linkFailureThis = entryThis.result();
          }
          if ((linkFailureOther == null 
                || linkFailureOther == CreateTransferResult.LinkedEventFailed)
              && entryThis.result() != CreateTransferResult.Ok) {
            linkFailureOther = entryOther.result();
          }

          if (!TransferFlags.hasLinked(entryThis.value().flags())) {
            // Check consistency at end of chain, when we've decided which event's failure to use.
            Assert.that(
                linkFailureThis == linkFailureOther
                  || linkFailureOther == CreateTransferResult.Exists
                  || linkFailureOther == CreateTransferResult.IdAlreadyFailed,
                "entries at index %d do not have idempotency-consistent results: %s / %s",
                i,
                linkFailureThis,
                linkFailureOther);

            // Reset failures at end of chain.
            linkFailureThis = null;
            linkFailureOther = null;
          }
        }
      }
      default -> throw new AssertionError("%s != %s".formatted(
            getClass().getSimpleName(), 
            other.getClass().getSimpleName()));
    }
  }

  @Override 
  public void render(StringBuilder builder, boolean onNewLine, String indent) {
    for (var entry : entries) {
      entry.render(builder, onNewLine, indent + "  ");
      builder.append(AccountFlags.hasLinked(entry.value().flags()) ? " ⇩" : "");
      builder.append("\n");
      onNewLine = true;
    }
  }
}

record RetryCommand(
    Command command,
    Result originalResult
) implements Command {
  @Override
  public RetryResult execute(Client client) {
    var retryResult = command.execute(client);
    return new RetryResult(originalResult, retryResult);
  }

  @Override 
  public void render(StringBuilder builder, boolean onNewLine, String indent) {
    builder
      .append(indent).append("retry:\n")
      .append(indent + "  ").append("original command:\n");

    command.render(builder, true, indent + "    ");

    builder
      .append(indent + "  ").append("original result:\n");

    originalResult.render(builder, true, indent + "    ");

    builder.append("\n");
  }
}


record RetryResult(Result originalResult, Result retryResult)
    implements Result {

  @Override
  public void reconcile(Model model) {
    originalResult.assertIdempotent(retryResult);
  }


  @Override
  public void assertIdempotent(Result other) {
    throw new UnsupportedOperationException("RetryResult.assertIdempotent() not supported");
  }

  @Override 
  public void render(StringBuilder builder, boolean onNewLine, String indent) {
    builder
      .append(indent).append("retry:\n")
      .append(indent + "  ").append("original result:\n");

    originalResult.render(builder, true, indent + "    ");

    builder
      .append(indent + "  ").append("retry result:\n");

    retryResult.render(builder, true, indent + "    ");

    builder.append("\n");
  }

}

record LookupAccounts(long[] ids) implements Command {

  @Override
  public LookupAccountsResult execute(Client client) {
    IdBatch ids = new IdBatch(this.ids.length);
    for (long id : this.ids) {
      ids.add(id);
    }

    AccountBatch accounts = client.lookupAccounts(ids);

    // We assume all ids we lookup are from successfully created accounts.
    Assert.that(accounts.getLength() == this.ids.length, "expected all ids of lookup_accounts to exist");

    var results = new ArrayList<QueriedAccount>(accounts.getLength());
    while (accounts.next()) {
      results.add(new QueriedAccount(accounts.getId(UInt128.LeastSignificant), accounts.getLedger(),
          accounts.getCode(), accounts.getFlags(), accounts.getDebitsPosted(),
          accounts.getCreditsPosted()));
    }

    return new LookupAccountsResult(results);
  }
}


record QueriedAccount(long id, int ledger, int code, int flags, BigInteger debitsPosted,
    BigInteger creditsPosted) {
}


record LookupAccountsResult(ArrayList<QueriedAccount> accountsFound) implements Result {
  @Override
  public void reconcile(Model model) {
    // NOTE: These checks assume all known accounts in the ledger were queried.

    // All created accounts are found, and no others.
    Assert.that(
        model.accounts.keySet().equals(
          accountsFound.stream().map(a -> a.id()).collect(Collectors.toSet())),
        "all created accounts were not found by query");

    // All accounts found are in the correct ledger.
    for (var account : accountsFound) {
      Assert.that(
          account.ledger() == model.ledger,
          "found account with another ledger than the model ({} != {})",
          account.ledger(), 
          model.ledger);
    }

    // Total credits and total debits must be equal over all accounts.
    var diff = this.debitsCreditsDifference(accountsFound);
    Assert.that(
        diff == BigInteger.ZERO, 
        "expected debits and credits to be equal, but got diff: %d", 
        diff);
  }

  BigInteger debitsCreditsDifference(ArrayList<QueriedAccount> accounts) {
    var debits = BigInteger.ZERO;
    var credits = BigInteger.ZERO;
    for (var account : accounts) {
      debits = debits.add(account.debitsPosted());
      credits = credits.add(account.creditsPosted());
    }
    return debits.subtract(credits);
  }

  @Override
  public void assertIdempotent(Result other) {
    throw new UnsupportedOperationException("LookupAccountsResult.assertIdempotent() not supported");
  }
}

