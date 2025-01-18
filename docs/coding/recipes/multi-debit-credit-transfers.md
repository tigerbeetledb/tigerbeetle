# Multi-Debit, Multi-Credit Transfers

TigerBeetle is designed for maximum performance. In order to keep it lean, the database only
supports simple transfers with a single debit and a single credit.

However, you'll probably run into cases where you want transactions with multiple debits and/or
credits. For example, you might have a transfer where you want to extract fees and/or taxes.

Read on to see how to implement one-to-many and many-to-many transfers!

> Note that all of these examples use the
> [Linked Transfers flag (`flags.linked`)](../../reference/transfer.md#flagslinked) to ensure
> that all of the transfers succeed or fail together.

## One-to-Many Transfers

Transactions that involve multiple debits and a single credit OR a single debit and multiple credits
are relatively straightforward.

You can use multiple linked transfers as depicted below.

### Single Debit, Multiple Credits

This example debits a single account and credits multiple accounts. It uses the following accounts:

- A _source account_ `A`, on the `USD` ledger.
- Three _destination accounts_ `X`, `Y`, and `Z`, on the `USD` ledger.

| Ledger | Debit Account | Credit Account | Amount | `flags.linked` |
| -----: | ------------: | -------------: | -----: | -------------: |
|    USD |           `A` |            `X` |  10000 |           true |
|    USD |           `A` |            `Y` |     50 |           true |
|    USD |           `A` |            `Z` |     10 |          false |

### Multiple Debits, Single Credit

This example debits multiple accounts and credits a single account. It uses the following accounts:

- Three _source accounts_ `A`, `B`, and `C` on the `USD` ledger.
- A _destination account_ `X` on the `USD` ledger.

| Ledger | Debit Account | Credit Account | Amount | `flags.linked` |
| -----: | ------------: | -------------: | -----: | -------------: |
|    USD |           `A` |            `X` |  10000 |           true |
|    USD |           `B` |            `X` |     50 |           true |
|    USD |           `C` |            `X` |     10 |          false |

### Multiple Debits, Single Credit, Balancing debits

This example debits multiple accounts and credits a single account.
The total amount to transfer to the credit account is known (in this case, `100`), but the balances
of the individual debit accounts are not known. That is, each debit account should contribute as
much as possible (in order of precedence) up to the target, cumulative transfer amount.

It uses the following accounts:

- Three _source accounts_ `A`, `B`, and `C`, with [`flags.debits_must_not_exceed_credits`](../../reference/account.md#flagsdebits_must_not_exceed_credits).
- A _destination account_ `X`.
- A control account `LIMIT`, with [`flags.debits_must_not_exceed_credits`](../../reference/account.md#flagsdebits_must_not_exceed_credits).
- A control account `SETUP`, for setting up the `LIMIT` account.

| Id | Ledger | Debit Account | Credit Account | Amount | Flags |
| -: | -----: | ------------: | -------------: | -----: | :------------- |
|  1 |    USD |       `SETUP` |        `LIMIT` |    100 | [`linked`](../../reference/transfer.md#flagslinked) |
|  2 |    USD |           `A` |        `SETUP` |    100 | [`linked`](../../reference/transfer.md#flagslinked), [`balancing_debit`](../../reference/transfer.md#flagsbalancing_debit), [`balancing_credit`](../../reference/transfer.md#flagsbalancing_credit) |
|  3 |    USD |           `B` |        `SETUP` |    100 | [`linked`](../../reference/transfer.md#flagslinked), [`balancing_debit`](../../reference/transfer.md#flagsbalancing_debit), [`balancing_credit`](../../reference/transfer.md#flagsbalancing_credit) |
|  4 |    USD |           `C` |        `SETUP` |    100 | [`linked`](../../reference/transfer.md#flagslinked), [`balancing_debit`](../../reference/transfer.md#flagsbalancing_debit), [`balancing_credit`](../../reference/transfer.md#flagsbalancing_credit) |
|  5 |    USD |       `SETUP` |            `X` |    100 | [`linked`](../../reference/transfer.md#flagslinked) |
|  6 |    USD |       `LIMIT` |        `SETUP` |     -0 | [`balancing_credit`](../../reference/transfer.md#flagsbalancing_credit) |

If the cumulative [credit balance](../data-modeling.md#credit-balances) of `A + B + C` is less than
`100`, the chain will fail (transfer `6` will return `exceeds_credits`).

## Many-to-Many Transfers

Transactions with multiple debits and multiple credits are a bit more involved (but you got this!).

This is where the accounting concept of a Control Account comes in handy. We can use this as an
intermediary account, as illustrated below.

In this example, we'll use the following accounts:

- Two _source accounts_ `A` and `B` on the `USD` ledger.
- Three _destination accounts_ `X`, `Y`, and `Z`, on the `USD` ledger.
- A _compound entry control account_ `Control` on the `USD` ledger.

| Ledger | Debit Account | Credit Account | Amount | `flags.linked` |
| -----: | ------------: | -------------: | -----: | -------------: |
|    USD |           `A` |      `Control` |  10000 |           true |
|    USD |           `B` |      `Control` |     50 |           true |
|    USD |     `Control` |            `X` |   9000 |           true |
|    USD |     `Control` |            `Y` |   1000 |           true |
|    USD |     `Control` |            `Z` |     50 |          false |

Here, we use two transfers to debit accounts `A` and `B` and credit the `Control` account, and
another three transfers to credit accounts `X`, `Y`, and `Z`.

If you looked closely at this example, you may have noticed that we could have debited `B` and
credited `Z` directly because the amounts happened to line up. That is true!

For a little more extreme performance, you _might_ consider implementing logic to circumvent the
control account where possible, to reduce the number of transfers to implement a compound journal
entry.

However, if you're just getting started, you can avoid premature optimizations (we've all been
there!). You may find it easier to program these compound journal entries _always_ using a control
account -- and you can then come back to squeeze this performance out later!
