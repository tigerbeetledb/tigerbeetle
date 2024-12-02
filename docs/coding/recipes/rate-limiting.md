# Rate Limiting

TigerBeetle can be used to account for non-financial resources.

In this recipe, we will show you how to use it to implement rate limiting using the
[leaky bucket algorithm](https://en.wikipedia.org/wiki/Leaky_bucket) based on the user request rate,
bandwidth, and money.

## Mechanism

For each type of resource we want to limit, we will have a ledger specifically for that resource. On
that ledger, we have an operator account and an account for each user. Each user's account will have
a balance limit applied.

To set up the rate limiting system, we will first transfer the resource limit amount to each of the
users. For each user request, we will then create a
[pending transfer](../two-phase-transfers.md#reserve-funds-pending-transfer) with a
[timeout](../two-phase-transfers.md#expire-pending-transfer). We will never post or void these
transfers, but will instead let them expire.

Since each account's "balance" is limited, we cannot create pending transfers that would exceed the
rate limit. However, when each pending transfer expires, it automatically replenishes the user's
balance.

## Request Rate Limiting

Let's say we want to limit each user to 10 requests per minute.

We need our user account to have a limited balance.

| Ledger       | Account  | Flags                                                                                              |
| ------------ | -------- | -------------------------------------------------------------------------------------------------- |
| Request Rate | Operator | `0`                                                                                                |
| Request Rate | User     | [`debits_must_not_exceed_credits`](../../reference/account.md#flagsdebits_must_not_exceed_credits) |

We'll first transfer 10 units from the operator to the user.

| Transfer |       Ledger | Debit Account | Credit Account | Amount |
| -------- | -----------: | ------------: | -------------: | -----: |
| 1        | Request Rate |      Operator |           User |     10 |

Then, for each incoming request, we will create a pending transfer for 1 unit back to the operator
from the user:

| Transfer |       Ledger | Debit Account | Credit Account | Amount | Timeout |                                                 Flags |
| -------- | -----------: | ------------: | -------------: | -----: | ------- | ----------------------------------------------------: |
| 2-N      | Request Rate |          User |       Operator |      1 | 60      | [`pending`](../../reference/transfer.md#flagspending) |

Note that we use a timeout of 60 (seconds), because we wanted to limit each user to 10 requests _per
minute_.

That's it! Each of these transfers will "reserve" some of the user's balance and then replenish the
balance after they expire.

## Bandwidth Limiting

To limit user requests based on bandwidth as opposed to request rate, we can apply the same
technique but use amounts that represent the request size.

Let's say we wanted to limit each user to 10 MB (10,000,000 bytes) per minute.

Our account setup is the same as before:

| Ledger    | Account  | Flags                                                                                              |
| --------- | -------- | -------------------------------------------------------------------------------------------------- |
| Bandwidth | Operator | 0                                                                                                  |
| Bandwidth | User     | [`debits_must_not_exceed_credits`](../../reference/account.md#flagsdebits_must_not_exceed_credits) |

Now, we'll transfer 10,000,000 units (bytes in this case) from the operator to the user:

| Transfer |    Ledger | Debit Account | Credit Account |   Amount |
| -------- | --------: | ------------: | -------------: | -------: |
| 1        | Bandwidth |      Operator |           User | 10000000 |

For each incoming request, we'll create a pending transfer where the amount is equal to the request
size:

| Transfer |    Ledger | Debit Account | Credit Account |       Amount | Timeout |                                                 Flags |
| -------- | --------: | ------------: | -------------: | -----------: | ------- | ----------------------------------------------------: |
| 2-N      | Bandwidth |          User |       Operator | Request Size | 60      | [`pending`](../../reference/transfer.md#flagspending) |

We're again using a timeout of 60 seconds, but you could adjust this to be whatever time window you
want to use to limit requests.

## Transfer Amount Limiting

Now, let's say you wanted to limit each account to transferring no more than a certain amount of
money per time window. We can do that using 2 ledgers and linked transfers.

| Ledger        | Account  | Flags                                                                                              |
| ------------- | -------- | -------------------------------------------------------------------------------------------------- |
| Rate Limiting | Operator | 0                                                                                                  |
| Rate Limiting | User     | [`debits_must_not_exceed_credits`](../../reference/account.md#flagsdebits_must_not_exceed_credits) |
| USD           | Operator | 0                                                                                                  |
| USD           | User     | [`debits_must_not_exceed_credits`](../../reference/account.md#flagsdebits_must_not_exceed_credits) |

Let's say we wanted to limit each account to sending no more than 1000 USD per day.

To set up, we transfer 1000 from the Operator to the User on the Rate Limiting ledger:

| Transfer |        Ledger | Debit Account | Credit Account | Amount |
| -------- | ------------: | ------------: | -------------: | -----: |
| 1        | Rate Limiting |      Operator |           User |   1000 |

For each transfer the user wants to do, we will create 2 transfers that are
[linked](../../reference/requests/README.md#linked-events):

| Transfer |        Ledger | Debit Account | Credit Account |          Amount | Timeout |                                                                        Flags (Note `\|` sets multiple flags) |
| -------- | ------------: | ------------: | -------------: | --------------: | ------- | -----------------------------------------------------------------------------------------------------------: |
| 2N       | Rate Limiting |          User |       Operator | Transfer Amount | 86400   | [`pending`](../../reference/transfer.md#flagspending) \| [`linked`](../../reference/transfer.md#flagslinked) |
| 2N + 1   |           USD |          User |    Destination | Transfer Amount | 0       |                                                                                                            0 |

Note that we are using a timeout of 86400 seconds, because this is the number of seconds in a day.

These are linked such that if the first transfer fails, because the user has already transferred too
much money in the past day, the second transfer will also fail.
