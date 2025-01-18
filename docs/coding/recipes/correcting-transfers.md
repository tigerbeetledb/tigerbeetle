# Correcting Transfers

[`Transfer`s](../../reference/transfer.md) in TigerBeetle are immutable, so once they are created
they cannot be modified or deleted.

Immutability is useful for creating an auditable log of all of the business events, but it does
raise the question of what to do when a transfer was made in error or some detail such as the amount
was incorrect.

## Always Add More Transfers

Correcting transfers or entries in TigerBeetle are handled with more transfers to reverse or adjust
the effects of the previous transfer(s).

This is important because adding transfers as opposed to deleting or modifying incorrect ones adds
more information to the history. The log of events includes the original error, when it took place,
as well as any attempts to correct the record and when they took place. A correcting entry might
even be wrong, in which case it itself can be corrected with yet another transfer. All of these
events form a timeline of the particular business event, which is stored permanently.

Another way to put this is that TigerBeetle is the lowest layer of the accounting stack and
represents the finest-resolution data that is stored. At a higher-level reporting layer, you can
"downsample" the data to show only the corrected transfer event. However, it would not be possible
to go back if the original record were modified or deleted.

Two specific recommendations for correcting transfers are:

1. You may want to have a [`Transfer.code`](../../reference/transfer.md#code) that indicates a given
   transfer is a correction, or you may want multiple codes where each one represents a different
   reason why the correction has taken place.
2. If you use the [`Transfer.user_data_128`](../../reference/transfer.md#user_data_128) to store an
   ID that links multiple transfers within TigerBeetle or points to a
   [record in an external database](../system-architecture.md), you may want to use the same
   `user_data_128` field on the correction transfer(s), even if they happen at a later point.

### Example

Let's say you had a couple of transfers, from account `A` to accounts `Y` and `Z`:

| Ledger | Debit Account | Credit Account | Amount | `code` | `user_data_128` | `flags.linked` |
| -----: | ------------: | -------------: | -----: | -----: | --------------: | -------------: |
|    USD |           `A` |            `X` |  10000 |    600 |          123456 |           true |
|    USD |           `A` |            `Y` |     50 |   9000 |          123456 |          false |

Now, let's say we realized the amount was wrong and we need to adjust both of the amounts by 10%. We
would submit two **additional** transfers going in the opposite direction:

| Ledger | Debit Account | Credit Account | Amount | `code` | `user_data_128` | `flags.linked` |
| -----: | ------------: | -------------: | -----: | -----: | --------------: | -------------: |
|    USD |           `X` |            `A` |   1000 |  10000 |          123456 |           true |
|    USD |           `Y` |            `A` |      5 |  10000 |          123456 |          false |

Note that the codes used here don't have any actual meaning, but you would want to
[enumerate your business events](../data-modeling.md#code) and map each to a numeric code value,
including the initial reasons for transfers and the reasons they might be corrected.
