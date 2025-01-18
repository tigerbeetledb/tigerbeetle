# Financial Accounting

For developers with non-financial backgrounds, TigerBeetle's use of accounting concepts like debits
and credits may be one of the trickier parts to understand. However, these concepts have been the
language of business for hundreds of years, so we promise it's worth it!

This page goes a bit deeper into debits and credits, double-entry bookkeeping, and how to think
about your accounts as part of a type system.

## Building Intuition with Two Simple Examples

If you have an outstanding loan and owe a bank $100, is your balance $100 or -$100? Conversely, if
you have $200 in your bank account, is the balance $200 or -$200?

Thinking about these two examples, we can start to build an intuition that the **positive or
negative sign of the balance depends on whose perspective we're looking from**. That $100 you owe
the bank represents a "bad" thing for you, but a "good" thing for the bank. We might think about
that same debt differently if we're doing your accounting or the bank's.

These examples also hint at the **different types of accounts**. We probably want to think about a
debt as having the opposite "sign" as the funds in your your bank account. At the same time, the
types of these accounts look different depending on whether you are considering them from the
perspective of you or the bank.

Now, back to our original questions: is the loan balance $100 or -$100 and is the bank account
balance $200 or -$200? On some level, this feels a bit arbitrary.

Wouldn't it be nice if there were some **commonly agreed-upon standards** so we wouldn't have to
make such an arbitrary decision? Yes! This is exactly what debits and credits and the financial
accounting type system provide.

## Types of Accounts

In financial accounting, there are 5 main types of accounts:

- **Asset** - what you own, which could produce income or which you could sell.
- **Liability** - what you owe to other people.
- **Equity** - value of the business owned by the owners or shareholders, or "the residual interest
  in the assets of the entity after deducting all its liabilities."[^1]
- **Income** - money or things of value you receive for selling products or services, or "increases
  in assets, or decreases in liabilities, that result in increases in equity, other than those
  relating to contributions from holders of equity claims."[^1]
- **Expense** - money you spend to pay for products or services, or "decreases in assets, or
  increases in liabilities, that result in decreases in equity, other than those relating to
  distributions to holders of equity claims."[^1]

[^1]:
    IFRS. _Conceptual Framework for Financial Reporting_. IFRS Foundation, 2018.
    <https://www.ifrs.org/content/dam/ifrs/publications/pdf-standards/english/2021/issued/part-a/conceptual-framework-for-financial-reporting.pdf>

As mentioned above, the type of account depends on whose perspective you are doing the accounting
from. In those examples, the loan you have from the bank is liability for you, because you owe the
amount to the bank. However, that same loan is an asset from the bank's perspective. In contrast,
the money in your bank account is an asset for you but it is a liability for the bank.

Each of these major categories are further subdivided into more specific types of accounts. For
example, in your personal accounting you would separately track the cash in your physical wallet
from the funds in your checking account, even though both of those are assets. The bank would split
out mortgages from car loans, even though both of those are also assets for the bank.

## Double-Entry Bookkeeping

Categorizing accounts into different types is useful for organizational purposes, but it also
provides a key error-correcting mechanism.

Every record in our accounting is not only recorded in one place, but in two. This is double-entry
bookkeeping. Why would we do that?

Let's think about the bank loan in our example above. When you took out the loan, two things
actually happened at the same time. On the one hand, you now owe the bank $100. At the same time,
the bank gave you $100. These are the two entries that comprise the loan transaction.

From your perspective, your liability to the bank increased by $100 while your assets also increased
by $100. From the bank's perspective, their assets (the loan to you) increased by $100 while their
liabilities (the money in your bank account) also increased by $100.

Double-entry bookkeeping ensures that funds are always accounted for. Money never just appears.
**Funds always go from somewhere to somewhere.**

## Keeping Accounts in Balance

Now we understand that there are different types of accounts and every transaction will be recorded
in two (or more) accounts -- but which accounts?

The [Fundamental Accounting Equation](https://en.wikipedia.org/wiki/Accounting_equation) stipulates
that:

**Assets - Liabilities = Equity**

Using our loan example, it's no accident that the loan increases assets and liabilities at the same
time. Assets and liabilities are on the opposite sides of the equation, and both sides must be
exactly equal. Loans increase assets and liabilities equally.

Here are some other types of transactions that would affect assets, liabilities, and equity, while
maintaining this balance:

- If you withdraw $100 in cash from your bank account, your total assets stay the same. Your bank
  account balance (an asset) would decrease while your physical cash (another asset) would increase.
- From the perspective of the bank, you withdrawing $100 in cash decreases their assets in the form
  of the cash they give you, while also decreasing their liabilities because your bank balance
  decreases as well.
- If a shareholder invests $1000 in the bank, that increases both the bank's assets and equity.

Assets, liabilities, and equity represent a point in time. The other two main categories, income and
expenses, represent flows of money in and out.

Income and expenses impact the position of the business over time. The expanded accounting equation
can be written as:

**Assets - Liabilities = Equity + Income − Expenses**

You don't need to memorize these equations (unless you're training as an accountant!). However, it
is useful to understand that those main account types lie on different sides of this equation.

## Debits and Credits vs Signed Integers

Instead of using a positive or negative integer to track a balance, TigerBeetle and double-entry
bookkeeping systems use **debits and credits**.

The two entries that give "double-entry bookkeeping" its name are the debit and the credit: every
transaction has at least one debit and at least one credit. (Note that for efficiency's sake,
TigerBeetle `Transfer`s consist of exactly one debit and one credit. These can be composed into more
complex [multi-debit, multi-credit transfers](./recipes/multi-debit-credit-transfers.md).) Which
entry is the debit and which is the credit? The answer is easy once you understand that **accounting
is a type system**. An account increases with a debit or credit according to its type.

When our example loan increases the assets and liabilities, we need to assign each of these entries
to either be a debit or a credit. At some level, this is completely arbitrary. For clarity,
accountants have used the same standards for hundreds of years:

### How Debits and Credits Increase or Decrease Account Balances

- **Assets and expenses are increased with debits, decreased with credits**
- **Liabilities, equity, and income are increased with credits, decreased with debits**

Or, in a table form:

|           | Debit | Credit |
| --------- | ----- | ------ |
| Asset     | +     | -      |
| Liability | -     | +      |
| Equity    | -     | +      |
| Income    | -     | +      |
| Expense   | +     | -      |

From the perspective of our example bank:

- You taking out a loan debits (increases) their loan assets and credits (increases) their bank
  account balance liabilities.
- You paying off the loan debits (decreases) their bank account balance liabilities and credits
  (decreases) their loan assets.
- You depositing cash debits (increases) their cash assets and credits (increases) their bank
  account balance liabilities.
- You withdrawing cash debits (decreases) their bank account balance liabilities and credits
  (decreases) their cash assets.

Note that accounting conventions also always write the debits first, to represent the funds flowing
_from_ the debited account _to_ the credited account.

If this seems arbitrary and confusing, we understand! It's a convention, just like how most
programmers need to learn zero-based array indexing and then at some point it becomes second nature.

### Account Types and the "Normal Balance"

Some other accounting systems have the concept of a "normal balance", which would indicate whether a
given account's balance is increased by debits or credits.

When designing for TigerBeetle, we recommend thinking about account types instead of "normal
balances". This is because the type of balance follows from the type of account, but the type of
balance doesn't tell you the type of account. For example, an account might have a normal balance on
the debit side but that doesn't tell you whether it is an asset or expense.

## Takeaways

- Accounts are categorized into types. The 5 main types are asset, liability, equity, income, and
  expense.
- Depending on the type of account, an increase is recorded as either a debit or a credit.
- All transfers consist of two entries, a debit and a credit. Double-entry bookkeeping ensures that
  all funds come from somewhere and go somewhere.

When you get started using TigerBeetle, we would recommend writing a list of all the types of
accounts in your system that you can think of. Then, think about whether, from the perspective of
your business, each account represents an asset, liability, equity, income, or expense. That
determines whether the given type of account is increased with a debit or a credit.

## Want More Help Understanding Debits and Credits?

### Ask the Community

Have questions about debits and credits, TigerBeetle's data model, how to design your application on
top of it, or anything else?

Come join our [Community Slack](https://slack.tigerbeetle.com/invite) and ask any and all questions
you might have!

### Dedicated Consultation

Would you like the TigerBeetle team to help you design your chart of accounts and leverage the power
of TigerBeetle in your architecture?

Let us help you get it right. Contact our CEO, Joran Dirk Greef, at <joran@tigerbeetle.com> to set
up a call.
