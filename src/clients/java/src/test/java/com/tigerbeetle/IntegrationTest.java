package com.tigerbeetle;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.math.BigInteger;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests using a TigerBeetle instance.
 */
public class IntegrationTest {
    private static final byte[] clusterId = new byte[16];

    private static Server server;
    private static Client client;

    private static AccountBatch generateAccounts(final byte[]... ids) {
        final var accounts = new AccountBatch(ids.length);

        for (var id : ids) {
            accounts.add();
            accounts.setId(id);
            accounts.setUserData128(100, 0);
            accounts.setUserData64(101);
            accounts.setUserData32(102);
            accounts.setLedger(720);
            accounts.setCode(1);
            accounts.setFlags(AccountFlags.NONE);
        }

        accounts.beforeFirst();
        return accounts;
    }

    @BeforeClass
    public static void initialize() throws Exception {
        server = new Server("tests");
        client = new Client(clusterId, new String[] {server.address});
    }

    @AfterClass
    public static void cleanup() throws Exception {
        client.close();
        server.close();
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorNullReplicaAddresses() throws Throwable {
        try (final var client = new Client(clusterId, null)) {
            assert false;
        }
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorNullElementReplicaAddresses() throws Throwable {
        final var replicaAddresses = new String[] {"3001", null};
        try (final var client = new Client(clusterId, replicaAddresses)) {
            assert false;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorEmptyReplicaAddresses() throws Throwable {
        final var replicaAddresses = new String[0];
        try (final var client = new Client(clusterId, replicaAddresses)) {
            assert false;
        }
    }

    @Test
    public void testConstructorReplicaAddressesLimitExceeded() throws Throwable {
        final var replicaAddresses = new String[100];
        for (int i = 0; i < replicaAddresses.length; i++) {
            replicaAddresses[i] = "3000";
        }

        try (final var client = new Client(clusterId, replicaAddresses)) {
            assert false;
        } catch (InitializationException initializationException) {
            assertEquals(InitializationStatus.AddressLimitExceeded.value,
                    initializationException.getStatus());
        }
    }

    @Test
    public void testConstructorEmptyStringReplicaAddresses() throws Throwable {
        final var replicaAddresses = new String[] {"", "", ""};
        try (final var client = new Client(clusterId, replicaAddresses)) {
            assert false;
        } catch (InitializationException initializationException) {
            assertEquals(InitializationStatus.AddressInvalid.value,
                    initializationException.getStatus());
        }
    }

    @Test
    public void testConstructorInvalidReplicaAddresses() throws Throwable {
        final var replicaAddresses = new String[] {"127.0.0.1:99999"};
        try (final var client = new Client(clusterId, replicaAddresses)) {
            assert false;
        } catch (InitializationException initializationException) {
            assertEquals(InitializationStatus.AddressInvalid.value,
                    initializationException.getStatus());
        }
    }

    public void testConstructorCluster() throws Throwable {
        final var clusterId = UInt128.id();
        final var replicaAddresses = new String[] {"3001"};
        try (final var client = new Client(clusterId, replicaAddresses)) {
            assertArrayEquals(clusterId, client.getClusterID());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorInvalidCluster() throws Throwable {
        final var clusterIdInvalid = new byte[] {0, 0, 0};
        final var replicaAddresses = new String[] {"3001"};
        try (final var client = new Client(clusterIdInvalid, replicaAddresses)) {
            assert false;
        }
    }

    @Test
    public void testCreateAccounts() throws Throwable {
        final var account1Id = UInt128.id();
        final var account2Id = UInt128.id();
        final var accounts = generateAccounts(account1Id, account2Id);

        final var createAccountErrors = client.createAccounts(accounts);
        assertTrue(createAccountErrors.getLength() == 0);
        assertHeader(accounts, createAccountErrors);

        final var lookupAccounts = client.lookupAccounts(new IdBatch(account1Id, account2Id));
        assertEquals(2, lookupAccounts.getLength());

        assertTrue(accounts.next());
        assertTrue(lookupAccounts.next());
        assertAccounts(accounts, lookupAccounts);

        assertTrue(accounts.next());
        assertTrue(lookupAccounts.next());
        assertAccounts(accounts, lookupAccounts);
    }

    @Test
    public void testCreateInvalidAccount() throws Throwable {
        final var zeroedAccounts = new AccountBatch(1);
        zeroedAccounts.add();

        final var createAccountErrors = client.createAccounts(zeroedAccounts);
        assertHeader(zeroedAccounts, createAccountErrors);

        assertTrue(createAccountErrors.getLength() == 1);
        assertTrue(createAccountErrors.next());
        assertEquals(CreateAccountResult.IdMustNotBeZero, createAccountErrors.getResult());
    }

    @Test
    public void testCreateAccountsAsync() throws Throwable {
        final var account1Id = UInt128.id();
        final var account2Id = UInt128.id();
        final var accounts = generateAccounts(account1Id, account2Id);

        CompletableFuture<CreateAccountResultBatch> accountsFuture =
                client.createAccountsAsync(accounts);

        final var createAccountErrors = accountsFuture.get();
        assertTrue(createAccountErrors.getLength() == 0);
        assertHeader(accounts, createAccountErrors);

        CompletableFuture<AccountBatch> lookupFuture =
                client.lookupAccountsAsync(new IdBatch(account1Id, account2Id));

        final var lookupAccounts = lookupFuture.get();
        assertEquals(2, lookupAccounts.getLength());

        assertTrue(accounts.next());
        assertTrue(lookupAccounts.next());
        assertAccounts(accounts, lookupAccounts);

        assertTrue(accounts.next());
        assertTrue(lookupAccounts.next());
        assertAccounts(accounts, lookupAccounts);
    }

    @Test
    public void testCreateTransfers() throws Throwable {
        final var account1Id = UInt128.id();
        final var account2Id = UInt128.id();
        final var transfer1Id = UInt128.id();
        final var transfer2Id = UInt128.id();

        final var accounts = generateAccounts(account1Id, account2Id);

        // Creating the accounts.
        final var createAccountErrors = client.createAccounts(accounts);
        assertTrue(createAccountErrors.getLength() == 0);

        // Creating a transfer.
        final var transfers = new TransferBatch(2);

        transfers.add();
        transfers.setId(transfer1Id);
        transfers.setCreditAccountId(account1Id);
        transfers.setDebitAccountId(account2Id);
        transfers.setLedger(720);
        transfers.setCode(1);
        transfers.setFlags(TransferFlags.NONE);
        transfers.setAmount(100);

        final var createTransferErrors = client.createTransfers(transfers);
        assertTrue(createTransferErrors.getLength() == 0);
        assertHeader(transfers, createTransferErrors);

        // Looking up the accounts.
        final var lookupAccounts = client.lookupAccounts(new IdBatch(account1Id, account2Id));
        assertTrue(lookupAccounts.getLength() == 2);

        accounts.beforeFirst();

        // Asserting the first account for the credit.
        assertTrue(accounts.next());
        assertTrue(lookupAccounts.next());
        assertAccounts(accounts, lookupAccounts);

        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());
        assertEquals(BigInteger.valueOf(100), lookupAccounts.getCreditsPosted());
        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());

        // Asserting the second account for the debit.
        assertTrue(accounts.next());
        assertTrue(lookupAccounts.next());
        assertAccounts(accounts, lookupAccounts);

        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());
        assertEquals(BigInteger.valueOf(100), lookupAccounts.getDebitsPosted());

        // Looking up and asserting the transfer.
        final var lookupTransfers = client.lookupTransfers(new IdBatch(transfer1Id, transfer2Id));
        assertEquals(1, lookupTransfers.getLength());

        transfers.beforeFirst();

        assertTrue(transfers.next());
        assertTrue(lookupTransfers.next());
        assertTransfers(transfers, lookupTransfers);
        assertNotEquals(0L, lookupTransfers.getTimestamp());
    }

    @Test
    public void testCreateTransfersAsync() throws Throwable {
        final var account1Id = UInt128.id();
        final var account2Id = UInt128.id();
        final var transfer1Id = UInt128.id();
        final var transfer2Id = UInt128.id();

        final var accounts = generateAccounts(account1Id, account2Id);

        // Creating the accounts.
        CompletableFuture<CreateAccountResultBatch> future = client.createAccountsAsync(accounts);

        final var createAccountErrors = future.get();
        assertTrue(createAccountErrors.getLength() == 0);

        // Creating a transfer.
        final var transfers = new TransferBatch(2);

        transfers.add();
        transfers.setId(transfer1Id);
        transfers.setCreditAccountId(account1Id);
        transfers.setDebitAccountId(account2Id);
        transfers.setLedger(720);
        transfers.setCode(1);
        transfers.setAmount(100);

        CompletableFuture<CreateTransferResultBatch> transfersFuture =
                client.createTransfersAsync(transfers);
        final var createTransferErrors = transfersFuture.get();
        assertTrue(createTransferErrors.getLength() == 0);
        assertHeader(transfers, createTransferErrors);

        // Looking up the accounts.
        CompletableFuture<AccountBatch> lookupAccountsFuture =
                client.lookupAccountsAsync(new IdBatch(account1Id, account2Id));
        final var lookupAccounts = lookupAccountsFuture.get();
        assertTrue(lookupAccounts.getLength() == 2);

        accounts.beforeFirst();

        // Asserting the first account for the credit.
        assertTrue(accounts.next());
        assertTrue(lookupAccounts.next());
        assertAccounts(accounts, lookupAccounts);

        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());
        assertEquals(BigInteger.valueOf(100), lookupAccounts.getCreditsPosted());
        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());

        // Asserting the second account for the debit.
        assertTrue(accounts.next());
        assertTrue(lookupAccounts.next());
        assertAccounts(accounts, lookupAccounts);

        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());
        assertEquals(BigInteger.valueOf(100), lookupAccounts.getDebitsPosted());

        // Looking up and asserting the transfer.
        CompletableFuture<TransferBatch> lookupTransfersFuture =
                client.lookupTransfersAsync(new IdBatch(transfer1Id, transfer2Id));
        final var lookupTransfers = lookupTransfersFuture.get();
        assertEquals(1, lookupTransfers.getLength());

        transfers.beforeFirst();

        assertTrue(transfers.next());
        assertTrue(lookupTransfers.next());
        assertTransfers(transfers, lookupTransfers);
        assertNotEquals(BigInteger.ZERO, lookupTransfers.getTimestamp());
    }

    @Test
    public void testCreateInvalidTransfer() throws Throwable {
        final var zeroedTransfers = new TransferBatch(1);
        zeroedTransfers.add();

        final var createTransfersErrors = client.createTransfers(zeroedTransfers);
        assertTrue(createTransfersErrors.getLength() == 1);
        assertHeader(zeroedTransfers, createTransfersErrors);
        assertTrue(createTransfersErrors.next());
        assertEquals(CreateTransferResult.IdMustNotBeZero, createTransfersErrors.getResult());
    }

    @Test
    public void testCreatePendingTransfers() throws Throwable {
        final var account1Id = UInt128.id();
        final var account2Id = UInt128.id();
        final var transfer1Id = UInt128.id();
        final var transfer2Id = UInt128.id();

        final var accounts = generateAccounts(account1Id, account2Id);

        // Creating the accounts.
        final var createAccountErrors = client.createAccounts(accounts);
        assertTrue(createAccountErrors.getLength() == 0);

        // Creating a pending transfer.
        final var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setId(transfer1Id);
        transfers.setCreditAccountId(account1Id);
        transfers.setDebitAccountId(account2Id);
        transfers.setLedger(720);
        transfers.setCode(1);
        transfers.setAmount(100);
        transfers.setFlags(TransferFlags.PENDING);
        transfers.setTimeout(Integer.MAX_VALUE);

        final var createTransfersErrors = client.createTransfers(transfers);
        assertTrue(createTransfersErrors.getLength() == 0);

        // Looking up the accounts.
        var lookupAccounts = client.lookupAccounts(new IdBatch(account1Id, account2Id));
        assertTrue(lookupAccounts.getLength() == 2);

        accounts.beforeFirst();

        // Asserting the first account for the pending credit.
        assertTrue(accounts.next());
        assertTrue(lookupAccounts.next());
        assertAccounts(accounts, lookupAccounts);

        assertEquals(BigInteger.valueOf(100), lookupAccounts.getCreditsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());
        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());

        // Asserting the second account for the pending debit.
        assertTrue(accounts.next());
        assertTrue(lookupAccounts.next());
        assertAccounts(accounts, lookupAccounts);

        assertEquals(BigInteger.valueOf(100), lookupAccounts.getDebitsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());
        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());

        // Looking up and asserting the pending transfer.
        var lookupTransfers = client.lookupTransfers(new IdBatch(transfer1Id));
        assertEquals(1, lookupTransfers.getLength());

        transfers.beforeFirst();

        assertTrue(transfers.next());
        assertTrue(lookupTransfers.next());
        assertTransfers(transfers, lookupTransfers);
        assertNotEquals(0L, lookupTransfers.getTimestamp());

        // Creating a post_pending transfer.
        final var confirmTransfers = new TransferBatch(1);
        confirmTransfers.add();
        confirmTransfers.setId(transfer2Id);
        confirmTransfers.setCreditAccountId(account1Id);
        confirmTransfers.setDebitAccountId(account2Id);
        confirmTransfers.setLedger(720);
        confirmTransfers.setCode(1);
        confirmTransfers.setAmount(100);
        confirmTransfers.setFlags(TransferFlags.POST_PENDING_TRANSFER);
        confirmTransfers.setPendingId(transfer1Id);

        final var createPostTransfersErrors = client.createTransfers(confirmTransfers);
        assertEquals(0, createPostTransfersErrors.getLength());

        // Looking up the accounts again for the updated balance.
        lookupAccounts = client.lookupAccounts(new IdBatch(account1Id, account2Id));
        assertTrue(lookupAccounts.getLength() == 2);

        accounts.beforeFirst();

        // Asserting the pending credit was posted for the first account.
        assertTrue(accounts.next());
        assertTrue(lookupAccounts.next());
        assertAccounts(accounts, lookupAccounts);

        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());
        assertEquals(BigInteger.valueOf(100), lookupAccounts.getCreditsPosted());
        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());

        // Asserting the pending debit was posted for the second account.
        assertTrue(accounts.next());
        assertTrue(lookupAccounts.next());
        assertAccounts(accounts, lookupAccounts);

        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());
        assertEquals(BigInteger.valueOf(100), lookupAccounts.getDebitsPosted());

        // Looking up and asserting the post_pending transfer.
        final var lookupVoidTransfers = client.lookupTransfers(new IdBatch(transfer2Id));
        assertEquals(1, lookupVoidTransfers.getLength());

        confirmTransfers.beforeFirst();

        assertTrue(confirmTransfers.next());
        assertTrue(lookupVoidTransfers.next());
        assertTransfers(confirmTransfers, lookupVoidTransfers);
        assertNotEquals(0L, lookupVoidTransfers.getTimestamp());
    }

    @Test
    public void testCreatePendingTransfersAndVoid() throws Throwable {
        final var account1Id = UInt128.id();
        final var account2Id = UInt128.id();
        final var transfer1Id = UInt128.id();
        final var transfer2Id = UInt128.id();

        final var accounts = generateAccounts(account1Id, account2Id);

        // Creating the accounts.
        final var createAccountErrors = client.createAccounts(accounts);
        assertTrue(createAccountErrors.getLength() == 0);

        // Creating a pending transfer.
        final var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setId(transfer1Id);
        transfers.setCreditAccountId(account1Id);
        transfers.setDebitAccountId(account2Id);
        transfers.setLedger(720);
        transfers.setCode(1);
        transfers.setAmount(100);
        transfers.setFlags(TransferFlags.PENDING);
        transfers.setTimeout(Integer.MAX_VALUE);

        final var createTransfersErrors = client.createTransfers(transfers);
        assertTrue(createTransfersErrors.getLength() == 0);

        // Looking up the accounts.
        var lookupAccounts = client.lookupAccounts(new IdBatch(account1Id, account2Id));
        assertTrue(lookupAccounts.getLength() == 2);

        accounts.beforeFirst();

        // Asserting the first account for the pending credit.
        assertTrue(accounts.next());
        assertTrue(lookupAccounts.next());
        assertAccounts(accounts, lookupAccounts);

        assertEquals(BigInteger.valueOf(100), lookupAccounts.getCreditsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());
        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());

        // Asserting the second account for the pending credit.
        assertTrue(accounts.next());
        assertTrue(lookupAccounts.next());
        assertAccounts(accounts, lookupAccounts);

        assertEquals(BigInteger.valueOf(100), lookupAccounts.getDebitsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());
        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());

        // Looking up and asserting the pending transfer.
        var lookupTransfers = client.lookupTransfers(new IdBatch(transfer1Id));
        assertEquals(1, lookupTransfers.getLength());

        transfers.beforeFirst();

        assertTrue(transfers.next());
        assertTrue(lookupTransfers.next());
        assertTransfers(transfers, lookupTransfers);
        assertNotEquals(0L, lookupTransfers.getTimestamp());

        // Creating a void_pending transfer.
        final var voidTransfers = new TransferBatch(2);
        voidTransfers.add();
        voidTransfers.setId(transfer2Id);
        voidTransfers.setCreditAccountId(account1Id);
        voidTransfers.setDebitAccountId(account2Id);
        voidTransfers.setLedger(720);
        voidTransfers.setCode(1);
        voidTransfers.setAmount(100);
        voidTransfers.setFlags(TransferFlags.VOID_PENDING_TRANSFER);
        voidTransfers.setPendingId(transfer1Id);

        final var createVoidTransfersErrors = client.createTransfers(voidTransfers);
        assertEquals(0, createVoidTransfersErrors.getLength());

        // Looking up the accounts again for the updated balance.
        lookupAccounts = client.lookupAccounts(new IdBatch(account1Id, account2Id));
        assertTrue(lookupAccounts.getLength() == 2);

        accounts.beforeFirst();

        // Asserting the pending credit was voided for the first account.
        assertTrue(accounts.next());
        assertTrue(lookupAccounts.next());
        assertAccounts(accounts, lookupAccounts);

        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());
        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());

        // Asserting the pending debit was voided for the second account.
        assertTrue(accounts.next());
        assertTrue(lookupAccounts.next());
        assertAccounts(accounts, lookupAccounts);

        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());
        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());

        // Looking up and asserting the void_pending transfer.
        final var lookupVoidTransfers = client.lookupTransfers(new IdBatch(transfer2Id));
        assertEquals(1, lookupVoidTransfers.getLength());

        voidTransfers.beforeFirst();

        assertTrue(voidTransfers.next());
        assertTrue(lookupVoidTransfers.next());
        assertTransfers(voidTransfers, lookupVoidTransfers);
        assertNotEquals(0L, lookupVoidTransfers.getTimestamp());
    }

    @Test
    public void testCreatePendingTransfersAndVoidExpired() throws Throwable {
        final var account1Id = UInt128.id();
        final var account2Id = UInt128.id();
        final var transfer1Id = UInt128.id();
        final var transfer2Id = UInt128.id();

        final var accounts = generateAccounts(account1Id, account2Id);

        // Creating the accounts.
        final var createAccountErrors = client.createAccounts(accounts);
        assertTrue(createAccountErrors.getLength() == 0);

        // Creating a pending transfer.
        final var transfers = new TransferBatch(1);
        transfers.add();

        transfers.setId(transfer1Id);
        transfers.setCreditAccountId(account1Id);
        transfers.setDebitAccountId(account2Id);
        transfers.setLedger(720);
        transfers.setCode(1);
        transfers.setAmount(100);
        transfers.setFlags(TransferFlags.PENDING);
        transfers.setTimeout(1);

        final var createTransfersErrors = client.createTransfers(transfers);
        assertTrue(createTransfersErrors.getLength() == 0);

        // Looking up the accounts.
        var lookupAccounts = client.lookupAccounts(new IdBatch(account1Id, account2Id));
        assertTrue(lookupAccounts.getLength() == 2);

        accounts.beforeFirst();

        // Asserting the first account for the pending credit.
        assertTrue(accounts.next());
        assertTrue(lookupAccounts.next());
        assertAccounts(accounts, lookupAccounts);

        assertEquals(BigInteger.valueOf(100), lookupAccounts.getCreditsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());
        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());

        // Asserting the second account for the pending credit.
        assertTrue(accounts.next());
        assertTrue(lookupAccounts.next());
        assertAccounts(accounts, lookupAccounts);

        assertEquals(BigInteger.valueOf(100), lookupAccounts.getDebitsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());
        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());

        // Looking up and asserting the pending transfer.
        final var lookupTransfers = client.lookupTransfers(new IdBatch(transfer1Id));
        assertEquals(1, lookupTransfers.getLength());

        transfers.beforeFirst();

        assertTrue(transfers.next());
        assertTrue(lookupTransfers.next());
        assertTransfers(transfers, lookupTransfers);
        assertNotEquals(0L, lookupTransfers.getTimestamp());

        // We need to wait 1s for the server to expire the transfer, however the
        // server can pulse the expiry operation anytime after the timeout,
        // so adding an extra delay to avoid flaky tests.
        final var timeout_ms = TimeUnit.SECONDS.toMillis(lookupTransfers.getTimeout());
        final var currentMilis = System.currentTimeMillis();
        final var extra_wait_time = 250L;
        Thread.sleep(timeout_ms + extra_wait_time);
        assertTrue(System.currentTimeMillis() - currentMilis > timeout_ms);

        // Looking up the accounts again for the updated balance.
        lookupAccounts = client.lookupAccounts(new IdBatch(account1Id, account2Id));
        assertTrue(lookupAccounts.getLength() == 2);

        accounts.beforeFirst();

        // Asserting the pending credit was voided.
        assertTrue(accounts.next());
        assertTrue(lookupAccounts.next());
        assertAccounts(accounts, lookupAccounts);

        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());
        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());

        // Asserting the pending debit was voided.
        assertTrue(accounts.next());
        assertTrue(lookupAccounts.next());
        assertAccounts(accounts, lookupAccounts);

        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());
        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());

        // Creating a void_pending transfer.
        final var voidTransfers = new TransferBatch(1);
        voidTransfers.add();
        voidTransfers.setId(transfer2Id);
        voidTransfers.setCreditAccountId(account1Id);
        voidTransfers.setDebitAccountId(account2Id);
        voidTransfers.setLedger(720);
        voidTransfers.setCode(1);
        voidTransfers.setAmount(100);
        voidTransfers.setFlags(TransferFlags.VOID_PENDING_TRANSFER);
        voidTransfers.setPendingId(transfer1Id);

        final var createVoidTransfersErrors = client.createTransfers(voidTransfers);
        assertEquals(1, createVoidTransfersErrors.getLength());
        assertTrue(createVoidTransfersErrors.next());
        assertEquals(CreateTransferResult.PendingTransferExpired,
                createVoidTransfersErrors.getResult());
    }

    @Test
    public void testCreateLinkedTransfers() throws Throwable {
        final var account1Id = UInt128.id();
        final var account2Id = UInt128.id();
        final var transfer1Id = UInt128.id();
        final var transfer2Id = UInt128.id();

        final var accounts = generateAccounts(account1Id, account2Id);

        final var createAccountErrors = client.createAccounts(accounts);
        assertTrue(createAccountErrors.getLength() == 0);

        final var transfers = new TransferBatch(2);
        transfers.add();
        transfers.setId(transfer1Id);
        transfers.setCreditAccountId(account1Id);
        transfers.setDebitAccountId(account2Id);
        transfers.setLedger(720);
        transfers.setCode(1);
        transfers.setAmount(100);
        transfers.setFlags(TransferFlags.LINKED);

        transfers.add();
        transfers.setId(transfer2Id);
        transfers.setCreditAccountId(account2Id);
        transfers.setDebitAccountId(account1Id);
        transfers.setLedger(720);
        transfers.setCode(1);
        transfers.setAmount(49);
        transfers.setFlags(TransferFlags.NONE);

        final var createTransfersErrors = client.createTransfers(transfers);
        assertTrue(createTransfersErrors.getLength() == 0);

        final var lookupAccounts = client.lookupAccounts(new IdBatch(account1Id, account2Id));
        assertEquals(2, lookupAccounts.getLength());

        accounts.beforeFirst();

        assertTrue(accounts.next());
        assertTrue(lookupAccounts.next());
        assertAccounts(accounts, lookupAccounts);

        assertEquals(BigInteger.valueOf(100), lookupAccounts.getCreditsPosted());
        assertEquals(BigInteger.valueOf(49), lookupAccounts.getDebitsPosted());
        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());

        assertTrue(accounts.next());
        assertTrue(lookupAccounts.next());
        assertAccounts(accounts, lookupAccounts);

        assertEquals(BigInteger.valueOf(49), lookupAccounts.getCreditsPosted());
        assertEquals(BigInteger.valueOf(100), lookupAccounts.getDebitsPosted());
        assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPending());
        assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPending());

        final var lookupTransfers = client.lookupTransfers(new IdBatch(transfer1Id, transfer2Id));
        assertEquals(2, lookupTransfers.getLength());

        transfers.beforeFirst();

        assertTrue(transfers.next());
        assertTrue(lookupTransfers.next());
        assertTransfers(transfers, lookupTransfers);
        assertNotEquals(0L, lookupTransfers.getTimestamp());

        assertTrue(transfers.next());
        assertTrue(lookupTransfers.next());

        assertTransfers(transfers, lookupTransfers);
        assertNotEquals(0L, lookupTransfers.getTimestamp());
    }

    @Test
    public void testCreateClosingTransfers() throws Throwable {
        final var account1Id = UInt128.id();
        final var account2Id = UInt128.id();
        final var closingTransferId = UInt128.id();

        final var accounts = generateAccounts(account1Id, account2Id);

        final var createAccountErrors = client.createAccounts(accounts);
        assertTrue(createAccountErrors.getLength() == 0);

        var transfers = new TransferBatch(1);
        transfers.add();
        transfers.setId(closingTransferId);
        transfers.setCreditAccountId(account1Id);
        transfers.setDebitAccountId(account2Id);
        transfers.setLedger(720);
        transfers.setCode(1);
        transfers.setAmount(0);
        transfers.setFlags(
                TransferFlags.CLOSING_CREDIT | TransferFlags.CLOSING_DEBIT | TransferFlags.PENDING);

        var createTransfersErrors = client.createTransfers(transfers);
        assertTrue(createTransfersErrors.getLength() == 0);

        var lookupAccounts = client.lookupAccounts(new IdBatch(account1Id, account2Id));
        assertEquals(2, lookupAccounts.getLength());

        accounts.beforeFirst();

        while (lookupAccounts.next()) {
            assertTrue(accounts.next());
            assertFalse(accounts.getFlags() == lookupAccounts.getFlags());
            assertTrue(AccountFlags.hasClosed(lookupAccounts.getFlags()));
        }

        transfers = new TransferBatch(1);
        transfers.add();
        transfers.setId(UInt128.id());
        transfers.setCreditAccountId(account1Id);
        transfers.setDebitAccountId(account2Id);
        transfers.setLedger(720);
        transfers.setCode(1);
        transfers.setAmount(0);
        transfers.setPendingId(closingTransferId);
        transfers.setFlags(TransferFlags.VOID_PENDING_TRANSFER);

        createTransfersErrors = client.createTransfers(transfers);
        assertTrue(createTransfersErrors.getLength() == 0);

        lookupAccounts = client.lookupAccounts(new IdBatch(account1Id, account2Id));
        assertEquals(2, lookupAccounts.getLength());

        accounts.beforeFirst();

        while (lookupAccounts.next()) {
            assertTrue(accounts.next());
            assertAccounts(accounts, lookupAccounts);
            assertFalse(AccountFlags.hasClosed(lookupAccounts.getFlags()));
        }
    }

    @Test
    public void testCreateAccountTooMuchData() throws Throwable {
        final int TOO_MUCH_DATA = 10_000;
        final var accounts = new AccountBatch(TOO_MUCH_DATA);
        for (int i = 0; i < TOO_MUCH_DATA; i++) {
            accounts.add();
            accounts.setId(UInt128.id());
            accounts.setCode(1);
            accounts.setLedger(1);
        }

        try {
            client.createAccounts(accounts);
            assert false;
        } catch (RequestException requestException) {
            assertEquals(PacketStatus.TooMuchData.value, requestException.getStatus());
        }
    }

    @Test
    public void testCreateAccountTooMuchDataAsync() throws Throwable {
        final int TOO_MUCH_DATA = 10_000;
        final var accounts = new AccountBatch(TOO_MUCH_DATA);
        for (int i = 0; i < TOO_MUCH_DATA; i++) {
            accounts.add();
            accounts.setId(UInt128.id());
            accounts.setCode(1);
            accounts.setLedger(1);
        }

        try {
            CompletableFuture<CreateAccountResultBatch> future =
                    client.createAccountsAsync(accounts);
            assert future != null;

            future.get();
            assert false;
        } catch (ExecutionException executionException) {
            assertTrue(executionException.getCause() instanceof RequestException);

            final var requestException = (RequestException) executionException.getCause();
            assertEquals(PacketStatus.TooMuchData.value, requestException.getStatus());
        }
    }

    @Test
    public void testCreateTransferTooMuchData() throws Throwable {
        final int TOO_MUCH_DATA = 10_000;
        final var transfers = new TransferBatch(TOO_MUCH_DATA);

        for (int i = 0; i < TOO_MUCH_DATA; i++) {
            transfers.add();
            transfers.setId(UInt128.id());
            transfers.setDebitAccountId(UInt128.id());
            transfers.setDebitAccountId(UInt128.id());
            transfers.setCode(1);
            transfers.setLedger(1);
        }

        try {
            client.createTransfers(transfers);
            assert false;
        } catch (RequestException requestException) {
            assertEquals(PacketStatus.TooMuchData.value, requestException.getStatus());
        }
    }

    @Test
    public void testCreateTransferTooMuchDataAsync() throws Throwable {
        final int TOO_MUCH_DATA = 10_000;
        final var transfers = new TransferBatch(TOO_MUCH_DATA);

        for (int i = 0; i < TOO_MUCH_DATA; i++) {
            transfers.add();
            transfers.setId(UInt128.id());
            transfers.setDebitAccountId(UInt128.id());
            transfers.setDebitAccountId(UInt128.id());
            transfers.setCode(1);
            transfers.setLedger(1);
        }

        try {
            CompletableFuture<CreateTransferResultBatch> future =
                    client.createTransfersAsync(transfers);
            assert future != null;

            future.get();
            assert false;

        } catch (ExecutionException executionException) {
            assertTrue(executionException.getCause() instanceof RequestException);

            final var requestException = (RequestException) executionException.getCause();
            assertEquals(PacketStatus.TooMuchData.value, requestException.getStatus());
        }
    }

    @Test
    public void testClientEvicted() throws Throwable {
        final int CLIENTS_MAX = 64;

        // Use a separate server to avoid evicting the test's shared client.
        try (final var server = new Server("testClientEvicted")) {
            try (final var client_evict =
                    new Client(clusterId, new String[] {server.getAddress()})) {
                var accounts_first = client_evict.lookupAccounts(new IdBatch(UInt128.id()));
                assertTrue(accounts_first.getLength() == 0);


                for (int i = 0; i < CLIENTS_MAX; i++) {
                    try (final var client =
                            new Client(clusterId, new String[] {server.getAddress()})) {
                        var accounts = client.lookupAccounts(new IdBatch(UInt128.id()));
                        assertTrue(accounts.getLength() == 0);
                    }
                }

                try {
                    var accounts = client_evict.lookupAccounts(new IdBatch(UInt128.id()));
                    assertTrue(accounts.getLength() == 0);
                    assert false;
                } catch (RequestException requestException) {
                    assertEquals(PacketStatus.ClientEvicted.value, requestException.getStatus());
                }
            }
        }
    }

    @Test
    public void testZeroLengthCreateAccounts() throws Throwable {
        final var accounts = new AccountBatch(1); // Capacity 1 but zero items.
        final var createAccountErrors = client.createAccounts(accounts);
        assertTrue(createAccountErrors.getLength() == 0);
        assertHeader(accounts, createAccountErrors);
    }

    @Test
    public void testZeroLengthCreateAccountsAsync() throws Throwable {
        final var accounts = new AccountBatch(1); // Capacity 1 but zero items.
        final var createAccountErrorsFuture = client.createAccountsAsync(accounts);
        final var createAccountErrors = createAccountErrorsFuture.get();
        assertTrue(createAccountErrors.getLength() == 0);
        assertHeader(accounts, createAccountErrors);
    }

    @Test
    public void testZeroLengthCreateTransfers() throws Throwable {
        final var transfers = new TransferBatch(0);
        final var createTransfersErrors = client.createTransfers(transfers);
        assertTrue(createTransfersErrors.getLength() == 0);
        assertHeader(transfers, createTransfersErrors);
    }

    @Test
    public void testZeroLengthCreateTransfersAsync() throws Throwable {
        final var transfers = new TransferBatch(0);
        final var createTransfersErrorsFuture = client.createTransfersAsync(transfers);
        final var createTransfersErrors = createTransfersErrorsFuture.get();
        assertTrue(createTransfersErrors.getLength() == 0);
        assertHeader(transfers, createTransfersErrors);
    }

    @Test
    public void testZeroLengthLookupAccounts() throws Throwable {
        final var ids = new IdBatch(0);
        final var accounts = client.lookupAccounts(ids);
        assertTrue(accounts.getLength() == 0);
        assertHeader(ids, accounts);
    }

    @Test
    public void testZeroLengthLookupAccountsAsync() throws Throwable {
        final var ids = new IdBatch(0);
        final var accountsFuture = client.lookupAccountsAsync(ids);
        final var accounts = accountsFuture.get();
        assertTrue(accounts.getLength() == 0);
        assertHeader(ids, accounts);
    }

    @Test
    public void testZeroLengthLookupTransfers() throws Throwable {
        final var ids = new IdBatch(0);
        final var transfers = client.lookupTransfers(ids);
        assertTrue(transfers.getLength() == 0);
        assertHeader(ids, transfers);
    }

    @Test
    public void testZeroLengthLookupTransfersAsync() throws Throwable {
        final var ids = new IdBatch(0);
        final var transfersFuture = client.lookupTransfersAsync(ids);
        final var transfers = transfersFuture.get();
        assertTrue(transfers.getLength() == 0);
        assertHeader(ids, transfers);
    }

    /**
     * This test asserts that the client can handle parallel threads.
     */
    @Test
    public void testConcurrentTasks() throws Throwable {
        final int TASKS_COUNT = 1000;
        final var barrier = new CountDownLatch(TASKS_COUNT);

        try (final var client = new Client(clusterId, new String[] {server.getAddress()})) {
            final var account1Id = UInt128.id();
            final var account2Id = UInt128.id();
            final var accounts = generateAccounts(account1Id, account2Id);

            final var createAccountErrors = client.createAccounts(accounts);
            assertTrue(createAccountErrors.getLength() == 0);

            final var tasks = new TransferTask[TASKS_COUNT];
            for (int i = 0; i < TASKS_COUNT; i++) {
                // Starting multiple threads submitting transfers.
                tasks[i] = new TransferTask(client, account1Id, account2Id, TransferFlags.NONE,
                        barrier, new CountDownLatch(0));
                tasks[i].start();
            }

            // Wait for all threads:
            for (int i = 0; i < TASKS_COUNT; i++) {
                tasks[i].join();
                assertTrue(tasks[i].result.getLength() == 0);
                assertNotNull(tasks[i].result.getHeader());
                assertTrue(tasks[i].result.getHeader().getTimestamp() != 0L);
            }

            // Asserting if all transfers were submitted correctly.
            final var lookupAccounts = client.lookupAccounts(new IdBatch(account1Id, account2Id));
            assertEquals(2, lookupAccounts.getLength());

            accounts.beforeFirst();

            assertTrue(accounts.next());
            assertTrue(lookupAccounts.next());
            assertAccounts(accounts, lookupAccounts);

            assertEquals(BigInteger.valueOf(100 * TASKS_COUNT), lookupAccounts.getCreditsPosted());
            assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());

            assertTrue(accounts.next());
            assertTrue(lookupAccounts.next());
            assertAccounts(accounts, lookupAccounts);

            assertEquals(BigInteger.valueOf(100 * TASKS_COUNT), lookupAccounts.getDebitsPosted());
            assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());
        }
    }

    /**
     * This test asserts that a linked chain is consistent across concurrent requests.
     */
    @Test
    public void testConcurrentLinkedChainsTasks() throws Throwable {
        final int TASKS_COUNT = 10_000;
        final var barrier = new CountDownLatch(TASKS_COUNT);

        try (final var client = new Client(clusterId, new String[] {server.getAddress()})) {
            final var account1Id = UInt128.id();
            final var account2Id = UInt128.id();
            final var accounts = generateAccounts(account1Id, account2Id);

            final var createAccountErrors = client.createAccounts(accounts);
            assertTrue(createAccountErrors.getLength() == 0);

            final var tasks = new TransferTask[TASKS_COUNT];
            for (int i = 0; i < TASKS_COUNT; i++) {
                // Starting multiple threads submitting transfers.
                // The Linked flag will cause the
                // batch to fail due to LinkedEventChainOpen.
                final var flag = i % 10 == 0 ? TransferFlags.LINKED : TransferFlags.NONE;
                tasks[i] = new TransferTask(client, account1Id, account2Id, flag, barrier,
                        new CountDownLatch(0));
                tasks[i].start();
            }

            // Wait for all threads:
            for (int i = 0; i < TASKS_COUNT; i++) {
                tasks[i].join();

                if (i % 10 == 0) {
                    assertTrue(tasks[i].result.getLength() == 1);
                    assertTrue(tasks[i].result.next());
                    assertTrue(tasks[i].result
                            .getResult() == CreateTransferResult.LinkedEventChainOpen);
                } else {
                    assertTrue(tasks[i].result.getLength() == 0);
                }
            }
        }
    }

    /**
     * This test asserts that client.close() will wait for all ongoing request to complete And new
     * threads trying to submit a request after the client was closed will fail with
     * IllegalStateException.
     */
    @Test
    public void testCloseWithConcurrentTasks() throws Throwable {
        // The goal here is to queue many concurrent requests,
        // so we have a good chance of testing "client.close()" not only before/after
        // calling "submit()", but also during the native call.
        //
        // Unfortunately this is a hacky test, but a reasonable one:
        // Since our JNI module does not expose the acquire_packet function,
        // we cannot insert a lock/wait in between "acquire_packet" and "submit"
        // in order to cause and assert each variant.
        final int TASKS_COUNT = 256;
        final var enterBarrier = new CountDownLatch(TASKS_COUNT);
        final var exitBarrier = new CountDownLatch(1);

        try (final var client = new Client(clusterId, new String[] {server.getAddress()})) {
            final var account1Id = UInt128.id();
            final var account2Id = UInt128.id();
            final var accounts = generateAccounts(account1Id, account2Id);

            final var createAccountErrors = client.createAccounts(accounts);
            assertTrue(createAccountErrors.getLength() == 0);

            final var tasks = new TransferTask[TASKS_COUNT];
            for (int i = 0; i < TASKS_COUNT; i++) {

                // Starting multiple threads submitting transfers.
                tasks[i] = new TransferTask(client, account1Id, account2Id, TransferFlags.NONE,
                        enterBarrier, exitBarrier);
                tasks[i].start();
            }

            // Waits until one thread finish.
            exitBarrier.await();

            // And then close the client while threads are still working
            // Some of them have already submitted the request, while others will fail
            // due to "shutdown".
            client.close();

            int failedCount = 0;
            int succeededCount = 0;

            for (int i = 0; i < TASKS_COUNT; i++) {

                // The client.close must wait until all submitted requests have completed
                // Asserting that either the task succeeded or failed while waiting.
                tasks[i].join();

                final var succeeded = tasks[i].result != null && tasks[i].result.getLength() == 0;

                // Can fail due to client closed.
                final var failed = tasks[i].exception != null
                        && tasks[i].exception instanceof IllegalStateException;

                assertTrue(failed || succeeded);

                if (failed) {
                    failedCount += 1;
                } else if (succeeded) {
                    succeededCount += 1;
                }
            }

            assertTrue(succeededCount + failedCount == TASKS_COUNT);
        }
    }

    /**
     * This test asserts that submit a request after the client was closed will fail with
     * IllegalStateException.
     */
    @Test
    public void testClose() throws Throwable {
        try {
            // As we call client.close() explicitly,
            // we don't need to use a try-with-resources block here.
            final var client = new Client(clusterId, new String[] {server.getAddress()});

            // Creating accounts.
            final var account1Id = UInt128.id();
            final var account2Id = UInt128.id();
            final var accounts = generateAccounts(account1Id, account2Id);

            final var createAccountErrors = client.createAccounts(accounts);
            assertTrue(createAccountErrors.getLength() == 0);

            // Closing the client.
            client.close();

            // Creating a transfer with a closed client.
            final var transfers = new TransferBatch(2);

            transfers.add();
            transfers.setId(UInt128.id());
            transfers.setCreditAccountId(account1Id);
            transfers.setDebitAccountId(account2Id);
            transfers.setLedger(720);
            transfers.setCode(1);
            transfers.setFlags(TransferFlags.NONE);
            transfers.setAmount(100);

            client.createTransfers(transfers);
            assert false;

        } catch (Throwable any) {
            assertEquals(IllegalStateException.class, any.getClass());
        }
    }

    /**
     * Smoke test that concurrent close does not crash the JVM.
     */
    @Test
    public void testCloseConcurrent() throws Throwable {
        final int threadCount = 16;
        final var clients = IntStream.range(0, threadCount)
                .mapToObj((index) -> new Client(clusterId, new String[] {server.getAddress()}))
                .collect(Collectors.toList());
        final var threads = IntStream.range(0, threadCount).mapToObj((index) -> {
            final var thread = new Thread(() -> {
                final var client = clients.get(index);
                for (int i = 0; i < 1000; i++) {
                    try {
                        client.createAccounts(generateAccounts(UInt128.id()));
                    } catch (IllegalStateException e) {
                        break;
                    }
                }
            });
            thread.start();
            return thread;
        }).collect(Collectors.toList());
        for (var client : clients) {
            client.close();
        }
        for (var thread : threads) {
            thread.join();
        }
    }

    /**
     * This test asserts that async calls will not block.
     */
    @Test
    public void testAsyncTasks() throws Throwable {
        final int TASKS_COUNT = 1_000_000;
        try (final var client = new Client(clusterId, new String[] {server.getAddress()})) {

            final var account1Id = UInt128.id();
            final var account2Id = UInt128.id();
            final var accounts = generateAccounts(account1Id, account2Id);

            final var createAccountErrors = client.createAccounts(accounts);
            assertTrue(createAccountErrors.getLength() == 0);

            final var tasks = new CompletableFuture[TASKS_COUNT];
            for (int i = 0; i < TASKS_COUNT; i++) {

                final var transfers = new TransferBatch(1);
                transfers.add();

                transfers.setId(UInt128.id());
                transfers.setCreditAccountId(account1Id);
                transfers.setDebitAccountId(account2Id);
                transfers.setLedger(720);
                transfers.setCode(1);
                transfers.setAmount(100);

                tasks[i] = client.createTransfersAsync(transfers);
            }

            // Wait for all tasks.
            for (int i = 0; i < TASKS_COUNT; i++) {
                @SuppressWarnings("unchecked")
                final var future = (CompletableFuture<CreateTransferResultBatch>) tasks[i];
                final var result = future.get();
                assertEquals(0, result.getLength());
                assertNotNull(result.getHeader());
                assertTrue(result.getHeader().getTimestamp() != 0L);
            }

            // Asserting if all transfers were submitted correctly.
            final var lookupAccounts = client.lookupAccounts(new IdBatch(account1Id, account2Id));
            assertEquals(2, lookupAccounts.getLength());

            accounts.beforeFirst();

            assertTrue(accounts.next());
            assertTrue(lookupAccounts.next());
            assertAccounts(accounts, lookupAccounts);

            assertEquals(BigInteger.valueOf(100 * TASKS_COUNT), lookupAccounts.getCreditsPosted());
            assertEquals(BigInteger.ZERO, lookupAccounts.getDebitsPosted());

            assertTrue(accounts.next());
            assertTrue(lookupAccounts.next());
            assertAccounts(accounts, lookupAccounts);

            assertEquals(BigInteger.valueOf(100 * TASKS_COUNT), lookupAccounts.getDebitsPosted());
            assertEquals(BigInteger.ZERO, lookupAccounts.getCreditsPosted());
        }
    }

    /**
     * This test asserts that the client can handle thread interruption.
     */
    @Test
    public void testConcurrentInterruptedTasks() throws Throwable {
        final int TASKS_COUNT = 256;
        final var barrier = new CountDownLatch(TASKS_COUNT);
        final var zeroedId = UInt128.asBytes(0L);

        // Connect to an invalid cluster, so the calls never complete.
        try (final var client = new Client(clusterId, new String[] {"0"})) {
            final var tasks = new TransferTask[TASKS_COUNT];
            for (int i = 0; i < TASKS_COUNT; i++) {
                // Starting multiple threads.
                tasks[i] = new TransferTask(client, zeroedId, zeroedId, TransferFlags.NONE, barrier,
                        new CountDownLatch(0));
                tasks[i].start();
            }

            // Waits until all threads start.
            barrier.await();

            // Interrupt all threads.
            for (final var task : tasks) {
                task.interrupt();
                task.join();

                assertTrue(task.getState() == Thread.State.TERMINATED);
                assertTrue(task.result == null);
                assertTrue(task.exception != null);
                assertTrue(task.exception instanceof InterruptedException ||
                // TODO: Interrupted operations throw unchecked `AssertionError`.
                // We should change the client API adding `throws InterruptedException` instead.
                        (task.exception instanceof AssertionError
                                && task.exception.getCause() instanceof InterruptedException));
            }
        }
    }

    @Test
    public void testAccountTransfers() throws Throwable {
        final var account1Id = UInt128.id();
        final var account2Id = UInt128.id();

        {
            final var accounts = generateAccounts(account1Id, account2Id);

            // Enabling AccountFlags.HISTORY:
            while (accounts.next()) {
                accounts.setFlags(accounts.getFlags() | AccountFlags.HISTORY);
            }
            accounts.beforeFirst();

            // Creating the accounts.
            final var createAccountsErrors = client.createAccounts(accounts);
            assertTrue(createAccountsErrors.getLength() == 0);
        }

        {
            // Creating a transfer.
            final var transfers = new TransferBatch(10);
            for (int i = 0; i < 10; i++) {
                transfers.add();
                transfers.setId(UInt128.id());

                // Swap the debit and credit accounts:
                if (i % 2 == 0) {
                    transfers.setCreditAccountId(account1Id);
                    transfers.setDebitAccountId(account2Id);
                } else {
                    transfers.setCreditAccountId(account2Id);
                    transfers.setDebitAccountId(account1Id);
                }

                transfers.setLedger(720);
                transfers.setCode(1);
                transfers.setFlags(TransferFlags.NONE);
                transfers.setAmount(100);
            }

            final var createTransfersErrors = client.createTransfers(transfers);
            assertTrue(createTransfersErrors.getLength() == 0);
        }

        {
            // Querying transfers where:
            // `debit_account_id=$account1Id OR credit_account_id=$account1Id
            // ORDER BY timestamp ASC`.
            final var filter = new AccountFilter();
            filter.setAccountId(account1Id);
            filter.setTimestampMin(0);
            filter.setTimestampMax(0);
            filter.setLimit(254);
            filter.setDebits(true);
            filter.setCredits(true);
            filter.setReversed(false);
            final var accountTransfers = client.getAccountTransfers(filter);
            final var accountBalances = client.getAccountBalances(filter);
            assertTrue(accountTransfers.getLength() == 10);
            assertTrue(accountBalances.getLength() == 10);
            long timestamp = 0;
            while (accountTransfers.next()) {
                assertTrue(Long.compareUnsigned(accountTransfers.getTimestamp(), timestamp) > 0);
                timestamp = accountTransfers.getTimestamp();

                assertTrue(accountBalances.next());
                assertEquals(accountTransfers.getTimestamp(), accountBalances.getTimestamp());
            }
        }

        {
            // Querying transfers where:
            // `debit_account_id=$account2Id OR credit_account_id=$account2Id
            // ORDER BY timestamp DESC`.
            final var filter = new AccountFilter();
            filter.setAccountId(account2Id);
            filter.setTimestampMin(0);
            filter.setTimestampMax(0);
            filter.setLimit(254);
            filter.setDebits(true);
            filter.setCredits(true);
            filter.setReversed(true);
            final var accountTransfers = client.getAccountTransfers(filter);
            final var accountBalances = client.getAccountBalances(filter);

            assertTrue(accountTransfers.getLength() == 10);
            assertTrue(accountBalances.getLength() == 10);
            long timestamp = Long.MIN_VALUE; // MIN_VALUE is the unsigned MAX_VALUE.
            while (accountTransfers.next()) {
                assertTrue(Long.compareUnsigned(accountTransfers.getTimestamp(), timestamp) < 0);
                timestamp = accountTransfers.getTimestamp();

                assertTrue(accountBalances.next());
                assertEquals(accountTransfers.getTimestamp(), accountBalances.getTimestamp());
            }
        }

        {
            // Querying transfers where:
            // `debit_account_id=$account1Id
            // ORDER BY timestamp ASC`.
            final var filter = new AccountFilter();
            filter.setAccountId(account1Id);
            filter.setTimestampMin(0);
            filter.setTimestampMax(0);
            filter.setLimit(254);
            filter.setDebits(true);
            filter.setCredits(false);
            filter.setReversed(false);
            final var accountTransfers = client.getAccountTransfers(filter);
            final var accountBalances = client.getAccountBalances(filter);

            assertTrue(accountTransfers.getLength() == 5);
            assertTrue(accountBalances.getLength() == 5);
            long timestamp = 0;
            while (accountTransfers.next()) {
                assertTrue(Long.compareUnsigned(accountTransfers.getTimestamp(), timestamp) > 0);
                timestamp = accountTransfers.getTimestamp();

                assertTrue(accountBalances.next());
                assertEquals(accountTransfers.getTimestamp(), accountBalances.getTimestamp());
            }
        }

        {
            // Querying transfers where:
            // `credit_account_id=$account2Id
            // ORDER BY timestamp DESC`.
            final var filter = new AccountFilter();
            filter.setAccountId(account2Id);
            filter.setTimestampMin(1);
            filter.setTimestampMax(-2L); // -2L == ulong max - 1
            filter.setLimit(254);
            filter.setDebits(false);
            filter.setCredits(true);
            filter.setReversed(true);
            final var accountTransfers = client.getAccountTransfers(filter);
            final var accountBalances = client.getAccountBalances(filter);

            assertTrue(accountTransfers.getLength() == 5);
            assertTrue(accountBalances.getLength() == 5);

            long timestamp = Long.MIN_VALUE; // MIN_VALUE is the unsigned MAX_VALUE.
            while (accountTransfers.next()) {
                assertTrue(Long.compareUnsigned(accountTransfers.getTimestamp(), timestamp) < 0);
                timestamp = accountTransfers.getTimestamp();

                assertTrue(accountBalances.next());
                assertEquals(accountTransfers.getTimestamp(), accountBalances.getTimestamp());
            }
        }

        {
            // Querying transfers where:
            // `debit_account_id=$account1Id OR credit_account_id=$account1Id
            // ORDER BY timestamp ASC LIMIT 5`.
            final var filter = new AccountFilter();
            filter.setAccountId(account1Id);
            filter.setTimestampMin(0);
            filter.setTimestampMax(0);
            filter.setLimit(5);
            filter.setDebits(true);
            filter.setCredits(true);
            filter.setReversed(false);

            // First 5 items:
            final var accountTransfers1 = client.getAccountTransfers(filter);
            final var accountBalances1 = client.getAccountBalances(filter);

            assertTrue(accountTransfers1.getLength() == 5);
            assertTrue(accountBalances1.getLength() == 5);

            long timestamp = 0;
            while (accountTransfers1.next()) {
                assertTrue(Long.compareUnsigned(accountTransfers1.getTimestamp(), timestamp) > 0);
                timestamp = accountTransfers1.getTimestamp();

                assertTrue(accountBalances1.next());
                assertEquals(accountTransfers1.getTimestamp(), accountBalances1.getTimestamp());
            }

            // Next 5 items from this timestamp:
            filter.setTimestampMin(timestamp + 1);
            final var accountTransfers2 = client.getAccountTransfers(filter);
            final var accountBalances2 = client.getAccountBalances(filter);

            assertTrue(accountTransfers2.getLength() == 5);
            assertTrue(accountBalances2.getLength() == 5);

            while (accountTransfers2.next()) {
                assertTrue(Long.compareUnsigned(accountTransfers2.getTimestamp(), timestamp) > 0);
                timestamp = accountTransfers2.getTimestamp();

                assertTrue(accountBalances2.next());
                assertEquals(accountTransfers2.getTimestamp(), accountBalances2.getTimestamp());
            }

            // No more results after that timestamp:
            filter.setTimestampMin(timestamp + 1);
            final var accountTransfers3 = client.getAccountTransfers(filter);
            final var accountBalances3 = client.getAccountBalances(filter);

            assertTrue(accountTransfers3.getLength() == 0);
            assertTrue(accountBalances3.getLength() == 0);
        }

        {
            // Querying transfers where:
            // `debit_account_id=$account2Id OR credit_account_id=$account2Id
            // ORDER BY timestamp DESC LIMIT 5`.
            final var filter = new AccountFilter();
            filter.setAccountId(account2Id);
            filter.setTimestampMin(0);
            filter.setTimestampMax(0);
            filter.setLimit(5);
            filter.setDebits(true);
            filter.setCredits(true);
            filter.setReversed(true);

            // First 5 items:
            final var accountTransfers1 = client.getAccountTransfers(filter);
            final var accountBalances1 = client.getAccountBalances(filter);

            assertTrue(accountTransfers1.getLength() == 5);
            assertTrue(accountTransfers1.getLength() == 5);

            long timestamp = Long.MIN_VALUE; // MIN_VALUE is the unsigned MAX_VALUE.
            while (accountTransfers1.next()) {
                assertTrue(Long.compareUnsigned(accountTransfers1.getTimestamp(), timestamp) < 0);
                timestamp = accountTransfers1.getTimestamp();

                assertTrue(accountBalances1.next());
                assertEquals(accountTransfers1.getTimestamp(), accountBalances1.getTimestamp());
            }

            // Next 5 items from this timestamp:
            filter.setTimestampMax(timestamp - 1);
            final var accountTransfers2 = client.getAccountTransfers(filter);
            final var accountBalances2 = client.getAccountBalances(filter);

            assertTrue(accountTransfers2.getLength() == 5);
            assertTrue(accountBalances2.getLength() == 5);

            while (accountTransfers2.next()) {
                assertTrue(Long.compareUnsigned(accountTransfers2.getTimestamp(), timestamp) < 0);
                timestamp = accountTransfers2.getTimestamp();

                assertTrue(accountBalances2.next());
                assertEquals(accountTransfers2.getTimestamp(), accountBalances2.getTimestamp());
            }

            // No more results before that timestamp:
            filter.setTimestampMax(timestamp - 1);
            final var accountTransfers3 = client.getAccountTransfers(filter);
            final var accountBalances3 = client.getAccountBalances(filter);

            assertTrue(accountTransfers3.getLength() == 0);
            assertTrue(accountBalances3.getLength() == 0);
        }

        // For those tests it doesn't matter using the sync or async version. We use the async
        // version here for test coverage purposes, but there's no need to duplicate the tests.

        {
            // Empty filter:
            final var filter = new AccountFilter();
            assertTrue(client.getAccountTransfersAsync(filter).get().getLength() == 0);
            assertTrue(client.getAccountBalancesAsync(filter).get().getLength() == 0);
        }

        {
            // Invalid account:
            final var filter = new AccountFilter();
            filter.setAccountId(0);
            filter.setTimestampMin(0);
            filter.setTimestampMax(0);
            filter.setLimit(254);
            filter.setDebits(true);
            filter.setCredits(true);
            filter.setReversed(false);
            assertTrue(client.getAccountTransfersAsync(filter).get().getLength() == 0);
            assertTrue(client.getAccountBalancesAsync(filter).get().getLength() == 0);
        }

        {
            // Invalid timestamp min:
            final var filter = new AccountFilter();
            filter.setAccountId(account2Id);
            filter.setTimestampMin(-1L); // -1L == ulong max value
            filter.setTimestampMax(0);
            filter.setLimit(254);
            filter.setDebits(true);
            filter.setCredits(true);
            filter.setReversed(false);
            assertTrue(client.getAccountTransfersAsync(filter).get().getLength() == 0);
            assertTrue(client.getAccountBalancesAsync(filter).get().getLength() == 0);
        }

        {
            // Invalid timestamp max:
            final var filter = new AccountFilter();
            filter.setAccountId(account2Id);
            filter.setTimestampMin(0);
            filter.setTimestampMax(-1L); // -1L == ulong max value
            filter.setLimit(254);
            filter.setDebits(true);
            filter.setCredits(true);
            filter.setReversed(false);
            assertTrue(client.getAccountTransfersAsync(filter).get().getLength() == 0);
            assertTrue(client.getAccountBalancesAsync(filter).get().getLength() == 0);
        }

        {
            // Invalid timestamp min > max:
            final var filter = new AccountFilter();
            filter.setAccountId(account2Id);
            filter.setTimestampMin(-2L); // -2L == ulong max - 1
            filter.setTimestampMax(1);
            filter.setLimit(254);
            filter.setDebits(true);
            filter.setCredits(true);
            filter.setReversed(false);
            assertTrue(client.getAccountTransfersAsync(filter).get().getLength() == 0);
            assertTrue(client.getAccountBalancesAsync(filter).get().getLength() == 0);
        }

        {
            // Zero limit:
            final var filter = new AccountFilter();
            filter.setAccountId(account2Id);
            filter.setTimestampMin(0);
            filter.setTimestampMax(0);
            filter.setLimit(0);
            filter.setDebits(true);
            filter.setCredits(true);
            filter.setReversed(false);
            assertTrue(client.getAccountTransfersAsync(filter).get().getLength() == 0);
            assertTrue(client.getAccountBalancesAsync(filter).get().getLength() == 0);
        }

        {
            // Zero flags:
            final var filter = new AccountFilter();
            filter.setAccountId(account2Id);
            filter.setTimestampMin(0);
            filter.setTimestampMax(0);
            filter.setLimit(0);
            filter.setDebits(false);
            filter.setCredits(false);
            filter.setReversed(false);
            assertTrue(client.getAccountTransfersAsync(filter).get().getLength() == 0);
            assertTrue(client.getAccountBalancesAsync(filter).get().getLength() == 0);
        }
    }

    @Test
    public void testQueryAccounts() throws Throwable {

        {
            // Creating accounts.
            final var accounts = new AccountBatch(10);
            for (int i = 0; i < 10; i++) {
                accounts.add();
                accounts.setId(UInt128.id());

                if (i % 2 == 0) {
                    accounts.setUserData128(1000L);
                    accounts.setUserData64(100L);
                    accounts.setUserData32(10);
                } else {
                    accounts.setUserData128(2000L);
                    accounts.setUserData64(200L);
                    accounts.setUserData32(20);
                }

                accounts.setCode(999);
                accounts.setLedger(720);
                accounts.setFlags(TransferFlags.NONE);
            }

            final var createAccountsErrors = client.createAccounts(accounts);
            assertTrue(createAccountsErrors.getLength() == 0);
        }

        {
            // Querying accounts where:
            // `user_data_128=1000 AND user_data_64=100 AND user_data_32=10
            // AND code=999 AND ledger=720 ORDER BY timestamp ASC`.
            final var filter = new QueryFilter();
            filter.setUserData128(1000L);
            filter.setUserData64(100L);
            filter.setUserData32(10);
            filter.setCode(999);
            filter.setLedger(720);
            filter.setLimit(254);
            filter.setReversed(false);
            final AccountBatch query = client.queryAccounts(filter);
            assertTrue(query.getLength() == 5);
            long timestamp = 0;
            while (query.next()) {
                assertTrue(Long.compareUnsigned(query.getTimestamp(), timestamp) > 0);
                timestamp = query.getTimestamp();

                assertArrayEquals(filter.getUserData128(), query.getUserData128());
                assertTrue(filter.getUserData64() == query.getUserData64());
                assertTrue(filter.getUserData32() == query.getUserData32());
                assertTrue(filter.getLedger() == query.getLedger());
                assertTrue(filter.getCode() == query.getCode());
            }
        }

        {
            // Querying accounts where:
            // `user_data_128=2000 AND user_data_64=200 AND user_data_32=20
            // AND code=999 AND ledger=720 ORDER BY timestamp DESC`.
            final var filter = new QueryFilter();
            filter.setUserData128(2000L);
            filter.setUserData64(200L);
            filter.setUserData32(20);
            filter.setCode(999);
            filter.setLedger(720);
            filter.setLimit(254);
            filter.setReversed(true);
            final AccountBatch query = client.queryAccounts(filter);
            assertTrue(query.getLength() == 5);
            long timestamp = Long.MIN_VALUE; // MIN_VALUE is the unsigned MAX_VALUE.
            while (query.next()) {
                assertTrue(Long.compareUnsigned(query.getTimestamp(), timestamp) < 0);
                timestamp = query.getTimestamp();

                assertArrayEquals(filter.getUserData128(), query.getUserData128());
                assertTrue(filter.getUserData64() == query.getUserData64());
                assertTrue(filter.getUserData32() == query.getUserData32());
                assertTrue(filter.getLedger() == query.getLedger());
                assertTrue(filter.getCode() == query.getCode());
            }
        }

        {
            // Querying accounts where:
            // `code=999 ORDER BY timestamp ASC`.
            final var filter = new QueryFilter();
            filter.setCode(999);
            filter.setLimit(254);
            filter.setReversed(false);
            final AccountBatch query = client.queryAccounts(filter);
            assertTrue(query.getLength() == 10);
            long timestamp = 0L;
            while (query.next()) {
                assertTrue(Long.compareUnsigned(query.getTimestamp(), timestamp) > 0);
                timestamp = query.getTimestamp();

                assertTrue(filter.getCode() == query.getCode());
            }
        }

        {
            // Querying accounts where:
            // `code=999 ORDER BY timestamp DESC LIMIT 5`.
            final var filter = new QueryFilter();
            filter.setCode(999);
            filter.setLimit(5);
            filter.setReversed(true);

            // First 5 items:
            AccountBatch query = client.queryAccounts(filter);
            assertTrue(query.getLength() == 5);
            long timestamp = Long.MIN_VALUE; // MIN_VALUE is the unsigned MAX_VALUE.
            while (query.next()) {
                assertTrue(Long.compareUnsigned(query.getTimestamp(), timestamp) < 0);
                timestamp = query.getTimestamp();

                assertTrue(filter.getCode() == query.getCode());
            }

            // Next 5 items from this timestamp:
            filter.setTimestampMax(timestamp - 1);

            query = client.queryAccounts(filter);
            assertTrue(query.getLength() == 5);

            while (query.next()) {
                assertTrue(Long.compareUnsigned(query.getTimestamp(), timestamp) < 0);
                timestamp = query.getTimestamp();

                assertTrue(filter.getCode() == query.getCode());
            }

            // No more results:
            filter.setTimestampMax(timestamp - 1);

            query = client.queryAccounts(filter);
            assertTrue(query.getLength() == 0);
        }

        {
            // Not found:
            final var filter = new QueryFilter();
            filter.setUserData64(200);
            filter.setUserData32(10);
            filter.setLimit(254);
            filter.setReversed(false);
            assertTrue(client.queryAccounts(filter).getLength() == 0);
        }
    }


    @Test
    public void testQueryTransfers() throws Throwable {
        final var account1Id = UInt128.id();
        final var account2Id = UInt128.id();

        {
            // Creating the accounts.
            final var accounts = generateAccounts(account1Id, account2Id);

            final var createAccountsErrors = client.createAccounts(accounts);
            assertTrue(createAccountsErrors.getLength() == 0);
        }

        {
            // Creating transfers.
            final var transfers = new TransferBatch(10);
            for (int i = 0; i < 10; i++) {
                transfers.add();
                transfers.setId(UInt128.id());

                if (i % 2 == 0) {
                    transfers.setCreditAccountId(account1Id);
                    transfers.setDebitAccountId(account2Id);
                    transfers.setUserData128(1000L);
                    transfers.setUserData64(100L);
                    transfers.setUserData32(10);
                } else {
                    transfers.setCreditAccountId(account2Id);
                    transfers.setDebitAccountId(account1Id);
                    transfers.setUserData128(2000L);
                    transfers.setUserData64(200L);
                    transfers.setUserData32(20);
                }

                transfers.setCode(999);
                transfers.setLedger(720);
                transfers.setFlags(TransferFlags.NONE);
                transfers.setAmount(100);
            }

            final var createTransfersErrors = client.createTransfers(transfers);
            assertTrue(createTransfersErrors.getLength() == 0);
        }

        {
            // Querying transfers where:
            // `user_data_128=1000 AND user_data_64=100 AND user_data_32=10
            // AND code=999 AND ledger=720 ORDER BY timestamp ASC`.
            final var filter = new QueryFilter();
            filter.setUserData128(1000L);
            filter.setUserData64(100L);
            filter.setUserData32(10);
            filter.setCode(999);
            filter.setLedger(720);
            filter.setLimit(254);
            filter.setReversed(false);
            final TransferBatch query = client.queryTransfers(filter);
            assertTrue(query.getLength() == 5);
            long timestamp = 0;
            while (query.next()) {
                assertTrue(Long.compareUnsigned(query.getTimestamp(), timestamp) > 0);
                timestamp = query.getTimestamp();

                assertArrayEquals(filter.getUserData128(), query.getUserData128());
                assertTrue(filter.getUserData64() == query.getUserData64());
                assertTrue(filter.getUserData32() == query.getUserData32());
                assertTrue(filter.getLedger() == query.getLedger());
                assertTrue(filter.getCode() == query.getCode());
            }
        }

        {
            // Querying transfers where:
            // `user_data_128=2000 AND user_data_64=200 AND user_data_32=20
            // AND code=999 AND ledger=720 ORDER BY timestamp DESC`.
            final var filter = new QueryFilter();
            filter.setUserData128(2000L);
            filter.setUserData64(200L);
            filter.setUserData32(20);
            filter.setCode(999);
            filter.setLedger(720);
            filter.setLimit(254);
            filter.setReversed(true);
            final TransferBatch query = client.queryTransfers(filter);
            assertTrue(query.getLength() == 5);
            long timestamp = Long.MIN_VALUE; // MIN_VALUE is the unsigned MAX_VALUE.
            while (query.next()) {
                assertTrue(Long.compareUnsigned(query.getTimestamp(), timestamp) < 0);
                timestamp = query.getTimestamp();

                assertArrayEquals(filter.getUserData128(), query.getUserData128());
                assertTrue(filter.getUserData64() == query.getUserData64());
                assertTrue(filter.getUserData32() == query.getUserData32());
                assertTrue(filter.getLedger() == query.getLedger());
                assertTrue(filter.getCode() == query.getCode());
            }
        }

        {
            // Querying transfers where:
            // `code=999 ORDER BY timestamp ASC`.
            final var filter = new QueryFilter();
            filter.setCode(999);
            filter.setLimit(254);
            filter.setReversed(false);
            final TransferBatch query = client.queryTransfers(filter);
            assertTrue(query.getLength() == 10);
            long timestamp = 0L;
            while (query.next()) {
                assertTrue(Long.compareUnsigned(query.getTimestamp(), timestamp) > 0);
                timestamp = query.getTimestamp();

                assertTrue(filter.getCode() == query.getCode());
            }
        }

        {
            // Querying transfers where:
            // `code=999 ORDER BY timestamp DESC LIMIT 5`.
            final var filter = new QueryFilter();
            filter.setCode(999);
            filter.setLimit(5);
            filter.setReversed(true);

            // First 5 items:
            TransferBatch query = client.queryTransfers(filter);
            assertTrue(query.getLength() == 5);
            long timestamp = Long.MIN_VALUE; // MIN_VALUE is the unsigned MAX_VALUE.
            while (query.next()) {
                assertTrue(Long.compareUnsigned(query.getTimestamp(), timestamp) < 0);
                timestamp = query.getTimestamp();

                assertTrue(filter.getCode() == query.getCode());
            }

            // Next 5 items from this timestamp:
            filter.setTimestampMax(timestamp - 1);

            query = client.queryTransfers(filter);
            assertTrue(query.getLength() == 5);

            while (query.next()) {
                assertTrue(Long.compareUnsigned(query.getTimestamp(), timestamp) < 0);
                timestamp = query.getTimestamp();

                assertTrue(filter.getCode() == query.getCode());
            }

            // No more results:
            filter.setTimestampMax(timestamp - 1);

            query = client.queryTransfers(filter);
            assertTrue(query.getLength() == 0);
        }

        {
            // Not found:
            final var filter = new QueryFilter();
            filter.setUserData64(200);
            filter.setUserData32(10);
            filter.setLimit(254);
            filter.setReversed(false);
            assertTrue(client.queryTransfers(filter).getLength() == 0);
        }
    }

    @Test
    public void testInvalidQueryFilter() throws Throwable {
        // For those tests it doesn't matter using the sync or async version. We use the async
        // version here for test coverage purposes, but there's no need to duplicate the tests.

        {
            // Empty filter with zero limit:
            final var filter = new QueryFilter();
            assertTrue(client.queryAccountsAsync(filter).get().getLength() == 0);
            assertTrue(client.queryTransfersAsync(filter).get().getLength() == 0);
        }

        {
            // Invalid timestamp min:
            final var filter = new QueryFilter();
            filter.setTimestampMin(-1L); // -1L == ulong max value
            filter.setTimestampMax(0);
            filter.setLimit(254);
            assertTrue(client.queryAccountsAsync(filter).get().getLength() == 0);
            assertTrue(client.queryTransfersAsync(filter).get().getLength() == 0);
        }

        {
            // Invalid timestamp max:
            final var filter = new QueryFilter();
            filter.setTimestampMin(0);
            filter.setTimestampMax(-1L); // -1L == ulong max value
            filter.setLimit(254);
            assertTrue(client.queryAccountsAsync(filter).get().getLength() == 0);
            assertTrue(client.queryTransfersAsync(filter).get().getLength() == 0);
        }

        {
            // Invalid timestamp min > max:
            final var filter = new QueryFilter();
            filter.setTimestampMin(-2); // -2L == ulong max - 1
            filter.setTimestampMax(1);
            filter.setLimit(254);
            assertTrue(client.queryAccountsAsync(filter).get().getLength() == 0);
            assertTrue(client.queryTransfersAsync(filter).get().getLength() == 0);
        }
    }

    @Test
    public void testImportedFlag() throws Throwable {
        // Gets the last timestamp recorded and waits for 10ms so the
        // timestamp can be used as reference for importing past movements.
        var timestamp = getTimestampLast();
        Thread.sleep(10);

        final var account1Id = UInt128.id();
        final var account2Id = UInt128.id();
        final var transferId = UInt128.id();

        final var accounts = generateAccounts(account1Id, account2Id);
        while (accounts.next()) {
            accounts.setFlags(AccountFlags.IMPORTED);
            accounts.setTimestamp(timestamp + accounts.getPosition() + 1);
        }
        accounts.beforeFirst();

        // Creating the accounts.
        final var createAccountErrors = client.createAccounts(accounts);
        assertTrue(createAccountErrors.getLength() == 0);

        // Creating a transfer.
        final var transfers = new TransferBatch(1);

        transfers.add();
        transfers.setId(transferId);
        transfers.setCreditAccountId(account1Id);
        transfers.setDebitAccountId(account2Id);
        transfers.setLedger(720);
        transfers.setCode((short) 1);
        transfers.setFlags(TransferFlags.IMPORTED);
        transfers.setAmount(100);
        transfers.setTimestamp(timestamp + accounts.getLength() + 1);

        final var createTransferErrors = client.createTransfers(transfers);
        assertTrue(createTransferErrors.getLength() == 0);

        // Looking up and asserting the transfer.
        final var lookupTransfers = client.lookupTransfers(new IdBatch(transferId));
        assertEquals(1, lookupTransfers.getLength());

        transfers.beforeFirst();

        assertTrue(transfers.next());
        assertTrue(lookupTransfers.next());
        assertTransfers(transfers, lookupTransfers);
        assertEquals(timestamp + accounts.getLength() + 1, lookupTransfers.getTimestamp());
    }

    @Test
    public void testImportedFlagAsync() throws Throwable {
        var timestamp = getTimestampLast();
        Thread.sleep(10);

        final var account1Id = UInt128.id();
        final var account2Id = UInt128.id();
        final var transferId = UInt128.id();

        final var accounts = generateAccounts(account1Id, account2Id);
        while (accounts.next()) {
            accounts.setTimestamp(timestamp + accounts.getPosition() + 1);
            accounts.setFlags(AccountFlags.IMPORTED);
        }
        accounts.beforeFirst();

        // Creating the accounts.
        CompletableFuture<CreateAccountResultBatch> future = client.createAccountsAsync(accounts);

        final var createAccountErrors = future.get();
        assertTrue(createAccountErrors.getLength() == 0);

        // Creating a transfer.
        final var transfers = new TransferBatch(1);

        transfers.add();
        transfers.setId(transferId);
        transfers.setCreditAccountId(account1Id);
        transfers.setDebitAccountId(account2Id);
        transfers.setLedger(720);
        transfers.setCode((short) 1);
        transfers.setAmount(100);
        transfers.setFlags(TransferFlags.IMPORTED);
        transfers.setTimestamp(timestamp + accounts.getLength() + 1);

        CompletableFuture<CreateTransferResultBatch> transfersFuture =
                client.createTransfersAsync(transfers);
        final var createTransferErrors = transfersFuture.get();
        assertTrue(createTransferErrors.getLength() == 0);

        // Looking up and asserting the transfer.
        CompletableFuture<TransferBatch> lookupTransfersFuture =
                client.lookupTransfersAsync(new IdBatch(transferId));
        final var lookupTransfers = lookupTransfersFuture.get();
        assertEquals(1, lookupTransfers.getLength());

        transfers.beforeFirst();

        assertTrue(transfers.next());
        assertTrue(lookupTransfers.next());
        assertTransfers(transfers, lookupTransfers);
        assertEquals(timestamp + accounts.getLength() + 1, lookupTransfers.getTimestamp());
    }

    /**
     * Asserts that empty replies are not shared.
     * https://github.com/tigerbeetle/tigerbeetle/pull/2495
     */
    @Test
    public void testEmptyReply() throws Throwable {
        final int TASKS_COUNT = 100;

        try (final var client2 = new Client(clusterId, new String[] {server.getAddress()})) {
            for (int i = 0; i < TASKS_COUNT; i++) {
                var request1 = client.lookupAccountsAsync(new IdBatch(UInt128.id()));
                var request2 = client2.lookupAccountsAsync(new IdBatch(UInt128.id()));

                var reply1 = request1.get();
                var reply2 = request2.get();
                assertTrue(reply1.getLength() == 0);
                assertTrue(reply1.isReadOnly());
                assertTrue(reply2.getLength() == 0);
                assertTrue(reply2.isReadOnly());

                assertNotSame(reply1, reply2);
                assertNotEquals(reply1.getHeader().getTimestamp(),
                        reply2.getHeader().getTimestamp());
            }
        }
    }

    private long getTimestampLast() {
        final var id = UInt128.id();
        final var accounts = generateAccounts(id);

        final var createAccountErrors = client.createAccounts(accounts);
        assertTrue(createAccountErrors.getLength() == 0);

        final var lookupAccounts = client.lookupAccounts(new IdBatch(id));
        assertEquals(1, lookupAccounts.getLength());

        assertTrue(lookupAccounts.next());
        return lookupAccounts.getTimestamp();
    }

    private static void assertAccounts(AccountBatch account1, AccountBatch account2) {
        assertArrayEquals(account1.getId(), account2.getId());
        assertArrayEquals(account1.getUserData128(), account2.getUserData128());
        assertEquals(account1.getUserData64(), account2.getUserData64());
        assertEquals(account1.getUserData32(), account2.getUserData32());
        assertEquals(account1.getLedger(), account2.getLedger());
        assertEquals(account1.getCode(), account2.getCode());
        assertEquals(account1.getFlags(), account2.getFlags());
    }

    private static void assertTransfers(TransferBatch transfer1, TransferBatch transfer2) {
        assertArrayEquals(transfer1.getId(), transfer2.getId());
        assertArrayEquals(transfer1.getDebitAccountId(), transfer2.getDebitAccountId());
        assertArrayEquals(transfer1.getCreditAccountId(), transfer2.getCreditAccountId());
        assertEquals(transfer1.getAmount(), transfer2.getAmount());
        assertArrayEquals(transfer1.getPendingId(), transfer2.getPendingId());
        assertArrayEquals(transfer1.getUserData128(), transfer2.getUserData128());
        assertEquals(transfer1.getUserData64(), transfer2.getUserData64());
        assertEquals(transfer1.getUserData32(), transfer2.getUserData32());
        assertEquals(transfer1.getTimeout(), transfer2.getTimeout());
        assertEquals(transfer1.getLedger(), transfer2.getLedger());
        assertEquals(transfer1.getCode(), transfer2.getCode());
        assertEquals(transfer1.getFlags(), transfer2.getFlags());
    }

    private static void assertHeader(Batch request, Batch response) {
        assertNull(request.getHeader());
        assertNotNull(response.getHeader());
        assertTrue(response.getHeader().getTimestamp() != 0L);
    }

    private static class TransferTask extends Thread {
        public final Client client;
        public CreateTransferResultBatch result;
        public Throwable exception;
        private byte[] account1Id;
        private byte[] account2Id;
        private int flags;
        private CountDownLatch enterBarrier;
        private CountDownLatch exitBarrier;

        public TransferTask(Client client, byte[] account1Id, byte[] account2Id, int flags,
                CountDownLatch enterBarrier, CountDownLatch exitBarrier) {
            this.client = client;
            this.result = null;
            this.exception = null;
            this.account1Id = account1Id;
            this.account2Id = account2Id;
            this.flags = flags;
            this.enterBarrier = enterBarrier;
            this.exitBarrier = exitBarrier;
        }

        @Override
        public synchronized void run() {

            final var transfers = new TransferBatch(1);
            transfers.add();

            transfers.setId(UInt128.asBytes(UUID.randomUUID()));
            transfers.setCreditAccountId(account1Id);
            transfers.setDebitAccountId(account2Id);
            transfers.setLedger(720);
            transfers.setCode(1);
            transfers.setAmount(100);
            transfers.setFlags(flags);

            try {
                enterBarrier.countDown();
                enterBarrier.await();
                result = client.createTransfers(transfers);
            } catch (Throwable any) {
                exception = any;
            } finally {
                exitBarrier.countDown();
            }
        }
    }

    private static class Server implements AutoCloseable {

        public static final String TB_SERVER = "../../../zig-out/bin/tigerbeetle";

        public final String tb_file;
        private final Process process;
        private String address;

        public Server(final String label) throws IOException, Exception, InterruptedException {
            this.tb_file = "./0_0.tigerbeetle." + label;

            cleanUp();

            String exe;
            switch (JNILoader.OS.getOS()) {
                case windows:
                    exe = TB_SERVER + ".exe";
                    break;
                default:
                    exe = TB_SERVER;
                    break;
            }

            final var format = Runtime.getRuntime().exec(new String[] {exe, "format", "--cluster=0",
                    "--replica=0", "--replica-count=1", "--development", tb_file});
            if (format.waitFor() != 0) {
                final var reader =
                        new BufferedReader(new InputStreamReader(format.getErrorStream()));
                final var error = reader.lines().collect(Collectors.joining(". "));
                throw new Exception("Format failed. " + error);
            }

            this.process = new ProcessBuilder()
                    .command(new String[] {exe, "start", "--addresses=0", "--development", tb_file})
                    .redirectOutput(Redirect.PIPE).redirectError(Redirect.INHERIT).start();

            final var stdout = process.getInputStream();
            try (final var reader = new BufferedReader(new InputStreamReader(stdout))) {
                this.address = reader.readLine().trim();
            }
        }

        public String getAddress() {
            return address;
        }

        @Override
        public void close() throws Exception {
            cleanUp();
        }

        private void cleanUp() throws Exception {
            try {
                if (process != null && process.isAlive()) {
                    process.destroy();
                }

                final var file = new File("./" + tb_file);
                file.delete();
            } catch (Throwable any) {
                throw new Exception("Cleanup has failed");
            }
        }
    }
}
