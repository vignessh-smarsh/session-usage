package com.smarsh.sessionusage;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoQueryException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class SessionUsageApplicationTests {

  private static final String connectionString = "mongodb://admin:password@localhost:27018";
  private MongoClient client;

  @BeforeEach
  void setUp() {
    client = MongoClients.create(connectionString);
  }

  @AfterEach
  void tearDown() {
    client.close();
  }

  private long getActiveSessionCount(MongoClient client) {
    Document status = client.getDatabase("admin")
            .runCommand(new Document("serverStatus", 1));
    Document sessionCache = (Document) status.get("logicalSessionRecordCache");
    return sessionCache.getInteger("activeSessionsCount");
  }

  @Test
  void reproducesTooManyLogicalSessions() {
    MongoCollection<Document> collection = client.getDatabase("test").getCollection("test");
    MongoQueryException caughtException = null;

    try {
      for (int i = 0; i < 20; i++) {
        MongoCursor<Document> cursor = collection.find().iterator();
        cursor.next();
      }
    } catch (MongoQueryException e) {
      caughtException = e;
      System.out.println("Got expected error: " + e.getErrorCode()); // 261
    }

    assertNotNull(caughtException);
    assertEquals(261, caughtException.getErrorCode());
  }

  @Test
  void fixesCursorLeaks() {
    MongoCollection<Document> collection = client.getDatabase("test").getCollection("test");
    MongoQueryException caughtException = null;

    try {
      for (int i = 0; i < 20; i++) {
        try (MongoCursor<Document> cursor = collection.find().iterator()) {
          cursor.next();
        }
      }
    } catch (MongoQueryException e) {
      caughtException = e;
    }

    assertNull(caughtException);
  }

  @Test
  void drainedCursorFreesSessionWithoutExplicitClose() {
    // When ALL documents are read from a cursor it is automatically exhausted:
    // the driver sends killCursors, returns the session to its pool, and the
    // server-side slot in maxSessions is freed — even if close() is never called.
    MongoCollection<Document> collection = client.getDatabase("test").getCollection("test");

    long sessionsBefore = getActiveSessionCount(client);
    System.out.println("Sessions before: " + sessionsBefore);

    MongoQueryException caughtException = null;
    try {
      for (int i = 0; i < 15; i++) {
        MongoCursor<Document> cursor = collection.find().limit(3).iterator();
        while (cursor.hasNext()) {
          cursor.next(); // reads all 3 docs — cursor exhausted, session auto-freed
        }
        // close() deliberately not called — session is already returned by exhaustion
        long current = getActiveSessionCount(client);
        System.out.println("Iteration " + (i + 1) + " — active sessions: " + current);
      }
    } catch (MongoQueryException e) {
      caughtException = e;
    }

    long sessionsAfter = getActiveSessionCount(client);
    System.out.println("Sessions after: " + sessionsAfter);

    assertNull(caughtException, "Exhausted cursors free sessions automatically — no error 261 expected");
  }

  @Test
  void undrainedCursorLeaksSessionWithoutExplicitClose() {
    // Reading only SOME documents leaves the cursor open on the server.
    // The session slot stays occupied until TTL (30 min) — close() was never called
    // and the cursor was never exhausted, so the driver has no trigger to free it.
    // Each iteration leaks one more session until maxSessions is hit.
    MongoCollection<Document> collection = client.getDatabase("test").getCollection("test");

    long sessionsBefore = getActiveSessionCount(client);
    System.out.println("Sessions before: " + sessionsBefore);

    MongoQueryException caughtException = null;
    int iterationsCompleted = 0;
    try {
      for (int i = 0; i < 15; i++) {
        MongoCursor<Document> cursor = collection.find().iterator();
        cursor.next(); // reads only first doc — cursor still open, session leaks
        // close() deliberately not called — session stays registered server-side
        iterationsCompleted++;
        try {
          long current = getActiveSessionCount(client);
          System.out.println("Iteration " + (i + 1) + " — active sessions: " + current);
        } catch (Exception ignored) {
          // session cache may already be full — monitoring call itself can't get a session
          System.out.println("Iteration " + (i + 1) + " — session count unavailable (limit reached)");
        }
      }
    } catch (MongoQueryException e) {
      caughtException = e;
    }

    try {
      long sessionsAfter = getActiveSessionCount(client);
      System.out.println("Failed after " + iterationsCompleted + " iterations — sessions at: " + sessionsAfter);
    } catch (Exception ignored) {
      System.out.println("Failed after " + iterationsCompleted + " iterations — session cache full, count unavailable");
    }

    assertNotNull(caughtException, "Undrained cursors leak sessions — error 261 expected");
    assertEquals(261, caughtException.getErrorCode());
  }

  @Test
  void reproducesTooManyLogicalSessionsInParallel() throws InterruptedException {
    MongoCollection<Document> collection = client.getDatabase("test").getCollection("test");
    ExecutorService executor = Executors.newFixedThreadPool(20);
    List<Future<?>> futures = new ArrayList<>();

    for (int i = 0; i < 20; i++) {
      futures.add(executor.submit(() -> {
        MongoCursor<Document> cursor = collection.find().iterator();
        cursor.next();
      }));
    }

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);

    MongoQueryException caughtException = null;
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        if (e.getCause() instanceof MongoQueryException mqe && caughtException == null) {
          caughtException = mqe;
          System.out.println("Got expected error: " + mqe.getErrorCode()); // 261
        }
      }
    }

    assertNotNull(caughtException);
    assertEquals(261, caughtException.getErrorCode());
  }

  @Test
  void tryWithResourcesAloneIsInsufficientForHighConcurrency() throws InterruptedException {
    // try-with-resources ensures each cursor is closed, but if thread count exceeds maxSessions,
    // all threads race to register sessions before any cursor is closed — still hits error 261.
    // A Semaphore or bounded thread pool is also required.
    MongoCollection<Document> collection = client.getDatabase("test").getCollection("test");
    ExecutorService executor = Executors.newFixedThreadPool(20);
    List<Future<?>> futures = new ArrayList<>();

    for (int i = 0; i < 20; i++) {
      futures.add(executor.submit(() -> {
        try (MongoCursor<Document> cursor = collection.find().iterator()) {
          cursor.next();
        }
      }));
    }

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);

    MongoQueryException caughtException = null;
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        if (e.getCause() instanceof MongoQueryException mqe && caughtException == null) {
          caughtException = mqe;
          System.out.println("Got expected error: " + mqe.getErrorCode()); // 261
        }
      }
    }

    assertNotNull(caughtException);
    assertEquals(261, caughtException.getErrorCode());
  }

  @Test
  void fixesCursorLeaksInParallelWithSemaphore() throws InterruptedException {
    MongoCollection<Document> collection = client.getDatabase("test").getCollection("test");
    ExecutorService executor = Executors.newFixedThreadPool(20);
    Semaphore semaphore = new Semaphore(9);
    List<Future<?>> futures = new ArrayList<>();

    for (int i = 0; i < 20; i++) {
      futures.add(executor.submit(() -> {
        semaphore.acquireUninterruptibly();
        try (MongoCursor<Document> cursor = collection.find().iterator()) {
          cursor.next();
        } finally {
          semaphore.release();
        }
      }));
    }

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);

    MongoQueryException caughtException = null;
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        if (e.getCause() instanceof MongoQueryException mqe && caughtException == null) {
          caughtException = mqe;
        }
      }
    }

    assertNull(caughtException);
  }

  @Test
  void fixesCursorLeaksInParallelWithBoundedThreadPool() throws InterruptedException {
    MongoCollection<Document> collection = client.getDatabase("test").getCollection("test");
    ExecutorService executor = Executors.newFixedThreadPool(9);
    List<Future<?>> futures = new ArrayList<>();

    for (int i = 0; i < 20; i++) {
      futures.add(executor.submit(() -> {
        try (MongoCursor<Document> cursor = collection.find().iterator()) {
          cursor.next();
        }
      }));
    }

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);

    MongoQueryException caughtException = null;
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        if (e.getCause() instanceof MongoQueryException mqe && caughtException == null) {
          caughtException = mqe;
        }
      }
    }

    assertNull(caughtException);
  }

  @Test
  void connectionPoolSizeAloneDoesNotFixCursorLeaksInParallel() throws InterruptedException {
    MongoClientSettings settings = MongoClientSettings.builder()
        .applyConnectionString(new ConnectionString(connectionString))
        .applyToConnectionPoolSettings(builder -> builder.maxSize(9))
        .build();

    try (MongoClient pooledClient = MongoClients.create(settings)) {

      ExecutorService executor = Executors.newFixedThreadPool(20);
      List<Future<?>> futures = new ArrayList<>();

      for (int i = 0; i < 20; i++) {
        futures.add(executor.submit(() -> {
            MongoCollection<Document> collection = pooledClient.getDatabase("test").getCollection("test");
            MongoCursor<Document> cursor = collection.find().iterator();
            cursor.next();
            // cursor intentionally not closed — pool size limits connections but not sessions
        }));
      }

      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);

      MongoQueryException caughtException = null;
      for (Future<?> future : futures) {
        try {
          future.get();
        } catch (ExecutionException e) {
          if (e.getCause() instanceof MongoQueryException mqe && caughtException == null) {
            caughtException = mqe;
          }
        }
      }

      assertNotNull(caughtException);
      assertEquals(261, caughtException.getErrorCode());
    }
  }

  @Test
  void boundedThreadPoolWithoutClosingCursorsStillLeaksSessions() throws InterruptedException {
    MongoCollection<Document> collection = client.getDatabase("test").getCollection("test");
    ExecutorService executor = Executors.newFixedThreadPool(7);
    List<Future<?>> futures = new ArrayList<>();

    for (int i = 0; i < 20; i++) {
      futures.add(executor.submit(() -> {
        MongoCursor<Document> cursor = collection.find().iterator();
        cursor.next();
      }));
    }

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);

    MongoQueryException caughtException = null;
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        if (e.getCause() instanceof MongoQueryException mqe && caughtException == null) {
          caughtException = mqe;
          System.out.println("Got expected error: " + mqe.getErrorCode()); // 261
        }
      }
    }

    assertNotNull(caughtException);
    assertEquals(261, caughtException.getErrorCode());
  }

  @Test
  void stormTopologySimulatesSessionExhaustion() throws InterruptedException {
    final int TOPOLOGY_COUNT = 3;
    final int WORKERS_PER_TOPOLOGY = 3; // 9 total workers — below maxSessions=10
    final int TUPLES_PER_WORKER = 15;   // each worker leaks 1 session per tuple; 9 * 15 = 135 — limit hit early

    MongoCollection<Document> collection = client.getDatabase("test").getCollection("test");

    List<ExecutorService> topologies = new ArrayList<>();
    for (int t = 0; t < TOPOLOGY_COUNT; t++) {
      topologies.add(Executors.newFixedThreadPool(WORKERS_PER_TOPOLOGY));
    }

    List<Future<?>> futures = new ArrayList<>();
    for (ExecutorService topology : topologies) {
      for (int w = 0; w < WORKERS_PER_TOPOLOGY; w++) {
        futures.add(topology.submit(() -> {
          for (int tuple = 0; tuple < TUPLES_PER_WORKER; tuple++) {
            MongoCursor<Document> cursor = collection.find().iterator();
            cursor.next();
            // cursor intentionally not closed — simulates the bug in a bolt
          }
        }));
      }
    }

    for (ExecutorService topology : topologies) {
      topology.shutdown();
      topology.awaitTermination(30, TimeUnit.SECONDS);
    }

    MongoQueryException caughtException = null;
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        if (e.getCause() instanceof MongoQueryException mqe && caughtException == null) {
          caughtException = mqe;
          System.out.println("Session limit hit: " + mqe.getErrorCode()); // 261
        }
      }
    }

    assertNotNull(caughtException, "Sessions should be exhausted as cursors are never closed");
    assertEquals(261, caughtException.getErrorCode());
  }

  @Test
  void stormTopologyFixedWithProperCursorManagement() throws InterruptedException {
    final int TOPOLOGY_COUNT = 3;
    final int WORKERS_PER_TOPOLOGY = 3; // 9 total workers — below maxSessions=10
    final int TUPLES_PER_WORKER = 15;   // even with 15 tuples per worker, sessions stay at most 9
                                         // because each cursor is closed before the next one opens

    MongoCollection<Document> collection = client.getDatabase("test").getCollection("test");

    List<ExecutorService> topologies = new ArrayList<>();
    for (int t = 0; t < TOPOLOGY_COUNT; t++) {
      topologies.add(Executors.newFixedThreadPool(WORKERS_PER_TOPOLOGY));
    }

    List<Future<?>> futures = new ArrayList<>();
    for (ExecutorService topology : topologies) {
      for (int w = 0; w < WORKERS_PER_TOPOLOGY; w++) {
        futures.add(topology.submit(() -> {
          for (int tuple = 0; tuple < TUPLES_PER_WORKER; tuple++) {
            try (MongoCursor<Document> cursor = collection.find().iterator()) {
              cursor.next();
            }
          }
        }));
      }
    }

    for (ExecutorService topology : topologies) {
      topology.shutdown();
      topology.awaitTermination(30, TimeUnit.SECONDS);
    }

    MongoQueryException caughtException = null;
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        if (e.getCause() instanceof MongoQueryException mqe && caughtException == null) {
          caughtException = mqe;
        }
      }
    }

    assertNull(caughtException, "No session errors expected — cursors are properly closed after each tuple");
  }

  @Test
  void smallNodeBehaviorExhaustsSessions() throws InterruptedException {
    // Small node: limited memory/disk → server's session cleanup job is slow (disk-bound)
    // Leaked sessions accumulate faster than they are reclaimed → maxSessions exhausted
    final int TOPOLOGY_COUNT = 3;
    final int WORKERS_PER_TOPOLOGY = 3; // 9 total workers, below maxSessions=10
    final int TUPLES_PER_WORKER = 15;   // 9 * 15 = 135 leaked sessions — limit hit during run

    MongoCollection<Document> collection = client.getDatabase("test").getCollection("test");
    List<ExecutorService> topologies = new ArrayList<>();
    for (int t = 0; t < TOPOLOGY_COUNT; t++) {
      topologies.add(Executors.newFixedThreadPool(WORKERS_PER_TOPOLOGY));
    }

    List<Future<?>> futures = new ArrayList<>();
    for (ExecutorService topology : topologies) {
      for (int w = 0; w < WORKERS_PER_TOPOLOGY; w++) {
        futures.add(topology.submit(() -> {
          for (int tuple = 0; tuple < TUPLES_PER_WORKER; tuple++) {
            MongoCursor<Document> cursor = collection.find().iterator();
            cursor.next();
            // cursor not closed — no cleanup mechanism to reclaim sessions on a slow node
          }
        }));
      }
    }

    for (ExecutorService topology : topologies) {
      topology.shutdown();
      topology.awaitTermination(30, TimeUnit.SECONDS);
    }

    MongoQueryException caughtException = null;
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        if (e.getCause() instanceof MongoQueryException mqe && caughtException == null) {
          caughtException = mqe;
          System.out.println("Small node — session limit hit: " + mqe.getErrorCode());
        }
      }
    }

    assertNotNull(caughtException, "Sessions should be exhausted — no cleanup keeping pace with leaks");
    assertEquals(261, caughtException.getErrorCode());
  }

  @Test
  void largeNodeBehaviorSessionsReclaimed() throws InterruptedException {
    // Large node: fast memory/disk → server's session cleanup job runs quickly
    // Simulated by a background thread that periodically closes tracked leaked sessions,
    // representing MongoDB reclaiming sessions before maxSessions is reached.
    // Uses explicit ClientSession objects so that session.close() sends endSessions
    // to the server immediately (works correctly in all driver versions).
    final int TOPOLOGY_COUNT = 3;
    final int WORKERS_PER_TOPOLOGY = 3; // 9 total workers, below maxSessions=10
    final int TUPLES_PER_WORKER = 15;

    MongoCollection<Document> collection = client.getDatabase("test").getCollection("test");

    // Tracks sessions that were "leaked" (not closed by the application code)
    CopyOnWriteArrayList<ClientSession> leakedSessions = new CopyOnWriteArrayList<>();

    // Simulates MongoDB's background session cleanup job on a large (fast) node:
    // runs frequently and ends idle sessions server-side, freeing their slots in maxSessions.
    // Wrapped in try-catch so any unexpected exception doesn't silently stop the scheduler.
    ScheduledExecutorService cleanupSimulator = Executors.newScheduledThreadPool(1);
    cleanupSimulator.scheduleAtFixedRate(() -> {
      try {
        List<ClientSession> batch = new ArrayList<>(leakedSessions);
        leakedSessions.removeAll(batch);
        batch.forEach(s -> { try { s.close(); } catch (Exception ignored) {} });
      } catch (Exception ignored) {}
    }, 1, 1, TimeUnit.MILLISECONDS);

    List<ExecutorService> topologies = new ArrayList<>();
    for (int t = 0; t < TOPOLOGY_COUNT; t++) {
      topologies.add(Executors.newFixedThreadPool(WORKERS_PER_TOPOLOGY));
    }

    List<Future<?>> futures = new ArrayList<>();
    for (ExecutorService topology : topologies) {
      for (int w = 0; w < WORKERS_PER_TOPOLOGY; w++) {
        futures.add(topology.submit(() -> {
          for (int tuple = 0; tuple < TUPLES_PER_WORKER; tuple++) {
            ClientSession session = client.startSession();
            collection.find(session).iterator().next();
            leakedSessions.add(session); // register for cleanup — simulates server reclaiming idle session
            try { Thread.sleep(2); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
          }
        }));
      }
    }

    for (ExecutorService topology : topologies) {
      topology.shutdown();
      topology.awaitTermination(30, TimeUnit.SECONDS);
    }

    cleanupSimulator.shutdown();
    cleanupSimulator.awaitTermination(5, TimeUnit.SECONDS);

    MongoQueryException caughtException = null;
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        if (e.getCause() instanceof MongoQueryException mqe && caughtException == null) {
          caughtException = mqe;
        }
      }
    }

    assertNull(caughtException, "Large node cleanup keeps session count below maxSessions despite leaks");
  }
}
