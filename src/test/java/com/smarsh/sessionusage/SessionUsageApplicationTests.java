package com.smarsh.sessionusage;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoQueryException;
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
  void fixesCursorLeaksInParallel() throws InterruptedException {
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
        }
      }
    }

    assertNull(caughtException);
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
        .applyToConnectionPoolSettings(builder -> builder.maxSize(6))
        .build();

    try (MongoClient pooledClient = MongoClients.create(settings)) {

      ExecutorService executor = Executors.newFixedThreadPool(20);
      List<Future<?>> futures = new ArrayList<>();

      for (int i = 0; i < 20; i++) {
        futures.add(executor.submit(() -> {
            MongoCollection<Document> collection = pooledClient.getDatabase("test").getCollection("test");
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

      assertNotNull(caughtException);
      assertEquals(261, caughtException.getErrorCode());
    }
  }
}
