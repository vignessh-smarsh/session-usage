package com.smarsh.sessionusage;

import com.mongodb.MongoQueryException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SessionUsageApplicationTests {

  private static final String connectionString = "mongodb://admin:password@localhost:27017";
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
  void fixedCursorLeaks() {
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
}
