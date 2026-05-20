# Getting Started

## Prerequisites
* Colima
* Java 17
* MongoDB 5.0.22

## Setup

Run MongoDB using Docker

```bash
docker run -d \
  --name my-mongodb \
  -p 27017:27017 \
  -v mongodb_data:/data/db \
  -e MONGO_INITDB_ROOT_USERNAME=admin \
  -e MONGO_INITDB_ROOT_PASSWORD=password \
  mongo:5.0.22 \
  --setParameter maxSessions=10
```
** This will create a MongoDB instance with a maximum of 10 sessions.**

Exec into the shell via

```bash
docker exec -it my-mongodb mongosh -u admin -p password
```

Insert test data via

```bash
use test

// 1. Initialize an empty array and batch size
let bulkData = [];
const batchSize = 1000;

// 2. Loop 10,000 times to create data
for (let i = 1; i <= 10000; i++) {
  bulkData.push({
    userId: i,
    username: "user_" + i,
    score: Math.floor(Math.random() * 100),
    status: i % 2 === 0 ? "active" : "inactive",
    createdAt: new Date()
  });

  // 3. Insert in batches of 1,000 to optimize memory
  if (bulkData.length === batchSize) {
    db.test.insertMany(bulkData);
    bulkData = []; // Clear array for next batch
  }
}

// Insert any remaining documents
if (bulkData.length > 0) {
  db.test.insertMany(bulkData);
}
```

Run the test reproducesTooManyLogicalSessions via

```bash
./gradlew test --tests SessionUsageApplicationTests.reproducesTooManyLogicalSessions
```

This will spike the number of sessions beyond the limit of 10 and throw an exception.

Now restart your MongoDB instance via

```bash
docker restart my-mongodb
```

And run the test fixedCursorLeaks via

```bash
./gradlew test --tests SessionUsageApplicationTests.fixedCursorLeaks
```
This should pass consistently irrespective of the number of times you run it.



