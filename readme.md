# From Sequential to Parallel: The 10M Row Performance Journey


## Table of Contents

- [Project Overview](https://github.com/otifi3/scala_rule_engine/blob/main/README.md#project-overview)
- [Features](https://github.com/otifi3/scala_rule_engine/blob/main/README.md#features)
- [Technical Details](https://github.com/otifi3/scala_rule_engine/blob/main/README.md#technical-details)
- [Project Structure](https://github.com/otifi3/scala_rule_engine/blob/main/README.md#project-structure)
- [Setup and Running](https://github.com/otifi3/scala_rule_engine/blob/main/README.md#setup-and-running)
- [Discount Rules Summary](https://github.com/otifi3/scala_rule_engine/blob/main/README.md#discount-rules-summary)
- [Logging](https://github.com/otifi3/scala_rule_engine/blob/main/README.md#logging)
- [Project References](https://github.com/otifi3/scala_rule_engine/blob/main/README.md#project-references)
- [Notes](https://github.com/otifi3/scala_rule_engine/blob/main/README.md#notes)

## Project Overview

This Scala project implements a **rule engine** that processes retail store orders to automatically calculate applicable discounts based on a set of business rules. It reads transaction data from a CSV file, applies qualifying rules to determine discounts, computes the final prices, and loads the processed data into a MySql database. The system also logs important events and errors in a dedicated log file.

---

## Features

- **Qualifying rules** based on:
    - Product expiry date (less than 30 days remaining)
    - Product category discounts (cheese, wine)
    - Special date discounts (orders on March 23rd)
    - Quantity-based discounts (bulk purchase)
    - Channel and payment method discounts (app sales, Visa payments)

- **Discount calculation**:
    - If multiple discounts apply, the two highest discounts are averaged.
    - Final price is calculated after applying the average discount.

- **Data persistence**:
    - Results are stored in PostgreSQL tables for further analysis or reporting.

- **Logging**:
    - Engine events, errors, and processing status are logged in `logs/rule_engine.log` with timestamps and log levels.

---

## From 30 to 5.7 Minutes 

**Stage 1 - Initial State**
- I was able to process **1,000 rows** from `TRX1000.csv` perfectly fine. The sequential approach was simple, and executed in under a second.
- Everything changed when we scaled to **10,000,000 rows** (`TRX10M.csv`):

```scala
// Original sequential approach that was processing each order at a time
Using(Source.fromFile(fileName))(_.getLines().toList)
```

```scala
// It always got me this error when I switched to the 10M file
  Exception in thread "main" java.lang.OutOfMemoryError: Java heap space
```
- It failed cause 
	- It Loaded all 10M rows into RAM before touching a single one
	- It Stored all data into list of string then process them
	- And created 10M order objects -> 10M order instance 
	- Then Inserted row by row to MYSQL 

 What I changed was :
 
 **Stage 2 - Streaming Line by Line**
```scala
Using(Source.fromFile("TRX10M.csv")) { source =>
  source.getLines().drop(1).foreach { line =>
    processOrder(to_order(line))
  }
}
```
- Memory problem solved — only one row exists in memory at any moment. but It took a whole **45 min** to finish the file
- It read line by line and only one row in memory at a time
- Only **1 CPU core** utilization, other cores sat idle

 **Stage 3 — Batching (30 Minutes)**
```scala
lines.grouped(BATCH_SIZE).foreach { batch => val processed = batch.map(line => processOrder(to_order(line))) 
// Insert entire batch at once with addBatch() / executeBatch() 
}
```
- Instead of 10M individual DB insertions, there are now `10M / 80K = 125` batch inserts -> processing 80K row at a time
- but that meant using 1 core of all my device capabilities and it took 30 min


**Stage 4 — Parallel Collections + ForkJoinPool (5.7 Minutes)**

```scala 
// Auto-detect how many cores the machine has 
val PARALLELISM = Runtime.getRuntime.availableProcessors()
// Create a thread pool with half the cores so device won't crash
val forkJoinPool = new java.util.concurrent.ForkJoinPool(PARALLELISM / 2)
```

```scala
// 1. Starting with 80,000 lines
val batch: Seq[String] = 80,000 lines
// 2. Convert to parallel collection so the work is distributed btw cores
val parBatch: ParSeq[String] = batch.par
// 3. The ForkJoinPool with 4 threads takes over
// Each thread gets ~20,000 lines
Thread 1 (Pool Thread 1): Processes lines 1-20,000
Thread 2 (Pool Thread 2): Processes lines 20,001-40,000  
Thread 3 (Pool Thread 3): Processes lines 40,001-60,000
Thread 4 (Pool Thread 4): Processes lines 60,001-80,000
// 4. Each thread runs flatMap on its own chunk
// flatMap means: map then flatten (remove None results)
Thread 1: 
  line1 → Try(processOrder(...)) → Success(result) → Some(result)
  line2 → Try(processOrder(...)) → Failure(exception) → None (skipped!)
  line3 → Try(processOrder(...)) → Success(result) → Some(result)
  ...
  Result: Seq[ProcessedOrder] (no None values)
  
// 5. All threads complete and results are combined
// .seq converts back to regular sequential collection
val processed: Seq[ProcessedOrder] = combined results from all threads

```
- converting the collection into a parallel collection
- map and flatmap -> make it run on multiple threads 
- and then convert the result to a normal list again

**Stage 5 — MySQL Batch Rewriting**

```scala
// Added to the JDBC connection string: 
rewriteBatchedStatements=true
```
- Even with `executeBatch()`, MySQL by default still sends each statement in the batch as a separate network packet for ex: `INSERT INTO order_result VALUES (...)`
- this approach will utilize the statements more and will rewrite as batch into one sql statement `INSERT INTO order_result VALUES (...), (...), (...), ...`

## Technical Details

- **Functional Programming**:  
    Core logic is implemented using pure functions with no mutable variables or loops.  
    All functions are deterministic and side-effect free.
    
- **Scala Features Used**:
    - Case classes for immutable data modeling.
    - Higher-order functions for rule definitions.
    - `Using` for safe resource management.
    - Strong typing with `val` only.

- **Database**:  
    MySql database connection is established using JDBC.  
    Connection details can be found in the `db_connection` object.
    
- **Logging**:  
    Java’s built-in `java.util.logging` framework is configured to write logs to a file.
    

---

## Project Structure

```
src/
└── main/
├── scala/
│   └── rules/
│       ├── qualification_rules.scala  # Main engine
│       ├── logger.scala   # Logger configuration
│       └── db_connection.scala   # Database connection utility
│       └── order.scala    # case class for order instance
└── resources/
│      └── TRX1000.csv     # Input CSV file with orders
│      └── DDL.txt         # ddl file for table creation
logs/
└── rule\_engine.log       # Log file generated during execution
```

---

## Setup and Running

1. **Prerequisites**:
    - SBT build tool installed.
    - Mysql running locally with a database named `orders_db`.
    - Mysql JDBC driver added as dependency.
2. **Configure Database**:
    - Update the connection details in `db_connection.scala` if needed (URL, user, password).
    - Create the attached table  in `DDL.txt` in your database 
3. **Input Data**:
    - Place your input CSV file at `src/main/resources/TRX1000.csv`.
---

## Discount Rules Summary

|Rule|Discount Calculation|
|---|---|
|Less than 30 days to product expiry|(30 - days_remaining) % discount (e.g., 29 days = 1%)|
|Cheese product|10% discount|
|Wine product|5% discount|
|Sold on March 23|50% discount|
|Quantity 6-9|5% discount|
|Quantity 10-14|7% discount|
|Quantity 15+|10% discount|
|Sold via App|Discount based on rounded quantity in multiples of 5|
|Payment via Visa|5% discount|

If multiple discounts apply, the **top 2 discounts** are averaged for final discount.

---

## Logging

- Logs are saved to `logs/rule_engine.log`.
- Format: `TIMESTAMP LOGLEVEL MESSAGE`
- Example entries:
    ```
    2025-05-14 22:00:00 INFO Starting order processing pipeline...
    2025-05-14 22:01:00 SEVERE Error reading lines from CSV: File not found
    ```
    

---
## Project References

- [Project requirements and video guide](https://youtu.be/6uwRajbkaqI?si=6OJW_oCXE8Fcq36I)

---

## Note

- LLMs were partially used in polishing the my code documentation

---

