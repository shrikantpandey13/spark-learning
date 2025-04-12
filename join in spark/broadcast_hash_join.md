# Broadcast Hash Join in Spark

## What is a Broadcast Hash Join?

A Broadcast Hash Join is an optimization technique used in Spark SQL (and other distributed query engines) to perform joins between a large table (DataFrame) and a small table efficiently.

Instead of shuffling both tables across the network so that rows with the same join key end up on the same worker node (which is what happens in a typical Shuffle Sort Merge Join), Spark takes a different approach:

1.  **Broadcast Phase:** The *entire* small table is collected by the Spark driver and then sent (broadcasted) to *every executor node* involved in processing the large table.
2.  **Hash Table Build Phase:** Each executor receives the broadcasted small table data and builds an in-memory hash table using the join key(s).
3.  **Join (Probe) Phase:** Each executor then processes its assigned partitions of the *large* table. For each row in its partition of the large table, it probes the in-memory hash table (built from the small table) using the join key to find matching rows. If a match is found, the joined rows are produced.

## Why Use It? (Advantages)

* **Avoids Expensive Shuffle:** The biggest advantage is that it completely avoids shuffling the *large* table across the network. Network I/O and the associated serialization/deserialization are often major bottlenecks in distributed joins. Only the small table is moved across the network once (broadcasted).
* **Speed:** When applicable (one table is small), it's generally much faster than shuffle-based joins because hash table lookups are very fast (O(1) on average), and the costly shuffle step for the large table is eliminated.

## When Does Spark Use It? (Conditions)

Spark's Catalyst optimizer automatically decides whether to use a Broadcast Hash Join based on a few factors:

1.  **Table Size:** One of the tables must be significantly smaller than the other.
2.  **Configuration Threshold:** The estimated size of the table to be broadcast must be below the configuration parameter `spark.sql.autoBroadcastJoinThreshold` (default is often 10MB, but can be configured). Spark uses statistics (if available) or estimates to determine the size.
3.  **Join Type:** It's typically used for Inner joins, Left Outer joins (broadcasting the right table), Right Outer joins (broadcasting the left table), Left Semi joins, and Left Anti joins. It's generally *not* suitable for Full Outer joins because both sides might need to be probed.
4.  **Memory:** The broadcasted table, when built into a hash table, must fit comfortably within the memory of each executor node.

**Explicit Hint:** You can also explicitly tell Spark to use a broadcast join using the `broadcast()` function hint, even if the table size slightly exceeds the threshold (use with caution!).

## Disadvantages/Limitations

* **Small Table Constraint:** Only feasible if one table is genuinely small enough to fit in the driver's memory (for collection) and each executor's memory (for the hash table). Broadcasting a very large table will likely cause OutOfMemoryErrors (OOM).
* **Driver Bottleneck:** Collecting the small table on the driver can become a bottleneck if the table is near the threshold size or if many broadcast joins happen concurrently.
* **Executor Memory:** Requires sufficient executor memory.

*  **Initial State:**

sales_df (Large) is partitioned across Executors.
store_df (Small) exists as a DataFrame, known to the Driver.


```text
+---------------------+          +-----------------------+      +-----------------------+
|    [ Spark Driver ] |          |    [ Executor 1 ]     |      |    [ Executor 2 ]     |
|                     |          |-----------------------|      |-----------------------|
| Knows about:        |          | sales_df Partition 1  |      | sales_df Partition 2  |
|  - sales_df (Large) |          | (1, 101, 50.0)        |      | (2, 102, 150.0)       |
|  - store_df (Small) |          | (3, 101, 75.0)        |      | (5, 102, 200.0)       |
|                     |          | (6, 101, 90.0)        |      | (4, 103, 25.0)        |
|                     |          | (9, 101, 65.0)        |      | (8, 103, 45.0)        |
+---------------------+          | (7, 104, 120.0)       |      |  ... (more rows) ...  |
                                 +-----------------------+      +-----------------------+
                                     (Other Executors...)

      Small DataFrame (store_df):
      +----------+-----------------+----------+
      | store_id | store_name      | city     |
      +----------+-----------------+----------+
      | 101      | Main St Store   | New York |
      | 102      | Oak Ave Shop    | London   |
      | 103      | Pine Plaza      | Tokyo    |
      | 105      | Elm Center      | Paris    |
      +----------+-----------------+----------+



---

Okay, here is the complete explanation, including the conceptual steps and the Markdown-based visualization, formatted as a single Markdown block:

Markdown

# Broadcast Hash Join in Spark

## What is a Broadcast Hash Join?

A Broadcast Hash Join is an optimization technique used in Spark SQL (and other distributed query engines) to perform joins between a large table (DataFrame) and a small table efficiently.

Instead of shuffling both tables across the network so that rows with the same join key end up on the same worker node (which is what happens in a typical Shuffle Sort Merge Join), Spark takes a different approach:

1.  **Broadcast Phase:** The *entire* small table is collected by the Spark driver and then sent (broadcasted) to *every executor node* involved in processing the large table.
2.  **Hash Table Build Phase:** Each executor receives the broadcasted small table data and builds an in-memory hash table using the join key(s).
3.  **Join (Probe) Phase:** Each executor then processes its assigned partitions of the *large* table. For each row in its partition of the large table, it probes the in-memory hash table (built from the small table) using the join key to find matching rows. If a match is found, the joined rows are produced.

## Why Use It? (Advantages)

* **Avoids Expensive Shuffle:** The biggest advantage is that it completely avoids shuffling the *large* table across the network. Network I/O and the associated serialization/deserialization are often major bottlenecks in distributed joins. Only the small table is moved across the network once (broadcasted).
* **Speed:** When applicable (one table is small), it's generally much faster than shuffle-based joins because hash table lookups are very fast (O(1) on average), and the costly shuffle step for the large table is eliminated.

## When Does Spark Use It? (Conditions)

Spark's Catalyst optimizer automatically decides whether to use a Broadcast Hash Join based on a few factors:

1.  **Table Size:** One of the tables must be significantly smaller than the other.
2.  **Configuration Threshold:** The estimated size of the table to be broadcast must be below the configuration parameter `spark.sql.autoBroadcastJoinThreshold` (default is often 10MB, but can be configured). Spark uses statistics (if available) or estimates to determine the size.
3.  **Join Type:** It's typically used for Inner joins, Left Outer joins (broadcasting the right table), Right Outer joins (broadcasting the left table), Left Semi joins, and Left Anti joins. It's generally *not* suitable for Full Outer joins because both sides might need to be probed.
4.  **Memory:** The broadcasted table, when built into a hash table, must fit comfortably within the memory of each executor node.

**Explicit Hint:** You can also explicitly tell Spark to use a broadcast join using the `broadcast()` function hint, even if the table size slightly exceeds the threshold (use with caution!).

## Disadvantages/Limitations

* **Small Table Constraint:** Only feasible if one table is genuinely small enough to fit in the driver's memory (for collection) and each executor's memory (for the hash table). Broadcasting a very large table will likely cause OutOfMemoryErrors (OOM).
* **Driver Bottleneck:** Collecting the small table on the driver can become a bottleneck if the table is near the threshold size or if many broadcast joins happen concurrently.
* **Executor Memory:** Requires sufficient executor memory.

---


Initial State:

sales_df (Large) is partitioned across Executors.
store_df (Small) exists as a DataFrame, known to the Driver.
Markdown

+---------------------+          +-----------------------+      +-----------------------+
|    [ Spark Driver ] |          |    [ Executor 1 ]     |      |    [ Executor 2 ]     |
|                     |          |-----------------------|      |-----------------------|
| Knows about:        |          | sales_df Partition 1  |      | sales_df Partition 2  |
|  - sales_df (Large) |          | (1, 101, 50.0)        |      | (2, 102, 150.0)       |
|  - store_df (Small) |          | (3, 101, 75.0)        |      | (5, 102, 200.0)       |
|                     |          | (6, 101, 90.0)        |      | (4, 103, 25.0)        |
|                     |          | (9, 101, 65.0)        |      | (8, 103, 45.0)        |
+---------------------+          | (7, 104, 120.0)       |      |  ... (more rows) ...  |
                                 +-----------------------+      +-----------------------+
                                     (Other Executors...)

      Small DataFrame (store_df):
      +----------+-----------------+----------+
      | store_id | store_name      | city     |
      +----------+-----------------+----------+
      | 101      | Main St Store   | New York |
      | 102      | Oak Ave Shop    | London   |
      | 103      | Pine Plaza      | Tokyo    |
      | 105      | Elm Center      | Paris    |
      +----------+-----------------+----------+

**Step 1: Collect & Broadcast (Small DataFrame)**

The Driver determines store_df is small enough (or sees the broadcast() hint). It collects all data from store_df to its own memory and then broadcasts this data to all Executors working on sales_df.

```text

+---------------------+  ---- Collect store_df ----> (Data Fits in Driver Memory)
|    [ Spark Driver ] |
|                     |
| Holds collected     |
|   store_df data     |
|                     |  -- Broadcast store_df Data --+
+---------------------+                             |
         |                                          |
         V                                          V
+-----------------------+                 +-----------------------+
|    [ Executor 1 ]     |                 |    [ Executor 2 ]     |
|-----------------------|                 |-----------------------|
| Receives store_df     |                 | Receives store_df     |
|   (Broadcast Variable)|                 |   (Broadcast Variable)|
|                       |                 |                       |
| sales_df Partition 1  |                 | sales_df Partition 2  |
| (1, 101, 50.0)        |                 | (2, 102, 150.0)       |
| (3, 101, 75.0)        |                 | (5, 102, 200.0)       |
| ...                   |                 | ...                   |
+-----------------------+                 +-----------------------+

Okay, here is the complete explanation, including the conceptual steps and the Markdown-based visualization, formatted as a single Markdown block:

Markdown

# Broadcast Hash Join in Spark

## What is a Broadcast Hash Join?

A Broadcast Hash Join is an optimization technique used in Spark SQL (and other distributed query engines) to perform joins between a large table (DataFrame) and a small table efficiently.

Instead of shuffling both tables across the network so that rows with the same join key end up on the same worker node (which is what happens in a typical Shuffle Sort Merge Join), Spark takes a different approach:

1.  **Broadcast Phase:** The *entire* small table is collected by the Spark driver and then sent (broadcasted) to *every executor node* involved in processing the large table.
2.  **Hash Table Build Phase:** Each executor receives the broadcasted small table data and builds an in-memory hash table using the join key(s).
3.  **Join (Probe) Phase:** Each executor then processes its assigned partitions of the *large* table. For each row in its partition of the large table, it probes the in-memory hash table (built from the small table) using the join key to find matching rows. If a match is found, the joined rows are produced.

## Why Use It? (Advantages)

* **Avoids Expensive Shuffle:** The biggest advantage is that it completely avoids shuffling the *large* table across the network. Network I/O and the associated serialization/deserialization are often major bottlenecks in distributed joins. Only the small table is moved across the network once (broadcasted).
* **Speed:** When applicable (one table is small), it's generally much faster than shuffle-based joins because hash table lookups are very fast (O(1) on average), and the costly shuffle step for the large table is eliminated.

## When Does Spark Use It? (Conditions)

Spark's Catalyst optimizer automatically decides whether to use a Broadcast Hash Join based on a few factors:

1.  **Table Size:** One of the tables must be significantly smaller than the other.
2.  **Configuration Threshold:** The estimated size of the table to be broadcast must be below the configuration parameter `spark.sql.autoBroadcastJoinThreshold` (default is often 10MB, but can be configured). Spark uses statistics (if available) or estimates to determine the size.
3.  **Join Type:** It's typically used for Inner joins, Left Outer joins (broadcasting the right table), Right Outer joins (broadcasting the left table), Left Semi joins, and Left Anti joins. It's generally *not* suitable for Full Outer joins because both sides might need to be probed.
4.  **Memory:** The broadcasted table, when built into a hash table, must fit comfortably within the memory of each executor node.

**Explicit Hint:** You can also explicitly tell Spark to use a broadcast join using the `broadcast()` function hint, even if the table size slightly exceeds the threshold (use with caution!).

## Disadvantages/Limitations

* **Small Table Constraint:** Only feasible if one table is genuinely small enough to fit in the driver's memory (for collection) and each executor's memory (for the hash table). Broadcasting a very large table will likely cause OutOfMemoryErrors (OOM).
* **Driver Bottleneck:** Collecting the small table on the driver can become a bottleneck if the table is near the threshold size or if many broadcast joins happen concurrently.
* **Executor Memory:** Requires sufficient executor memory.

---

## Example

Let's say we have a large DataFrame of `sales` records and a small DataFrame of `store_info`. We want to join them on `store_id` to add store details to the sales records.

### Setup (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# Create a SparkSession
# In a real scenario, configure memory etc. appropriately
spark = SparkSession.builder \
    .appName("BroadcastJoinExample") \
    .config("spark.sql.autoBroadcastJoinThreshold", "20m") # Example: Set threshold to 20MB
    .getOrCreate()

# --- Create Sample Data ---

# Large DataFrame: Sales Data
sales_data = [
    (1, 101, 50.0), (2, 102, 150.0), (3, 101, 75.0),
    (4, 103, 25.0), (5, 102, 200.0), (6, 101, 90.0),
    (7, 104, 120.0), (8, 103, 45.0), (9, 101, 65.0)
    # ... imagine millions or billions of rows
] * 1000 # Making it slightly larger for demo purposes, still small in reality
sales_schema = ["sale_id", "store_id", "amount"]
sales_df = spark.createDataFrame(sales_data, sales_schema).repartition(4) # Simulate partitions

# Small DataFrame: Store Information
store_data = [
    (101, "Main St Store", "New York"),
    (102, "Oak Ave Shop", "London"),
    (103, "Pine Plaza", "Tokyo"),
    (105, "Elm Center", "Paris") # Note: Store 104 from sales is missing here
]
store_schema = ["store_id", "store_name", "city"]
store_df = spark.createDataFrame(store_data, store_schema)

# --- Perform the Join ---

# Option 1: Rely on Auto-Broadcast (if store_df is small enough)
# Spark's optimizer will likely choose BroadcastHashJoin automatically
# if store_df's size < spark.sql.autoBroadcastJoinThreshold

print("--- Join relying on auto-broadcast ---")
joined_df_auto = sales_df.join(store_df, sales_df["store_id"] == store_df["store_id"], "inner")

# To see the execution plan:
print("Execution Plan (Auto):")
joined_df_auto.explain()
# Look for "BroadcastHashJoin" or "BHJ" in the physical plan output

# joined_df_auto.show() # Showing might be too large with multiplier


# Option 2: Explicitly Hint for Broadcast Join
# Useful if you know store_df is small or want to force the strategy

print("\n--- Join with explicit broadcast hint ---")
# Use the broadcast() function around the smaller DataFrame
joined_df_explicit = sales_df.join(broadcast(store_df), sales_df["store_id"] == store_df["store_id"], "inner")

# Check the plan again
print("Execution Plan (Explicit Hint):")
joined_df_explicit.explain()
# You should definitely see "BroadcastHashJoin" or "BHJ" here

# joined_df_explicit.show()


# Example with Left Outer Join (Broadcasting the right side)
print("\n--- Left Outer Join with broadcast hint ---")
left_join_df = sales_df.join(broadcast(store_df), sales_df["store_id"] == store_df["store_id"], "left_outer")

print("Execution Plan (Left Outer Hint):")
left_join_df.explain()
# left_join_df.show()

# Stop the SparkSession
spark.stop()

Explanation of the Example Execution (Conceptual)
Optimization: Spark's Catalyst optimizer analyzes the join sales_df.join(store_df, ...) or sales_df.join(broadcast(store_df), ...).
Size Check: It estimates the size of sales_df (large) and store_df (small).
Threshold Comparison: It compares the size of store_df against spark.sql.autoBroadcastJoinThreshold.
Decision (Auto): If store_df is smaller than the threshold (and other conditions met), the optimizer plans a Broadcast Hash Join.
Decision (Explicit): If broadcast(store_df) is used, the optimizer is forced to plan a Broadcast Hash Join (unless fundamentally impossible, e.g., driver/executor memory constraints).
Broadcast: The driver collects all data from store_df into its memory.
Send: The driver serializes and broadcasts the store_df data to all executors working on sales_df partitions. This is sent as a read-only Broadcast Variable.
Build Hash Table: Each executor receives the broadcasted store_df data and builds an in-memory hash table (like a Python dictionary/Java HashMap) keyed by store_id.
Process Large Table: Each executor reads its assigned partition(s) of sales_df row by row.
Probe: For each sales_df row, it takes the store_id and looks it up in the locally built store_df hash table.
Combine: If a match is found (for inner join), it combines the columns from the sales_df row and the matching store_df row(s) and outputs the result. For a left outer join, if no match is found, it still outputs the sales_df row combined with nulls for the store_df columns.
Result: The final joined DataFrame is constructed from the results generated by all executors. No shuffle of the large sales_df occurred.
Visual Representation (Markdown)
Imagine we have a Spark cluster with a Driver and multiple Executors.

Initial State:

sales_df (Large) is partitioned across Executors.
store_df (Small) exists as a DataFrame, known to the Driver.
Markdown

+---------------------+          +-----------------------+      +-----------------------+
|    [ Spark Driver ] |          |    [ Executor 1 ]     |      |    [ Executor 2 ]     |
|                     |          |-----------------------|      |-----------------------|
| Knows about:        |          | sales_df Partition 1  |      | sales_df Partition 2  |
|  - sales_df (Large) |          | (1, 101, 50.0)        |      | (2, 102, 150.0)       |
|  - store_df (Small) |          | (3, 101, 75.0)        |      | (5, 102, 200.0)       |
|                     |          | (6, 101, 90.0)        |      | (4, 103, 25.0)        |
|                     |          | (9, 101, 65.0)        |      | (8, 103, 45.0)        |
+---------------------+          | (7, 104, 120.0)       |      |  ... (more rows) ...  |
                                 +-----------------------+      +-----------------------+
                                     (Other Executors...)

      Small DataFrame (store_df):
      +----------+-----------------+----------+
      | store_id | store_name      | city     |
      +----------+-----------------+----------+
      | 101      | Main St Store   | New York |
      | 102      | Oak Ave Shop    | London   |
      | 103      | Pine Plaza      | Tokyo    |
      | 105      | Elm Center      | Paris    |
      +----------+-----------------+----------+
Step 1: Collect & Broadcast (Small DataFrame)

The Driver determines store_df is small enough (or sees the broadcast() hint). It collects all data from store_df to its own memory and then broadcasts this data to all Executors working on sales_df.

Markdown

+---------------------+  ---- Collect store_df ----> (Data Fits in Driver Memory)
|    [ Spark Driver ] |
|                     |
| Holds collected     |
|   store_df data     |
|                     |  -- Broadcast store_df Data --+
+---------------------+                             |
         |                                          |
         V                                          V
+-----------------------+                 +-----------------------+
|    [ Executor 1 ]     |                 |    [ Executor 2 ]     |
|-----------------------|                 |-----------------------|
| Receives store_df     |                 | Receives store_df     |
|   (Broadcast Variable)|                 |   (Broadcast Variable)|
|                       |                 |                       |
| sales_df Partition 1  |                 | sales_df Partition 2  |
| (1, 101, 50.0)        |                 | (2, 102, 150.0)       |
| (3, 101, 75.0)        |                 | (5, 102, 200.0)       |
| ...                   |                 | ...                   |
+-----------------------+                 +-----------------------+

**Step 2: Build Hash Table (On Each Executor)**

Each Executor takes the broadcasted store_df data and builds an efficient, in-memory Hash Table using the join key (store_id).

```text

+-----------------------------------+      +-----------------------------------+
|         [ Executor 1 ]            |      |         [ Executor 2 ]            |
|-----------------------------------|      |-----------------------------------|
|                                   |      |                                   |
|  Build Hash Table (from store_df) |      |  Build Hash Table (from store_df) |
|  HashTable [store_id -> (name, city)] |  |  HashTable [store_id -> (name, city)] |
|    101: ('Main St', 'NY')         |      |    101: ('Main St', 'NY')         |
|    102: ('Oak Ave', 'London')     |      |    102: ('Oak Ave', 'London')     |
|    103: ('Pine Plaza', 'Tokyo')   |      |    103: ('Pine Plaza', 'Tokyo')   |
|    105: ('Elm Center', 'Paris')   |      |    105: ('Elm Center', 'Paris')   |
|                                   |      |                                   |
|-----------------------------------|      |-----------------------------------|
| sales_df Partition 1              |      | sales_df Partition 2              |
| (1, 101, 50.0)                    |      | (2, 102, 150.0)                   |
| (3, 101, 75.0)                    |      | (5, 102, 200.0)                   |
| (6, 101, 90.0)                    |      | (4, 103, 25.0)                    |
| (9, 101, 65.0)                    |      | (8, 103, 45.0)                    |
| (7, 104, 120.0)                   |      |  ...                              |
+-----------------------------------+      +-----------------------------------+

**Step 3: Join/Probe (Locally on Each Executor)**

Each Executor iterates through its local partition of the large sales_df. For each row, it takes the store_id and probes (looks up) the Hash Table. If a match is found (for an inner join), it combines the row from sales_df and the data from the hash table.

```text

+-----------------------------------+      +-----------------------------------+
|         [ Executor 1 ]            |      |         [ Executor 2 ]            |
|-----------------------------------|      |-----------------------------------|
|  HashTable [store_id -> (name, city)] |  |  HashTable [store_id -> (name, city)] |
|    101: ('Main St', 'NY')         |      |    101: ('Main St', 'NY')         |
|    102: ('Oak Ave', 'London')     |      |    102: ('Oak Ave', 'London')     |
|    103: ('Pine Plaza', 'Tokyo')   |      |    103: ('Pine Plaza', 'Tokyo')   |
|    105: ('Elm Center', 'Paris')   |      |    105: ('Elm Center', 'Paris')   |
|-----------------------------------|      |-----------------------------------|
| Process sales_df Partition 1      |      | Process sales_df Partition 2      |
|                                   |      |                                   |
| (1, 101, 50.0) --Probe(101)--> Match! |  | (2, 102, 150.0) --Probe(102)--> Match! |
| (3, 101, 75.0) --Probe(101)--> Match! |  | (5, 102, 200.0) --Probe(102)--> Match! |
| (6, 101, 90.0) --Probe(101)--> Match! |  | (4, 103, 25.0)  --Probe(103)--> Match! |
| (9, 101, 65.0) --Probe(101)--> Match! |  | (8, 103, 45.0)  --Probe(103)--> Match! |
| (7, 104, 120.0) --Probe(104)--> No Match |  |  ...                              |
|                                   |      |                                   |
| --> Output Partition 1 (Joined)   |      | --> Output Partition 2 (Joined)   |
| (1, 101, 50.0, 'Main St', 'NY')   |      | (2, 102, 150.0, 'Oak Ave', 'London') |
| (3, 101, 75.0, 'Main St', 'NY')   |      | (5, 102, 200.0, 'Oak Ave', 'London') |
| (6, 101, 90.0, 'Main St', 'NY')   |      | (4, 103, 25.0, 'Pine Plaza', 'Tokyo')  |
| (9, 101, 65.0, 'Main St', 'NY')   |      | (8, 103, 45.0, 'Pine Plaza', 'Tokyo')  |
+-----------------------------------+      +-----------------------------------+

(Note: For an inner join, the row with store_id 104 is dropped as it has no match in the hash table. For a left outer join, it would be kept with nulls for store_name and city)

**Step 4: Final Result**

The joined partitions from all Executors are combined to form the final resulting DataFrame. No shuffling of the large sales_df was needed.

```text

Result Partition 1                      Result Partition 2
+-----------------------------------+      +------------------------------------------+
| (1, 101, 50.0, 'Main St', 'NY')   |      | (2, 102, 150.0, 'Oak Ave', 'London')    |
| (3, 101, 75.0, 'Main St', 'NY')   |      | (5, 102, 200.0, 'Oak Ave', 'London')    |
| (6, 101, 90.0, 'Main St', 'NY')   |      | (4, 103, 25.0, 'Pine Plaza', 'Tokyo')   |
| (9, 101, 65.0, 'Main St', 'NY')   |      | (8, 103, 45.0, 'Pine Plaza', 'Tokyo')   |
+-----------------------------------+      +------------------------------------------+
                   |                                      |
                   +---------------\      /---------------+
                                    \    /
                             [ Final Joined DataFrame ]
              +-----+----------+--------+-----------------+----------+
              | ... | store_id | amount | store_name      | city     |
              +-----+----------+--------+-----------------+----------+
              | 1   | 101      | 50.0   | Main St Store   | New York |
              | 3   | 101      | 75.0   | Main St Store   | New York |
              | 6   | 101      | 90.0   | Main St Store   | New York |
              | 9   | 101      | 65.0   | Main St Store   | New York |
              | 2   | 102      | 150.0  | Oak Ave Shop    | London   |
              | 5   | 102      | 200.0  | Oak Ave Shop    | London   |
              | 4   | 103      | 25.0   | Pine Plaza      | Tokyo    |
              | 8   | 103      | 45.0   | Pine Plaza      | Tokyo    |
              +-----+----------+--------+-----------------+----------+
