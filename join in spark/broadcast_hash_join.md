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



