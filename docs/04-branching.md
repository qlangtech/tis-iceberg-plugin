# Branching and Tagging — Apache Iceberg™ (latest / 1.12.0)

## Overview

Iceberg table metadata maintains a snapshot log, which represents the changes applied to a table. Snapshots are fundamental in Iceberg as they are the basis for reader isolation and time travel queries. For controlling metadata size and storage costs, Iceberg provides snapshot lifecycle management procedures such as `expire_snapshots` for removing unused snapshots and no longer necessary data files based on table snapshot retention properties.

**For more sophisticated snapshot lifecycle management, Iceberg supports branches and tags which are named references to snapshots with their own independent lifecycles. This lifecycle is controlled by branch and tag level retention policies.** Branches are independent lineages of snapshots and point to the head of the lineage. Branches and tags have a maximum reference age property which control when the reference to the snapshot itself should be expired. Branches have retention properties which define the minimum number of snapshots to retain on a branch as well as the maximum age of individual snapshots to retain on the branch. These properties are used when the expireSnapshots procedure is run.

## Use Cases

Branching and tagging can be used for handling GDPR requirements and retaining important historical snapshots for auditing. Branches can also be used as part of data engineering workflows, for enabling experimental branches for testing and validating new jobs.

### Historical Tags

Tags can be used for retaining important historical snapshots for auditing purposes.

1. Retain 1 snapshot per week for 1 month.
   ```
   ALTER TABLE prod.db.table CREATE TAG `EOW-01` AS OF VERSION 7 RETAIN 7 DAYS;
   ```
2. Retain 1 snapshot per month for 6 months.
   ```
   ALTER TABLE prod.db.table CREATE TAG `EOM-01` AS OF VERSION 30 RETAIN 180 DAYS;
   ```
3. Retain 1 snapshot per year forever (default retention for branches and tags is forever).
   ```
   ALTER TABLE prod.db.table CREATE TAG `EOY-2023` AS OF VERSION 365;
   ```
4. Create a temporary "test-branch" retained for 7 days with the latest 2 snapshots retained.
   ```
   ALTER TABLE prod.db.table CREATE BRANCH `test-branch` RETAIN 7 DAYS WITH SNAPSHOT RETENTION 2 SNAPSHOTS;
   ```

### Audit Branch

1. First ensure `write.wap.enabled` is set.
   ```
   ALTER TABLE db.table SET TBLPROPERTIES ('write.wap.enabled'='true');
   ```
2. Create `audit-branch` starting from snapshot 3, retained for 1 week.
   ```
   ALTER TABLE db.table CREATE BRANCH `audit-branch` AS OF VERSION 3 RETAIN 7 DAYS;
   ```
3. Writes are performed on a separate `audit-branch` independent from the main table history.
   ```
   SET spark.wap.branch=audit-branch
   INSERT INTO prod.db.table VALUES (3, 'c');
   ```
4. A validation workflow can validate the state of `audit-branch`.
5. After validation, the main branch can be `fastForward` to the head of `audit-branch`.
   ```
   CALL catalog_name.system.fast_forward('prod.db.table', 'main', 'audit-branch');
   ```
6. The branch reference will be removed when `expireSnapshots` is run 1 week later.

## Usage

Creating, querying and writing to branches and tags are supported in the Iceberg Java library, and in Spark and Flink engine integrations.

- Iceberg Java Library
- Spark DDLs
- Spark Reads
- Spark Branch Writes
- Flink Reads
- Flink Branch Writes

## Schema selection with branches and tags

It is important to understand that the schema tracked for a table is valid across all branches. When working with branches, the table's schema is used as that's the schema being validated when writing data to a branch. On the other hand, querying a tag uses the snapshot's schema, which is the schema id that snapshot pointed to when the snapshot was created.

Example:

```
CREATE TABLE db.table (id bigint, data string, col float);
INSERT INTO db.table VALUES (1, 'a', 1.0), (2, 'b', 2.0), (3, 'c', 3.0);

ALTER TABLE db.table CREATE BRANCH test_branch;
SELECT * FROM db.table.branch_test_branch;

ALTER TABLE db.table DROP COLUMN col;
ALTER TABLE db.table ADD COLUMN new_col date;
INSERT INTO db.table VALUES (4, 'd', date('2024-04-04')), (5, 'e', date('2024-05-05'));

-- Querying the head of the branch uses the TABLE's schema
SELECT * FROM db.table.branch_test_branch;
SELECT * FROM db.table VERSION AS OF 'test_branch';

-- Time travel by snapshot id uses the SNAPSHOT's schema
SELECT * FROM db.table VERSION AS OF 8109744798576441359;

-- Writing to a branch uses the TABLE's schema for validation
INSERT INTO db.table.branch_test_branch VALUES (6, 'e', date('2024-06-06')), (7, 'g', date('2024-07-07'));
```

> Source: https://iceberg.apache.org/docs/latest/branching/
