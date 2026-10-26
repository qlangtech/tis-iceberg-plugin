# Schemas — Apache Iceberg™ (latest / 1.12.0)

Iceberg tables support the following types:

| Type | Description | Notes |
|------|-------------|-------|
| `boolean` | True or false | |
| `int` | 32-bit signed integers | Can promote to `long` |
| `long` | 64-bit signed integers | |
| `float` | 32-bit IEEE 754 floating point | Can promote to `double` |
| `double` | 64-bit IEEE 754 floating point | |
| `decimal(P,S)` | Fixed-point decimal; precision P, scale S | Scale is fixed and precision must be 38 or less |
| `date` | Calendar date without timezone or time | |
| `time` | Time of day without date, timezone | Stored as microseconds |
| `timestamp` | Timestamp without timezone | Stored as microseconds |
| `timestamptz` | Timestamp with timezone | Stored as microseconds |
| `timestamp_ns` | Timestamp without timezone, nanosecond precision | Stored as nanoseconds; added in v3 |
| `timestamptz_ns` | Timestamp with timezone, nanosecond precision | Stored as nanoseconds; added in v3 |
| `string` | Arbitrary-length character sequences | Encoded with UTF-8 |
| `uuid` | Universally unique identifiers | |
| `fixed(L)` | Fixed-length byte array of length L | |
| `binary` | Arbitrary-length byte array | |
| `variant` | Semi-structured data (JSON-like) | Added in v3 |
| `geometry(C)` | Geospatial features with CRS parameter | Linear edge-interpolation; added in v3 |
| `geography(C, A)` | Geospatial features with CRS and edge algorithm | Non-linear edge-interpolation; added in v3 |
| `unknown` | Placeholder type for undetermined columns | Must be optional; added in v3 |
| `struct<...>` | A record with named fields of any data type | |
| `list<E>` | A list with elements of any data type | |
| `map<K, V>` | A map with keys and values of any data type | |

Iceberg tracks each field in a table schema using an ID that is never reused in a table. See correctness guarantees for more information.

> Source: https://iceberg.apache.org/docs/latest/schemas/
