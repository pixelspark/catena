# SQL as understood by Catena

## Variables

Catena exposes several variables in queries:

| Variable | Type | Description |
|----------|-------|---------------|
| $invoker | BLOB (32 bytes) | The SHA-256 hash of the public key of the invoker of the query |
| $blockHeight | INT | The index of the block of which this query's transaction is part |
| $blockSignature | BLOB (32 bytes) | The signature of the block of which this query's transaction is part |
| $previousBlockSignature | BLOB (32 bytes) | The signature of the block before the block of which this query's transaction is part |
| $miner | BLOB (32 bytes) | The SHA-256 hash of the public key of the miner of the block that contains this transaction |
| $timestamp | INT | The UNIX timestamp of the block that contains this transaction |


## Parameters

In Catena, a query can contain _bound_ and _unbound_ parameters. An _unbound_ parameter is a placeholder for a literal value.
Queries that contain unbound parameters cannot be executed - they are only used as templates, where parameters are later
substituted with _bound_ parameters or the bound values themselves. A _bound_ parameter is a parameter that has a value
bound to it. A query containing bound parameters can be executed once the bound parameters have been replaced with their
value.

Parameter names follow variable name rules (i.e. should start with an alphanumeric character, may contain numbers and
underscores afterwards). An unbound parameter is written as `?name`. A bound parameter is written as `?name:value` where
`value` is a constant literal (e.g. a string, integer, blob, null or a variable whose value is known before the query executes). Hence
`value` may not be another parameter or a column reference.

