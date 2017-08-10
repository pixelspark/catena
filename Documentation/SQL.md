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


