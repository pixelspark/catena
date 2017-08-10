# Implementation notes

## Database structure

## Metadata

Catena provides an ordinary SQL database. The following system tables are defined and visible to clients:

* _grants_: holds information about database privileges (see 'authentication' below).

These tables are created and maintained 'on chain' (that is, the CREATE statement for it is included in the blockchain). They are also subject to the privilege system.

### Internal metadata tables

Internally, Catena stores metadata in the following tables that are *not* visible to (nor modifyable by) clients:

* __info_: holds information about the current block hash and index. When a block transaction is executed, this contains information on the *last* block processed (i.e. not the block the transaction is part of)
* __blocks_: holds an archive of all blocks in the chain.
* __users_: holds the transaction counter for each transaction invoker public key (SHA-256 hashed)

## Authentication

Catena uses Ed25519 key pairs for authentication. A transaction contains an 'invoker' field which holds the public key of the
invoker of the query. The transactions signature needs to validate for the public key of the invoker.

### Privileges

Before a transaction is executed, the query parser determines the privileges required for the query. Currently, the followjng
kinds of privileges are recognized:

* "create" (CREATE TABLE)
* "delete" (DELETE FROM)
* "drop" (DROP TABLE)
* "insert" (INSERT INTO)
* "update" (UPDATE)

Privileges are checked against a grants table. The grants table has three columns:
* "user": the SHA-256 hash of the public key of the user that is allowed the privilege
* "kind": the privilege kind (one of the above strings)
* "table": the table to which the privilege applies. NULL if the privilege applies to all tables.

Note: associated privileges are *not* automatically removed when referenced tables are dropped, nor are grants automatically created when a table is created.

The "table" parameter for a grant can be "grants", in which case the user can perform the indicated operation on the grants table (use wisely). Regardless of the grants table, other special tables (such as _info_ and _blocks_ are never writable).

A transaction can execute if the required privileges exist after processing of the block previous to the one it is part of. A transaction hence cannot depend on privileges created in the same block. When a transaction cannot execute due to missing privileges, it is simply ignored.

The SHA-256 hash of the invoker's public key is stored in `user` instead of the real public key. This ensures that the public key only
becomes known when it is used for the first time, mitigating possible future attacks against (weak) public keys. A similar
protection is present in Bitcoin (where transaction outputs are linked to a hash of the receiver's address).

### Replay protection

A transaction includes a counter. A transaction will only execute if its counter is exactly equal to the counter value of the previous transaction executed by the invoker (in previous blocks or within the current block), or 0 when the invoker has not yet executed a transaction. When multiple transactions from the same invoker are in the same block, they are executed in the order of increasing counter. The relative ordering of the execution of transactions from different invokers is undefined.

## Proof of Work

A block signature is the SHA-256 hash of the following:

* _block version_ (64-bit, unsigned, little endian)
* _block index_ (64-bit, unsigned, little endian)
* _block nonce_ (64-bit, unsigned, little endian)
* _previous block hash_ (32 bytes)
* _miner public key hash_ (SHA256 hash of public key, 32 bytes)
* _block timestamp_  (64-bit, unsigned, little endian, UNIX-timestamp; omitted for genesis blocks)
* _payload data for signing_ (see below)

The payload data for signing is constructed as follows:

* If the block is a genesis block, the payload data for signing is the seed string encoded as UTF-8 without zero termination.
* If the block is a regular block, the payload data for signing is the concatenation of the transaction signatures

A transaction signature is the Ed25519 signature of the following data, using the private key of the invoker:

* _invoker key_: The public key of the invoker
* _transaction counter_: the transaction counter (64-bit, unsigned, little endian)
* _transaction statement_: the SQL statement for the transaction, encoded as UTF-8.

## Limits

The following limits are currently enforced;

* _Block size_: a block's payload may not be larger than 1 MiB (measured against the data to be signed)
* _Transactions per block_: a block may not contain more than 100 transactions
* _Transaction size_: a transaction may not be larger than 10 KiB (measured against the data to be signed)

## Genesis blocks

In Catena, two blocks are 'special':

* The genesis block (block #0) in Catena is 'special' in the sense that instead of transactions, it contains a special 'seed string'. The block is deterministically mined (starting from nonce=0) so that a given seed string and difficulty will lead to a specific genesis block and signature.
* The configuration block (block #1) is special because unlike the other blocks, it does not check grants before executing transactions. This block is actually required to contain the transactions that set up the grants table.

## Transaction playback

When a node receives a valid block, it appends it to a queue. When the queue grows beyond a preset size, the oldest block
in the queue is persisted on disk. When the chain needs to be spliced and the splice happens inside the queue, the client can
perform this splice efficiently. If the splice happens in a block older than the oldest block in the queue, the client needs to
replay the full set of transactions from block #0 to the splice point.

All read queries (sent over the Postgres wire protocol) execute in a 'hypothetical' transaction - this is a transaction that is started
before the query and in which the queued block transactions have been executed already. The transaction is automatically
rolled back after the query is finished, such that the changes from the queued blocks are not persisted to disk.
