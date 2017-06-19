# Implementation notes

## Database structure

Catena provides an ordinary SQL database. The following system tables are defined:

* _info_: holds information about the current block hash and index. When a block transaction is executed, this contains information on the *last* block processed (i.e. not the block the transaction is part of)
* _blocks_: holds an archive of all blocks in the chain.
* _grants_: holds information about database privileges (see 'authentication' below).

Only the _grants_ table is 'on chain' (that is, the CREATE statement for it is included in the blockchain). The other tables are
created by the client at initialization time (or can be 'virtual' depending on implementation).

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

Note: associated privileges are *not* automatically removed when referenced tables are dropped, nor are grants automatically
created when a table is created.

The "table" parameter for a grant can be "grants", in which case the user can perform the indicated operation on the grants
table (use wisely). Regardless of the grants table, other special tables (such as _info_ and _blocks_ are never writable).

A transaction can execute if the required privileges exist after processing of the block previous to the one it is part of. A transaction hence cannot depend on privileges created in the same block. When a transaction cannot execute due to missing privileges, it is simply ignored.

A hash of the invoker's public key is stored in `user` instead of the real public key. This ensures that the public key only
becomes known when it is used for the first time, mitigating possible future attacks against (weak) public keys. A similar
protection is present in Bitcoin (where transaction outputs are linked to a hash of the receiver's address).

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

## Protocol

Nodes use a Gossip protocol to perform the following tasks:

* Notify other nodes of their presence, temporary id (UUID) and incoming port ("query")
* Return information about their current view on the chain ("index")
* Notify other nodes of the successful mining of a new block ("block")
* Fetch blocks from other nodes by hash ("fetch" and "block")

The protoocol is message-based, encoded in JSON, and transported over WebSocket. Messages can be sent either as binary
or (UTF-8) text frames. The WebSocket ping/pong mechanism may be used to test connection liveness.

A connection can be initiated by either node, and on a single connection, queries and responses may flow both ways. 
The peer that initiates the connection is required to send two headers when connecting:

* X-UUID, set to the UUID of the connecting peer. This is needed to prevent nodes from accidentally connecting with themselves. Any peer will deny connections with a UUID that is equal to their own.
* X-Port, set to the port number of the server on the connecting peer's side that accepts connections. This is used for peer exchange. Note that the port may not be reachable from other peers.
* X-Version, set to the protocol version number (currently 1). Peers may reject incompatible versions (older or newer)

Each message has a counter, which allows the sender to correlate responses with requests. Multiple requests may be active
in both ways at any time. Counter values may be re-used or wrap around at any time, as long as the client can still correlate
requests (e.g. if a client never allows more than 10 simultaneous outgoing requests, counter values 0...9 can be used). 

The peer that initiated the connection uses even counters (starting at 0 and incrementing with 2 each time) whereas the
peer that accepted the connection uses uneven counters (starting at 1 and also incrementing with 2).

A message also contains a payload,  which is a dictionary with (at least) the key 't' in it,  associated with the request type.

````
[counter, {"t": "action", ...}]
````

If there is more data in the payload than expected, receiving peers may reject the message. 

### query (Request)

Requests the peer on the other side to return a summary of their current view on the chain (the 'index'). This request has
no additional payload data.


### index (Reply)

Response that contains a summary of the peer's view on the chain ('index'). The payload includes the following keys:

* "genesis" (String): the hash of the genesis block 
* "peers" (Array containing String): an array of URLs (as string) to other peers known
* "highest" (String): the hash of the highest block on the longest chain
* "height" (Integer): the height (index) of the highest block on the longest chain

### fetch (Request)

Request a block from the other peer with a certain signature hash.

### block (Request/Reply)

Sends a block to the peer on the other side. The message may be in response to a "fetch" request (in which case this must
be the requested block and it's signature must match the one requested) or it may be unsolicited (i.e. sent as request),
in which case this is a new block (e.g. mined by the sending peer) proposed for consideration by the receiving peer.

### error (Reply)

Sent in reply to requests that couldn't be fulfilled. Fields:

* "message" (String): human-readable description of why the request failed.
