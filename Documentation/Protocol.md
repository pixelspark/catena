#  Protocol

Nodes use a Gossip protocol to perform the following tasks:

* Notify other nodes of their presence, temporary id (UUID) and incoming port ("query")
* Return information about their current view on the chain ("index")
* Notify other nodes of the successful mining of a new block ("block")
* Fetch blocks from other nodes by hash ("fetch" and "block")

## Peer states

A peer can be in any of the following states:

* `new`: the address (and possibly UUID) of a peer are known, but no connection attempt has been made by either party.
* `connecting`: the address (and possibly UUID) of a peer is known and an outgoing connection is being set up.
* `connected`: a connection (incoming or outgoing) has been established with the peer, but the peer has not been queried yet.
* `querying`: a query request was sent to the peer and the sender is awaiting the reply.
* `queried`: the peer sent a reply in response to the most recent `query` request.
* `ignored`: the peer is ignored forever (either because it did not comply with the protocol or it is a loopback)
* `failed`: connecting to the peer failed. A new connection attempt may be performed later on.
* `passive`: the peer has, in reply to a `query` request, indicated that it is a passive peer. This means the peer should not be queried, but wants to be informed of new blocks and transactions. The peer might initiate other queries.

## Data format

Clients communicate over WebSocket connections using a bidirectional request-response protocol. The protoocol is message-based, encoded in JSON, and transported over WebSocket. Messages can be sent either as binary
or (UTF-8) text frames. The WebSocket ping/pong mechanism may be used to test connection liveness.

## Connection set-up

A connection can be initiated by either node, and on a single connection, queries and responses may flow both ways.
The peer that initiates the connection is required to send the following URL query parameters when initializing the WebSocket:

* uuid, set to the UUID of the connecting peer. This is needed to prevent nodes from accidentally connecting with themselves. Any peer will deny connections with a UUID that is equal to their own.
* port, set to the port number of the server on the connecting peer's side that accepts connections. This is used for peer exchange. Note that the port may not be reachable from other peers. When the connecting peer does not support incoming connections ('client only'), it may send port=0 or omit the port parameter.

The WebSocket request *must* include the following headers:

* Sec-WebSocket-Protocol, set to the protocol version string (currently "catena-v1"). Peers may reject incompatible versions (older or newer).

## Packet format

Packets are encoded as JSON objects. A request and response packet has the following format:

````
[n, {"t": type, ...}]
````

Each request has a counter value (`n`) that is unique for this connection. Responses are correlated with their requests using the counter value; the peer responding to a request returns the response with the same counter value. The peer that initiated the socket connection uses even counter values (starting at 0) for its requests, whereas the accepting peer uses uneven counter values (starting at 1) for its requests.

The packet contents are encoded as a JSON object, with the special "t" key indicating the type of message.

Hashes are encoded as hexadecimal strings.

## Message types

| Message type | Reply type(s) | Description |
|------------------|----------------|---------------|
| query               | index or passive or error | Request peer status (index) |
| passive           |                        | Indicate that the peer is passive in response to a query request |
| fetch                | block or error | Request a specific block from the recipient's chain |
| block               |                       | Send a block to the recipient, either in response to a fetch request, or unsolicited (newly mined blocks) |
| transaction      |                       | Send a transaction to the recipient for addition to its memory pool |
| error           |                        | Indicate an error (in reply to a request) |

### query

````
{"t":"query"}
````

Requests status information from the other peer. Generally this is the first request made when two peers connect. The request is typically repeated periodically. The receiving peer should respond with a message of type `index`.

### fetch

````
{"t":"fetch", "hash": blockHash}
````

Requests the other peer to return it the block with the indicated hash. The receiving peer responds with a `block` message, or an `error` message in case it does not have the block with the requested hash.

### error

````
{"t":"error", "message": errorMessage}
````

Whenever a request can't be fulfilled, the error message can be sent as reply instead.

### index

````
{"t":"index", "index":
	"highest": highestHash,
	"height": height,
	"genesis": genesisHash,
	"peers": [ peerURLs ]
}
````

The `index` message conveys general status information for a peer. It is sent in reply to the `query` message. The index contains:

* highestHash: the hash of the block at the head of the longest blockchain as seen by the peer (String).
* height: the height of the highest block (Integer)
* genesis: the hash of the genesis block (String)
* peers: an array of URLs for other, valid peers seen by the peer (Array of String)

### block

````
{"t":"block", "block": {
	"previous": previousHash,
	"hash": blockHash,
	"nonce": blockNonce,
	"index": blockHeight,
	"payload": blockPayload
}}
````

A `block` message is sent in reply to a `fetch` message, or sent as request (`unsolicited`), in which case the message should contain a newly mined block that is not yet part of the blockchain of the recipient. The recipient will not send a reply to this message. The block contains the following data:

* previous: the hash of the block this block follows to (hash as String)
* hash: the hash signature of this block (hash as String)
* nonce: the nonce value for this block (Integer)
* payload: the payload of this block (as Base-64 encoded String). The format of the payload is application-specific; for Catena SQL blocks, it is a JSON array of transactions for all blocks except the genesis block. The transaction JSON format is described below.

### passive

````
{"t":"passive"}
````

Sent in reply to a `query` request to let the other side know that this peer is only passively participating (e.g. does not maintain a tree or transaction cache, just wants to be able to query the peer and be notified of new blocks/transactions).

### transaction

````
{"t": "tx", "tx": {
	"tx": {
		"sql": txSQL,
		"counter": txCounter,
		"invoker": txInvoker
	},
	"signature": transactionSignature
}}
````

A transaction request is sent for each new transaction that sender wants to add to the memory pool of recipient.

