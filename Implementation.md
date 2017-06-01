# Implementation notes

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

A message also contains a payload,  which is a dictionary with (at least) the key 'q' in it,  associated with the request type.

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
