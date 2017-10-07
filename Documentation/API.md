
# API

Catena exposes three interfaces:

* [The WebSocket interface](Protocol.md), used by nodes to talk to each other. Additionally, it can be used by clients to obtain information on blocks, the chain, and to submit transactions.
* An HTTP interface, which is intended to be used by clients to submit queries and obtain information that cannot be obtained through the WebSocket interface.
* A PostgreSQL wire-protocol compatible interface, which is intended to be used by clients to submit queries.

## HTTP API

The HTTP API is available at the '/api' path at the node's port. Access control restrictions apply
when fetching from this interface from a browser (unless the `--allow-domains` option is used to allow
additional origins to access the endpoints.)

The following endpoints are available:

| Path | Request | Decsription |
|------|---------|-------|
| /api/query | POST (json) | Submit a query |
| /api/counter/:hash | GET | Obtain the current transaction counter for a public key |

### /api/query

This endpoint expects a POST request with a JSON object as body. The JSON object contains a single
`sql` field which contains an SQL statement.

The endpoint will return HTTP status `200` (OK) when the query could be executed. The response will
be a JSON object that looks like this:

````
{
	"columns": ["colA", "colB"],
	"rows": [
		[1, 2],
		[3, 4]
	]
}
````

The endpoint will return HTTP status `406` (unacceptable) when a mutating query was submitted. The
response will be a JSON object that contains a single "sql" key, containing the canonically formatted
SQL statement (a client can use this to compose a transaction).

When an error occurs (e.g. syntax error in the query), the endpoint will return a 50x status code.
The response will be a JSON object containing a single "message" key, which is a string describing the
error that occurred.

### /api/counter/:hash

This endpoint expects a GET request. In the path, `:hash` must be replaced with a valid, Base58check
formatted public key.

The response will be a JSON object containing a single `counter` field. The field value is `null` when
no counter value is known for the indicated key, or an integer value if a counter value is known.
