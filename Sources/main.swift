import Foundation
import Kitura
import CommandLineKit

let databaseFileOption = StringOption(shortFlag: "d", longFlag: "database", required: false, helpMessage: "Backing database file (default: catena.sqlite)")
let seedOption = StringOption(shortFlag: "s", longFlag: "seed", required: false, helpMessage: "Genesis block seed string (default: empty)")
let helpOption = BoolOption(shortFlag: "h", longFlag: "help", helpMessage: "Show usage")
let netPortOption = IntOption(shortFlag: "p", longFlag: "gossip-port", helpMessage: "Listen port for peer-to-peer communications (default: 8338)")
let queryPortOption = IntOption(shortFlag: "q", longFlag: "query-port", helpMessage: "Listen port for query communications (default: networking port + 1)")
let peersOption = MultiStringOption(shortFlag: "j", helpMessage: "Peer to connect to ('hostname:port' or just 'hostname')")
let mineOption = BoolOption(shortFlag: "m", longFlag: "mine", helpMessage: "Enable mining of blocks")

let cli = CommandLineKit.CommandLine()
cli.addOptions(databaseFileOption, helpOption, seedOption, netPortOption, queryPortOption, peersOption, mineOption)

do {
	try cli.parse()
}
catch {
	cli.printUsage(error)
	exit(EX_USAGE)
}

if helpOption.wasSet {
	cli.printUsage()
	exit(EX_OK)
}

let database = Database()
if !database.open(databaseFileOption.value ?? "catena.sqlite") {
	fatalError("Could not open database")
}

_ = database.perform("DROP TABLE IF EXISTS test")
_ = database.perform("CREATE TABLE test (origin TEXT, x INT)")

let seedValue = seedOption.value ?? ""
let genesisPayload = SQLPayload(transactions: [
	SQLTransaction(statement: "SELECT '\(seedValue)';")
])

var genesisBlock = SQLBlock(index: 0, previous: Hash.zeroHash, payload: genesisPayload)
genesisBlock.mine(difficulty: 14)
print("Genesis block=\(genesisBlock.debugDescription)) \(genesisBlock.isSignatureValid)")

var ledger = SQLLedger(genesis: genesisBlock, database: database)
let netPort = netPortOption.value ?? 8338
let node = Node<SQLBlock>(ledger: ledger, port: netPort)

// Add peers from command line
for p in peersOption.value ?? [] {
	if var u = URL(string: "http://\(p)") {
		node.add(peer: Peer<SQLBlock>(URL: u))
	}
}

// Query server
let queryServerV4 = NodeQueryServer(node: node, port: queryPortOption.value ?? (netPort+1), family: .ipv4)
let queryServerV6 = NodeQueryServer(node: node, port: queryPortOption.value ?? (netPort+1), family: .ipv6)
queryServerV6.run()
queryServerV4.run()

node.miner.enabled = mineOption.value
node.start()

print("Start submitting demo blocks")
var i = 0
while true {
	i += 1
	let q = "INSERT INTO test (origin, x) VALUES ('\(node.uuid.uuidString)', \(i))"
	let payload = SQLPayload(transactions: [SQLTransaction(statement: q)])
	print("Submit \(q)")
	node.submit(payload: payload.data)
	sleep(10)
}
