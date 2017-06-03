import Foundation
import Kitura
import CommandLineKit
import LoggerAPI
import HeliumLogger

let databaseFileOption = StringOption(shortFlag: "d", longFlag: "database", required: false, helpMessage: "Backing database file (default: catena.sqlite)")
let seedOption = StringOption(shortFlag: "s", longFlag: "seed", required: false, helpMessage: "Genesis block seed string (default: empty)")
let helpOption = BoolOption(shortFlag: "h", longFlag: "help", helpMessage: "Show usage")
let netPortOption = IntOption(shortFlag: "p", longFlag: "gossip-port", helpMessage: "Listen port for peer-to-peer communications (default: 8338)")
let queryPortOption = IntOption(shortFlag: "q", longFlag: "query-port", helpMessage: "Listen port for query communications (default: networking port + 1)")
let peersOption = MultiStringOption(shortFlag: "j", helpMessage: "Peer to connect to ('hostname:port' or just 'hostname')")
let mineOption = BoolOption(shortFlag: "m", longFlag: "mine", helpMessage: "Enable mining of blocks")
let logOption = StringOption(shortFlag: "v", longFlag: "log", helpMessage: "The log level: debug, verbose, info, warning (default: info)")

let cli = CommandLineKit.CommandLine()
cli.addOptions(databaseFileOption, helpOption, seedOption, netPortOption, queryPortOption, peersOption, mineOption, logOption)

do {
	try cli.parse()
}
catch {
	cli.printUsage(error)
	exit(64) /* EX_USAGE */
}

// Print usage
if helpOption.wasSet {
	cli.printUsage()
	exit(0)
}

// Configure logging
let logLevel = logOption.value ?? "info"
let logLevelType: LoggerMessageType

switch logLevel {
	case "verbose": logLevelType = .verbose
	case "debug": logLevelType = .debug
	case "warning": logLevelType = .warning
	case "info": logLevelType = .info
	default: fatalError("Invalid setting for --log")
}

let logger = HeliumLogger(logLevelType)
logger.details = false
Log.logger = logger

let database = Database()
if !database.open(databaseFileOption.value ?? "catena.sqlite") {
	fatalError("Could not open database")
}

_ = database.perform("DROP TABLE IF EXISTS test")
_ = database.perform("CREATE TABLE test (origin TEXT, x INT)")

let seedValue = seedOption.value ?? ""
let genesisTransaction = try! SQLTransaction(statement: "SELECT '\(seedValue)';")
Log.debug("Genesis transaction is \(genesisTransaction.root.sql)")
let genesisPayload = SQLPayload(transactions: [genesisTransaction])

var genesisBlock = SQLBlock(index: 0, previous: Hash.zeroHash, payload: genesisPayload)
genesisBlock.mine(difficulty: 10)
Log.info("Genesis block=\(genesisBlock.debugDescription)) \(genesisBlock.isSignatureValid)")

var ledger = SQLLedger(genesis: genesisBlock, database: database)
let netPort = netPortOption.value ?? 8338
let node = Node<SQLBlock>(ledger: ledger, port: netPort)

// Add peers from command line
for p in peersOption.value ?? [] {
	if var u = URL(string: "ws://\(p)/") {
		node.add(peer: u)
	}
}

// Query server
let queryServerV4 = NodeQueryServer(node: node, port: queryPortOption.value ?? (netPort+1), family: .ipv4)
let queryServerV6 = NodeQueryServer(node: node, port: queryPortOption.value ?? (netPort+1), family: .ipv6)
queryServerV6.run()
queryServerV4.run()

node.miner.enabled = mineOption.value
node.start()

Log.info("Start submitting demo blocks")
do {
	var i = 0
	while true {
		i += 1
		let q = "INSERT INTO test (origin,x) VALUES ('\(node.uuid.uuidString)',\(i));"
		let payload = SQLPayload(transactions: [try SQLTransaction(statement: q)])
		Log.info("Submit \(q)")
		node.submit(payload: payload.data)
		sleep(10)
	}
}
catch {
	Log.error("\(error.localizedDescription)")
}
