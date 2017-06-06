import Foundation
import Kitura
import CommandLineKit
import LoggerAPI
import HeliumLogger
import Ed25519
import Base58

let databaseFileOption = StringOption(shortFlag: "d", longFlag: "database", required: false, helpMessage: "Backing database file (default: catena.sqlite)")
let seedOption = StringOption(shortFlag: "s", longFlag: "seed", required: false, helpMessage: "Genesis block seed string (default: empty)")
let helpOption = BoolOption(shortFlag: "h", longFlag: "help", helpMessage: "Show usage")
let netPortOption = IntOption(shortFlag: "p", longFlag: "gossip-port", helpMessage: "Listen port for peer-to-peer communications (default: 8338)")
let queryPortOption = IntOption(shortFlag: "q", longFlag: "query-port", helpMessage: "Listen port for query communications (default: networking port + 1)")
let peersOption = MultiStringOption(shortFlag: "j", longFlag: "join", helpMessage: "Peer to connect to ('hostname:port' or just 'hostname')")
let mineOption = BoolOption(shortFlag: "m", longFlag: "mine", helpMessage: "Enable mining of blocks")
let logOption = StringOption(shortFlag: "v", longFlag: "log", helpMessage: "The log level: debug, verbose, info, warning (default: info)")
let testOption = BoolOption(shortFlag: "t", helpMessage: "Submit test queries to the chain periodically (default: off)")

let initializeOption = BoolOption(shortFlag: "i", helpMessage: "Generate transactions to initialize basic database structure (default: false)")

let cli = CommandLineKit.CommandLine()
cli.addOptions(databaseFileOption, helpOption, seedOption, netPortOption, queryPortOption, peersOption, mineOption, logOption, testOption, initializeOption)

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

// Generate genesis block
let seedValue = seedOption.value ?? ""
var genesisBlock = SQLBlock(genesisBlockWith: seedValue)
genesisBlock.mine(difficulty: 10)
Log.info("Genesis block=\(genesisBlock.debugDescription)) \(genesisBlock.isSignatureValid)")

var ledger = try! SQLLedger(genesis: genesisBlock, database: databaseFileOption.value ?? "catena.sqlite")
let netPort = netPortOption.value ?? 8338
let node = Node<SQLBlock>(ledger: ledger, port: netPort)

// Add peers from command line
for p in peersOption.value ?? [] {
	if let u = URL(string: "ws://\(p)/") {
		node.add(peer: u)
	}
}

// Query server
let queryServerV4 = NodeQueryServer(node: node, port: queryPortOption.value ?? (netPort+1), family: .ipv4)
let queryServerV6 = NodeQueryServer(node: node, port: queryPortOption.value ?? (netPort+1), family: .ipv6)
queryServerV6.run()
queryServerV4.run()

node.miner.isEnabled = mineOption.value

// Initialize database if we have to
if initializeOption.value {
	// Generate root keypair
	let identity = try! Identity()
	Log.info("Root private key: \(identity.privateKey.stringValue)")
	Log.info("Root public key: \(identity.publicKey.stringValue)")

	// Create grants table, etc.
}

// Start submitting test blocks if that's what the user requested
if testOption.value {
	let identity = try! Identity()

	node.start(blocking: false)
	let q = try! SQLStatement("CREATE TABLE test (origin TEXT, x TEXT);");
	Log.info("Submit \(q)")
	node.submit(transaction: try SQLTransaction(statement: q, invoker: identity.publicKey))

	Log.info("Start submitting demo blocks")
	do {
		var i = 0
		while true {
			i += 1
			let q = try! SQLStatement("INSERT INTO test (origin,x) VALUES ('\(node.uuid.uuidString)',\(i));")
			Log.info("Submit \(q)")
			node.submit(transaction: try SQLTransaction(statement: q, invoker: identity.publicKey).sign(with: identity.privateKey))
			sleep(10)
		}
	}
	catch {
		Log.error("\(error.localizedDescription)")
	}
}
else {
	node.start(blocking: true)
}
