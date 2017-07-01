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
let peersOption = MultiStringOption(shortFlag: "j", longFlag: "join", helpMessage: "Peer to connect to ('hostname:port' or just 'hostname')")
let mineOption = BoolOption(shortFlag: "m", longFlag: "mine", helpMessage: "Enable mining of blocks")
let logOption = StringOption(shortFlag: "v", longFlag: "log", helpMessage: "The log level: debug, verbose, info, warning (default: info)")
let testOption = BoolOption(shortFlag: "t", longFlag: "test", helpMessage: "Submit test queries to the chain periodically (default: off)")
let initializeOption = BoolOption(shortFlag: "i", longFlag: "initialize", helpMessage: "Generate transactions to initialize basic database structure (default: false)")
let noReplayOption = BoolOption(shortFlag: "n", longFlag: "no-replay", helpMessage: "Do not replay database operations, just participate and validate transactions (default: false)")

let cli = CommandLineKit.CommandLine()
cli.addOptions(databaseFileOption, helpOption, seedOption, netPortOption, queryPortOption, peersOption, mineOption, logOption, testOption, initializeOption, noReplayOption)

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
let databaseFile = databaseFileOption.value ?? "catena.sqlite"
let seedValue = seedOption.value ?? ""
var genesisBlock = SQLBlock(genesisBlockWith: seedValue)
genesisBlock.mine(difficulty: 10)
Log.info("Genesis block=\(genesisBlock.debugDescription)) \(genesisBlock.isSignatureValid)")

if initializeOption.value {
	_ = unlink(databaseFile.cString(using: .utf8)!)
}

do {
	let ledger = try SQLLedger(genesis: genesisBlock, database: databaseFile, replay: !noReplayOption.value)
	let netPort = netPortOption.value ?? 8338
	let node = Node<SQLBlockchain>(ledger: ledger, port: netPort)
	let _ = SQLAPIEndpoint(node: node, router: node.server.router)

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
		let identity = try Identity()
		Log.info("Root private key: \(identity.privateKey.stringValue)")
		Log.info("Root public key: \(identity.publicKey.stringValue)")
		Swift.print("\r\nPGPASSWORD=\(identity.privateKey.stringValue) psql -h localhost -p \(netPort+1) -U \(identity.publicKey.stringValue)\r\n")

		// Create grants table, etc.
		let create = SQLStatement.create(table: SQLTable(name: SQLMetadata.grantsTableName), schema: SQLGrants.schema)
		let createTransaction = try SQLTransaction(statement: create, invoker: identity.publicKey)

		let grant = SQLStatement.insert(SQLInsert(
			orReplace: false,
			into: SQLTable(name: SQLMetadata.grantsTableName),
			columns: ["user", "kind", "table"].map { SQLColumn(name: $0) },
			values: [
				[.literalBlob(identity.publicKey.data.sha256), .literalString(SQLPrivilege.Kind.create.rawValue), .null],
				[.literalBlob(identity.publicKey.data.sha256), .literalString(SQLPrivilege.Kind.drop.rawValue), .null],
				[.literalBlob(identity.publicKey.data.sha256), .literalString(SQLPrivilege.Kind.insert.rawValue), .literalString(SQLMetadata.grantsTableName)],
				[.literalBlob(identity.publicKey.data.sha256), .literalString(SQLPrivilege.Kind.delete.rawValue), .literalString(SQLMetadata.grantsTableName)]
			]
		))
		let grantTransaction = try SQLTransaction(statement: grant, invoker: identity.publicKey)

		try node.submit(transaction: try createTransaction.sign(with: identity.privateKey))
		try node.submit(transaction: try grantTransaction.sign(with: identity.privateKey))
	}

	// Start submitting test blocks if that's what the user requested
	if testOption.value {
		let identity = try Identity()

		node.start(blocking: false)
		let q = try SQLStatement("CREATE TABLE test (origin TEXT, x TEXT);");
		Log.info("Submit \(q)")
		try node.submit(transaction: try SQLTransaction(statement: q, invoker: identity.publicKey))

		Log.info("Start submitting demo blocks")
		do {
			var i = 0
			while true {
				i += 1
				let q = try SQLStatement("INSERT INTO test (origin,x) VALUES ('\(node.uuid.uuidString)',\(i));")
				Log.info("Submit \(q)")
				try node.submit(transaction: try SQLTransaction(statement: q, invoker: identity.publicKey).sign(with: identity.privateKey))
				sleep(10)
			}
		}
		catch {
			Log.error(error.localizedDescription)
		}
	}
	else {
		node.start(blocking: true)
	}
}
catch {
	Log.error(error.localizedDescription)
}
