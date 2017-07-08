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
let peerDatabaseFileOption = StringOption(longFlag: "peer-database", required: false, helpMessage: "Backing database file for peer database (default: catena-peers.sqlite)")

let cli = CommandLineKit.CommandLine()
cli.addOptions(databaseFileOption, helpOption, seedOption, netPortOption, queryPortOption, peersOption, mineOption, logOption, testOption, initializeOption, noReplayOption, peerDatabaseFileOption)

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

	// Set up peer database
	let peerDatabaseFile = peerDatabaseFileOption.value ?? "catena-peers.sqlite"
	if !peerDatabaseFile.isEmpty {
		let peerDatabase = SQLiteDatabase()
		try peerDatabase.open(peerDatabaseFile)
		let peerTable = try SQLPeerDatabase(database: peerDatabase, table: SQLTable(name: "_peers"))

		// Add peers from database
		for p in try peerTable.peers() {
			node.add(peer: p)
		}

		node.peerDatabase = peerTable
	}

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
	var rootCounter = 0
	let rootIdentity = try Identity()

	if initializeOption.value {
		// Generate root keypair

		Log.info("Root private key: \(rootIdentity.privateKey.stringValue)")
		Log.info("Root public key: \(rootIdentity.publicKey.stringValue)")
		Swift.print("\r\nPGPASSWORD=\(rootIdentity.privateKey.stringValue) psql -h localhost -p \(netPort+1) -U \(rootIdentity.publicKey.stringValue)\r\n")

		// Create grants table, etc.
		let create = SQLStatement.create(table: SQLTable(name: SQLMetadata.grantsTableName), schema: SQLGrants.schema)
		let createTransaction = try SQLTransaction(statement: create, invoker: rootIdentity.publicKey, counter: rootCounter)
		rootCounter += 1

		let grant = SQLStatement.insert(SQLInsert(
			orReplace: false,
			into: SQLTable(name: SQLMetadata.grantsTableName),
			columns: ["user", "kind", "table"].map { SQLColumn(name: $0) },
			values: [
				[.literalBlob(rootIdentity.publicKey.data.sha256), .literalString(SQLPrivilege.Kind.create.rawValue), .null],
				[.literalBlob(rootIdentity.publicKey.data.sha256), .literalString(SQLPrivilege.Kind.drop.rawValue), .null],
				[.literalBlob(rootIdentity.publicKey.data.sha256), .literalString(SQLPrivilege.Kind.insert.rawValue), .literalString(SQLMetadata.grantsTableName)],
				[.literalBlob(rootIdentity.publicKey.data.sha256), .literalString(SQLPrivilege.Kind.delete.rawValue), .literalString(SQLMetadata.grantsTableName)]
			]
		))
		let grantTransaction = try SQLTransaction(statement: grant, invoker: rootIdentity.publicKey, counter: rootCounter)
		rootCounter += 1

		try node.receive(transaction: try createTransaction.sign(with: rootIdentity.privateKey), from: nil)
		try node.receive(transaction: try grantTransaction.sign(with: rootIdentity.privateKey), from: nil)
	}

	// Start submitting test blocks if that's what the user requested
	if testOption.value {
		let identity = try Identity()

		node.start(blocking: false)
		let q = try SQLStatement("CREATE TABLE test (origin TEXT, x TEXT);");
		try node.receive(transaction: try SQLTransaction(statement: q, invoker: rootIdentity.publicKey, counter: rootCounter).sign(with: rootIdentity.privateKey), from: nil)
		rootCounter += 1

		// Grant to user
		let grant = SQLStatement.insert(SQLInsert(
			orReplace: false,
			into: SQLTable(name: SQLMetadata.grantsTableName),
			columns: ["user", "kind", "table"].map { SQLColumn(name: $0) },
			values: [
				[.literalBlob(identity.publicKey.data.sha256), .literalString(SQLPrivilege.Kind.insert.rawValue), .literalString("test")]
			]
		))
		try node.receive(transaction: try SQLTransaction(statement: grant, invoker: rootIdentity.publicKey, counter: rootCounter).sign(with: rootIdentity.privateKey), from: nil)
		rootCounter += 1

		sleep(10)

		Log.info("Start submitting demo blocks")
		var testCounter = 0
		do {
			var i = 0
			while true {
				i += 1
				let q = try SQLStatement("INSERT INTO test (origin,x) VALUES ('\(node.uuid.uuidString)',\(i));")
				let tr = try SQLTransaction(statement: q, invoker: identity.publicKey, counter: testCounter).sign(with: identity.privateKey)
				Log.info("[Test] submit \(tr)")
				try node.receive(transaction: tr, from: nil)
				testCounter += 1
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
