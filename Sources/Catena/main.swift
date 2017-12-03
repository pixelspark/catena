import Foundation
import Dispatch
import Kitura
import CommandLineKit
import LoggerAPI
import HeliumLogger
import CatenaCore
import CatenaSQL
import class NetService.NetService
import protocol NetService.NetServiceDelegate
import class Socket.Socket

let databaseFileOption = StringOption(shortFlag: "d", longFlag: "database", required: false, helpMessage: "Backing database file (default: catena.sqlite)")
let memoryDatabaseFileOption = BoolOption(longFlag: "in-memory-database", helpMessage: "Use an in-memory (transient) database. Cannot be used with -d")
let seedOption = StringOption(shortFlag: "s", longFlag: "seed", required: false, helpMessage: "Genesis block seed string (default: empty)")
let helpOption = BoolOption(shortFlag: "h", longFlag: "help", helpMessage: "Show usage")
let netPortOption = IntOption(shortFlag: "p", longFlag: "gossip-port", helpMessage: "Listen port for peer-to-peer communications (default: 8338)")
let queryPortOption = IntOption(shortFlag: "q", longFlag: "query-port", helpMessage: "Listen port for query communications (default: networking port + 1)")
let peersOption = MultiStringOption(shortFlag: "j", longFlag: "join", helpMessage: "Peer URL to connect to ('ws://nodeid@hostname:port')")
let mineOption = BoolOption(shortFlag: "m", longFlag: "mine", helpMessage: "Enable mining of blocks")
let logOption = StringOption(shortFlag: "v", longFlag: "log", helpMessage: "The log level: debug, verbose, info, warning (default: info)")
let initializeOption = BoolOption(shortFlag: "i", longFlag: "initialize", helpMessage: "Perform all initialization steps, then exit before starting the node.")
let noReplayOption = BoolOption(shortFlag: "n", longFlag: "no-replay", helpMessage: "Do not replay database operations, just participate and validate transactions (default: false)")
let nodeDatabaseFileOption = StringOption(longFlag: "node-database", required: false, helpMessage: "Backing database file for instance database (default: catena-node.sqlite)")
let noLocalPeersOption = BoolOption(longFlag: "no-local-discovery", helpMessage: "Disable local peer discovery")
let noWebClient = BoolOption(longFlag: "no-web-client", helpMessage: "Disable serving of the web client")
let nodeUUIDOption = StringOption(longFlag: "node-uuid", required: false, helpMessage: "Set the node's UUID (default: a randomly generated UUID)")
let allowCorsDomains = StringOption(longFlag: "allow-domain", required: false, helpMessage: "Domains from which to allow HTTP API requests (set to '*' to allow all)")

let cli = CommandLineKit.CommandLine()
cli.addOptions(databaseFileOption, helpOption, seedOption, netPortOption, queryPortOption, peersOption, mineOption, logOption, initializeOption, noReplayOption, nodeDatabaseFileOption, memoryDatabaseFileOption, noLocalPeersOption, nodeUUIDOption, allowCorsDomains, noWebClient)

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

// Handle SIGTERM (this ensures we can cleanly exit when running under Docker)
signal(SIGTERM) { s in
	exit(0)
}

signal(SIGINT) { s in
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
if memoryDatabaseFileOption.value && databaseFileOption.value != nil {
	fatalError("The -dm and -d flags cannot be set at the same time.")
}

do {
	// Set up node database
	let nodeDatabaseFile = nodeDatabaseFileOption.value ?? "catena-node.sqlite"
	var peerTable: SQLPeerDatabase? = nil
	var configurationTable: SQLKeyValueTable? = nil
	if !nodeDatabaseFile.isEmpty {
		let nodeDatabase = SQLiteDatabase()
		try nodeDatabase.open(nodeDatabaseFile)
		peerTable = try SQLPeerDatabase(database: nodeDatabase, table: SQLTable(name: "peers"))
		configurationTable = try SQLKeyValueTable(database: nodeDatabase, table: SQLTable(name: "config"))
        
        if initializeOption.wasSet {
            try peerTable!.forgetAllPeers()
        }
	}

	// Initialize database if we have to
	let databaseFile = memoryDatabaseFileOption.value ? ":memory:" : (databaseFileOption.value ?? "catena.sqlite")

	// Determine genesis seed
	let seedValue: String
	if let sv = seedOption.value {
		seedValue = sv
		try configurationTable?.set(key: "genesisSeed", value: seedValue)
	}
	else if let storedSeed = try configurationTable?.get("genesisSeed") {
		seedValue = storedSeed
	}
	else {
		seedValue = ""
	}

	// Find genesis block
	var genesisBlock = try SQLBlock.genesis(seed: seedValue, version: 1)
	genesisBlock.mine(difficulty: 10)
	Log.info("Genesis seed=\(seedValue) block=\(genesisBlock.debugDescription)) \(genesisBlock.isSignatureValid)")

	// If the database is in a file and we are initializing or configuring, remove anything that was there before
	if initializeOption.value && !memoryDatabaseFileOption.value {
		_ = unlink(databaseFile.cString(using: .utf8)!)
	}

	// Obtain root identity from the node database (if available) and not initializing; otherwise generate one
	let rootIdentity: Identity
	if let pubString = try configurationTable?.get("publicKey"),
		let privString = try configurationTable?.get("privateKey"),
		let pubKey = PublicKey(string: pubString),
		let privKey = PrivateKey(string: privString), !initializeOption.value {
		rootIdentity = Identity(publicKey: pubKey, privateKey: privKey)
	}
	else {
		// Generate root identity
		rootIdentity = try Identity()
		try configurationTable?.set(key: "publicKey", value: rootIdentity.publicKey.stringValue)
		try configurationTable?.set(key: "privateKey", value: rootIdentity.privateKey.stringValue)

		if initializeOption.value {
			Log.info("Root private key: \(rootIdentity.privateKey.stringValue)")
			Log.info("Root public key: \(rootIdentity.publicKey.stringValue)")
			Log.info("Root identity: \(SHA256Hash(of: rootIdentity.publicKey.data).stringValue)")
		}
	}

	// Determine node UUID
	let uuid: UUID
	if let nu = nodeUUIDOption.value {
		if let nuuid = UUID(uuidString: nu) {
			uuid = nuuid
		}
		else {
			fatalError("Invalid value for --node-uuid option; needs to be a valid UUID")
		}
	}
	else if let uuidString = try configurationTable?.get("uuid"), let storedUUID = UUID(uuidString: uuidString) {
		uuid = storedUUID
	}
	else {
		uuid = UUID()
	}
	try configurationTable?.set(key: "uuid", value: uuid.uuidString)

	let ledger = try SQLLedger(genesis: genesisBlock, database: databaseFile, replay: !noReplayOption.value)
	let netPort = netPortOption.value ?? 8338
	let node = try Node<SQLLedger>(ledger: ledger, port: netPort, miner: SHA256Hash(of: rootIdentity.publicKey.data), uuid: uuid)
	let agent = SQLAgent(node: node)
    
    if(!noWebClient.wasSet) {
        let _ = SQLAPIEndpoint(agent: agent, router: node.server.router, allowCorsOrigin: allowCorsDomains.value)
    }

	// Add peers from database
	if let pd = peerTable {
		for p in try pd.peers() {
			node.add(peer: p)
		}
		node.peerDatabase = pd
	}

	// Add peers from command line
	for p in peersOption.value ?? [] {
		if let u = URL(string: p) {
			node.add(peer: u)
		}
	}

	// Query server
	if !initializeOption.value {
		let queryServerV4 = NodeQueryServer(agent: agent, port: queryPortOption.value ?? (netPort+1), family: .ipv4)
		let queryServerV6 = NodeQueryServer(agent: agent, port: queryPortOption.value ?? (netPort+1), family: .ipv6)
		queryServerV6.run()
		queryServerV4.run()

		node.miner.isEnabled = mineOption.value

		// Check whether local discovery should be enabled
		if !noLocalPeersOption.value {
			var hostname = Data(count: 255)
			let hostnameString = try hostname.withUnsafeMutableBytes { (bytes: UnsafeMutablePointer<CChar>) -> String in
				try posix(gethostname(bytes, 255))
				return String(cString: bytes)
			}

			if !hostnameString.hasSuffix(".local") {
				Log.info("[LocalDiscovery] Not enabling local discovery as host (\(hostnameString)) is not in local domain.")
			}
			else {
				node.announceLocally = true
				node.discoverLocally = true
			}
		}

		Log.info("Node URL: \(node.url.absoluteString)")
		Swift.print("\r\nPGPASSWORD=\(rootIdentity.privateKey.stringValue) psql -h localhost -p \(netPort+1) -U \(rootIdentity.publicKey.stringValue)\r\n")

		node.start(blocking: false)

		// Set up signal handler
		signal(SIGINT, SIG_IGN)

		let sigintSrc = DispatchSource.makeSignalSource(signal: SIGINT, queue: .main)
		sigintSrc.setEventHandler {
			exit(0)
		}
		sigintSrc.resume()

		// Run
		withExtendedLifetime(node) {
			RunLoop.main.run()
		}
	}
}
catch {
	Log.error(error.localizedDescription)
}
