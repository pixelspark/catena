import Foundation
import LoggerAPI
import CatenaCore

public class SQLLedger: Ledger {
	public typealias BlockchainType = SQLBlockchain
	public typealias ParametersType = SQLParameters

	public let mutex = Mutex()
	public let orphans = Orphans<SQLBlock>()
	public var longest: SQLBlockchain

	/** Instantiate an SQLLedger that tracks chains starting at the indicated genesis block and uses the indicated database
	file for storage. When`replay` is true, the ledger will process database transactions, whereas if it is false, the
	ledger will only validate transactions and participate in grant and other metadata processing. */
	public init(genesis: SQLBlock, database path: String, replay: Bool) throws {
		self.longest = try SQLBlockchain(genesis: genesis, database: path, replay: replay)
	}

	public func canAccept(transaction: SQLTransaction, pool: SQLBlock?) throws -> Eligibility {
		if !transaction.isSignatureValid {
			Log.info("[Ledger] cannot accept tx \(transaction): signature invalid")
			return .never
		}

		// Perhaps the transaction is appendable to another pooled transaction?
		if let p = pool {
			for tr in p.payload.transactions {
				if tr.invoker == transaction.invoker && (tr.counter + 1) == transaction.counter {
					return .now
				}
			}
		}

		/* A transaction is acceptable when its counter is one above the stored counter for the invoker key, or zero when
		the invoker has no counter yet (not executed any transactions before on this chain). */
		let counter = try self.mutex.locked { () -> SQLTransaction.CounterType? in
			return try self.longest.withUnverifiedTransactions { chain in
				return try chain.meta.users.counter(for: transaction.invoker)
			}
		}

		if let counter = counter {
			/* The transaction counter directly follows the counter in the ledger (but may conflict with another
			transaction in the pool). It is nevertheless acceptable. */
			if (counter + 1) == transaction.counter {
				return .now
			}
			else if (counter + 1) < transaction.counter {
				/* The counter is too far ahead of the current counter, so it is acceptable in the future if transactions
				appear that use the intermediate counter values. */
				return .future
			}
			else {
				/* The counter value is lower than what is currently in the ledger, so this transaction will never become
				acceptable. */
				return .never
			}
		}
		else {
			/* No transactions have been made by this invoker yet on the ledger, so this should be the first one (with
			counter=0) to be acceptable now. If the counter value is higher, it may become acceptable in the future. */
			if transaction.counter == 0 {
				return .now
			}
			else {
				return .future
			}
		}
	}
}

public struct SQLParameters: Parameters {
	public static let protocolVersion = "catena-v1"
}

public class SQLBlockchain: Blockchain {
	public typealias BlockType = SQLBlock

	public let genesis: SQLBlock
	public var highest: SQLBlock

	/** The SQL blockchain maintains a database (permanent) and a queue. When transactions are received, they are inserted
	in a queue. When this queue exceeds a certain size (`maxQueueSize`), the transactions are processed in the permanent
	database. If a chain splice/switch occurs that required rewinding to less than maxQueueSize blocks, this can be done
	efficiently by removing blocks from the queue. If the splice happens earlier, the full database needs to be rebuilt.*/
	private(set) var database: Database
	public private(set) var meta: SQLMetadata
	private let maxQueueSize = 7
	private var queue: [SQLBlock] = []
	private let mutex = Mutex()
	let replay: Bool

	/** Number of blocks after which a difficulty retarget is performed. An interval of 10 will determine
	the difficulty for block 11 on blocks 0...10. */
	let difficultyRetargetInterval: BlockType.IndexType = 10

	/** The desired average time between blocks in seconds. A retarget will occur on average every
	`desiredTimeBetweenBlocks` * `difficultyRetargetInterval`. */
	let desiredTimeBetweenBlocks: SQLBlock.TimestampType = 10

	/** Lower bound for the amount of work that is ever required to be performed per block. */
	let minimumAmountOfWorkPerBlock: SQLBlock.WorkType = 10

	/** Upper bound for the amount of work that is ever required to be performed per block. */
	let maximumAmountOfWorkPerBlock: SQLBlock.WorkType = 200

	let databasePath: String

	private init(genesis: SQLBlock, highest: SQLBlock, database: Database, meta: SQLMetadata, replay: Bool, databasePath: String) {
		self.genesis = genesis
		self.highest = highest
		self.database = database
		self.meta = meta
		self.replay = replay
		self.databasePath = databasePath
	}

	/** Instantiate an SQL blockchain that starts at the indicated genesis block and is stored in the indicated database
	file. When `replay` is true, the chain also processes database transactions. When it is false, only validation and
	metadata operations are performed. */
	init(genesis: SQLBlock, database path: String, replay: Bool) throws {
		let permDatabase = SQLiteDatabase()
		try permDatabase.open(path)

		self.replay = replay
		self.genesis = genesis
		self.highest = genesis
		self.databasePath = path
		self.database = permDatabase
		self.meta = try SQLMetadata(database: database)

		// Is this database already initialized? If so, check whether it replays
		if let databaseReplay = self.meta.isReplaying {
			if databaseReplay != replay {
				throw SQLMetadataError.replayMismatchError
			}
		}
		else {
			try self.meta.set(replaying: replay)
		}

		// Load chain from storage
		if let hh = self.meta.headHash {
			self.highest = try self.meta.get(block: hh)!
			Log.info("[SQLLedger] Get highest: \(self.highest.signature!.stringValue)")
		}
		else {
			try self.meta.database.transaction {
				try self.meta.archive(block: genesis)
				try self.meta.set(head: genesis.signature!, index: genesis.index)
			}
		}
	}

	public func get(block hash: BlockType.HashType) throws -> SQLBlock? {
		if let b =  try self.meta.get(block: hash) {
			return b
		}

		// Search queue
		for b in queue {
			if b.signature! == hash {
				return b
			}
		}

		return nil
	}

	public func get(at index: BlockType.IndexType) throws -> SQLBlock? {
		if let b =  try self.meta.archive.get(at: index) {
			return b
		}

		// Search queue
		for b in queue {
			if b.index == index {
				return b
			}
		}

		return nil
	}

	private func totalWork(between: CountableClosedRange<SQLBlock.IndexType>) throws -> SQLBlock.WorkType {
		var totalWork: SQLBlock.WorkType = try self.meta.archive.totalWork(between: between)

		for b in queue {
			if b.index >= between.lowerBound && b.index <= between.upperBound {
				totalWork += b.work
			}
		}

		return totalWork
	}

	func process(block: SQLBlock) throws {
		try self.mutex.locked {
			try block.apply(database: self.database, meta: self.meta, replay: self.replay)
		}
	}

	/** Returns the set of block indices for the blocks that are included in the difficulty calculation
	for a block *following* the indicated block. If the block has index 20 and interval is 5, then
	the range will be 15...20. */
	private func retargetingInterval(forBlockFollowing precedingBlock: SQLBlock) -> CountableClosedRange<SQLBlock.IndexType> {
		assert(difficultyRetargetInterval > self.maxQueueSize)
		let lastRetargetEnd = precedingBlock.index - (precedingBlock.index % difficultyRetargetInterval)

		if lastRetargetEnd < difficultyRetargetInterval {
			return 0...0
		}

		let lastRetargetStart = max(0, lastRetargetEnd - difficultyRetargetInterval)
		return lastRetargetStart...lastRetargetEnd
	}

	public func difficulty(forBlockFollowing precedingBlock: SQLBlock) throws -> SQLBlock.WorkType {
		return try self.mutex.locked {
			let retargetRange = self.retargetingInterval(forBlockFollowing: precedingBlock)
			let lowerBlock = try self.get(at: retargetRange.lowerBound)
			let upperBlock = try self.get(at: retargetRange.upperBound)

			let totalWork = try self.totalWork(between: retargetRange)
			let totalTime = Int(upperBlock!.timestamp - lowerBlock!.timestamp)
			if totalTime <= 0 {
				return self.genesis.work
			}

			let desiredTotalTime = Int(desiredTimeBetweenBlocks) * Int(retargetRange.count)
			let previousDifficulty = totalWork / SQLBlock.WorkType(retargetRange.count)

			if totalTime > desiredTotalTime {
				return min(maximumAmountOfWorkPerBlock, max(minimumAmountOfWorkPerBlock, previousDifficulty - 1))
			}
			else {
				return min(maximumAmountOfWorkPerBlock, max(minimumAmountOfWorkPerBlock, previousDifficulty + 1))
			}
		}
	}

	public func append(block: SQLBlock) throws -> Bool {
		return try self.mutex.locked {
			if try self.canAppend(block: block, to: self.highest) {
				self.queue.append(block)
				self.highest = block

				if self.queue.count > maxQueueSize {
					let promoted = self.queue.removeFirst()
					Log.debug("[SQLBlockchain] promoting block \(promoted.index) to permanent storage which is now at \(self.meta.headIndex!)")

					if (self.meta.headIndex! + 1) != promoted.index {
						Log.info("[SQLBlockchain] need to replay first to \(promoted.index-1)")
						let prev = try self.get(block: promoted.previous)!
						try self.replayPermanentStorage(to: prev)
					}
					try self.process(block: promoted)
					Log.debug("[SQLBlockchain] promoted block \(promoted.index) to permanent storage; qs=\(self.queue.count)")
				}

				return true
			}
			return false
		}
	}

	public func unwind(to: SQLBlock) throws {
		do {
			try self.mutex.locked {
				Log.info("[SQLBlockchain] Unwind from #\(self.highest.index) to #\(to.index)")

				if self.meta.headIndex! <= to.index {
					// Unwinding within queue
					self.queue = self.queue.filter { return $0.index <= to.index }
					self.highest = to
					Log.debug("[SQLBlockchain] Permanent is at \(self.meta.headIndex!), replayed up to+including \(to.index), queue size is \(self.queue.count)")
				}
				else {
					// To-block is earlier than the head of permanent storage. Need to replay the full chain
					Log.info("[SQLBlockchain] Unwind requires a replay of the full chain, because target block (\(to.index)) << head of permanent history (\(self.meta.headIndex!)) ")
					try self.replayPermanentStorage(to: to)
				}

				// Right now we should be at the desired point
				assert(self.highest == to)
			}
		}
		catch {
			fatalError("[SQLBlockchain] unwind error: \(error.localizedDescription)")
		}
	}

	private func replayPermanentStorage(to: SQLBlock) throws {
		try self.mutex.locked {
			// Find blocks to be replayed
			// TODO: refactor so we can just walk the chain from old to new without creating a giant array
			var replay: [SQLBlock] = []
			var current = to
			repeat {
				replay.append(current)

				if current.index != 0 {
					current = try self.get(block: current.previous)!
				}
				else {
					break
				}
			} while true

			// Empty the queue
			self.queue = []

			// Remove database
			self.database.close()
			if self.databasePath != ":memory:" {
				let e = self.databasePath.withCString { cs -> Int32 in
					return unlink(cs)
				}

				if e != 0 {
					fatalError("[SQLLedger] Could not delete permanent database; err=\(e)")
				}
			}

			// Create new database
			let db = SQLiteDatabase()
			try db.open(self.databasePath)
			self.database = db
			self.meta = try SQLMetadata(database: self.database)


			// Replay blocks
			try self.database.transaction {
				for block in replay.reversed() {
					try self.process(block: block)
					self.highest = block
				}
				Log.info("[SQLLedger] replay on permanent storage is complete")
			}

			assert(self.highest == to, "replay should end up at desired block")
		}
	}

	public func withUnverifiedTransactions<T>(_ block: @escaping ((SQLBlockchain) throws -> (T))) rethrows -> T {
		return try self.mutex.locked {
			return try self.database.hypothetical {
				// Replay queued blocks
				for block in self.queue {
					try block.apply(database: self.database, meta: self.meta, replay: self.replay)
				}

				let unverifiedChain = SQLBlockchain(genesis: self.genesis, highest: self.highest, database: self.database, meta: self.meta, replay: self.replay, databasePath: self.databasePath)
				return try block(unverifiedChain)
			}
		}
	}
}

public class SQLUsersTable {
	let database: Database
	let table: SQLTable

	private let userColumn = SQLColumn(name: "user")
	private let counterColumn = SQLColumn(name: "counter")

	init(database: Database, table: SQLTable) throws {
		self.database = database
		self.table = table

		// Ensure the table exists
		try database.transaction {
			if !(try database.exists(table: self.table.name)) {
				var cols = OrderedDictionary<SQLColumn, SQLType>()
				cols.append(.blob, forKey: self.userColumn)
				cols.append(.int, forKey: self.counterColumn)
				let createStatement = SQLStatement.create(table: self.table, schema: SQLSchema(
					columns: cols,
					primaryKey: self.userColumn
				))
				try _ = self.database.perform(createStatement.sql(dialect: self.database.dialect))
			}
		}
	}

	public func counter(for key: PublicKey) throws -> SQLTransaction.CounterType? {
		let selectStatement = SQLStatement.select(SQLSelect(
			these: [.column(self.counterColumn)],
			from: self.table,
			joins: [],
			where: SQLExpression.binary(.column(self.userColumn), .equals, .literalBlob(key.data.sha256)),
			distinct: false,
			orders: []
		))

		let r = try self.database.perform(selectStatement.sql(dialect: self.database.dialect))
		if r.hasRow, case .int(let value) = r.values[0] {
			return SQLTransaction.CounterType(value)
		}
		return nil
	}

	func setCounter(for key: PublicKey, to: SQLTransaction.CounterType) throws {
		let insertStatement = SQLStatement.insert(SQLInsert(
			orReplace: true,
			into: self.table,
			columns: [self.userColumn, self.counterColumn],
			values: [[SQLExpression.literalBlob(key.data.sha256), SQLExpression.literalInteger(Int(to))]]
		))

		try _ = self.database.perform(insertStatement.sql(dialect: self.database.dialect))
	}

	public func counters() throws -> [Data: Int] {
		let selectStatement = SQLStatement.select(SQLSelect(
			these: [.column(self.userColumn), .column(self.counterColumn)],
			from: self.table,
			joins: [],
			where: nil,
			distinct: false,
			orders: []
		))

		let r = try self.database.perform(selectStatement.sql(dialect: self.database.dialect))
		var data: [Data: Int] = [:]

		while r.hasRow {
			if case .int(let counter) = r.values[1], case .blob(let user) = r.values[0] {
				data[user] = counter
			}
			r.step()
		}

		return data
	}
}

public class SQLKeyValueTable {
	let database: Database
	let table: SQLTable

	private let keyColumn = SQLColumn(name: "key")
	private let valueColumn = SQLColumn(name: "value")

	public init(database: Database, table: SQLTable) throws {
		self.database = database
		self.table = table

		// Ensure the table exists
		try database.transaction {
			if !(try database.exists(table: self.table.name)) {
				var cols = OrderedDictionary<SQLColumn, SQLType>()
				cols.append(.text, forKey: self.keyColumn)
				cols.append(.text, forKey: self.valueColumn)
				let createStatement = SQLStatement.create(table: self.table, schema: SQLSchema(
					columns: cols,
					primaryKey: self.keyColumn))
				try _ = self.database.perform(createStatement.sql(dialect: self.database.dialect))
			}
		}
	}

	public func get(_ key: String) throws -> String? {
		return try database.transaction {
			let selectStatement = SQLStatement.select(SQLSelect(
				these: [.column(self.valueColumn)],
				from: self.table,
				joins: [],
				where: SQLExpression.binary(.column(self.keyColumn), .equals, .literalString(key)),
				distinct: false,
				orders: []
			))

			let r = try self.database.perform(selectStatement.sql(dialect: self.database.dialect))
			if r.hasRow, case .text(let value) = r.values[0] {
				return value
			}
			return nil
		}
	}

	public func set(key: String, value: String) throws {
		try database.transaction {
			let insertStatement = SQLStatement.insert(SQLInsert(
				orReplace: true,
				into: self.table,
				columns: [self.keyColumn, self.valueColumn],
				values: [[SQLExpression.literalString(key), SQLExpression.literalString(value)]]
			))
			try _ = self.database.perform(insertStatement.sql(dialect: self.database.dialect))
		}
	}
}

class SQLBlockArchive {
	let database: Database
	let table: SQLTable

	init(table: SQLTable, database: Database) throws {
		self.table = table
		self.database = database

		// This is a new file?
		if !(try database.exists(table: self.table.name)) {
			// Create block table
			try self.database.transaction(name: "init-block-archive") {
				// Version, nonce and timestamp need to be stored as blob because SQLite does not fully support Uint64s
				var cols = OrderedDictionary<SQLColumn, SQLType>()
				cols.append(SQLType.blob, forKey: SQLColumn(name: "signature"))
				cols.append(SQLType.blob, forKey: SQLColumn(name: "version"))
				cols.append(SQLType.int, forKey: SQLColumn(name: "index"))
				cols.append(SQLType.blob, forKey: SQLColumn(name: "nonce"))
				cols.append(SQLType.blob, forKey: SQLColumn(name: "previous"))
				cols.append(SQLType.blob, forKey: SQLColumn(name: "timestamp"))
				cols.append(SQLType.blob, forKey: SQLColumn(name: "miner"))
				cols.append(SQLType.blob, forKey: SQLColumn(name: "payload"))
				cols.append(SQLType.int, forKey: SQLColumn(name: "work"))

				let createStatement = SQLStatement.create(table: table, schema: SQLSchema(columns: cols, primaryKey: SQLColumn(name: "signature")))
				_ = try self.database.perform(createStatement.sql(dialect: self.database.dialect))

				let createIndexStatement = SQLStatement.createIndex(table: table, index: SQLIndex(name: SQLIndexName(name: "idx_index"), on: OrderedSet<SQLColumn>([
					SQLColumn(name: "index")
				]), unique: true))
				_ = try self.database.perform(createIndexStatement.sql(dialect: self.database.dialect))
			}
		}
	}

	func archive(block: SQLBlock) throws {
		var nonce = block.nonce
		var ts = block.timestamp
		var version = block.version

		let insertStatement = SQLStatement.insert(SQLInsert(
			orReplace: false,
			into: self.table,
			columns: ["signature", "index", "timestamp", "nonce", "previous", "payload", "version", "miner", "work"].map(SQLColumn.init),
			values: [[
				.literalBlob(block.signature!.hash),
				.literalInteger(Int(block.index)),
				.literalBlob(Data(bytes: &ts, count: MemoryLayout<SQLBlock.TimestampType>.size)),
				.literalBlob(Data(bytes: &nonce, count: MemoryLayout<SQLBlock.NonceType>.size)),
				.literalBlob(block.previous.hash),
				.literalBlob(block.payloadData),
				.literalBlob(Data(bytes: &version, count: MemoryLayout<SQLBlock.VersionType>.size)),
				.literalBlob(block.miner.hash),
				.literalInteger(Int(block.signature!.difficulty))
			]]))
		_ = try database.perform(insertStatement.sql(dialect: database.dialect))
	}

	func remove(block hash: SQLBlock.HashType) throws {
		let stmt = SQLStatement.delete(from: self.table, where: SQLExpression.binary(SQLExpression.column(SQLColumn(name: "signature")), .equals, .literalBlob(hash.hash)))
		_ = try self.database.perform(stmt.sql(dialect: self.database.dialect))
	}

	func totalWork(between: CountableClosedRange<SQLBlock.IndexType>) throws -> SQLBlock.WorkType {
		let stmt = SQLStatement.select(SQLSelect(
			these: [SQLExpression.call(SQLFunction(name: "SUM"), parameters: [SQLExpression.column(SQLColumn(name: "work"))])],
			from: self.table,
			joins: [],
			where: SQLExpression.binary(
				SQLExpression.binary(SQLExpression.column(SQLColumn(name: "index")), .greaterThanOrEqual, .literalUnsigned(UInt(between.lowerBound))),
				SQLBinary.and,
				SQLExpression.binary(SQLExpression.column(SQLColumn(name: "index")), .lessThanOrEqual, .literalUnsigned(UInt(between.upperBound)))
			),
			distinct: false
		))

		let res = try self.database.perform(stmt.sql(dialect: self.database.dialect))
		if res.hasRow, case .int(let work) = res.values[0] {
			return SQLBlock.WorkType(work)
		}
		else {
			throw SQLBlockError.metadataError
		}
	}

	func get(at index: SQLBlock.IndexType) throws -> SQLBlock? {
		return try get(having: SQLExpression.binary(SQLExpression.column(SQLColumn(name: "index")), .equals, .literalUnsigned(UInt(index))))
	}

	func get(block hash: SQLBlock.HashType) throws -> SQLBlock? {
		let b = try get(having: SQLExpression.binary(SQLExpression.column(SQLColumn(name: "signature")), .equals, .literalBlob(hash.hash)))
		assert(b == nil || b!.signature == hash, "signature from archive doesn't match?!")
		return b
	}

	private func get(having: SQLExpression) throws -> SQLBlock? {
		let stmt = SQLStatement.select(SQLSelect(
			these: ["signature", "index", "nonce", "previous", "payload", "timestamp", "version", "miner"].map { return SQLExpression.column(SQLColumn(name: $0)) },
			from: self.table,
			joins: [],
			where: having,
			distinct: false
		))

		let res = try self.database.perform(stmt.sql(dialect: self.database.dialect))
		if res.hasRow,
			case .int(let index) = res.values[1],
			case .blob(let nonce) = res.values[2],
			case .blob(let previousData) = res.values[3],
			case .blob(let timestamp) = res.values[5],
			case .blob(let version) = res.values[6],
			case .blob(let minerData) = res.values[7],
			case .blob(let signatureData) = res.values[0],
			version.count == MemoryLayout<SQLBlock.VersionType>.size,
			nonce.count == MemoryLayout<SQLBlock.NonceType>.size {
			let signature = try SQLBlock.HashType(hash: signatureData)
			let previous = try SQLBlock.HashType(hash: previousData)
			let miner = try SQLBlock.HashType(hash: minerData)
			assert(index >= 0, "Index must be positive")

			var nonceValue: SQLBlock.NonceType = 0
			let buffer = UnsafeMutableBufferPointer(start: &nonceValue, count: 1)
			guard nonce.copyBytes(to: buffer) == MemoryLayout<SQLBlock.NonceType>.size else {
				throw SQLBlockError.metadataError
			}

			var versionValue: SQLBlock.VersionType = 0
			let versionBuffer = UnsafeMutableBufferPointer(start: &versionValue, count: 1)
			guard version.copyBytes(to: versionBuffer) == MemoryLayout<SQLBlock.VersionType>.size else {
				throw SQLBlockError.metadataError
			}

			var tsValue: SQLBlock.TimestampType = 0
			let tsBuffer = UnsafeMutableBufferPointer(start: &tsValue, count: 1)
			guard timestamp.copyBytes(to: tsBuffer) == MemoryLayout<SQLBlock.TimestampType>.size else {
				throw SQLBlockError.metadataError
			}

			// Payload can be null or block
			let payload: Data
			if case .blob(let payloadData) = res.values[4] {
				payload = payloadData
			}
			else if case .null = res.values[4] {
				payload = Data()
			}
			else {
				fatalError("invalid payload")
			}

			var block = try SQLBlock(version: versionValue, index: SQLBlock.IndexType(index), nonce: nonceValue, previous: previous, miner: miner, timestamp: tsValue, payload: payload)
			block.signature = signature
			assert(block.isSignatureValid, "persisted block signature is invalid! \(block)")
			return block
		}

		assert(!res.hasRow, "invalid block data from storage")
		return nil
	}
}

public class SQLPeerDatabase: PeerDatabase {
	let database: Database
	let table: SQLTable
	let uuidColumn = SQLColumn(name: "uuid")
	let urlColumn = SQLColumn(name: "url")

	public init(database: Database, table: SQLTable) throws {
		self.database = database
		self.table = table

		if try !self.database.exists(table: table.name) {
			let create = SQLStatement.create(table: self.table, schema: SQLSchema(primaryKey: uuidColumn, columns:
				(uuidColumn, .text),
				(urlColumn, .text)
			))
			try _ = self.database.perform(create.sql(dialect: self.database.dialect))
		}
	}

	public func rememberPeer(url: URL) throws {
		let uuid = UUID(uuidString: url.user!)!

		let insert = SQLStatement.insert(SQLInsert(orReplace: true, into: self.table, columns: [uuidColumn, urlColumn], values: [[
			SQLExpression.literalString(uuid.uuidString),
			SQLExpression.literalString(url.absoluteString)
		]]))
		try _ = self.database.perform(insert.sql(dialect: self.database.dialect))
	}
    
    public func forgetAllPeers() throws {
        let delete = SQLStatement.delete(from: self.table, where: nil)
        try _ = self.database.perform(delete.sql(dialect: self.database.dialect))
    }

	public func forgetPeer(uuid: UUID) throws {
		let delete = SQLStatement.delete(from: self.table, where: SQLExpression.binary(
			SQLExpression.column(self.uuidColumn), .equals, SQLExpression.literalString(uuid.uuidString)
		))
		try _ = self.database.perform(delete.sql(dialect: self.database.dialect))
	}

	public func peers() throws -> [URL] {
		let select = SQLStatement.select(SQLSelect(these: [SQLExpression.column(self.urlColumn)], from: self.table, joins: [], where: nil, distinct: false, orders: []))
		let res = try self.database.perform(select.sql(dialect: self.database.dialect))
		var urls: [URL] = []
		while res.hasRow {
			if case .text(let urlString) = res.values[0], let u = URL(string: urlString) {
				urls.append(u)
			}
			res.step()
		}

		return urls
	}
}

public struct SQLMetadata {
	public static let grantsTableName = "grants"
	static let infoTableName = "_info"
	static let blocksTableName = "_blocks"
	static let usersTableName = "_users"

	/** All tables maintained for metadata that are visible to chain queries. */
	static let specialVisibleTables = [grantsTableName]

	/** All tables that are maintained for metadata, but invisible to chain queries. */
	static let specialInvisibleTables = [infoTableName, blocksTableName, usersTableName]

	let info: SQLKeyValueTable
	public let grants: SQLGrants
	public let users: SQLUsersTable
	let database: Database
	internal let archive: SQLBlockArchive

	private let infoHeadHashKey = "head"
	private let infoHeadIndexKey = "index"
	private let infoReplayingKey = "replaying"
	private let enforcingGrantsKey = "enforcingGrants"

	private let infoTrueValue = "true"
	private let infoFalseValue = "false"

	public init(database: Database) throws {
		self.database = database
		self.info = try SQLKeyValueTable(database: database, table: SQLTable(name: SQLMetadata.infoTableName))
		self.archive = try SQLBlockArchive(table: SQLTable(name: SQLMetadata.blocksTableName), database: database)
		self.grants = try SQLGrants(database: database, table: SQLTable(name: SQLMetadata.grantsTableName))
		self.users = try SQLUsersTable(database: database, table: SQLTable(name: SQLMetadata.usersTableName))
	}

	var headHash: SQLBlock.HashType? {
		do {
			if let h = try self.info.get(self.infoHeadHashKey) {
				return try SQLBlock.HashType(hash: h)
			}
			return nil
		}
		catch {
			return nil
		}
	}

	var headIndex: SQLBlock.IndexType? {
		do {
			if let h = try self.info.get(self.infoHeadIndexKey), let hi = UInt(h) {
				return SQLBlock.IndexType(hi)
			}
			return nil
		}
		catch {
			return nil
		}
	}

	func set(head: SQLBlock.HashType, index: SQLBlock.IndexType) throws {
		try self.database.transaction(name: "metadata-set-\(index)-\(head.stringValue)") {
			try self.info.set(key: self.infoHeadHashKey, value: head.stringValue)
			try self.info.set(key: self.infoHeadIndexKey, value: String(index))
		}
	}

	func set(enforcingGrants: Bool) throws {
		try self.database.transaction(name: "metadata-set-enforcing") {
			try self.info.set(key: self.enforcingGrantsKey, value: enforcingGrants ? self.infoTrueValue : self.infoFalseValue)
		}
	}

	var isEnforcingGrants: Bool {
		do {
			if let r = try self.info.get(self.enforcingGrantsKey) {
				return r == self.infoTrueValue
			}
			return false
		}
		catch {
			return false
		}
	}

	func set(replaying: Bool) throws {
		try self.database.transaction(name: "metadata-set-replay-\(index)") {
			try self.info.set(key: self.infoReplayingKey, value: replaying ? self.infoTrueValue : self.infoFalseValue)
		}
	}

	var isReplaying: Bool? {
		do {
			if let r = try self.info.get(self.infoReplayingKey) {
				return r == self.infoTrueValue
			}
			return nil
		}
		catch {
			return nil
		}
	}

	func archive(block: SQLBlock) throws {
		try self.archive.archive(block: block)
	}

	func remove(block hash: SQLBlock.HashType) throws {
		try self.archive.remove(block: hash)
	}

	func get(block hash: SQLBlock.HashType) throws -> SQLBlock? {
		return try self.archive.get(block: hash)
	}
}

enum SQLMetadataError: LocalizedError {
	case replayMismatchError

	var errorDescription: String? {
		switch self {
		case .replayMismatchError: return "the persisted database was created with a different replay setting."
		}
	}
}

enum SQLBlockError: LocalizedError {
	case metadataError
	case inconsecutiveBlockError
	case blockSignatureError
	case payloadSignatureError
	case tooManyTransactionsInBlockError

	var errorDescription: String? {
		switch self {
		case .metadataError: return "metadata error"
		case .inconsecutiveBlockError: return "inconsecutive block error"
		case .blockSignatureError: return "block signature error"
		case .payloadSignatureError: return "payload signature error"
		case .tooManyTransactionsInBlockError: return "too many transactions in a block"
		}
	}
}
