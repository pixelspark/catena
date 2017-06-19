import Foundation
import LoggerAPI

class SQLBlockchain: Blockchain {
	typealias BlockType = SQLBlock

	let genesis: SQLBlock
	var highest: SQLBlock
	var database: Database

	/** The SQL blockchain maintains a database (permanent) and a queue. When transactions are received, they are inserted
	in a queue. When this queue exceeds a certain size (`maxQueueSize`), the transactions are processed in the permanent
	database. If a chain splice/switch occurs that required rewinding to less than maxQueueSize blocks, this can be done
	efficiently by removing blocks from the queue. If the splice happens earlier, the full database needs to be rebuilt.*/
	private let maxQueueSize = 7
	private var queue: [SQLBlock] = []
	private let mutex = Mutex()
	let meta: SQLMetadata

	let databasePath: String

	init(genesis: SQLBlock, database path: String) throws {
		let permDatabase = SQLiteDatabase()
		try permDatabase.open(path)

		self.genesis = genesis
		self.highest = genesis
		self.databasePath = path
		self.database = permDatabase
		self.meta = try SQLMetadata(database: database)

		if let hh = self.meta.headHash {
			self.highest = try self.meta.get(block: hh)!
			Log.info("Get highest: \(self.highest.signature!.stringValue)")
		}
		else {
			try self.meta.database.transaction {
				try self.meta.archive(block: genesis)
				try self.meta.set(head: genesis.signature!, index: genesis.index)
			}
		}
	}

	func get(block hash: Hash) throws -> SQLBlock? {
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

	func process(block: SQLBlock) throws {
		try self.mutex.locked {
			try block.apply(database: self.database, meta: self.meta)
		}
	}

	var difficulty: Int {
		// TODO: this should be made dynamic. Can potentially store required difficulty in SQL (info table)?
		return self.genesis.signature!.difficulty
	}

	func append(block: SQLBlock) throws -> Bool {
		return try self.mutex.locked {
			// Check if block can be appended
			if block.previous == self.highest.signature! && block.index == (self.highest.index + 1) && block.isSignatureValid && block.signature!.difficulty >= self.difficulty {
				self.queue.append(block)
				self.highest = block

				if self.queue.count > maxQueueSize {
					let promoted = self.queue.removeFirst()
					Log.info("[SQLBlockchain] promoting block \(promoted.index) to permanent storage which is now at \(self.meta.headIndex!)")

					if (self.meta.headIndex! + 1) != promoted.index {
						Log.info("[SQLBlockchain] need to replay first to \(promoted.index-1)")
						let prev = try self.get(block: promoted.previous)!
						try self.replayPermanentStorage(to: prev)
					}
					try self.process(block: promoted)
					Log.info("[SQLBlockchain] promoted block \(promoted.index) to permanent storage; qs=\(self.queue.count)")
				}

				return true
			}
			else {
				return false
			}
		}
	}

	func unwind(to: SQLBlock) throws {
		do {
			try self.mutex.locked {
				Log.info("[SQLBlockchain] Unwind from #\(self.highest.index) to #\(to.index)")

				if self.meta.headIndex! <= to.index {
					// Unwinding within queue
					self.queue = self.queue.filter { return $0.index <= to.index }
					Log.info("[SQLBlockchain] Permanent is at \(self.meta.headIndex!), replayed up to+including \(to.index)")
				}
				else {
					// To-block is earlier than the head of permanent storage. Need to replay the full chain
					Log.info("[SQLBlockchain] Unwind requires a replay of the full chain, because target block (\(to.index)) << head of permanent history (\(self.meta.headIndex!)) ")
					try self.replayPermanentStorage(to: to)
				}
			}
		}
		catch {
			fatalError("[SQLLedger] unwind error: \(error.localizedDescription)")
		}
	}

	private func replayPermanentStorage(to: SQLBlock) throws {
		try self.mutex.locked {
			// Find blocks to be replayed
			// TODO: refactor so we can just walk the chain from old to new without creating a giant array
			var replay: [SQLBlock] = []
			var current = to
			while current.index != 0 {
				replay.append(current)
				current = try self.get(block: current.previous)!
			}

			// Remove database
			self.database.close()
			let e = self.databasePath.withCString { cs -> Int32 in
				return unlink(cs)
			}

			if e != 0 {
				fatalError("[SQLLedger] Could not delete permanent database; err=\(e)")
			}

			// Create new database
			let db = SQLiteDatabase()
			try db.open(self.databasePath)
			self.database = db

			// Replay blocks
			try self.database.transaction {
				for block in replay.reversed() {
					try self.process(block: block)
				}
				Log.info("[SQLLedger] replay on permanent storage is complete")
			}
		}
	}

	func withUnverifiedTransactions<T>(_ block: ((Database) throws -> (T))) rethrows -> T {
		// FIXME use a separate database connection!
		return try self.mutex.locked {
			return try self.database.hypothetical {
				// Replay queued blocks
				for block in self.queue {
					try block.apply(database: self.database, meta: self.meta)
				}

				return try block(self.database)
			}
		}
	}
}

class SQLKeyValueTable {
	let database: Database
	let table: SQLTable

	private let keyColumn = SQLColumn(name: "key")
	private let valueColumn = SQLColumn(name: "value")

	init(database: Database, table: SQLTable) throws {
		self.database = database
		self.table = table

		// Ensure the table exists
		try database.transaction {
			// TODO this is SQLite-specific
			if !(try database.exists(table: self.table.name)) {
				var cols = OrderedDictionary<SQLColumn, SQLType>()
				cols.append(.text, forKey: keyColumn)
				cols.append(.text, forKey: valueColumn)
				let createStatement = SQLStatement.create(table: self.table, schema: SQLSchema(
					columns: cols,
					primaryKey: self.keyColumn))
				try _ = self.database.perform(createStatement.sql(dialect: self.database.dialect))
			}
		}
	}

	func get(_ key: String) throws -> String? {
		let selectStatement = SQLStatement.select(SQLSelect(
			these: [.column(self.valueColumn)],
			from: self.table,
			joins: [],
			where: SQLExpression.binary(.column(self.keyColumn), .equals, .literalString(key)),
			distinct: false
		))

		let r = try self.database.perform(selectStatement.sql(dialect: self.database.dialect))
		if r.hasRow, case .text(let value) = r.values[0] {
			return value
		}
		return nil
	}

	func set(key: String, value: String) throws {
		let insertStatement = SQLStatement.insert(SQLInsert(
			orReplace: true,
			into: self.table,
			columns: [self.keyColumn, self.valueColumn],
			values: [[SQLExpression.literalString(key), SQLExpression.literalString(value)]]
		))
		try _ = self.database.perform(insertStatement.sql(dialect: self.database.dialect))
	}
}

struct SQLBlockArchive {
	let database: Database
	let table: SQLTable

	init(table: SQLTable, database: Database) throws {
		self.table = table
		self.database = database

		// This is a new file?
		if !(try database.exists(table: self.table.name)) {
			// Create block table
			try self.database.transaction(name: "init-block-archive") {
				var cols = OrderedDictionary<SQLColumn, SQLType>()
				cols.append(SQLType.blob, forKey: SQLColumn(name: "signature"))
				cols.append(SQLType.int, forKey: SQLColumn(name: "index"))
				cols.append(SQLType.int, forKey: SQLColumn(name: "nonce"))
				cols.append(SQLType.blob, forKey: SQLColumn(name: "previous"))
				cols.append(SQLType.blob, forKey: SQLColumn(name: "payload"))

				let createStatement = SQLStatement.create(table: self.table, schema: SQLSchema(columns: cols, primaryKey: SQLColumn(name: "signature")))
				_ = try self.database.perform(createStatement.sql(dialect: self.database.dialect))
			}
		}
	}

	func archive(block: SQLBlock) throws {
		let insertStatement = SQLStatement.insert(SQLInsert(
			orReplace: false,
			into: self.table,
			columns: ["signature", "index", "nonce", "previous", "payload"].map(SQLColumn.init),
			values: [[
				.literalBlob(block.signature!.hash),
				.literalInteger(Int(block.index)),
				.literalInteger(Int(block.nonce)),
				.literalBlob(block.previous.hash),
				.literalBlob(block.payloadData)
				]]))
		_ = try database.perform(insertStatement.sql(dialect: database.dialect))
	}

	func remove(block hash: Hash) throws {
		let stmt = SQLStatement.delete(from: self.table, where: SQLExpression.binary(SQLExpression.column(SQLColumn(name: "signature")), .equals, .literalBlob(hash.hash)))
		_ = try self.database.perform(stmt.sql(dialect: self.database.dialect))
	}

	func get(block hash: Hash) throws -> SQLBlock? {
		let stmt = SQLStatement.select(SQLSelect(
			these: ["signature", "index", "nonce", "previous", "payload"].map { return SQLExpression.column(SQLColumn(name: $0)) },
			from: self.table,
			joins: [],
			where: SQLExpression.binary(SQLExpression.column(SQLColumn(name: "signature")), .equals, .literalBlob(hash.hash)),
			distinct: false
		))

		let res = try self.database.perform(stmt.sql(dialect: self.database.dialect))
		if res.hasRow,
			case .int(let index) = res.values[1],
			case .int(let nonce) = res.values[2],
			case .blob(let previousData) = res.values[3] {
			let previous = Hash(previousData)
			assert(index >= 0, "Index must be positive")

			// Payload can be null or block
			var block: SQLBlock
			if case .blob(let payloadData) = res.values[4] {
				block = try SQLBlock(index: UInt(index), previous: previous, payload: payloadData)
			}
			else if case .null = res.values[4] {
				block = try SQLBlock(index: UInt(index), previous: previous, payload: Data())
			}
			else {
				fatalError("invalid payload")
			}
			block.nonce = UInt(nonce)
			block.signature = hash
			assert(block.isSignatureValid, "persisted block signature is invalid! \(block)")
			return block
		}
		return nil
	}
}

struct SQLMetadata {
	static let grantsTableName = "grants"
	static let infoTableName = "_info"
	static let blocksTableName = "_blocks"

	let info: SQLKeyValueTable
	let grants: SQLGrants
	let database: Database
	private let archive: SQLBlockArchive

	private let infoHeadHashKey = "head"
	private let infoHeadIndexKey = "index"

	init(database: Database) throws {
		self.database = database
		self.info = try SQLKeyValueTable(database: database, table: SQLTable(name: SQLMetadata.infoTableName))
		self.archive = try SQLBlockArchive(table: SQLTable(name: SQLMetadata.blocksTableName), database: database)
		self.grants = try SQLGrants(database: database, table: SQLTable(name: SQLMetadata.grantsTableName))
	}

	var headHash: Hash? {
		do {
			if let h = try self.info.get(self.infoHeadHashKey) {
				return Hash(string: h)
			}
			return nil
		}
		catch {
			return nil
		}
	}

	var headIndex: UInt? {
		do {
			if let h = try self.info.get(self.infoHeadIndexKey) {
				return UInt(h)
			}
			return nil
		}
		catch {
			return nil
		}
	}

	func set(head: Hash, index: UInt) throws {
		try self.database.transaction(name: "metadata-set-\(index)-\(head.stringValue)") {
			try self.info.set(key: infoHeadHashKey, value: head.stringValue)
			try self.info.set(key: infoHeadIndexKey, value: String(index))
		}
	}

	func archive(block: SQLBlock) throws {
		try self.archive.archive(block: block)
	}

	func remove(block hash: Hash) throws {
		try self.archive.remove(block: hash)
	}

	func get(block hash: Hash) throws -> SQLBlock? {
		return try self.archive.get(block: hash)
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
