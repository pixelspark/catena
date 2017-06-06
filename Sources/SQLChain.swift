import Foundation
import LoggerAPI

struct SQLPayload {
	var transactions: [SQLTransaction]

	enum SQLPayloadError: Error {
		case formatError
	}

	init(data: Data) throws {
		if let arr = try JSONSerialization.jsonObject(with: data, options: []) as? [[String: Any]] {
			self.transactions = try arr.map { item in
				return try SQLTransaction(data: item)
			}
		}
		else {
			throw SQLPayloadError.formatError
		}
	}

	init(transactions: [SQLTransaction] = []) {
		self.transactions = transactions
	}

	var data: Data {
		let d = self.transactions.map { $0.data }
		return try! JSONSerialization.data(withJSONObject: d, options: [])
	}

	var isSignatureValid: Bool {
		for tr in self.transactions {
			if !tr.isSignatureValid {
				return false
			}
		}
		return true
	}
}

struct SQLBlock: Block, CustomDebugStringConvertible {
	typealias TransactionType = SQLTransaction

	var index: UInt
	var previous: Hash
	var payload: SQLPayload
	var nonce: UInt = 0
	var signature: Hash? = nil
	private let seed: String! // Only used for genesis blocks, in which case hash==zeroHash and payload is empty

	init() {
		self.index = 0
		self.previous = Hash.zeroHash
		self.payload = SQLPayload()
		self.seed = nil
	}

	init(genesisBlockWith seed: String) {
		self.index = 0
		self.seed = seed
		self.payload = SQLPayload()
		self.previous = Hash.zeroHash
	}

	init(index: UInt, previous: Hash, payload: Data) throws {
		self.index = index
		self.previous = previous
		self.payload = try SQLPayload(data: payload)
		self.seed = nil
	}

	init(index: UInt, previous: Hash, payload: SQLPayload) {
		self.index = index
		self.previous = previous
		self.payload = payload
		self.seed = nil
	}

	static func ==(lhs: SQLBlock, rhs: SQLBlock) -> Bool {
		return lhs.signedData == rhs.signedData
	}

	func isPayloadValid() -> Bool {
		if isAGenesisBlock {
			return self.payload.transactions.isEmpty 
		}
		return self.payload.isSignatureValid
	}

	mutating func append(transaction: SQLTransaction) {
		assert(self.seed == nil, "cannot append transactions to a genesis block")
		self.payload.transactions.append(transaction)
	}

	var payloadData: Data {
		return self.isAGenesisBlock ? self.seed.data(using: .utf8)! : self.payload.data
	}

	var debugDescription: String {
		return "#\(self.index) [nonce=\(self.nonce), previous=\(self.previous.stringValue), sig=\(self.signature?.stringValue ?? "")]";
	}
}

extension SQLStatement {
	func backendStatement() -> SQLStatement {
		switch self {
		case .create(table: _, schema: _):
			return self

		case .drop(table: _):
			return self

		case .delete(from: _, where: _):
			return self

		case .insert(into: _, columns: _, values: _):
			return self

		case .select(_):
			return self

		// This will be used to rewrite column names, table names, etc. for backend processing
		/*case .insert(into: let table, columns: let columns, values: let values):
			return SQLStatement.insert(into: SQLTable(name: "user_\(table.name)"), columns: columns, values: values)

		case .select(these: let expressions, from: let table):
			if let t = table {
				return SQLStatement.select(these: expressions, from: SQLTable(name: "user_\(t.name)"))
			}
			else {
				return SQLStatement.select(these: expressions, from: nil)
			}*/

		case .update:
			return self
		}
	}

	func backendSQL(dialect: SQLDialect) -> String {
		return self.backendStatement().sql(dialect: dialect)
	}
}

class SQLKeyValueTable {
	let database: Database
	let table: String

	private let keyColumnName = "key"
	private let valueColumnName = "value"

	init(database: Database, table: String) throws {
		self.database = database
		self.table = table

		// Ensure the table exists
		try database.transaction {
			let r = try database.perform("SELECT type FROM sqlite_master WHERE name=\(database.dialect.tableIdentifier(table))")
			if !r.hasRow {
				let kn = self.database.dialect.columnIdentifier(self.keyColumnName)
				let vn = self.database.dialect.columnIdentifier(self.valueColumnName)
				try _ = database.perform("CREATE TABLE \(database.dialect.tableIdentifier(table)) (\(kn) TEXT PRIMARY KEY, \(vn) TEXT)")
			}
		}
	}

	func get(_ key: String) throws -> String? {
		let kn = self.database.dialect.columnIdentifier(self.keyColumnName)
		let vn = self.database.dialect.columnIdentifier(self.valueColumnName)
		let r = try database.perform("SELECT \(vn) FROM \(database.dialect.tableIdentifier(table)) WHERE \(kn)=\(database.dialect.literalString(key))")
		if r.hasRow {
			return r.values[0]
		}
		return nil
	}

	func set(key: String, value: String) throws {
		let k = self.database.dialect.literalString(key)
		let v = self.database.dialect.literalString(value)
		let kn = self.database.dialect.columnIdentifier(self.keyColumnName)
		let vn = self.database.dialect.columnIdentifier(self.valueColumnName)
		try _ = self.database.perform("INSERT OR REPLACE INTO \(database.dialect.tableIdentifier(table)) (\(kn), \(vn)) VALUES (\(k), \(v))")
	}
}

class SQLHistory {
	let info: SQLKeyValueTable
	let database: Database
	var headHash: Hash
	var headIndex: UInt

	let mutex = Mutex()

	let infoHeadHashKey = "head"
	let infoHeadIndexKey = "index"

	init(genesis: SQLBlock, database: Database) throws {
		self.database = database
		self.info = try SQLKeyValueTable(database: database, table: "_info")

		// Obtain the current state of the history
		if let hi = try self.info.get(infoHeadIndexKey), let headIndex = UInt(hi),
			let hh = try self.info.get(infoHeadHashKey), let headHash = Hash(string: hh) {
			self.headHash = headHash
			self.headIndex = headIndex
		}
		else {
			self.headHash = genesis.signature!
			self.headIndex = genesis.index
			try self.info.set(key: infoHeadHashKey, value: self.headHash.stringValue)
			try self.info.set(key: infoHeadIndexKey, value: String(self.headIndex))
		}

		Log.info("Persisted history is at index \(self.headIndex), hash \(self.headHash.stringValue)")
	}

	func process(block: SQLBlock) throws {
		try self.mutex.locked {
			assert(block.isSignatureValid && block.isPayloadValid(), "Block is invalid")
			assert(block.index == self.headIndex + 1, "Block is not consecutive: \(block.index) upon \(self.headIndex)")

			let blockSavepointName = "block-\(block.signature!.stringValue)"

			try database.transaction(name: blockSavepointName) {
				for transaction in block.payload.transactions {
					do {
						let transactionSavepointName = "tr-\(transaction.signature?.base58encoded ?? "unsigned")"

						try database.transaction(name: transactionSavepointName) {
							let query = transaction.statement.backendSQL(dialect: self.database.dialect)
							_ = try database.perform(query)
						}
					}
					catch {
						// Transactions can fail, this is not a problem - the block can be processed
						Log.debug("Transaction failed, but block will continue to be processed: \(error.localizedDescription)")
					}
				}

				try self.info.set(key: self.infoHeadHashKey, value: block.signature!.stringValue)
				try self.info.set(key: self.infoHeadIndexKey, value: "\(block.index)")
				self.headIndex = block.index
				self.headHash = block.signature!
			}
		}
	}
}

class SQLLedger: Ledger<SQLBlock> {
	enum SQLLedgerError: Error {
		case databaseError
	}

	/** The SQL ledger maintains a database (permanent) and a queue. When transactions are received, they are inserted
	in a queue. When this queue exceeds a certain size (`maxQueueSize`), the transactions are processed in the permanent
	database. If a chain splice/switch occurs that required rewinding to less than maxQueueSize blocks, this can be done
	efficiently by removing blocks from the queue. If the splice happens earlier, the full database needs to be rebuilt.*/
	let maxQueueSize = 7
	var queue: [SQLBlock] = []

	let databasePath: String
	var permanentHistory: SQLHistory

	init(genesis: SQLBlock, database path: String) throws {
		self.databasePath = path
		let permanentDatabase = Database()
		try permanentDatabase.open(path)

		self.permanentHistory = try SQLHistory(genesis: genesis, database: permanentDatabase)

		super.init(genesis: genesis)
	}

	override func didUnwind(from: SQLBlock, to: SQLBlock) {
		do {
			try self.mutex.locked {
				Log.info("[SQLLedger] Unwind from #\(from.index) to #\(to.index)")

				if self.permanentHistory.headIndex <= to.index {
					// Unwinding within queue
					self.queue = self.queue.filter { return $0.index <= to.index }
					Log.info("[SQLLedger] Permanent is at \(self.permanentHistory.headIndex), replayed up to+including \(to.index)")
				}
				else {
					// To-block is earlier than the head of permanent storage. Need to replay the full chain
					Log.info("[SQLLedger] Unwind requires a replay of the full chain, because target block (\(to.index)) << head of permanent history (\(self.permanentHistory.headIndex)) ")
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
			// Remove database
			self.permanentHistory.database.close()
			let e = self.databasePath.withCString { cs -> Int32 in
				return unlink(cs)
			}

			if e != 0 {
				fatalError("[SQLLedger] Could not delete permanent database; err=\(e)")
			}

			// Create new database
			let db = Database()
			try db.open(self.databasePath)
			self.permanentHistory = try SQLHistory(genesis: self.longest.genesis, database: db)

			// Find blocks to be replayed
			// TODO: refactor so we can just walk the chain from old to new without creating a giant array
			var replay: [SQLBlock] = []
			var current = to
			while current.index != 0 {
				replay.append(current)
				current = self.longest.blocks[current.previous]!
			}

			// Replay blocks
			try self.permanentHistory.database.transaction {
				for block in replay.reversed() {
					try self.permanentHistory.process(block: block)
				}
				Log.info("[SQLLedger] replay on permanent storage is complete")
			}
		}
	}

	private func queue(block: SQLBlock) throws {
		try self.mutex.locked {
			self.queue.append(block)

			if self.queue.count > maxQueueSize {
				let promoted = self.queue.removeFirst()
				Log.info("[SQLLedger] promoting block \(promoted.index) to permanent storage which is now at \(self.permanentHistory.headIndex)")

				if (self.permanentHistory.headIndex + 1) != promoted.index {
					Log.info("[SQLLedger] need to replay first to \(promoted.index-1)")
					let prev = self.longest.blocks[promoted.previous]!
					try self.replayPermanentStorage(to: prev)
				}
				try permanentHistory.process(block: promoted)
				Log.info("[SQLLedger] promoted block \(promoted.index) to permanent storage; qs=\(self.queue.count)")
			}
		}
	}

	override func didAppend(block: SQLBlock) {
		do {
			Log.info("[SQLLedger] did append #\(block.index)")
			try self.mutex.locked {
				// Play the transaction on the head database
				try self.queue(block: block)
			}
		}
		catch {
			Log.error("Could not append block: \(error.localizedDescription)")
		}
	}
}
