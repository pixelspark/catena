import Foundation
import LoggerAPI

struct SQLPayload {
	var transactions: [SQLTransaction]

	enum SQLPayloadError: Error {
		case formatError
		case invalidVariableError
	}

	init(json: Data) throws {
		if let arr = try JSONSerialization.jsonObject(with: json, options: []) as? [[String: Any]] {
			self.transactions = try arr.map { item in
				return try SQLTransaction(json: item)
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
		let d = self.transactions.map { $0.json }
		return try! JSONSerialization.data(withJSONObject: d, options: [])
	}

	var dataForSigning: Data {
		var data = Data()

		for tr in self.transactions {
			let sigData = tr.signature!
			sigData.withUnsafeBytes { bytes in
				data.append(bytes, count: sigData.count)
			}
		}

		return data
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
	typealias HashType = SHA256Hash

	/// The maximum number of transactions a block is allowed to contain
	let maximumNumberOfTransactionsPerBlock = 100

	/// Maximum size of the payload data in a block (the 'data for signing' is used as reference)
	let maximumPayloadSizeBytes = 1024 * 1024 // 1 MiB

	var index: UInt
	var previous: HashType
	var payload: SQLPayload
	var nonce: UInt = 0
	var signature: HashType? = nil
	private let seed: String! // Only used for genesis blocks, in which case hash==zeroHash and payload is empty

	init() {
		self.index = 0
		self.previous = HashType.zeroHash
		self.payload = SQLPayload()
		self.seed = nil
	}

	init(genesisBlockWith seed: String) {
		self.index = 0
		self.seed = seed
		self.payload = SQLPayload()
		self.previous = HashType.zeroHash
	}

	init(index: UInt, previous: HashType, payload: Data) throws {
		self.index = index
		self.previous = previous

		// If this is a genesis block, the payload is used as seed
		if self.previous == HashType.zeroHash {
			self.payload = SQLPayload()
			self.seed = String(data: payload, encoding: .utf8)!
		}
		else {
			self.seed = nil
			self.payload = try SQLPayload(json: payload)
		}
	}

	init(index: UInt, previous: HashType, payload: SQLPayload) {
		self.index = index
		self.previous = previous
		self.payload = payload
		self.seed = nil
	}

	static func ==(lhs: SQLBlock, rhs: SQLBlock) -> Bool {
		return lhs.dataForSigning == rhs.dataForSigning
	}

	func isPayloadValid() -> Bool {
		// Payload must not contain too much transactions
		if self.payload.transactions.count > self.maximumNumberOfTransactionsPerBlock {
			return false
		}

		// Payload data must not be too large
		if self.payloadDataForSigning.count > self.maximumPayloadSizeBytes {
			return false
		}

		// For a genesis block, the payload must not contain any transactions
		if isAGenesisBlock {
			return self.payload.transactions.isEmpty 
		}

		// The signature of the payload must be valid
		return self.payload.isSignatureValid
	}

	mutating func append(transaction: SQLTransaction) throws -> Bool {
		assert(self.seed == nil, "cannot append transactions to a genesis block")
		assert(transaction.signature != nil, "transaction needs to have a signature")

		if (self.payload.transactions.count+1) > self.maximumNumberOfTransactionsPerBlock {
			throw SQLBlockError.tooManyTransactionsInBlockError
		}

		// Transaction already exists
		if self.payload.transactions.contains(where: { $0.signature! == transaction.signature! }) {
			return false
		}

		self.payload.transactions.append(transaction)
		return true
	}

	var payloadData: Data {
		return self.isAGenesisBlock ? self.seed.data(using: .utf8)! : self.payload.data
	}

	var payloadDataForSigning: Data {
		return self.isAGenesisBlock ? self.seed.data(using: .utf8)! : self.payload.dataForSigning
	}

	var debugDescription: String {
		return "#\(self.index) [nonce=\(self.nonce), previous=\(self.previous.stringValue), sig=\(self.signature?.stringValue ?? "")]";
	}
}

struct SQLContext {
	let metadata: SQLMetadata
	let invoker: PublicKey
}

struct SQLBackendVisitor: SQLVisitor {
	let context: SQLContext

	init(context: SQLContext) {
		self.context = context
	}

	func visit(column: SQLColumn) throws -> SQLColumn {
		/* Replace occurences of column 'rowid' with '$rowid'. The rowid column is special in SQLite and we are therefore
		masking it. */
		if column == SQLColumn(name: "rowid") {
			return SQLColumn(name: "$rowid")
		}
		return column
	}

	func visit(expression: SQLExpression) throws -> SQLExpression {
		switch expression {
		case .variable(let v):
			// Replace variables with corresponding literals
			switch v {
			case "invoker": return SQLExpression.literalBlob(context.invoker.data.sha256)
			default: throw SQLPayload.SQLPayloadError.invalidVariableError
			}

		default:
			return expression
		}
	}
}

extension SQLStatement {
	/** Translates a Catena SQL statement into something our backend (SQLite) can execute. Replace variables with their
	values (as literals), etc. */
	func backendStatement(context: SQLContext) throws -> SQLStatement {
		let visitor = SQLBackendVisitor(context: context)
		return try self.visit(visitor)
	}
}

extension SQLBlock {
	/** This is where the magic happens! When replay is false, do not process actual transaction queries, only validate
	transactions and perform metadata housekeeping. **/
	func apply(database: Database, meta: SQLMetadata, replay: Bool) throws {
		// Obtain current chain state
		guard let headIndex = meta.headIndex else { throw SQLBlockError.metadataError }
		guard let headHash = meta.headHash else { throw SQLBlockError.metadataError }

		// Check whether block is valid and consecutive
		if self.index != (headIndex + 1) || self.previous != headHash {
			throw SQLBlockError.inconsecutiveBlockError
		}

		if !self.isSignatureValid {
			throw SQLBlockError.blockSignatureError
		}

		if !self.isPayloadValid() {
			throw SQLBlockError.payloadSignatureError
		}

		// Start a database transaction
		let blockSavepointName = "block-\(self.signature!.stringValue)"
		try database.transaction(name: blockSavepointName) {
			/* Sort transactions by counter value, so that multiple transactions submitted by a single invoker execute
			in a defined order. */
			let sortedTransactions = self.payload.transactions.sorted(by: { (a, b) -> Bool in
				return b.counter > a.counter
			})

			/* Check transaction grants (only grants from previous blocks 'count'; as transactions can potentially change
			the grants, we need to check them up front) */
			var counterChanges: [PublicKey: Int] = [:]
			let privilegedTransactions = try sortedTransactions.filter { transaction -> Bool in
				if self.index == 1 {
					/* Block 1 is special in that it doesn't enforce grants - so that you can actually set them for the
					first time without getting the error that you don't have the required privileges. */
					return true
				}

				/* Does any of the privileges involve a 'special' table? If so, deny. Note: this should never happen
				anyway as these tables have a name starting with an underscore, which the parser does not accept */
				let requiredPrivileges = transaction.statement.requiredPrivileges
				let containsSpecialInvisible = requiredPrivileges.contains { p in
					if let t = p.table, SQLMetadata.specialInvisibleTables.contains(t.name) {
						return true
					}
					return false
				}

				if containsSpecialInvisible {
					return false
				}

				/* Check the current counter value for the invoker. Transactions can only be executed when the invoker
				does not yet have a counter (i.e. the invoker public key is used for the first time) or the transaction
				counter is exactly (counter+1).*/
				if let counter = counterChanges[transaction.invoker] {
					if transaction.counter != (counter + 1) {
						Log.debug("Block apply: denying transaction \(transaction.signature!) because counter mismatch \(counter+1) != \(transaction.counter) inside block")
						return false
					}
				}
				else {
					let counter = try meta.users.counter(for: transaction.invoker) ?? -1
					if transaction.counter != (counter + 1) {
						Log.debug("Block apply: denying transaction \(transaction.signature!) because counter mismatch \(counter+1) != \(transaction.counter)")
						return false
					}
				}

				counterChanges[transaction.invoker] = transaction.counter

				// Transaction should be executed when the invoker has the required privileges
				return try meta.grants.check(privileges: requiredPrivileges, forUser: transaction.invoker)
			}

			// Write block's transactions to the database
			for transaction in privilegedTransactions {
				do {
					let transactionSavepointName = "tr-\(transaction.signature?.base58encoded ?? "unsigned")"

					try database.transaction(name: transactionSavepointName) {
						let context = SQLContext(metadata: meta, invoker: transaction.invoker)
						let statement = transaction.statement
						let query = try statement.backendStatement(context: context).sql(dialect: database.dialect)

						if replay || transaction.shouldAlwaysBeReplayed {
							_ = try database.perform(query)
						}
					}
				}
				catch {
					// Transactions can fail, this is not a problem - the block can be processed
					Log.debug("Transaction failed, but block will continue to be processed: \(error.localizedDescription)")
				}

				// Update counter
				try meta.users.setCounter(for: transaction.invoker, to: transaction.counter)
			}

			// Write the block itself to archive
			try meta.archive(block: self)

			// Update info
			try meta.set(head: self.signature!, index: self.index)
		}
	}
}

class SQLLedger: Ledger<SQLBlockchain> {
	/** Instantiate an SQLLedger that tracks chains starting at the indicated genesis block and uses the indicated database
	file for storage. When`replay` is true, the ledger will process database transactions, whereas if it is false, the
	ledger will only validate transactions and participate in grant and other metadata processing. */
	init(genesis: SQLBlock, database path: String, replay: Bool) throws {
		let lb = try SQLBlockchain(genesis: genesis, database: path, replay: replay)
		super.init(longest: lb)
	}

	override func canAccept(transaction: SQLTransaction, pool: SQLBlock?) throws -> Bool {
		if !transaction.isSignatureValid {
			Log.info("[Ledger] cannot accept tx \(transaction): signature invalid")
			return false
		}

		/* A transaction is acceptable when its counter is one above the stored counter for the invoker key, or zero when
		the invoker has no counter yet (not executed any transactions before on this chain). */
		let appendable = try self.mutex.locked { () -> Bool in
			return try self.longest.withUnverifiedTransactions { chain in
				if let counter = try chain.meta.users.counter(for: transaction.invoker) {
					return (counter + 1) == transaction.counter
				}
				return transaction.counter == 0
			}
		}

		if appendable {
			return true
		}

		// Perhaps the transaction is appendable to another pooled transaction?
		if let p = pool {
			for tr in p.payload.transactions {
				if tr.invoker == transaction.invoker && (tr.counter + 1) == transaction.counter {
					return true
				}
			}
		}

		Log.info("[Ledger] cannot accept tx \(transaction): not appendable (\(transaction.counter))")
		return false
	}
}

