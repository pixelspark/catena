import Foundation
import LoggerAPI
import CatenaCore

public struct SQLPayload {
	var transactions: [SQLTransaction]

	enum SQLPayloadError: LocalizedError {
		case formatError

		/** The SQL query references a variable that is undefined. */
		case invalidVariableError(name: String)

		/** The SQL query contains an unbound parameter reference. */
		case unboundParameterError(name: String)

		/** The SQL query contains (at least) two bound parameters with the same name, but different values */
		case inconsistentParameterValue(name: String)

		/** An unknown function was referenced. */
		case invalidFunctionError(name: String)

		/** A function was called with an invalid parameter count */
		case invalidParameterCountError

		var errorDescription: String? {
			switch self {
			case .formatError: return "format error"
			case .invalidVariableError(name: let name): return "invalid variable: \(name)"
			case .unboundParameterError(name: let name): return "parameter is unbound: \(name)"
			case .inconsistentParameterValue(name: let name): return "inconsistent value for parameter: \(name)"
			case .invalidFunctionError(name: let name): return "unknown function: \(name)"
			case .invalidParameterCountError: return "function call has incorrect parameter count"
			}
		}
	}

	init(json: Data) throws {
		if json.isEmpty {
			self.transactions = []
		}
		else if let arr = try JSONSerialization.jsonObject(with: json, options: []) as? [[String: Any]] {
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

public struct SQLBlock: Block, CustomDebugStringConvertible {
	public typealias TransactionType = SQLTransaction
	public typealias HashType = SHA256Hash
	public typealias NonceType = UInt64
	public typealias IndexType = UInt64
	public typealias TimestampType = UInt64

	public static let basicVersion: Block.VersionType = 0x1

	/// The maximum number of transactions a block is allowed to contain
	let maximumNumberOfTransactionsPerBlock = 100

	/// Maximum size of the payload data in a block (the 'data for signing' is used as reference)
	let maximumPayloadSizeBytes = 1024 * 1024 // 1 MiB

	public var version: Block.VersionType = SQLBlock.basicVersion
	public var index: UInt64
	public var miner: SHA256Hash
	public var previous: HashType
	public var payload: SQLPayload
	public var nonce: UInt64 = 0
	public var timestamp: TimestampType = 0
	public var signature: HashType? = nil
	private let seed: String! // Only used for genesis blocks, in which case hash==zeroHash and payload is empty

	public init(version: VersionType, index: IndexType, nonce: NonceType, previous: HashType, miner: IdentityType, timestamp: TimestampType, payload: Data) throws {
		self.version = version
		self.index = index
		self.nonce = nonce
		self.previous = previous
		self.miner = miner
		self.timestamp = timestamp

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

	public static func ==(lhs: SQLBlock, rhs: SQLBlock) -> Bool {
		return lhs.dataForSigning == rhs.dataForSigning
	}

	public func isPayloadValid() -> Bool {
		// Payload must not contain too much transactions
		if self.payload.transactions.count > self.maximumNumberOfTransactionsPerBlock {
			return false
		}

		// Payload data must not be too large
		if self.payloadDataForSigning.count > self.maximumPayloadSizeBytes {
			return false
		}

		// For a genesis block, the payload must not contain any transactions
		if isAGenesisBlock && !self.payload.transactions.isEmpty {
			return false
		}


		// Non-genesis blocks must contain at least one transaction
		if !isAGenesisBlock && self.payload.transactions.isEmpty {
			return false
		}

		// The signature of the payload must be valid
		return self.payload.isSignatureValid
	}

	/** Whether the block can accomodate the `transaction`, disregarding any validation of the transaction itself. */
	public func hasRoomFor(transaction: SQLTransaction) -> Bool {
		assert(transaction.signature != nil, "transaction needs to have a signature")

		if (self.payload.transactions.count+1) > self.maximumNumberOfTransactionsPerBlock {
			return false
		}

		// Would adding the transaction increase payload size beyond the limit?
		let newSize = self.payloadDataForSigning.count + transaction.signature!.count
		if newSize > self.maximumPayloadSizeBytes {
			return false
		}

		return true
	}

	public mutating func append(transaction: SQLTransaction) throws -> Bool {
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

	public var payloadData: Data {
		return self.isAGenesisBlock ? self.seed.data(using: .utf8)! : self.payload.data
	}

	public var payloadDataForSigning: Data {
		return self.isAGenesisBlock ? self.seed.data(using: .utf8)! : self.payload.dataForSigning
	}

	public var debugDescription: String {
		return "#\(self.index) [nonce=\(self.nonce), previous=\(self.previous.stringValue), sig=\(self.signature?.stringValue ?? "")]";
	}
}

struct SQLContext {
	let metadata: SQLMetadata
	let invoker: PublicKey
	let block: SQLBlock
	var parameterValues: [String: SQLExpression] = [:]
}

/** Translates function calls in SQL to function calls (or other expressions) in the backend SQL. */
fileprivate enum SQLBackendFunction: String {
	case length = "length"
	case abs = "abs"

	var arity: Int? {
		switch self {
		case .length: return 1
		case .abs: return 1
		}
	}

	func backend(parameters: [SQLExpression]) throws -> SQLExpression {
		if let a = self.arity, parameters.count != a {
			throw SQLPayload.SQLPayloadError.invalidParameterCountError
		}

		switch self {
		case .length: return SQLExpression.call(SQLFunction(name: "LENGTH"), parameters: parameters)
		case .abs: return SQLExpression.call(SQLFunction(name: "ABS"), parameters: parameters)
		}
	}
}

/** Translates frontend to backend SQL queries by traversing the SQL parse tree and generating a new one. */
fileprivate class SQLBackendVisitor: SQLVisitor {
	var context: SQLContext

	init(context: SQLContext) {
		self.context = context
	}

	func visit(table: SQLTable) throws -> SQLTable {
		/* The table name may not start with 'sqlite_'. Replace with '$sqlite' (this name can never
		be created from SQL since the '_' character is disallowed as first character). */
		let forbiddenPrefix = "sqlite_"
		if table.name.starts(with: forbiddenPrefix) {
			return SQLTable(name: table.name.replacingOccurrences(of: forbiddenPrefix, with: "sqlite#", options: [], range: forbiddenPrefix.startIndex..<forbiddenPrefix.endIndex))
		}
		return table
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
		case .unboundParameter(name: let name):
			throw SQLPayload.SQLPayloadError.unboundParameterError(name: name)

		case .boundParameter(name: let name, value: let value):
			// If this parameter has appeared before, it needs to have the exact same value
			if let old = context.parameterValues[name], old != value {
				throw SQLPayload.SQLPayloadError.inconsistentParameterValue(name: name)
			}
			context.parameterValues[name] = value
			return value

		case .call(let function, parameters: let parameters):
			if let bef = SQLBackendFunction(rawValue: function.name.lowercased()) {
				return try bef.backend(parameters: parameters)
			}
			else {
				throw SQLPayload.SQLPayloadError.invalidFunctionError(name: function.name)
			}

		case .variable(let v):
			// Replace variables with corresponding literals
			switch v {
			case "invoker": return SQLExpression.literalBlob(context.invoker.data.sha256)
			case "miner": return SQLExpression.literalBlob(context.block.miner.hash)
			case "timestamp": return SQLExpression.literalInteger(Int(context.block.date.timeIntervalSince1970))
			case "blockSignature": return SQLExpression.literalBlob(context.block.signature!.hash)
			case "previousBlockSignature": return SQLExpression.literalBlob(context.block.previous.hash)
			case "blockHeight": return SQLExpression.literalInteger(Int(context.block.index))
			default: throw SQLPayload.SQLPayloadError.invalidVariableError(name: v)
			}

		default:
			return expression
		}
	}
}

enum SQLExecutionError: LocalizedError {
	/** The fail statement was invoked. */
	case failed

	var errorDescription: String? {
		switch self {
		case .failed: return "failed"
		}
	}
}

class SQLExecutive {
	let context: SQLContext
	let database: Database

	init(context: SQLContext, database: Database) {
		self.context = context
		self.database = database
	}

	func perform(_ statement: SQLStatement) throws -> Result {
		/* Translate the transaction SQL to SQL we can execute on our backend. This includes binding
		variable and parameter values. */
		let be = SQLBackendVisitor(context: context)
		let backendStatement = try statement.visit(be)

		// See if the backend can executive this type of statement
		switch backendStatement {
		case .fail:
			throw SQLExecutionError.failed

		case .show(let s):
			switch s {
			case .tables:
				// TODO make this database-independent (i.e. implement a Database.listOfTables protocol function)
				// NOTE: here we are translating back the 'sqlite#' to 'sqlite_' (see above)
				let query = "SELECT (CASE WHEN name LIKE 'sqlite#%' THEN ('sqlite_' || SUBSTR(name, 8)) ELSE name END) as name FROM sqlite_master WHERE type='table' AND NOT(name LIKE '\\_%' ESCAPE '\\');"
				return try database.perform(query)
			}

		default:
			let query = backendStatement.sql(dialect: database.dialect)
			return try database.perform(query)
		}
	}
}

extension SQLBlock {
	/** This is where the magic happens! When replay is false, do not process actual transaction queries, only validate
	transactions and perform metadata housekeeping. **/
	func apply(database: Database, meta: SQLMetadata, replay: Bool) throws {
		// Obtain current chain state. If there is no state, this is only allowable when this block has index 0 (genesis block)
		var headIndex: SQLBlock.IndexType! = meta.headIndex
		var headHash: SQLBlock.HashType! = meta.headHash
		if headIndex == nil || headHash == nil {
			if self.index == 0 {
				headIndex = 0
				headHash = SQLBlock.HashType.zeroHash
			}
			else {
				throw SQLBlockError.metadataError
			}
		}

		// Check whether block is valid and consecutive
		if self.index != 0 && (self.index != (headIndex + SQLBlock.IndexType(1)) || self.previous != headHash) {
			Log.debug("[SQLBlock] block is not appendable: \(self.index) vs. head=\(headIndex) \(self.previous.stringValue) vs. head=\(headHash.stringValue)")
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
			var counterChanges: [PublicKey: SQLTransaction.CounterType] = [:]
			var setEnforcingGrants = false

			let privilegedTransactions = try sortedTransactions.filter { transaction -> Bool in
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
						Log.debug("Block apply: denying transaction \(transaction) because counter mismatch \(counter+1) != \(transaction.counter) inside block")
						return false
					}
				}
				else {
					if let counter = try meta.users.counter(for: transaction.invoker) {
						if transaction.counter != (counter + 1) {
							Log.debug("Block apply: denying transaction \(transaction) because counter mismatch \(counter+1) != \(transaction.counter)")
							return false
						}
					}
					else {
						if transaction.counter != 0 {
							Log.debug("Block apply: denying transaction \(transaction) because counter is >0 while there is no counter for user yet")
							return false
						}
					}
				}

				counterChanges[transaction.invoker] = transaction.counter

				if meta.isEnforcingGrants {
					// Transaction should be executed when the invoker has the required privileges
					if try meta.grants.check(privileges: requiredPrivileges, forUser: transaction.invoker) {
						// Grants are present
						return true
					}
					else {
						// Perhaps there is a template grant
						return try meta.grants.check(privileges: [SQLPrivilege.template(hash: transaction.statement.templateHash)], forUser: transaction.invoker)
					}
				}
				else {
					/* If this transaction inserts a grant, and we are currently not enforcing grants,
					the next block starts enforcing grants */
					if requiredPrivileges.contains(where: { pr in pr == SQLPrivilege.insert(table: SQLTable(name: SQLMetadata.grantsTableName)) }) {
						setEnforcingGrants = true
					}

					// If grants are not enforced, allow everything
					Log.debug("[Block] NOT checking grants for block #\(self.index) because not enforcing")
					return true
				}
			}

			// Write block's transactions to the database
			for transaction in privilegedTransactions {
				if replay || transaction.shouldAlwaysBeReplayed {
					do {
						let transactionSavepointName = "tr-\(transaction.signature?.base58encoded ?? "unsigned")"
						try database.transaction(name: transactionSavepointName) {
							let context = SQLContext(metadata: meta, invoker: transaction.invoker, block: self, parameterValues: [:])
							let statement = transaction.statement

							let executive = SQLExecutive(context: context, database: database)
							_ = try executive.perform(statement)
						}
					}
					catch {
						// Transactions can fail, this is not a problem - the block can be processed
						Log.debug("Transaction failed, but block will continue to be processed: \(error.localizedDescription)")
					}
				}

				// Update counter
				try meta.users.setCounter(for: transaction.invoker, to: transaction.counter)
			}

			// Write the block itself to archive
			try meta.archive(block: self)

			// Update info
			if setEnforcingGrants {
				Log.debug("[SQLBlock] applying block \(self.index) sets grant enforce flag")
				try meta.set(enforcingGrants: true)
			}

			try meta.set(head: self.signature!, index: self.index)
		}
	}
}
