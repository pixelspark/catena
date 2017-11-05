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

	public var transactions: [SQLTransaction] {
		return self.isAGenesisBlock ? [] : self.payload.transactions
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
fileprivate class SQLiteBackendVisitor: SQLVisitor {
	var context: SQLContext

	init(context: SQLContext) {
		self.context = context
	}

	fileprivate func tableNameToBackendTableName(_ table: SQLTable) -> SQLTable {
		/* The table name may not start with 'sqlite_'. Replace with '$sqlite' (this name can never
		be created from SQL since the '_' character is disallowed as first character). */
		let forbiddenPrefix = "sqlite_"
		if table.name.starts(with: forbiddenPrefix) {
			return SQLTable(name: table.name.replacingOccurrences(of: forbiddenPrefix, with: "sqlite#", options: [], range: forbiddenPrefix.startIndex..<forbiddenPrefix.endIndex))
		}
		return table
	}

	func visit(table: SQLTable) throws -> SQLTable {
		return self.tableNameToBackendTableName(table)
	}

	func visit(column: SQLColumn) throws -> SQLColumn {
		/* Replace occurences of column 'rowid' with '$rowid'. The rowid column is special in SQLite
		and we are therefore masking it. The same goes for 'oid'. The reverse translation is done in
		SQLiteBackendResult. */
		switch column {
		case SQLColumn(name: "rowid"): return SQLColumn(name: "$rowid")
		case SQLColumn(name: "oid"): return SQLColumn(name: "$oid")
		default: return column
		}
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
			case "blockMiner": return SQLExpression.literalBlob(context.block.miner.hash)
			case "blockTimestamp": return SQLExpression.literalInteger(Int(context.block.date.timeIntervalSince1970))
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

	/** A required privilege was not granted. */
	case privilegeRequired

	/** A referenced table was not found */
	case tableDoesNotExist(String)

	/** The specified table cannot be created because it already exists */
	case tableAlreadyExists(String)

	/** A column was referenced outside the context of a table */
	case notInTableContext(String)

	/** A referenced column does not exist */
	case columnDoesNotExist(String)

	/** The same column was specified more than once in an INSERT, CREATE or UPDATE statement */
	case duplicateColumns

	var errorDescription: String? {
		switch self {
		case .failed: return "failed"
		case .privilegeRequired: return "privilege required"
		case .tableDoesNotExist(let t): return "table '\(t)' does not exist"
		case .tableAlreadyExists(let t): return "table '\(t)' already exists"
		case .notInTableContext(let c): return "column '\(c)' was referenced outside table context"
		case .columnDoesNotExist(let c): return "table does not contain referenced column '\(c)'"
		case .duplicateColumns: return "the same column was specified more than once"
		}
	}
}

/** Wraps a backend result and sanitizes it to hide implementation details from the frontend user. This
performs some of the opposite operations done in SQLiteBackendVisitor. */
private class SQLiteBackendResult: Result {
	private let result: Result

	init(result: Result) {
		self.result = result
	}

	var hasRow: Bool { return self.result.hasRow }
	var state: ResultState { return self.result.state }
	var values: [Value] { return self.result.values }
	func step() -> ResultState { return self.result.step() }

	var columns: [String] {
		/** Replace the dollar sign in the first position of a column name. The dollar sign is
		inserted by SQLiteBackendVisitor for 'special' column names ('rowid', 'oid'). It cannot be
		in a column name from the frontend (the dollar sign is forbidden). It can however appear as
		result of a "SELECT '$foo';" statement which leads to a column name "'$foo'". In this case
		the dollar sign is always at position 1 or later, and passes this transformation without issue. */
		return self.result.columns.map { $0.replacingOccurrences(of: "$", with: "", options: [], range: $0.startIndex..<($0.index(after: $0.startIndex))) }
	}
}

class SQLExecutive {
	let context: SQLContext
	let database: Database

	init(context: SQLContext, database: Database) {
		self.context = context
		self.database = database
	}

	func perform(_ statement: SQLStatement, withRegime regime: (([SQLPrivilege]) throws -> (Bool))) throws -> Result {
		// Check permissions
		if !(try regime(statement.requiredPrivileges)) {
			throw SQLExecutionError.privilegeRequired
		}

		/* Translate the transaction SQL to SQL we can execute on our backend. This includes binding
		variable and parameter values. */
		let be = SQLiteBackendVisitor(context: context)
		let backendStatement = try statement.visit(be)

		// See if the backend can executive this type of statement
		switch backendStatement {
		case .fail:
			throw SQLExecutionError.failed

		case .`if`(let sqlIf):
			// Evaluate each branch of the if statement until one of them matches
			for (condition, statement) in sqlIf.branches {
				// Evaluate condition
				let evaluationSQL = "SELECT CASE WHEN (\(condition.sql(dialect: database.dialect))) THEN 1 ELSE 0 END AS result;"
				let evaluationResult = try database.perform(evaluationSQL)
				assert(evaluationResult.hasRow, "evaluation of condition must return a row")

				if case .int(let result) = evaluationResult.values[0], result == 1 {
					// Evaluation was positive
					return try self.perform(statement, withRegime: regime)
				}
			}

			// Execute 'else'
			if let other = sqlIf.otherwise {
				return try self.perform(other, withRegime: regime)
			}
			else {
				throw SQLExecutionError.failed
			}

		case .show(let s):
			switch s {
			case .tables:
				// TODO make this database-independent (i.e. implement a Database.listOfTables protocol function)
				// NOTE: here we are translating back the 'sqlite#' to 'sqlite_' (see above)
				let query = "SELECT (CASE WHEN name LIKE 'sqlite#%' THEN ('sqlite_' || SUBSTR(name, 8)) ELSE name END) as name FROM sqlite_master WHERE type='table' AND NOT(name LIKE '\\_%' ESCAPE '\\');"
				return SQLiteBackendResult(result: try database.perform(query))
			}

		default:
			try backendStatement.verify(on: database)
			let query = backendStatement.sql(dialect: database.dialect)
			return SQLiteBackendResult(result: try database.perform(query))
		}
	}
}

extension SQLExpression {
	/** Checks whether the expression can be evaluated in the given database and context, and throws
	an error when it can't. Note that this should only be used on backend statements, e.g. the expression
	cannot contain variables nor parameters at this point. */
	func verify(on database: Database, context: TableDefinition?, isCreating: Bool = false) throws {
		switch self {
		case .allColumns:
			return

		case .binary(let a, _, let b):
			try a.verify(on: database, context: context, isCreating: isCreating)
			try b.verify(on: database, context: context, isCreating: isCreating)

		case .call(_, parameters: let e):
			try e.forEach { try $0.verify(on: database, context: context, isCreating: isCreating) }

		case .column(let c):
			if let ctx = context {
				if !ctx.contains(where: { SQLColumn(name: $0.0) == c }) {
					throw SQLExecutionError.columnDoesNotExist(c.name)
				}
			}
			else {
				if !isCreating {
					throw SQLExecutionError.notInTableContext(c.name)
				}
			}

		case .literalBlob(_), .literalInteger(_), .literalString(_), .literalUnsigned(_), .null:
			return

		case .unary(_, let e):
			try e.verify(on: database, context: context, isCreating: isCreating)

		case .variable(_), .unboundParameter(name: _), .boundParameter(name: _, value: _):
			fatalError("Variables/parameters cannot appear in a statement ready for database execution")

		case .when(let whens, else: let e):
			try e?.verify(on: database, context: context, isCreating: isCreating)
			try whens.forEach {
				try $0.when.verify(on: database, context: context, isCreating: isCreating)
				try $0.then.verify(on: database, context: context, isCreating: isCreating)
			}
		}
	}
}

extension SQLStatement {
	/** Perform checks to find out whether this statement will be able to execute on the database,
	and throw an error when it can't. */
	func verify(on database: Database) throws {
		switch self {
		case .select(let s):
			if let f = s.from {
				if try !database.exists(table: f.name) {
					throw SQLExecutionError.tableDoesNotExist(f.name)
				}

				let td = try database.definition(for: f.name)
				try s.`where`?.verify(on: database, context: td)
				try s.these.forEach { try $0.verify(on: database, context: td) }
			}
			else {
				// Select statement must not contain any column references
				try s.these.forEach { try $0.verify(on: database, context: nil, isCreating: false) }
			}

		case .create(table: let t, schema: _):
			if try database.exists(table: t.name) {
				throw SQLExecutionError.tableAlreadyExists(t.name)
			}

		case .drop(table: let t):
			if try !database.exists(table: t.name) {
				throw SQLExecutionError.tableDoesNotExist(t.name)
			}

		case .fail:
			return

		case .if(let sqlIf):
			try sqlIf.branches.forEach { try $0.1.verify(on: database) }

		case .insert(let i):
			if try !database.exists(table: i.into.name) {
				throw SQLExecutionError.tableDoesNotExist(i.into.name)
			}

			let td = try database.definition(for: i.into.name)

			// Do we have any duplicate columns?
			if Set(i.columns).count != i.columns.count {
				throw SQLExecutionError.duplicateColumns
			}

			// Check whether referenced columns exist
			try i.columns.forEach { col in
				if !td.contains { SQLColumn(name: $0.0) == col } {
					throw SQLExecutionError.columnDoesNotExist(col.name)
				}
			}

		case .show(_):
			return

		case .update(let u):
			if try !database.exists(table: u.table.name) {
				throw SQLExecutionError.tableDoesNotExist(u.table.name)
			}

			let td = try database.definition(for: u.table.name)
			try u.`where`?.verify(on: database, context: td, isCreating: false)

			for (col, val) in u.set {
				if !td.contains(where: { SQLColumn(name: $0.0) == col }) {
					throw SQLExecutionError.columnDoesNotExist(col.name)
				}
				try val.verify(on: database, context: td, isCreating: false)
			}

		case .delete(let from, let d):
			if try !database.exists(table: from.name) {
				throw SQLExecutionError.tableDoesNotExist(from.name)
			}
			let td = try database.definition(for: from.name)
			try d?.verify(on: database, context: td, isCreating: false)

		case .createIndex(let table, let idx):
			if try !database.exists(table: table.name) {
				throw SQLExecutionError.tableDoesNotExist(table.name)
			}

			let td = try database.definition(for: table.name)
			for col in idx.on {
				if !td.contains(where: { SQLColumn(name: $0.0) == col }) {
					throw SQLExecutionError.columnDoesNotExist(col.name)
				}
			}
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
				return true
			}

			// Write block's transactions to the database
			for transaction in privilegedTransactions {
				if replay || transaction.shouldAlwaysBeReplayed {
					do {
						let transactionSavepointName = "tr-\(transaction.signature?.base58encoded ?? "unsigned")"
						try database.transaction(name: transactionSavepointName) {
							let context = SQLContext(metadata: meta, invoker: transaction.invoker, block: self, parameterValues: [:])
							let statement = transaction.statement

							// Check if there is a template grant for this statement
							let templateGranted = try meta.isEnforcingGrants && meta.grants.check(privileges: [SQLPrivilege.template(hash: transaction.statement.templateHash)], forUser: transaction.invoker)

							let executive = SQLExecutive(context: context, database: database)
							_ = try executive.perform(statement) { requiredPrivileges in
								if meta.isEnforcingGrants {
									// All parts of a template-granted statement may be executed without further checks
									if templateGranted {
										return true
									}
									// A statement in a non-templated query should be executed only when the invoker has the required privileges
									else if try meta.grants.check(privileges: requiredPrivileges, forUser: transaction.invoker) {
										// Grants are present
										return true
									}
									else {
										return false
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

