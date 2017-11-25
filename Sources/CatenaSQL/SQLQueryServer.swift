import Foundation
import LoggerAPI
import CatenaCore
import PostgresWireServer

struct QueryError: LocalizedError {
	let message: String

	init(message: String) {
		self.message = message
	}

	var errorDescription: String? {
		return self.message
	}
}

extension Value {
	var pqValue: PQValue {
		switch self {
		case .text(let s): return PQValue.text(s)
		case .int(let i): return PQValue.int(Int32(i))
		case .float(let d): return PQValue.float8(d)
		case .bool(let b): return PQValue.bool(b)
		case .blob(let b): return PQValue.text(b.base64EncodedString())
		case .null: return PQValue.null
		}
	}
}

public class QueryServerPreparedStatement: PreparedStatement {
	let statement: SQLStatement
	let identity: Identity
	let agent: SQLAgent

	init(sql: String, identity: Identity, agent: SQLAgent) throws {
		var sql = sql
		if sql.isEmpty {
			throw QueryError.init(message: "query may not be empty")
		}

		// Insert ending semicolon if it hasn't been inserted
		if sql.last! != Character(";") {
			sql += ";"
		}

		self.statement = try SQLStatement(sql).visit(FrontEndStatementVisitor())
		self.identity = identity
		self.agent = agent
	}

	public var willReturnRows: Bool {
		return !statement.isPotentiallyMutating
	}

	func bound(to parameters: [PQValue]) -> SQLStatement {
		var bindings: [String: SQLExpression] = [:]
		for (idx, value) in parameters.enumerated() {
			switch value {
			case .text(let s): bindings["\(idx)"] = SQLExpression.literalString(s)
			case .bool(let b): bindings["\(idx)"] = SQLExpression.literalInteger(b ? 1 : 0)
			// FIXME: Support floating point
			case .float4(let f): bindings["\(idx)"] = SQLExpression.literalString("\(f)")
			case .float8(let f): bindings["\(idx)"] = SQLExpression.literalString("\(f)")
			case .int(let i): bindings["\(idx)"] = SQLExpression.literalInteger(Int(i))
			case .null: bindings["\(idx)"] = SQLExpression.null
			}
		}

		return self.statement.replacing(variables: bindings)
	}

	public func fields(for parameters: [PQValue]) throws -> [PQField] {
		let statement = self.bound(to: parameters)

		if statement.isPotentiallyMutating {
			// Mutating statements will not return any rows, ever
			return []
		}
		else if let cols = statement.columnsInResult {
			return cols.map { PQField(name: $0.name, type: .text) }
		}
		else {
			// Just perform and cache the result
			var result: [PQField] = []
			try self.agent.node.ledger.longest.withUnverifiedTransactions { chain in
				let context = SQLContext(metadata: chain.meta, invoker: self.identity.publicKey, block: chain.highest, parameterValues: [:])
				let ex = SQLExecutive(context: context, database: chain.database)
				let rs = try ex.perform(self.statement) { _ in return true }
				result = rs.columns.map { c in return PQField(name: c, type: .text) }
			}
			return result
		}
	}
}

class QueryServerResultSet: ResultSet {
	let result: Result

	init(result: Result) {
		self.result = result
	}

	func row() throws -> [PQValue] {
		assert(self.hasRow, "should not request next row when has no row")
		let v = self.result.values.map { return $0.pqValue }
		self.result.step()
		return v
	}

	var hasRow: Bool {
		switch self.result.state {
		case .row: return true
		case .done, .error(_): return false
		}
	}

	var error: String? {
		if case .error(let m) = result.state {
			return m
		}
		return nil
	}
}

public class NodeQueryServer: QueryServer<QueryServerPreparedStatement> {
	var agent: SQLAgent

	public init(agent: SQLAgent, port: Int, family: Family = .ipv6) {
		self.agent = agent
		super.init(port: port, family: family)
	}

	override public func prepare(_ sql: String, connection: QueryClientConnection<QueryServerPreparedStatement>) throws -> QueryServerPreparedStatement {
		// Get user public/private key
		guard let username = connection.username else {
			throw QueryError(message: "no username set")
		}

		guard let password = connection.password else {
			throw QueryError(message: "no password set")
		}

		let identity: Identity

		// for testing, autogenerate a keypair when the username is 'random'
		if username == "random" {
			identity = try Identity()
		}
		else {
			guard let invokerKey = PublicKey(string: username) else {
				throw QueryError(message: "No username set or username is not a public key. Connect with username 'random' to have the server generate a new identity for you.")
			}

			guard let passwordKey = PrivateKey(string: password) else {
				throw QueryError(message: "The given password is not a valid private key.")
			}

			identity = Identity(publicKey: invokerKey, privateKey: passwordKey)
		}

		return try QueryServerPreparedStatement(sql: sql, identity: identity, agent: self.agent)
	}

	public override func query(_ query: QueryServerPreparedStatement, parameters: [PQValue], connection: QueryClientConnection<QueryServerPreparedStatement>, callback: @escaping (ResultSet?) throws -> ()) throws {
		// Parse the statement
		let statement = try SQLStatement(query.bound(to: parameters).sql(dialect: SQLStandardDialect()))
		Log.info("[Query] Execute: \(statement.sql(dialect: SQLStandardDialect()))")

		// Mutating statements are queued
		if statement.isPotentiallyMutating {
			// This needs to go to the ledger
			let transaction = try SQLTransaction(statement: statement, invoker: query.identity.publicKey, counter: SQLTransaction.CounterType(0))
			_ = try self.agent.submit(transaction: transaction, signWith: query.identity.privateKey)
			try callback(nil)
		}
		else {
			try self.agent.node.ledger.longest.withUnverifiedTransactions { chain in
				let context = SQLContext(metadata: chain.meta, invoker: query.identity.publicKey, block: chain.highest, parameterValues: [:])
				let ex = SQLExecutive(context: context, database: chain.database)
				let result = try ex.perform(statement) { _ in return true }
				try callback(QueryServerResultSet(result: result))
			}
		}
	}
}
