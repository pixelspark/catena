import Foundation
import LoggerAPI
import Ed25519
import CatenaCore

public class SQLTransaction: Transaction, CustomDebugStringConvertible {
	public var hashValue: Int {
		return self.dataForSigning.hashValue
	}

	public static func ==(lhs: SQLTransaction, rhs: SQLTransaction) -> Bool {
		if let ls = lhs.signature, let rs = rhs.signature {
			return ls == rs
		}

		return
			lhs.counter == rhs.counter &&
			lhs.invoker == rhs.invoker &&
			lhs.dataForSigning == rhs.dataForSigning
	}

	public typealias CounterType = UInt64
	public var invoker: CatenaCore.PublicKey
	public var database: SQLDatabase
	public var counter: CounterType
	public var statement: SQLStatement
	var signature: Data? = nil

	/** The maximum size of a transaction (as measured by `dataForSigning`) */
	static let maximumSize = 1024 * 10 // 10 KiB

	enum SQLTransactionError: Error {
		case formatError
		case syntaxError
	}

	public init(statement: SQLStatement, invoker: CatenaCore.PublicKey, database: SQLDatabase, counter: CounterType = CounterType(0)) throws {
		self.invoker = invoker
		self.statement = statement
		self.counter = counter
		self.database = database
	}

	public required init(json: [String: Any]) throws {
		guard let tx = json["tx"] as? [String: Any] else { throw SQLTransactionError.formatError }
		guard let sql = tx["sql"] as? String else { throw SQLTransactionError.formatError }
		guard let database = tx["database"] as? String else { throw SQLTransactionError.formatError }
		guard let invoker = tx["invoker"] as? String else { throw SQLTransactionError.formatError }
		guard let counter = tx["counter"] as? Int else { throw SQLTransactionError.formatError }
		guard let invokerKey = PublicKey(string: invoker) else { throw SQLTransactionError.formatError }

		self.invoker = invokerKey
		self.statement = try SQLStatement(sql)
		self.counter = CounterType(counter)
		self.database = SQLDatabase(name: database)

		if let sig = json["signature"] as? String, let sigData = Data(base64Encoded: sig) {
			self.signature = sigData
		}
	}

	private var dataForSigning: Data {
		var d = Data()
		d.append(self.invoker.data)
		d.append(self.database.name.data(using: .utf8)!)
		d.appendRaw(self.counter.littleEndian)
		d.append(self.statement.sql(dialect: SQLStandardDialect()).data(using: .utf8)!)
		return d
	}

	@discardableResult public func sign(with privateKey: CatenaCore.PrivateKey) throws -> SQLTransaction {
		self.signature = try self.invoker.sign(data: self.dataForSigning, with: privateKey)
		return self
	}

	public var isSignatureValid: Bool {
		do {
			if let s = self.signature {
				let sd = self.dataForSigning

				// Check transaction size
				if sd.count > SQLTransaction.maximumSize {
					return false
				}
				return try self.invoker.verify(message: sd, signature: s)
			}
			return false
		}
		catch {
			return false
		}
	}

	public var json: [String: Any] {
		var json: [String: Any] = [
			"tx": [
				"sql": self.statement.sql(dialect: SQLStandardDialect()),
				"database": self.database.name,
				"counter": NSNumber(value: self.counter),
				"invoker": self.invoker.stringValue
			]
		]

		if let s = self.signature {
			json["signature"] = s.base64EncodedString()
		}

		return json
	}

	public var debugDescription: String {
		return "\(self.invoker.data.sha256.base64EncodedString())@\(self.counter)"
	}
}

fileprivate class SQLParameterVisitor: SQLVisitor {
	var parameters: [String: SQLExpression] = [:]

	func visit(expression: SQLExpression) throws -> SQLExpression {
		switch expression {
		case .unboundParameter(name: let s):
			self.parameters[s] = expression

		case .boundParameter(name: let s, value: let v):
			self.parameters[s] = v

		default:
			break
		}

		return expression
	}
}

fileprivate class SQLParameterBinder: SQLVisitor {
	let parameters: [String: SQLExpression]

	init(parameters: [String: SQLExpression]) {
		self.parameters = parameters
	}

	func visit(expression: SQLExpression) throws -> SQLExpression {
		switch expression {
		case .unboundParameter(name: let s):
			if let v = self.parameters[s] {
				return SQLExpression.boundParameter(name: s, value: v)
			}

		case .boundParameter(name: let s, value: _):
			if let v = self.parameters[s] {
				return .boundParameter(name: s, value: v)
			}

		default:
			break
		}

		return expression
	}
}

fileprivate class SQLParameterUnbinder: SQLVisitor {
	func visit(expression: SQLExpression) throws -> SQLExpression {
		switch expression {
		case .unboundParameter(name: _):
			return expression

		case .boundParameter(name: let s, value: _):
			return .unboundParameter(name: s)

		default:
			return expression
		}
	}
}

fileprivate class SQLVariableBinder: SQLVisitor {
	let variables: [String: SQLExpression]

	init(variables: [String: SQLExpression]) {
		self.variables = variables
	}

	func visit(expression: SQLExpression) throws -> SQLExpression {
		if case .variable(let s) = expression, let v = self.variables[s] {
			return v
		}
		return expression
	}
}

extension SQLStatement {
	/** All parameters present in this query. The value for an unbound parameter is an
	SQLExpression.unboundExpression. */
	var parameters: [String: SQLExpression] {
		let v = SQLParameterVisitor()
		_ = try! self.visit(v)
		return v.parameters
	}

	/** Returns a version of this statement where all bound parameters are replaced with unbound ones
	(e.g. the values are removed). Unbound parameters in the original statement are left untouched. */
	var unbound: SQLStatement {
		let b = SQLParameterUnbinder()
		return try! self.visit(b)
	}

	var templateHash: SHA256Hash {
		return SHA256Hash(of: self.unbound.sql(dialect: SQLStandardDialect()).data(using: .utf8)!)
	}

	/** Binds parameters in the query. Unbound parameters will be replaced with bound parameters if
	there is a corresponding value in the `parameters` dictionary. Bound parameters will have their
	values replaced with the values in the `parameter` dictionary. Bound and unbound parameters in
	the query remain unchanged if the `parameters` set does not have a new value for them. */
	func bound(to parameters: [String: SQLExpression]) -> SQLStatement {
		let b = SQLParameterBinder(parameters: parameters)
		return try! self.visit(b)
	}

	func replacing(variables: [String: SQLExpression]) -> SQLStatement {
		let b = SQLVariableBinder(variables: variables)
		return try! self.visit(b)
	}
}
