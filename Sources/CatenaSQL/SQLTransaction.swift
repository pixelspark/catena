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
	public let invoker: CatenaCore.PublicKey
	public let counter: CounterType
	public let statement: SQLStatement
	var signature: Data? = nil

	/** The maximum size of a transaction (as measured by `dataForSigning`) */
	static let maximumSize = 1024 * 10 // 10 KiB

	enum SQLTransactionError: Error {
		case formatError
		case syntaxError
	}

	public init(statement: SQLStatement, invoker: CatenaCore.PublicKey, counter: CounterType) throws {
		self.invoker = invoker
		self.statement = statement
		self.counter = counter
	}

	public required init(json: [String: Any]) throws {
		guard let tx = json["tx"] as? [String: Any] else { throw SQLTransactionError.formatError }
		guard let sql = tx["sql"] as? String else { throw SQLTransactionError.formatError }
		guard let invoker = tx["invoker"] as? String else { throw SQLTransactionError.formatError }
		guard let counter = tx["counter"] as? Int else { throw SQLTransactionError.formatError }
		guard let invokerKey = PublicKey(string: invoker) else { throw SQLTransactionError.formatError }

		self.invoker = invokerKey
		self.statement = try SQLStatement(sql)
		self.counter = CounterType(counter)

		if let sig = json["signature"] as? String, let sigData = Data(base64Encoded: sig) {
			self.signature = sigData
		}
	}

	private var dataForSigning: Data {
		var d = Data()
		d.append(self.invoker.data)
		d.appendRaw(self.counter.littleEndian)
		d.append(self.statement.sql(dialect: SQLStandardDialect()).data(using: .utf8)!)
		return d
	}

	@discardableResult public func sign(with privateKey: CatenaCore.PrivateKey) throws -> SQLTransaction {
		self.signature = try self.invoker.sign(data: self.dataForSigning, with: privateKey)
		return self
	}

	/** Whether the statement in this transaction should be executed by clients that only participate in validation and
	metadata replay. This includes queries that modify e.g. the grants table. */
	var shouldAlwaysBeReplayed: Bool {
		let privs = self.statement.requiredPrivileges
		for p in privs {
			if let t = p.table, SQLMetadata.specialVisibleTables.contains(t.name) {
				// Query requires a privilege to a special visible table, this should be replayed
				return true
			}
		}

		return false
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

