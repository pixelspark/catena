import Foundation
import LoggerAPI
import Ed25519

class SQLTransaction: Transaction {
	let invoker: PublicKey
	let statement: SQLStatement
	var signature: Data? = nil

	enum SQLTransactionError: Error {
		case formatError
		case syntaxError
	}

	init(statement: SQLStatement, invoker: PublicKey) throws {
		self.invoker = invoker
		self.statement = statement
	}

	convenience init(data: [String: Any]) throws {
		guard let tx = data["tx"] as? [String: Any] else { throw SQLTransactionError.formatError }
		guard let sql = tx["sql"] as? String else { throw SQLTransactionError.formatError }
		guard let invoker = tx["invoker"] as? String else { throw SQLTransactionError.formatError }
		guard let invokerKey = PublicKey(string: invoker) else { throw SQLTransactionError.formatError }

		try self.init(statement: try SQLStatement(sql), invoker: invokerKey)

		if let sig = data["signature"] as? String, let sigData = Data(base64Encoded: sig) {
			self.signature = sigData
		}
	}

	private var dataForSigning: Data {
		var d = Data()
		d.append(self.invoker.data)
		d.append(self.statement.sql(dialect: SQLStandardDialect()).data(using: .utf8)!)
		return d
	}

	@discardableResult func sign(with privateKey: PrivateKey) throws -> SQLTransaction {
		self.signature = try self.invoker.sign(data: self.dataForSigning, with: privateKey)
		return self
	}

	var isSignatureValid: Bool {
		do {
			if let s = self.signature {
				return try self.invoker.verify(message: self.dataForSigning, signature: s)
			}
			return false
		}
		catch {
			return false
		}
	}

	var data: [String: Any] {
		var json: [String: Any] = [
			"tx": [
				"sql": self.statement.sql(dialect: SQLStandardDialect()),
				"invoker": self.invoker.stringValue
			]
		]

		if let s = self.signature {
			json["signature"] = s.base64EncodedString()
		}

		return json
	}
}

