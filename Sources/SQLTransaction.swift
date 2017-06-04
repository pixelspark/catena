import Foundation
import LoggerAPI

class SQLTransaction {
	let root: SQLStatement

	enum SQLTransactionError: Error {
		case formatError
		case syntaxError
	}

	init(statement: String) throws {
		let parser = SQLParser()
		if !parser.parse(statement) {
			Log.debug("[SQLTransaction] Parsing failed: \(statement)")
			Log.debug(parser.debugDescription)
			throw SQLTransactionError.syntaxError
		}

		// Top-level item must be a statement
		if let root = parser.root, case .statement(let sq) = root {
			self.root = sq
		}
		else {
			throw SQLTransactionError.syntaxError
		}
	}

	convenience init(data: Any) throws {
		if let q = data as? String {
			try self.init(statement: q)
		}
		else {
			throw SQLTransactionError.formatError
		}
	}

	var data: Any {
		return self.root.sql(dialect: SQLStandardDialect())
	}

	var identifier: Hash {
		let sql = self.root.sql(dialect: SQLStandardDialect())
		return Hash(of: sql.data(using: .utf8)!)
	}
}

