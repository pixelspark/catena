import Foundation

class SQLTransaction {
	let query: String

	enum SQLTransactionError: Error {
		case formatException
	}

	init(statement: String) {
		self.query = statement
	}

	init(data: Any) throws {
		if let q = data as? String {
			self.query = q
		}
		else {
			throw SQLTransactionError.formatException
		}
	}

	var data: Any {
		return self.query
	}

	var identifier: Hash {
		return Hash(of: self.query.data(using: .utf8)!)
	}
}

