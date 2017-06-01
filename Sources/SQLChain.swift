import Foundation
struct SQLPayload {
	var transactions: [SQLTransaction]

	enum SQLPayloadError: Error {
		case formatError
	}

	init(data: Data) throws {
		if let arr = try JSONSerialization.jsonObject(with: data, options: []) as? [Any] {
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
}

struct SQLBlock: Block, CustomDebugStringConvertible {
	var index: UInt
	var previous: Hash
	let payload: SQLPayload
	var nonce: UInt = 0
	var signature: Hash? = nil

	static func ==(lhs: SQLBlock, rhs: SQLBlock) -> Bool {
		return lhs.signedData == rhs.signedData
	}

	init(index: UInt, previous: Hash, payload: Data) throws {
		self.index = index
		self.previous = previous
		self.payload = try SQLPayload(data: payload)
	}

	init(index: UInt, previous: Hash, payload: SQLPayload) {
		self.index = index
		self.previous = previous
		self.payload = payload
	}

	var payloadData: Data {
		return self.payload.data
	}

	var debugDescription: String {
		return "#\(self.index) [nonce=\(self.nonce), previous=\(self.previous.stringValue), sig=\(self.signature?.stringValue ?? "")]";
	}
}

extension SQLStatement {
	var backendSQL: String {
		return self.sql
	}
}

class SQLLedger: Ledger<SQLBlock> {
	let database: Database

	init(genesis: SQLBlock, database: Database) {
		self.database = database
		super.init(genesis: genesis)
	}

	override func didAppend(block: SQLBlock) {
		self.mutex.locked {
			let blockSavepointName = "block-\(block.signature!.stringValue)"
			switch self.database.perform("SAVEPOINT '\(blockSavepointName)' ") {
			case .success(_):
				for transaction in block.payload.transactions {
					let transactionSavepointName = "tr-\(transaction.identifier.stringValue)"
					switch self.database.perform("SAVEPOINT '\(transactionSavepointName)'") {
					case .success(_):
						let query = transaction.root.backendSQL

						switch self.database.perform(query) {
						case .success(_):
							// statement successfully executed, commit
							switch self.database.perform("RELEASE '\(transactionSavepointName)'") {
							case .success(_):
								break

							case .failure(let e):
								fatalError("Commitment issues: \(e)")
							}

						case .failure(let e):
							// Some error occurred, roll back the whole block
							print("[SQL] Transaction \(transaction.identifier.stringValue) in block #\(block.index) failed: \(e)")
							switch self.database.perform("ROLLBACK TO SAVEPOINT '\(transactionSavepointName)'") {
							case .success(_):
								break

							case .failure(let e):
								fatalError("Rollback issues: \(e)")
							}
						}

					case .failure(let e):
						fatalError("Commitment issues: \(e)")
					}
				}

				// Commit block
				switch self.database.perform("RELEASE SAVEPOINT '\(blockSavepointName)'") {
				case .success(_):
					// All OK
					return
				case .failure(let e):
					fatalError("Commitment issues: \(e)")
				}

			case .failure(let e):
				fatalError("Could not start transaction: \(e)")
			}
		}
	}
}
