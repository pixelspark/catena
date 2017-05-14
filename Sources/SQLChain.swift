import Foundation
struct SQLPayload {
	let statement: String

	init(statement: String) {
		self.statement = statement
	}

	var data: Data {
		return self.statement.data(using: .utf8)!
	}
}

struct SQLBlock: Block, CustomDebugStringConvertible {
	var index: UInt
	var previous: Hash
	let payload: SQLPayload
	var nonce: UInt = 0
	var signature: Hash? = nil

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

class SQLLedger: Ledger<SQLBlock> {
	let database: Database

	init(genesis: SQLBlock, database: Database) {
		self.database = database
		super.init(genesis: genesis)
	}

	override func didAppend(block: SQLBlock) {
		self.mutex.locked {
			switch self.database.perform("BEGIN TRANSACTION") {
			case .success(_):
				switch self.database.perform(block.payload.statement) {
				case .success(_):
					// statement successfully executed, commit
					switch self.database.perform("COMMIT") {
					case .success(_):
						// All OK
						return
					case .failure(let e):
						fatalError("Commitment issues: \(e)")
					}

				case .failure(let e):
					// Some error occurred, roll back the whole block
					print("Block #\(block.index) failed: \(e)")
					switch self.database.perform("ROLLBACK") {
					case .success(_):
						// All OK, failed block is ignored but added to history
						return

					case .failure(let e):
						fatalError("Rollback issues: \(e)")
					}
				}

			case .failure(let e):
				fatalError("Could not start transaction: \(e)")
			}
		}
	}
}
