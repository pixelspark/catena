import Foundation
import CSQLite

class Result {
	let database: Database
	private let resultset: OpaquePointer

	init(database: Database, resultset: OpaquePointer) {
		self.database = database
		self.resultset = resultset
	}

	deinit {
		sqlite3_finalize(self.resultset)
	}
}

class Database {
	private var db: OpaquePointer? = nil
	private let mutex = Mutex()

	func open(_ path: String) -> Bool {
		return self.mutex.locked {
			assert(self.db == nil, "database is already opened")
			return path.withCString { cs in
				return sqlite3_open(cs, &db) == SQLITE_OK
			}
		}
	}

	func close() {
		self.mutex.locked {
			if self.db != nil {
				sqlite3_close(self.db)
				self.db = nil
			}
		}
	}

	private var lastError: String {
		return String(cString: sqlite3_errmsg(self.db))
	}

	func perform(_ sql: String) -> Fallible<Result> {
		return self.mutex.locked { () -> Fallible<Result> in
			Swift.print("SQL: \(sql)")
			var resultSet: OpaquePointer? = nil
			if sqlite3_prepare_v2(self.db, sql.cString(using: .utf8), -1, &resultSet, nil) == SQLITE_OK {
				// Time to execute
				switch sqlite3_step(resultSet) {
				case SQLITE_DONE, SQLITE_ROW:
					return .success(Result(database: self, resultset: resultSet!))

				default:
					return .failure(self.lastError)
				}
			}
			else {
				return .failure(self.lastError)
			}
		}
	}

	deinit {
		close()
	}
}
