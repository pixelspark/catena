import Foundation

#if os(Linux)
	import CSQLiteLinux
#else
	import CSQLite
#endif

enum ResultState {
	case row
	case done
	case error(String)
}

class Result {
	private(set) var state: ResultState
	let database: Database
	private let resultset: OpaquePointer

	init(database: Database, resultset: OpaquePointer, rows: Bool) {
		self.database = database
		self.resultset = resultset
		self.state = rows ? .row : .done
	}

	var columns: [String] {
		let n = sqlite3_column_count(resultset)

		var cns: [String] = []
		for i in 0..<n {
			let name = String(cString: sqlite3_column_name(self.resultset, i))
			cns.append(name)
		}

		return cns
	}

	var values: [String] {
		let n = sqlite3_column_count(resultset)

		var cns: [String] = []
		for i in 0..<n {
			let name = String(cString: sqlite3_column_text(self.resultset, i))
			cns.append(name)
		}

		return cns
	}

	@discardableResult func step() -> ResultState {
		switch self.state {
		case .row:
			switch sqlite3_step(self.resultset) {
			case SQLITE_DONE:
				self.state = .done

			case SQLITE_ROW:
				self.state = .row

			case SQLITE_BUSY:
				self.state = .row

			case SQLITE_ERROR:
				self.state = .error(database.lastError)

			default:
				self.state = .error("Unknown error code")
			}

		case .done, .error(_):
			break
		}

		return self.state
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

	fileprivate var lastError: String {
		return String(cString: sqlite3_errmsg(self.db))
	}

	func perform(_ sql: String) -> Fallible<Result> {
		return self.mutex.locked { () -> Fallible<Result> in
			Swift.print("[SQL] \(sql)")

			var resultSet: OpaquePointer? = nil
			return sql.withCString { cString -> Fallible<Result>  in
				if sqlite3_prepare_v2(self.db, cString, -1, &resultSet, nil) == SQLITE_OK {
					// Time to execute
					switch sqlite3_step(resultSet) {
					case SQLITE_DONE:
						return .success(Result(database: self, resultset: resultSet!, rows: false))

					case SQLITE_ROW:
						return .success(Result(database: self, resultset: resultSet!, rows: true))

					default:
						return .failure(self.lastError)
					}
				}
				else {
					return .failure(self.lastError)
				}
			}
		}
	}

	deinit {
		close()
	}
}
