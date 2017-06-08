import Foundation
import LoggerAPI
import CSQLite

enum ResultState {
	case row
	case done
	case error(String)

	var hasRow: Bool {
		switch self {
		case .row: return true
		case .done, .error(_): return false
		}
	}
}

class Snapshot {
	fileprivate let snapshot: UnsafeMutablePointer<sqlite3_snapshot>

	init(snapshot: UnsafeMutablePointer<sqlite3_snapshot>) {
		self.snapshot = snapshot
	}

	deinit {
		sqlite3_snapshot_free(snapshot)
	}
}

protocol Result {
	var hasRow: Bool { get }
	var columns: [String] { get }
	var values: [String] { get }
	var state: ResultState { get }
	@discardableResult func step() -> ResultState
}

class SQLiteResult: Result {
	private(set) var state: ResultState
	let database: SQLiteDatabase
	private let resultset: OpaquePointer

	init(database: SQLiteDatabase, resultset: OpaquePointer, rows: Bool) {
		self.database = database
		self.resultset = resultset
		self.state = rows ? .row : .done
	}

	var hasRow: Bool {
		return self.state.hasRow
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

	// TODO: use a more appropriate type than string (variant type)
	var values: [String] {
		let n = sqlite3_column_count(resultset)

		var cns: [String] = []
		for i in 0..<n {
			if let v = sqlite3_column_text(self.resultset, i) {
				let name = String(cString: v)
				cns.append(name)
			}
			else {
				// TODO: this is NULL
				cns.append("")
			}
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

protocol SQLDialect {
	func literalString(_ string: String) -> String
	func tableIdentifier(_ table: String) -> String
	func columnIdentifier(_ column: String) -> String
}

struct SQLStandardDialect: SQLDialect {
	let stringEscape = "\\"
	let stringQualifierEscape = "\'\'"
	let stringQualifier = "\'"
	let identifierQualifier = "\""
	let identifierQualifierEscape = "\\\""

	func literalString(_ string: String) -> String {
		let escaped = string
			.replacingOccurrences(of: stringEscape, with: stringEscape+stringEscape)
			.replacingOccurrences(of: stringQualifier, with: stringQualifierEscape)
		return "\(stringQualifier)\(escaped)\(stringQualifier)"
	}

	func tableIdentifier(_ table: String) -> String {
		return "\(identifierQualifier)\(table.replacingOccurrences(of: identifierQualifier, with: identifierQualifierEscape))\(identifierQualifier)"
	}

	func columnIdentifier(_ column: String) -> String {
		return "\(identifierQualifier)\(column.replacingOccurrences(of: identifierQualifier, with: identifierQualifierEscape))\(identifierQualifier)"
	}
}

protocol Database {
	var dialect: SQLDialect { get }
	func transaction(name: String?, callback: (() throws -> ())) throws
	func perform(_ sql: String) throws -> Result
	func close()
	func exists(table: String) throws -> Bool
}

extension Database {
	func transaction(callback: (() throws -> ())) throws {
		try self.transaction(name: nil, callback: callback)
	}
}

class SQLiteDatabase: Database {
	private let schema = "main"
	private var db: OpaquePointer? = nil
	private let mutex = Mutex()
	private var counter = 0
	let dialect: SQLDialect = SQLStandardDialect()

	enum DatabaseError: LocalizedError {
		case error(String)

		var errorDescription: String? {
			switch self {
			case .error(let e): return e
			}
		}
	}

	func open(_ path: String) throws {
		try self.mutex.locked {
			assert(self.db == nil, "database is already opened")
			try path.withCString { cs in
				let res = sqlite3_open(cs, &db)
				if res != SQLITE_OK {
					throw DatabaseError.error("Error opening database: \(res)")
				}
			}

			try _ = self.perform("PRAGMA journal_mode=WAL")
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

	func exists(table: String) throws -> Bool {
		let r = try self.perform("SELECT type FROM sqlite_master WHERE name=\(self.dialect.literalString(table))")
		return r.hasRow
	}

	fileprivate var lastError: String {
		return String(cString: sqlite3_errmsg(self.db))
	}

	func snapshot() -> Snapshot? {
		return self.mutex.locked {
			var sn: UnsafeMutablePointer<sqlite3_snapshot>? = nil
			return withUnsafeMutablePointer(to: &sn) { ptr in
				if sqlite3_snapshot_get(self.db, self.schema, ptr) != SQLITE_OK {
					return nil
				}
				return Snapshot(snapshot: sn!)
			}
		}
	}

	func transaction(name: String? = nil, callback: (() throws -> ())) throws {
		let savepointName = self.dialect.literalString(name ?? "tx-\(self.counter)")

		try _ = self.perform("SAVEPOINT \(savepointName)")

		do {
			try callback()
			try _ = self.perform("RELEASE SAVEPOINT \(savepointName)")
		}
		catch {
			try! _ = self.perform("ROLLBACK TO SAVEPOINT \(savepointName)")
			throw error
		}
	}

	func perform(_ sql: String) throws -> Result {
		return try self.mutex.locked { () -> Result in
			Log.debug("[SQL] \(sql)")

			var resultSet: OpaquePointer? = nil
			return try sql.withCString { cString -> Result  in
				if sqlite3_prepare_v2(self.db, cString, -1, &resultSet, nil) == SQLITE_OK {
					// Time to execute
					switch sqlite3_step(resultSet) {
					case SQLITE_DONE:
						return SQLiteResult(database: self, resultset: resultSet!, rows: false)

					case SQLITE_ROW:
						return SQLiteResult(database: self, resultset: resultSet!, rows: true)

					default:
						throw DatabaseError.error(self.lastError)
					}
				}
				else {
					throw DatabaseError.error(self.lastError)
				}
			}
		}
	}

	deinit {
		close()
	}
}
