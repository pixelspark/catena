import Foundation
import LoggerAPI
import CSQLite
import CatenaCore

public enum ResultState {
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

public enum Value {
	case int(Int)
	case text(String)
	case blob(Data)
	case float(Double)
	case bool(Bool)
	case null
}

public protocol Result {
	var hasRow: Bool { get }
	var columns: [String] { get }
	var values: [Value] { get }
	var state: ResultState { get }
	@discardableResult func step() -> ResultState
}

public class SQLiteResult: Result {
	public private(set) var state: ResultState
	let database: SQLiteDatabase
	private let resultset: OpaquePointer

	init(database: SQLiteDatabase, resultset: OpaquePointer, rows: Bool) {
		self.database = database
		self.resultset = resultset
		self.state = rows ? .row : .done
	}

	public var hasRow: Bool {
		return self.state.hasRow
	}

	public var columns: [String] {
		let n = sqlite3_column_count(resultset)

		var cns: [String] = []
		for i in 0..<n {
			let name = String(cString: sqlite3_column_name(self.resultset, i))
			cns.append(name)
		}

		return cns
	}

	public var values: [Value] {
		let n = sqlite3_column_count(resultset)

		var cns: [Value] = []
		for i in 0..<n {
			let t = sqlite3_column_type(self.resultset, i)

			switch t {
			case SQLITE_INTEGER:
				cns.append(Value.int(Int(sqlite3_column_int64(self.resultset, i))))
				
				/*if let ptr = sqlite3_column_decltype(self.resultset, i) {
					let type = String(cString: ptr)
					if type.hasPrefix("BOOL") {
						cns.append(Value.bool(sqlite3_column_int64(self.resultset, i) != 0))
					}
					cns.append(Value.int(Int(sqlite3_column_int64(self.resultset, i))))
				}
				else {
					fatalError("could not get delctype from sqlite result")
				}*/

			case SQLITE_TEXT: cns.append(Value.text(String(cString: sqlite3_column_text(self.resultset, i))))
			case SQLITE_FLOAT: cns.append(Value.float(sqlite3_column_double(self.resultset, i)))
			case SQLITE_NULL: cns.append(Value.null)
			case SQLITE_BLOB:
				if let b = sqlite3_column_blob(self.resultset, i) {
					let sz = sqlite3_column_bytes(self.resultset, i)
					let data = Data(bytes: b, count: Int(sz))
					cns.append(Value.blob(data))
				}
				else {
					cns.append(Value.null)
				}
			default: fatalError("unknown SQLite value type: \(t)")
			}
		}

		return cns
	}

	@discardableResult public func step() -> ResultState {
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

			case SQLITE_MISUSE:
				fatalError("SQLite misuse")

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

public protocol SQLDialect {
	func literalString(_ string: String) -> String
	func tableIdentifier(_ table: String) -> String
	func columnIdentifier(_ column: String) -> String
	func literalBlob(_ blob: Data) -> String
}

public struct SQLStandardDialect: SQLDialect {
	let stringEscape = "\\"
	let stringQualifierEscape = "\'\'"
	let stringQualifier = "\'"
	let identifierQualifier = "\""
	let identifierQualifierEscape = "\\\""

	public func literalString(_ string: String) -> String {
		let escaped = string
			.replacingOccurrences(of: stringEscape, with: stringEscape+stringEscape)
			.replacingOccurrences(of: stringQualifier, with: stringQualifierEscape)
		return "\(stringQualifier)\(escaped)\(stringQualifier)"
	}

	public func literalBlob(_ blob: Data) -> String {
		let hex = blob.map { String(format: "%02hhx", $0) }.joined()
		return "X\(self.stringQualifier)\(hex)\(self.stringQualifier)"
	}

	public func tableIdentifier(_ table: String) -> String {
		return "\(identifierQualifier)\(table.replacingOccurrences(of: identifierQualifier, with: identifierQualifierEscape))\(identifierQualifier)"
	}

	public func columnIdentifier(_ column: String) -> String {
		return "\(identifierQualifier)\(column.replacingOccurrences(of: identifierQualifier, with: identifierQualifierEscape))\(identifierQualifier)"
	}
}

public protocol Database {
	var dialect: SQLDialect { get }
	func transaction<T>(name: String?, alwaysRollback: Bool, callback: @escaping (() throws -> (T))) throws -> T
	func perform(_ sql: String) throws -> Result
	func close()
	func exists(table: String) throws -> Bool
}

public extension Database {
	func transaction<T>(name: String? = nil, callback: @escaping (() throws -> (T))) throws -> T {
		return try self.transaction(name: name, alwaysRollback: false, callback: callback)
	}

	func hypothetical<T>(callback: @escaping (() throws -> (T))) throws -> T {
		return try self.transaction(name: nil, alwaysRollback: true, callback: callback)
	}
}

public class SQLiteDatabase: Database {
	private let schema = "main"
	private var db: OpaquePointer? = nil
	private let mutex = Mutex()
	private var counter = 0
	private var hypotheticalCounter = 0
	public let dialect: SQLDialect = SQLStandardDialect()

	enum DatabaseError: LocalizedError {
		case error(String)

		var errorDescription: String? {
			switch self {
			case .error(let e): return e
			}
		}
	}

	public init() {
	}

	public func open(_ path: String) throws {
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

	public func close() {
		self.mutex.locked {
			if self.db != nil {
				sqlite3_close(self.db)
				self.db = nil
			}
		}
	}

	public func exists(table: String) throws -> Bool {
		let r = try self.perform("SELECT type FROM sqlite_master WHERE name=\(self.dialect.literalString(table))")
		return r.hasRow
	}

	fileprivate var lastError: String {
		return String(cString: sqlite3_errmsg(self.db))
	}

	public func transaction<T>(name: String? = nil, alwaysRollback: Bool, callback: @escaping (() throws -> (T))) throws -> T {
		return try self.mutex.locked { () -> T in
			if alwaysRollback {
				self.hypotheticalCounter += 1
			}
			self.counter += 1

			defer {
				self.counter -= 1

				if alwaysRollback {
					self.hypotheticalCounter -= 1
				}
			}

			let savepointName = self.dialect.literalString("tx\(self.hypotheticalCounter)c\(self.counter)")

			if self.counter == 1 {
				try _ = self.perform("BEGIN")
			}
			else {
				try _ = self.perform("SAVEPOINT \(savepointName)")
			}

			do {
				let t = try callback()

				if alwaysRollback {
					if self.counter == 1 {
						try _ = self.perform("ROLLBACK")
					}
					else {
						try _ = self.perform("ROLLBACK TO SAVEPOINT \(savepointName)")
					}
				}
				else {
					if self.counter == 1 {
						try _ = self.perform("COMMIT")
					}
					else {
						try _ = self.perform("RELEASE SAVEPOINT \(savepointName)")
					}
				}

				return t
			}
			catch {
				if self.counter == 1 {
					try _ = self.perform("COMMIT")
				}
				else {
					try _ = self.perform("RELEASE SAVEPOINT \(savepointName)")
				}
				throw error
			}
		}
	}

	public func perform(_ sql: String) throws -> Result {
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
						Log.debug("[SQL] ERROR: \(self.lastError)")
						if resultSet != nil {
							sqlite3_finalize(resultSet)
						}
						throw DatabaseError.error(self.lastError)
					}
				}
				else {
					switch sqlite3_errcode(self.db) {
					case SQLITE_MISUSE:
						fatalError("[SQL] Misuse: \(self.lastError) \(sql)")

					default:
						Log.debug("[SQL] ERROR in prepare: \(self.lastError)")
						throw DatabaseError.error("SQLite error \(sqlite3_errcode(self.db)): \(self.lastError) (SQL: \(sql)")
					}
				}
			}
		}
	}

	deinit {
		close()
	}
}
