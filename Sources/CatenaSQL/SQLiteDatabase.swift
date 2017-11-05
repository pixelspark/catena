import Foundation
import CatenaCore
import LoggerAPI
import CSQLite

/** SQLite implementation of the Database interface. */
public class SQLiteDatabase: Database {
	private let schema = "main"
	private var db: OpaquePointer? = nil
	private let mutex = Mutex()
	private var counter = 0
	private var hypotheticalCounter = 0
	public let dialect: SQLDialect = SQLStandardDialect()

	enum SQLiteDatabaseError: LocalizedError {
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
					throw SQLiteDatabaseError.error("Error opening database: \(res)")
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
					try _ = self.perform("ROLLBACK")
				}
				else {
					try _ = self.perform("ROLLBACK TO SAVEPOINT \(savepointName)")
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
						throw SQLiteDatabaseError.error(self.lastError)
					}
				}
				else {
					switch sqlite3_errcode(self.db) {
					case SQLITE_MISUSE:
						fatalError("[SQL] Misuse: \(self.lastError) \(sql)")

					default:
						Log.debug("[SQL] ERROR in prepare: \(self.lastError)")
						throw SQLiteDatabaseError.error("SQLite error \(sqlite3_errcode(self.db)): \(self.lastError) (SQL: \(sql))")
					}
				}
			}
		}
	}

	public func definition(for table: String) throws -> TableDefinition {
		let res = try self.perform("PRAGMA table_info(\(self.dialect.tableIdentifier(table)))")

		var od = OrderedDictionary<String, ColumnDefinition>()
		while res.hasRow {
			let type: ColumnType
			switch res["type"]! {
			case .text("TEXT"): type = .text
			case .text("BLOB"): type = .blob
			case .text("INT"): type = .int
			case .text("FLOAT"): type = .float
			default: type = .blob
			}

			guard case .int(let isPrimaryKey) = res["pk"]! else { fatalError("SQLite disobeys own API") }
			guard case .int(let notNull) = res["notnull"]! else { fatalError("SQLite disobeys own API") }
			guard case .text(let name) = res["name"]! else { fatalError("SQLite disobeys own API") }

			let def = ColumnDefinition(type: type, isPartOfPrimaryKey: isPrimaryKey == 1, isNullable: notNull == 0)
			od.append(def, forKey: name)
			res.step()
		}

		return od
	}

	deinit {
		close()
	}
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

	public lazy var columns: [String] = { [unowned self] in
		let n = sqlite3_column_count(resultset)

		var cns: [String] = []
		for i in 0..<n {
			let name = String(cString: sqlite3_column_name(self.resultset, i))
			cns.append(name)
		}

		return cns
	}()

	private var valuesCached: [Value]? = nil

	public var values: [Value] {
		if let vc = self.valuesCached {
			return vc
		}

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

		self.valuesCached = cns
		return cns
	}

	@discardableResult public func step() -> ResultState {
		self.valuesCached = nil

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
