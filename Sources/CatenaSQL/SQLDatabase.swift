import Foundation
import LoggerAPI
import CatenaCore

public protocol Database {
	var dialect: SQLDialect { get }
	func transaction<T>(name: String?, alwaysRollback: Bool, callback: @escaping (() throws -> (T))) throws -> T
	func perform(_ sql: String) throws -> Result
	func close()
	func exists(table: String) throws -> Bool
	func definition(for table: String) throws -> TableDefinition
}

public protocol Result {
	var hasRow: Bool { get }
	var columns: [String] { get }
	var values: [Value] { get }
	var state: ResultState { get }
	@discardableResult func step() -> ResultState
}

public enum Value {
	case int(Int)
	case text(String)
	case blob(Data)
	case float(Double)
	case bool(Bool)
	case null

	var json: Any {
		switch self {
		case .int(let i): return i
		case .text(let s): return s
		case .blob(let d): return d.base64EncodedString()
		case .float(let d): return d
		case .bool(let b): return b
		case .null: return NSNull()
		}
	}
}

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

public enum ColumnType {
	case int
	case text
	case blob
	case float
}

public typealias TableDefinition = OrderedDictionary<String, ColumnDefinition>

public struct ColumnDefinition {
	var type: ColumnType
	var isPartOfPrimaryKey: Bool = false
	var isNullable: Bool = false
}

public extension Result {
	subscript(column: String) -> Value! {
		if let idx = self.columns.index(of: column) {
			return self.values[idx]
		}
		return nil
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

public extension Database {
	func transaction<T>(name: String? = nil, callback: @escaping (() throws -> (T))) throws -> T {
		return try self.transaction(name: name, alwaysRollback: false, callback: callback)
	}

	func hypothetical<T>(callback: @escaping (() throws -> (T))) throws -> T {
		return try self.transaction(name: nil, alwaysRollback: true, callback: callback)
	}
}
