import Foundation
import Socket
import Dispatch
import HeliumLogger

internal enum PQSeverity: String {
	case error = "ERROR"
	case fatal = "FATAL"
	case info = "INFO"
}

public struct PQField {
	var name: String
	var tableId: Int32 = 0
	var columnId: Int16 = 0
	var type: PQFieldType
	var typeModifier: Int32 = -1

	public init(name: String, type: PQFieldType) {
		self.name = name
		self.type = type
	}
}

/** List of Postgres types by Oid. More can be found by querying a Postgres instance:
SELECT ' case ' || typname || ' = ' || oid FROM pg_type; */
public enum PQFieldType: Int32 {
	case int = 23
	case text = 25
	case bool = 16
	case float4 = 700
	case float8 = 701
	case null = 0

	var typeSize: Int16 {
		switch self {
		case .int: return 4
		case .bool: return 1
		case .float4: return 4
		case .float8: return 8
		case .null: return 0
		case .text: return -1 // variable length
		}
	}
}

public enum PQValue {
	case int(Int32)
	case text(String)
	case bool(Bool)
	case float4(Double)
	case float8(Double)
	case null

	var type: PQFieldType {
		switch self {
		case .bool(_): return .bool
		case .float4(_): return .float4
		case .float8(_): return .float8
		case .int(_): return .int
		case .text(_): return .text
		case .null: return .null
		}
	}

	var text: String {
		switch self {
		case .text(let s): return s
		case .null: return ""
		case .bool(let b): return b ? "t" : "f"
		case .float4(let d): return "\(d)"
		case .float8(let d): return "\(d)"
		case .int(let i): return "\(i)"
		}
	}
}

public enum QueryServerError: LocalizedError {
	case protocolError
	case preparedStatementNotFound
	case portalAlreadyExists
	case portalNotFound
	case preparedStatementAlreadyExists

	public var errorDescription: String? {
		switch self {
		case .protocolError: return "protocol error"
		case .preparedStatementNotFound: return "prepared statement was not found"
		case .portalAlreadyExists: return "portal already exists"
		case .preparedStatementAlreadyExists: return "prepared statement already exists"
		case .portalNotFound: return "portal not found"
		}
	}
}

internal enum PQFrontendMessage: String {
	case password = "p"
	case simpleQuery = "Q"
	case parse = "P"
	case bind = "B"
	case execute = "E"
	case describe = "D"
	case close = "C"
	case flush = "H"
	case sync = "S"
	case functionCall = "F"
	case copyData = "d"
	case copyCompletion = "c"
	case copyFailure = "f"
	case termination = "X"
}
