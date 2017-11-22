import Foundation
import Socket
import Dispatch
import LoggerAPI

fileprivate extension UnsignedInteger {
	init(_ bytes: [UInt8]) {
		precondition(bytes.count <= MemoryLayout<Self>.size)

		var value : UInt64 = 0

		for byte in bytes {
			value <<= 8
			value |= UInt64(byte)
		}

		self.init(value)
	}
}

fileprivate extension Character {
	var codePoint: Int {
		get {
			let s = String(self).unicodeScalars
			return Int(s[s.startIndex].value)
		}
	}
}

fileprivate extension Data {
	mutating func append<T>(bytesOf value: T) {
		var value = value
		let byteCount = MemoryLayout<T>.size
		withUnsafePointer(to: &value) { ptr in
			ptr.withMemoryRebound(to: UInt8.self, capacity: byteCount) { rptr in
				self.append(rptr, count: byteCount)
			}
		}
	}
}

enum PQSeverity: String {
	case error = "ERROR"
	case fatal = "FATAL"
	case info = "INFO"
}

struct PQField {
	var name: String
	var tableId: Int32 = 0
	var columnId: Int16 = 0
	var type: PQFieldType
	var typeModifier: Int32 = -1
}

/** List of Postgres types by Oid. More can be found by querying a Postgres instance:
SELECT ' case ' || typname || ' = ' || oid FROM pg_type; */
enum PQFieldType: Int32 {
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

enum PQValue {
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

private enum PQFrontendMessage: String {
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

public final class QueryClientConnection {
	private enum State {
		case new
		case ready
		case querying
		case closed
	}

	let socket: Socket
	private(set) var username: String? = nil
	private(set) var password: String? = nil
	private var state: State = .new
	private static let bufferSize = 4096
	private weak var server: QueryServer?

	private static let isLittleEndian = Int32(42).littleEndian == Int32(42)

	init(socket: Socket, server: QueryServer) {
		self.socket = socket
		self.server = server
		self.run()
	}

	deinit {
		switch self.state {
		case .closed:
			break
		default:
			self.server?.connection(didClose: self)
			self.socket.close()
		}
	}

	private func readInt32() -> UInt32? {
		var data: [CChar] =   Array(repeating: CChar(0), count: 4)

		do {
			let n = try self.socket.read(into: &data, bufSize: 4, truncate: true)
			if n == 4 {
				let x = data.map { return UInt8(bitPattern: $0) }
				return QueryClientConnection.isLittleEndian ? UInt32(x) : UInt32(x).byteSwapped
			}
			else {
				return nil
			}
		}
		catch {
			return nil
		}
	}

	private func readParameters(length: UInt32) throws -> [String: String]? {
		if let data = try self.read(length: length) {
			let elements = data.split(separator: 0x0, maxSplits: Int.max, omittingEmptySubsequences: false)
			let strs = elements.map { d -> String in
				let dx = Data(d)
				return String(data: dx, encoding: .utf8) ?? ""
			}

			var parameters: [String: String] = [:]
			for idx in stride(from: 0, to: strs.count, by: 2) {
				parameters[strs[idx]] = strs[idx+1]
			}
			return parameters
		}
		return nil
	}

	private func read(length: UInt32) throws -> Data? {
		var data = Data(capacity: Int(length))
		while data.count < Int(length) {
			let n = try self.socket.read(into: &data)
			if n <= 0 {
				return nil
			}
		}
		return data
	}

	private func readByte() throws -> CChar? {
		var data: [CChar] =   Array(repeating: CChar(42), count: 1)

		let c = try self.socket.read(into: &data, bufSize: 1, truncate: true)
		if c == 0 {
			// Disconnected
			return nil
		}
		else if c < 0 {
			return nil
		}
		else {
			return data[0]
		}
	}

	private func readAuthentication() throws -> String? {
		if try self.readByte() == CChar(Character("p").codePoint) {
			// Password authentication, get password
			if let len = self.readInt32(), let pwData = try self.read(length: len - UInt32(4)) {
				return String(data: pwData.subdata(in: 0..<Int(len-4-1)), encoding: .utf8)
			}
			else {
				return nil
			}
		}
		else {
			return nil
		}
	}

	private func readQuery() throws -> Bool {
		if let type = try self.readByte() {
			if type == CChar(Character("Q").codePoint) {
				// Query
				if let len = self.readInt32(), let queryData = try self.read(length: len - UInt32(4)) {
					let trimmed = queryData.subdata(in: 0..<(queryData.endIndex.advanced(by: -1)))
					if let q = String(data: trimmed, encoding: .utf8) {
						self.state = .querying
						self.server?.query(q, connection: self)
						return true
					}
					return false
				}
				else {
					return false
				}
			}
			else if type == CChar(Character("X").codePoint) {
				// Exit
				return false
			}
			else if type == CChar(Character("P").codePoint) {
				/* Parse message: this should parse a statement into a prepared statement and store
				it somewhere in a [name: statement] dictionary. If the name is omitted, the statement
				is erased at the next Parse (with name=unnamed) or Query. A stored prepared statement
				cannot be overwritten unless first closed (except for the unnamed one).

				Byte1('P')	Identifies the message as a Parse command.
				Int32		Length of message contents in bytes, including self.
				String		The name of the destination prepared statement (an empty string selects
							the unnamed prepared statement).
				String		The query string to be parsed.
				Int16		The number of parameter data types specified (may be zero). Note that this
							is not an indication of the number of parameters that might appear in the
							query string, only the number that the frontend wants to prespecify types
							for.

				Then, for each parameter, there is the following:

				Int32		Specifies the object ID of the parameter data type. Placing a zero here is
							equivalent to leaving the type unspecified. */
				if let len = self.readInt32(), let messageData = try self.read(length: len - UInt32(4)) {
					/*let trimmed = queryData.subdata(in: 0..<(queryData.endIndex.advanced(by: -1)))
					return String(data: trimmed, encoding: .utf8)*/
					Log.info("[PSQL] Client attempts to use prepared statements, which is not currently supported.")
					Log.debug("[PSQL] Message data: \(messageData)")
					return false
				}
				else {
					return false
				}
			}
			else if type == CChar(Character("B").codePoint) {
				/* Bind message
				Byte1('B')	Identifies the message as a Bind command.
				Int32		Length of message contents in bytes, including self.
				String		The name of the destination portal (an empty string selects the unnamed
							portal).
				String		The name of the source prepared statement (an empty string selects the
							unnamed prepared statement).
				Int16		The number of parameter format codes that follow (denoted C below). This
							can be zero to indicate that there are no parameters or that the parameters
							all use the default format (text); or one, in which case the specified
							format code is applied to all parameters; or it can equal the actual number
							of parameters.
				Int16[C]	The parameter format codes. Each must presently be zero (text) or one (binary).
				Int16		The number of parameter values that follow (possibly zero). This must match
							the number of parameters needed by the query.

				Next, the following pair of fields appear for each parameter:

				Int32		The length of the parameter value, in bytes (this count does not include
							itself). Can be zero. As a special case, -1 indicates a NULL parameter
							value. No value bytes follow in the NULL case.
				Byten		The value of the parameter, in the format indicated by the associated
							format code. n is the above length.

				After the last parameter, the following fields appear:

				Int16		The number of result-column format codes that follow (denoted R below).
							This can be zero to indicate that there are no result columns or that the
							result columns should all use the default format (text); or one, in which
							case the specified format code is applied to all result columns (if any);
							or it can equal the actual number of result columns of the query.

				Int16[R]	The result-column format codes. Each must presently be zero (text) or
							one (binary).

				Should send message BindComplete to client:
				Byte1('2')	Identifies the message as a Bind-complete indicator.
				Int32(4)	Length of message contents in bytes, including self.
				*/
				return false
			}
			else if type == CChar(Character("E").codePoint) {
				/* Execute message.
				Byte1('E')	Identifies the message as an Execute command.
				Int32		Length of message contents in bytes, including self.
				String		The name of the portal to execute (an empty string selects the unnamed
							portal).
				Int32		Maximum number of rows to return, if portal contains a query that returns
							rows (ignored otherwise). Zero denotes "no limit". */
				return false
			}
			else if type == CChar(Character("C").codePoint) {
				/* Close message
				Byte1('C')	Identifies the message as a Close command.
				Int32		Length of message contents in bytes, including self.
				Byte1		'S' to close a prepared statement; or 'P' to close a portal.
				String		The name of the prepared statement or portal to close (an empty string
							selects the unnamed prepared statement or portal). */
				return false
			}
			else {
				return false
			}
		}
		else {
			return false
		}
	}

	func send(row: [PQValue]) throws {
		assert(self.state == .querying, "not querying!")
		var buf = Data()

		for value in row {
			switch value {
			case .null:
				buf.append(bytesOf: Int32(0).bigEndian)
			default:
				let data = value.text.data(using: .utf8)!
				buf.append(bytesOf: Int32(data.count + 1).bigEndian)
				buf.append(data)
				buf.append(0)
			}
		}

		var packet = Data()
		packet.append(UInt8(Character("D").codePoint))
		packet.append(bytesOf: Int32(buf.count + 4 + 2).bigEndian)
		packet.append(bytesOf: Int16(row.count).bigEndian)
		packet.append(buf)
		try self.socket.write(from: packet)
	}

	func send(error: String, severity: PQSeverity = .error, code: String = "42000", endsQuery: Bool = true) throws {
		assert(self.state == .querying, "not querying!")

		var buf = Data()
		buf.append(UInt8(Character("S").codePoint))
		let sd = severity.rawValue.data(using: .utf8)!
		buf.append(sd)
		buf.append(0)

		buf.append(UInt8(Character("C").codePoint))
		let cd = code.data(using: .utf8)!
		buf.append(cd)
		buf.append(0)

		buf.append(UInt8(Character("M").codePoint))
		let md = error.data(using: .utf8)!
		buf.append(md)
		buf.append(0)

		// Message terminator
		buf.append(0)


		var packet = Data()
		packet.append(UInt8(Character("E").codePoint))
		packet.append(bytesOf: Int32(buf.count + 4).bigEndian)
		packet.append(buf)
		try self.socket.write(from: packet)

		if endsQuery {
			self.state = .ready
			self.run()
		}
	}

	func send(description: [PQField]) throws {
		assert(self.state == .querying, "not querying!")
		var buffer = Data()

		for field in description {
			let fn = field.name.data(using: .utf8)
			buffer.append(fn!)
			buffer.append(0)
			buffer.append(bytesOf: field.tableId.bigEndian)
			buffer.append(bytesOf: field.columnId.bigEndian)
			buffer.append(bytesOf: field.type.rawValue.bigEndian)
			buffer.append(bytesOf: field.type.typeSize.bigEndian)
			buffer.append(bytesOf: field.typeModifier.bigEndian)
			buffer.append(bytesOf: Int16(0).bigEndian) // Binary=1, text=0
		}

		var packet = Data()
		packet.append(UInt8(Character("T").codePoint))
		packet.append(bytesOf: Int32(6 + buffer.count).bigEndian)
		packet.append(bytesOf: Int16(description.count).bigEndian)
		packet.append(buffer)
		try self.socket.write(from: packet)
	}

	func sendQueryComplete(tag: String) throws {
		let data = tag.data(using: .utf8)!

		var packet = Data()
		packet.append(bytesOf: UInt8(Character("C").codePoint))
		packet.append(bytesOf: UInt32(data.count + 4 + 1).bigEndian)
		packet.append(data)
		packet.append(UInt8(0))
		try self.socket.write(from: packet)

		self.state = .ready
		self.run()
	}

	private func run() {
		// Get the global concurrent queue...
		let queue = DispatchQueue.global(qos: .default)

		// Create the run loop work item and dispatch to the default priority global queue...
		queue.async { [unowned self] in
			var shouldKeepRunning = true
			do {
				switch self.state {
				case .new:
					if let len = self.readInt32(), let msg = self.readInt32() {
						if len == 8 && msg == 80877103 {
							// No SSL, thank you
							try self.socket.write(from: "N")
						}
						else if len > UInt32(8) {
							// Read client version number
							let majorVersion = msg >> 16
							let minorVersion = msg & 0xFFFF
							Log.debug("[PSQL] PSQL \(majorVersion).\(minorVersion)")

							// Read parameters
							if let p = try self.readParameters(length: len - UInt32(8)) {
								Log.debug("[PSQL] Parameters: \(p)")
								self.username = p["user"]

								// Send authentication request
								let buf = Data(bytes: [UInt8(Character("R").codePoint), 0, 0, 0, 8, 0, 0, 0, 3])
								try self.socket.write(from: buf)

								// Read authentication
								if let pw = try self.readAuthentication() {
									self.password = pw

									// Send authentication success
									let buf = Data(bytes: [UInt8(Character("R").codePoint), 0, 0, 0, 8, 0, 0, 0, 0])
									try self.socket.write(from: buf)

									self.state = .ready
								}
								else {
									shouldKeepRunning = false
								}
							}
							else {
								shouldKeepRunning = false
							}
						}
						else {
							self.state = .closed
							shouldKeepRunning = false
						}
					}

				case .ready:
					// Send 'ready for query' (Z 5 I)
					let buf = Data(bytes: [UInt8(Character("Z").codePoint), 0, 0, 0, 5, UInt8(Character("I").codePoint)])
					try self.socket.write(from: buf)
					if try self.readQuery() {
						return
					}
					else {
						shouldKeepRunning = false
					}

				case .querying:
					return

				case .closed:
					return
				}
			}
			catch {
				shouldKeepRunning = false
			}

			if shouldKeepRunning {
				self.run()
			}
			else {
				self.close()
			}
		}
	}

	func close() {
		switch self.state {
		case .closed:
			break
		default:
			self.socket.close()
			self.state = .closed
			self.server?.connection(didClose: self)
		}
	}
}

public class QueryServer {
	public enum Family {
		case ipv4
		case ipv6

		fileprivate var socketFamily: Socket.ProtocolFamily {
			switch self {
			case .ipv4: return Socket.ProtocolFamily.inet
			case .ipv6: return Socket.ProtocolFamily.inet6
			}
		}
	}

	let port: Int
	let family: Family

	private var connectedSockets = [Int32: QueryClientConnection]()
	private var listenSocket: Socket? = nil
	private var continueRunning = true

	private let socketLockQueue = DispatchQueue(label: "popsiql.socketLock")

	init(port: Int, family: Family = .ipv6) {
		self.port = port
		self.family = family
	}

	deinit {
		self.connectedSockets = [:]
		self.listenSocket?.close()
	}

	func connection(didClose connection: QueryClientConnection) {
		let fd = connection.socket.socketfd
		self.socketLockQueue.async {
			self.connectedSockets[fd] = nil
		}
	}

	func query(_ query: String, connection: QueryClientConnection) {
		fatalError("Must override")
	}

	public func run() {
		let queue = DispatchQueue.global(qos: .userInteractive)

		queue.async { [weak self] in
			do {
				// Create an IPV6 socket...
				if let s = self {
					s.listenSocket = try Socket.create(family: s.family.socketFamily)

					guard let socket = self?.listenSocket else {
						return
					}

					try socket.listen(on: s.port)
				}
				else {
					return
				}

				repeat {
					if let s = self?.listenSocket {
						let newSocket = try s.acceptClientConnection()
						self?.addNewConnection(socket: newSocket)
					}

				} while (self?.continueRunning ?? false)

			}
			catch let error {
				guard let socketError = error as? Socket.Error else {
					Log.error("[SocketServer] Unexpected error...")
					return
				}

				if self?.continueRunning ?? false {
					Log.error("[SocketServer] Error reported:\n \(socketError.description)")
				}
			}
		}
	}

	private func addNewConnection(socket: Socket) {
		do {
			try socket.setBlocking(mode: true)
		}
		catch {
			Log.error("Could not set blocking mode: \(error.localizedDescription)")
		}

		// Add the new socket to the list of connected sockets...
		socketLockQueue.sync { [unowned self, socket] in
			self.connectedSockets[socket.socketfd] = QueryClientConnection(socket: socket, server: self)
		}
	}
}
