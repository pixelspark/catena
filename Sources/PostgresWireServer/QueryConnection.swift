import Foundation
import Socket
import Dispatch
import LoggerAPI

public final class QueryClientConnection<PreparedStatementType: PreparedStatement> {
	private enum State {
		case new
		case ready // building a query
		case querying // sending results
		case closed
	}

	public private(set) var username: String? = nil
	public private(set) var password: String? = nil
	public private(set) var majorVersion: UInt16? = nil
	public private(set) var minorVersion: UInt16? = nil

	let socket: Socket
	private var state: State = .new
	private weak var server: QueryServer<PreparedStatementType>?
	private var portals: [String: Portal<PreparedStatementType>] = [:]
	private var preparedStatements: [String: PreparedStatementType] = [:]
	private var currentPortalName: String? = nil
	private var bufferedData = Data()

	private let bufferSize = 4096
	private let isLittleEndian = Int32(42).littleEndian == Int32(42)

	public init(socket: Socket, server: QueryServer<PreparedStatementType>) {
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
		do {
			if let data = try self.read(length: 4) {
				let x = data.map { return UInt8($0) }
				return self.isLittleEndian ? UInt32(x) : UInt32(x).byteSwapped
			}
			return nil
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

		// Do we have any leftover bytes?
		if !bufferedData.isEmpty {
			let maxBuffered = min(Int(length), bufferedData.count)
			data.append(bufferedData.subdata(in: 0..<maxBuffered))
			bufferedData.removeSubrange(0..<maxBuffered)
		}

		// Fetch bytes from socket
		while data.count < Int(length) {
			let n = try self.socket.read(into: &data)
			if n <= 0 {
				return nil
			}
		}

		// Save leftover bytes for later
		if data.count > length {
			bufferedData.append(data.subdata(in: Int(length)..<data.count))
			data.removeSubrange(Int(length)..<data.count)
		}
		return data
	}

	private func readByte() throws -> CChar? {
		if let data = try self.read(length: 1) {
			let values = data.map { $0 }
			return CChar(values[0])
		}
		return nil
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

	private func readBind() throws -> Bool {
		/* Bind message
		Byte1('B')	Identifies the message as a Bind command.
		Int32		Length of message contents in bytes, including self.
		String		The name of the destination portal (an empty string selects the unnamed
		portal).
		String		The name of the source prepared statement (an empty string selects the
		unnamed prepared statement).
		*/
		if let messageLength = self.readInt32(), let messageData = try self.read(length: messageLength - 4) {
			var reader = DataReader(data: messageData)
			if 	let destinationPortalName = reader.readZeroTerminatedString(),
				let preparedStatementName = reader.readZeroTerminatedString(),
				let numberOfParameterFormatCodes = reader.readUInt16() {
				/* Int16	The number of parameter format codes that follow (denoted C below). This
							can be zero to indicate that there are no parameters or that the parameters
							all use the default format (text); or one, in which case the specified
							format code is applied to all parameters; or it can equal the actual number
							of parameters.

				Int16[C]	The parameter format codes. Each must presently be zero (text) or one (binary). */
				let parameterFormatCodes = try (0..<numberOfParameterFormatCodes).map { _ -> PQFormat in
					guard let r = reader.readUInt16() else { throw QueryServerError.protocolError }
					guard let format = PQFormat(rawValue: r) else { throw QueryServerError.protocolError }
					return format
				}

				/*
				Int16	The number of parameter values that follow (possibly zero). This must match
						the number of parameters needed by the query.

				Next, the following pair of fields appear for each parameter:

				Int32	The length of the parameter value, in bytes (this count does not include
						itself). Can be zero. As a special case, -1 indicates a NULL parameter
						value. No value bytes follow in the NULL case.
				Byten	The value of the parameter, in the format indicated by the associated
						format code. n is the above length. */
				guard let numberOfParameterValues = reader.readUInt16() else { throw QueryServerError.protocolError }

				let parameterValues = try (0..<numberOfParameterValues).map { _ -> Data in
					guard let length = reader.readUInt32() else { throw QueryServerError.protocolError }
					guard let bytes = reader.readBytes(Int(length)) else { throw QueryServerError.protocolError }
					return bytes
				}

				/* After the last parameter, the following fields appear:

				Int16	The number of result-column format codes that follow (denoted R below).
						This can be zero to indicate that there are no result columns or that the
						result columns should all use the default format (text); or one, in which
						case the specified format code is applied to all result columns (if any);
						or it can equal the actual number of result columns of the query.

				Int16[R] The result-column format codes. Each must presently be zero (text) or
				one (binary). */
				/*guard let numberOfResultFormatCodes =  reader.readUInt16() else { throw QueryServerError.protocolError }
				let resultFormatCodes = try (0..<(Int(numberOfResultFormatCodes))).map { _ -> PQFormat in
					guard let r = reader.readUInt16() else { throw QueryServerError.protocolError }
					guard let format = PQFormat(rawValue: r) else { throw QueryServerError.protocolError }
					return format
				}*/

				let parsedParameterValues = try self.parse(parameters: parameterValues, formats: parameterFormatCodes)

				if let statement = self.preparedStatements[preparedStatementName] {
					if let _ = self.portals[destinationPortalName], !destinationPortalName.isEmpty {
						throw QueryServerError.portalAlreadyExists
					}
					else {
						self.portals[destinationPortalName] = Portal(statement: statement, parameters: parsedParameterValues)

						/* Should send message BindComplete to client:
						Byte1('2')	Identifies the message as a Bind-complete indicator.
						Int32(4)	Length of message contents in bytes, including self. */
						let buf = Data(bytes: [UInt8(Character("2").codePoint), 0, 0, 0, 5])
						try self.socket.write(from: buf)
						self.state = .ready
						return true
					}
				}
				else {
					// Statement not found
					throw QueryServerError.preparedStatementNotFound
				}
			}
			return true
		}
		else {
			return false
		}
	}

	private func parse(parameters: [Data], formats: [PQFormat]) throws -> [PQValue] {
		var values = Array<PQValue>(repeating: PQValue.null, count: parameters.count)
		for (idx, data) in parameters.enumerated() {
			let format: PQFormat
			if idx < formats.count {
				format = formats[idx]
			}
			else {
				// When there is only one format code, this is the one we will use
				// Otherwise, default to text.
				format = (formats.count == 1) ? formats[0] : .text
			}

			switch format {
			case .text:
				if let s = String(data: data, encoding: .utf8) {
					values.append(PQValue.text(s))
				}
				else {
					values.append(PQValue.null)
				}

			case .binary:
				if let s = String(data: data, encoding: .utf8) {
					values.append(PQValue.text(s))
				}
				else {
					values.append(PQValue.null)
				}
			}
		}
		return values
	}

	private func readClose() throws -> Bool {
		/* Close message
		Byte1('C')	Identifies the message as a Close command.
		Int32		Length of message contents in bytes, including self.
		Byte1		'S' to close a prepared statement; or 'P' to close a portal.
		String		The name of the prepared statement or portal to close (an empty string
		selects the unnamed prepared statement or portal). */
		if let messageLength = self.readInt32(), let messageData = try self.read(length: messageLength - 4) {
			var reader = DataReader(data: messageData)
			if let type = reader.readBytes(1), let name = reader.readZeroTerminatedString() {
				if type[0] == CChar(Character("S").codePoint) {
					// Close prepared statement
					if let _ = self.preparedStatements[name] {
						self.preparedStatements[name] = nil

						// Send close complete
						let buf = Data(bytes: [UInt8(Character("3").codePoint), 0, 0, 0, 5])
						try self.socket.write(from: buf)
						return true
					}
					else {
						throw QueryServerError.preparedStatementNotFound
					}
				}
				else if type[0] == CChar(Character("P").codePoint) {
					// Close portal
					if let _ = self.portals[name] {
						self.portals[name] = nil

						// Send close complete
						let buf = Data(bytes: [UInt8(Character("3").codePoint), 0, 0, 0, 5])
						try self.socket.write(from: buf)
						return true
					}
					else {
						throw QueryServerError.portalNotFound
					}
				}
				else {
					throw QueryServerError.protocolError
				}
			}
		}
		return false
	}

	private func readParse() throws -> Bool {
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
			var reader = DataReader(data: messageData)

			if let destinationName = reader.readZeroTerminatedString(),
				let query = reader.readZeroTerminatedString() {
				// let numParameterType = reader.readUInt16() {
				// let parameterTypes = (0..<numParameterType).map { _ in return reader.readUInt32() ?? 0 }

				// Remember prepared statement
				guard let server = self.server else { return false }
				let statement = try server.prepare(query, connection: self)
				if self.preparedStatements[destinationName] != nil && !destinationName.isEmpty {
					throw QueryServerError.preparedStatementAlreadyExists
				}
				self.preparedStatements[destinationName] = statement

				// Send ParseComplete message ('1' + In32(5) indicating length of total message)
				let buf = Data(bytes: [UInt8(Character("1").codePoint), 0, 0, 0, 5])
				try self.socket.write(from: buf)
				self.state = .ready
				return true
			}
		}
		return false
	}

	private func sendReadyForQuery() throws {
		// Send 'ready for query' (Z 5 I)
		let buf = Data(bytes: [UInt8(Character("Z").codePoint), 0, 0, 0, 5, UInt8(Character("I").codePoint)])
		try self.socket.write(from: buf)
	}

	private func sendRowDescription(for statement: PreparedStatementType) throws {
		if statement.willReturnRows {
			/* RowDescription (B)
			Byte1('T')	Identifies the message as a row description.
			Int32		Length of message contents in bytes, including self.
			Int16		Specifies the number of fields in a row (may be zero).

			Then, for each field, there is the following:

			String		The field name.
			Int32		If the field can be identified as a column of a specific table, the object ID
			of the table; otherwise zero.
			Int16		If the field can be identified as a column of a specific table, the attribute
			number of the column; otherwise zero.
			Int32		The object ID of the field's data type.
			Int16		The data type size (see pg_type.typlen). Note that negative values denote
			variable-width types.
			Int32		The type modifier (see pg_attribute.atttypmod). The meaning of the modifier
			is type-specific.
			Int16		The format code being used for the field. Currently will be zero (text) or
			one (binary). In a RowDescription returned from the statement variant of
			Describe, the format code is not yet known and will always be zero. */

			/// Request columns from prepared statement and send description
			if let cp = self.currentPortalName, let portal = self.portals[cp] {
				try send(description: try statement.fields(for: portal.parameters))
			}
			else {
				try send(description: try statement.fields(for: []))
			}
		}
		else {
			// Send NoData response
			let buf = Data(bytes: [UInt8(Character("n").codePoint), 0, 0, 0, 5])
			try self.socket.write(from: buf)
		}
	}

	/** Send a result set back to the client. */
	private func send(result: ResultSet) throws {
		if let e = result.error {
			try self.send(error: e)
			return
		}

		// Send back result
		while result.hasRow {
			let row = try result.row()
			try self.send(row: row)
		}
	}

	private func readDescribe() throws -> Bool {
		/* Describe (F)
		Byte1('D')	Identifies the message as a Describe command.
		Int32	Length of message contents in bytes, including self.
		Byte1	'S' to describe a prepared statement; or 'P' to describe a portal.
		String	The name of the prepared statement or portal to describe (an empty string
		selects the unnamed prepared statement or portal). */
		if let messageLength = self.readInt32(), let messageData = try self.read(length: messageLength - 4) {
			var reader = DataReader(data: messageData)

			if let type = reader.readBytes(1), let name = reader.readZeroTerminatedString() {
				if type[0] == UInt8(Character("S").codePoint) {
					// Describe statement
					guard let s = self.preparedStatements[name] else { throw QueryServerError.preparedStatementNotFound }

					// Send parameter description packet
					// 't' + length of message (Int32) + parameter count (Int16)
					let buf = Data(bytes: [UInt8(Character("t").codePoint), 0, 0, 0, 7, 0, 0])
					try self.socket.write(from: buf)
					try self.sendRowDescription(for: s)
					return true
				}
				else if type[0] == UInt8(Character("P").codePoint) {
					// Describe portal
					guard let s = self.portals[name] else { throw QueryServerError.portalNotFound }
					try self.sendRowDescription(for: s.statement)
					return true
				}
				else {
					throw QueryServerError.protocolError
				}
			}
			else {
				throw QueryServerError.protocolError
			}
		}
		return false
	}

	/** Reads the next packet in preparing/ready state. Returns whether the connection should continue
	to process packets. */
	private func readQuery() throws -> Bool {
		switch self.state {
		case .ready, .closed: break
		default: fatalError("invalid state")
		}

		if let type = try self.readByte() {
			if type == CChar(Character("Q").codePoint) {
				// Query
				if let len = self.readInt32(), let queryData = try self.read(length: len - UInt32(4)) {
					let trimmed = queryData.subdata(in: 0..<(queryData.endIndex.advanced(by: -1)))
					if let q = String(data: trimmed, encoding: .utf8) {
						if let server = self.server {
							let st = try server.prepare(q, connection: self)
							self.state = .querying
							self.currentPortalName = nil
							try self.sendRowDescription(for: st)

							// When result is nil, there is no result
							try server.query(st, parameters: [], connection: self) { resultSet in
								if let result = resultSet {
									assert(st.willReturnRows, "results may only be returned when the statement promised to do so")
									try self.send(result: result)
								}
								else {
									assert(!st.willReturnRows, "statements that promise to return rows should result in a non-nil result set")
								}

								// TODO: obtain tag from prepared statement
								try self.sendQueryComplete(tag: "SELECT")
								try self.sendReadyForQuery()
								self.state = .ready
							}
						}
						return true
					}
					return false
				}
				else {
					return false
				}
			}
			else if type == CChar(Character("X").codePoint) {
				return false
			}
			else if type == CChar(Character("P").codePoint) {
				return try self.readParse()
			}
			else if type == CChar(Character("B").codePoint) {
				return try self.readBind()
			}
			else if type == CChar(Character("E").codePoint) {
				/* Execute message.
				Byte1('E')	Identifies the message as an Execute command.
				Int32		Length of message contents in bytes, including self.
				String		The name of the portal to execute (an empty string selects the unnamed
				portal).
				Int32		Maximum number of rows to return, if portal contains a query that returns
				rows (ignored otherwise). Zero denotes "no limit". */
				if let messageLength = self.readInt32(), let messageData = try self.read(length: messageLength - 4) {
					var reader = DataReader(data: messageData)
					if let portalName = reader.readZeroTerminatedString() {
						guard let portal = self.portals[portalName] else { throw QueryServerError.portalNotFound }
						guard let s = self.server else { return false }
						self.state = .querying
						self.currentPortalName = portalName
						try s.query(portal.statement, parameters: portal.parameters, connection: self) { result in
							if let result = result {
								try self.send(result: result)
							}
							try self.sendQueryComplete(tag: "SELECT") // TODO fetch tag from command
						}

						return true
					}
				}
				return false
			}
			else if type == CChar(Character("C").codePoint) {
				return try self.readClose()
			}
			else if type == CChar(Character("D").codePoint) {
				return try self.readDescribe()
			}
			else if type == CChar(Character("S").codePoint) {
				/* Sync message
				Byte1('S')	Identifies the message as a Sync command.
				Int32(4)	Length of message contents in bytes, including self. */
				if let messageLength = self.readInt32() {
					_ = try self.read(length: messageLength - 4)

					// Close the active portal
					if let portalName = self.currentPortalName {
						self.portals[portalName] = nil
						self.currentPortalName = nil
						self.state = .ready
						try self.sendReadyForQuery()
						return true
					}
					else {
						throw QueryServerError.portalNotFound
					}
				}
				return false
			}
			else {
				// Unknown packet type
				throw QueryServerError.protocolError
			}
		}
		else {
			return false
		}
	}

	func send(row: [PQValue]) throws {
		switch self.state {
		case .querying, .closed: break
		default: fatalError("invalid state")
		}

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

	private func send(error: String, severity: PQSeverity = .error, code: String = "42000", endsQuery: Bool = true) throws {
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

	private func send(description: [PQField]) throws {
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

	private func sendQueryComplete(tag: String) throws {
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
							self.majorVersion = UInt16(msg >> 16)
							self.minorVersion = UInt16(msg & 0xFFFF)

							// Read parameters
							if let p = try self.readParameters(length: len - UInt32(8)) {
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
									try self.sendReadyForQuery()
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
							shouldKeepRunning = false
						}
					}

				case .ready:
					if try !self.readQuery() {
						shouldKeepRunning = false
					}

				case .querying:
					return

				case .closed:
					return
				}
			}
			catch {
				try? self.send(error: error.localizedDescription)
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

/** A portal is a prepared statement with bound parameters. */
fileprivate class Portal<PreparedStatementType: PreparedStatement> {
	let statement: PreparedStatementType
	let parameters: [PQValue]
	var result: ResultSet? = nil

	init(statement: PreparedStatementType, parameters: [PQValue]) {
		self.statement = statement
		self.parameters = parameters
	}
}


fileprivate struct DataReader {
	private static let isLittleEndian = Int32(42).littleEndian == Int32(42)
	private var data: Data

	init(data: Data) {
		self.data = data
	}

	mutating func readZeroTerminatedString() -> String? {
		if let nullIndex = data.index(of: 0), let str = String(data: data.subdata(in: 0..<nullIndex), encoding: .utf8) {
			data = data.subdata(in: nullIndex.advanced(by: 1)..<data.endIndex)
			return str
		}
		return nil
	}

	mutating func readBytes(_ length: Int) -> Data? {
		if data.count >= length {
			let read = data.subdata(in: 0..<length)
			data = data.subdata(in: data.startIndex.advanced(by: length)..<data.endIndex)
			return read
		}
		return nil
	}

	mutating func readUInt16() -> UInt16? {
		if data.count >= 2 {
			let values = data.subdata(in: 0..<2).map { $0 }
			let number = DataReader.isLittleEndian ? UInt16(values) : UInt16(values).byteSwapped
			data = data.subdata(in: 2..<data.endIndex)
			return number
		}
		return nil
	}

	mutating func readUInt32() -> UInt32? {
		if data.count >= 4 {
			let values = data.subdata(in: 0..<4).map { $0 }
			let number = DataReader.isLittleEndian ? UInt32(values) : UInt32(values).byteSwapped
			data = data.subdata(in: 4..<data.endIndex)
			return number
		}
		return nil
	}
}

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
