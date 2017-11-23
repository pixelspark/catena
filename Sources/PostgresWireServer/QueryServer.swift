import Foundation
import Socket
import LoggerAPI
import Dispatch

public protocol PreparedStatement {
	/** Whether execution of this statement will (can) return any rows. Usually 'true' for SELECT,
	'false' for DDL/DML statements. */
	var willReturnRows: Bool { get }

	func fields() throws -> [PQField]
}

public protocol ResultSet: class {
	var error: String? { get }
	var hasRow: Bool { get }
	func row() throws -> [PQValue]
}

open class QueryServer<PreparedStatementType: PreparedStatement> {
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

	private var connectedSockets = [Int32: QueryClientConnection<PreparedStatementType>]()
	private var listenSocket: Socket? = nil
	private var continueRunning = true

	private let socketLockQueue = DispatchQueue(label: "popsiql.socketLock")

	public init(port: Int, family: Family = .ipv6) {
		self.port = port
		self.family = family
	}

	deinit {
		self.connectedSockets = [:]
		self.listenSocket?.close()
	}

	func connection(didClose connection: QueryClientConnection<PreparedStatementType>) {
		let fd = connection.socket.socketfd
		self.socketLockQueue.async {
			self.connectedSockets[fd] = nil
		}
	}

	/** Overriden by child classes; returns a prepared statement for the given SQL */
	open func prepare(_ sql: String, connection: QueryClientConnection<PreparedStatementType>) throws -> PreparedStatementType {
		fatalError("Must override")
	}

	/** Overridden by child classes to perform queries. Should return nil for empty results (e.g.
	DML/DDL commands) when statement.willReturnRows is false. */
	open func query(_ query: PreparedStatementType, connection: QueryClientConnection<PreparedStatementType>, callback: @escaping (ResultSet?) throws -> ()) throws {
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
					return
				}

				if self?.continueRunning ?? false {
					Log.error("[PSQL] Error reported:\n \(socketError.description)")
				}
			}
		}
	}

	private func addNewConnection(socket: Socket) {
		do {
			try socket.setBlocking(mode: true)
		}
		catch {
			Log.error("[PSQL] Could not set blocking mode: \(error.localizedDescription)")
		}

		// Add the new socket to the list of connected sockets...
		socketLockQueue.sync { [unowned self, socket] in
			self.connectedSockets[socket.socketfd] = QueryClientConnection<PreparedStatementType>(socket: socket, server: self)
		}
	}
}
