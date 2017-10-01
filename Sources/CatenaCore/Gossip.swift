import Foundation
import Kitura
import KituraRequest
import LoggerAPI
import KituraWebSocket
import Dispatch

#if !os(Linux)
import Starscream
#endif

public enum GossipError: LocalizedError {
	case missingActionKey
	case unknownAction(String)
	case deserializationFailed

	public var localizedDescription: String {
		switch self {
		case .missingActionKey: return "action key is missing"
		case .unknownAction(let a): return "unknown action '\(a)'"
		case .deserializationFailed: return "deserialization of payload failed"
		}
	}
}

public enum Gossip<LedgerType: Ledger> {
	public typealias BlockchainType = LedgerType.BlockchainType
	public typealias BlockType = BlockchainType.BlockType

	/** Request the peer's index. The reply is either of type 'index' or 'passive'. */
	case query // -> index or passive

	/** Reply message that contains the peer's index. */
	case index(Index<BlockType>)

	/** Reply message that contains a block, either at the request of the peer, or unsolicited, when it is new */
	case block([String: Any]) // no reply

	/** Request a specific block from the peer. The reply is of type 'block' */
	case fetch(BlockType.HashType) // -> block

	/** An error has occurred. Only sent in reply to a request by the peer. */
	case error(String)

	/** A transaction the peer wants us to store/relay. */
	case transaction([String: Any])

	/** The peer indicates that it is a passive peer without an index (in response to a query request) */
	case passive

	/** The peer requests to be forgotten, most probably because its UUID does not match the requested UUID. */
	case forget

	init(json: [String: Any]) throws {
		if let q = json[LedgerType.ParametersType.actionKey] as? String {
			if q == "query" {
				self = .query
			}
			else if q == "block" {
				if let blockData = json["block"] as? [String: Any] {
					self = .block(blockData)
				}
				else {
					throw GossipError.deserializationFailed
				}
			}
			else if q == "fetch" {
				if let hash = json["hash"] as? String, let hashValue = BlockType.HashType(hash: hash) {
					self = .fetch(hashValue)
				}
				else {
					throw GossipError.deserializationFailed
				}
			}
			else if q == "index" {
				if let idx = json["index"] as? [String: Any], let index = Index<BlockType>(json: idx) {
					self = .index(index)
				}
				else {
					throw GossipError.deserializationFailed
				}
			}
			else if q == "error" {
				if let message = json["message"] as? String {
					self = .error(message)
				}
				else {
					throw GossipError.deserializationFailed
				}
			}
			else if q == "tx" {
				if let tx = json["tx"] as? [String: Any] {
					self = .transaction(tx)
				}
				else {
					throw GossipError.deserializationFailed
				}
			}
			else if q == "passive" {
				self = .passive
			}
			else if q == "forget" {
				self  = .forget
			}
			else {
				throw GossipError.unknownAction(q)
			}
		}
		else {
			throw GossipError.missingActionKey
		}
	}

	var json: [String: Any] {
		switch self {
		case .query:
			return [LedgerType.ParametersType.actionKey: "query"]

		case .block(let b):
			return [LedgerType.ParametersType.actionKey: "block", "block": b]

		case .index(let i):
			return [LedgerType.ParametersType.actionKey: "index", "index": i.json]

		case .fetch(let h):
			return [LedgerType.ParametersType.actionKey: "fetch", "hash": h.stringValue]

		case .transaction(let tx):
			return [LedgerType.ParametersType.actionKey: "tx", "tx": tx]

		case .error(let m):
			return [LedgerType.ParametersType.actionKey: "error", "message": m]

		case .passive:
			return [LedgerType.ParametersType.actionKey: "passive"]

		case .forget:
			return [LedgerType.ParametersType.actionKey: "forget"]
		}
	}
}

public class Server<LedgerType: Ledger>: WebSocketService {
	public typealias BlockchainType = LedgerType.BlockchainType
	public typealias BlockType = BlockchainType.BlockType

	public let router = Router()
	public let port: Int

	private let mutex = Mutex()
	private var gossipConnections = [String: PeerIncomingConnection<LedgerType>]()
	weak var node: Node<LedgerType>?

	init(node: Node<LedgerType>, port: Int) {
		self.node = node
		self.port = port

		WebSocket.register(service: self, onPath: "/")
		Kitura.addHTTPServer(onPort: port, with: router)
	}

	public func connected(connection: WebSocketConnection) {
		Log.debug("[Server] gossip connected incoming \(connection.request.remoteAddress) \(connection.request.urlURL.absoluteString)")

		self.mutex.locked {
			do {
				let pic = try PeerIncomingConnection<LedgerType>(connection: connection)
				self.node?.add(peer: pic)
				self.gossipConnections[connection.id] = pic
			}
			catch {
				Log.error("[Server] \(error.localizedDescription)")
			}
		}
	}

	public func disconnected(connection: WebSocketConnection, reason: WebSocketCloseReasonCode) {
		Log.info("[Server] disconnected gossip \(connection.id); reason=\(reason)")
		self.mutex.locked {
			self.gossipConnections.removeValue(forKey: connection.id)
		}
	}

	public func received(message: Data, from: WebSocketConnection) {
		do {
			if let d = try JSONSerialization.jsonObject(with: message, options: []) as? [Any] {
				try self.handleGossip(data: d, from: from)
			}
			else {
				Log.error("[Gossip] Invalid format")
			}
		}
		catch {
			Log.error("[Gossip] Invalid: \(error.localizedDescription)")
		}
	}

	public func received(message: String, from: WebSocketConnection) {
		do {
			if let d = try JSONSerialization.jsonObject(with: message.data(using: .utf8)!, options: []) as? [Any] {
				try self.handleGossip(data: d, from: from)
			}
			else {
				Log.error("[Gossip] Invalid format")
			}
		}
		catch {
			Log.error("[Gossip] Invalid: \(error.localizedDescription)")
		}
	}

	func handleGossip(data: [Any], from: WebSocketConnection) throws {
		Log.debug("[Gossip] received \(data)")

		self.mutex.locked {
			if let pic = self.gossipConnections[from.id] {
				DispatchQueue.global().async {
					pic.receive(data: data)
				}
			}
			else {
				Log.error("[Server] received gossip data for non-connection: \(from.id)")
			}
		}
	}
}

public struct Index<BlockType: Block>: Equatable {
	let genesis: BlockType.HashType
	let peers: [URL]
	let highest: BlockType.HashType
	let height: BlockType.IndexType
	let timestamp: BlockType.TimestampType

	init(genesis: BlockType.HashType, peers: [URL], highest: BlockType.HashType, height: BlockType.IndexType, timestamp: BlockType.TimestampType) {
		self.genesis = genesis
		self.peers = peers
		self.highest = highest
		self.height = height
		self.timestamp = timestamp
	}

	init?(json: [String: Any]) {
		if
			let genesisHash = json["genesis"] as? String,
			let highestHash = json["highest"] as? String,
			let genesis = BlockType.HashType(hash: genesisHash),
			let highest = BlockType.HashType(hash: highestHash),
			let peers = json["peers"] as? [String]
		{
			self.genesis = genesis
			self.highest = highest
			self.peers = peers.flatMap { return URL(string: $0) }

			if let height = json["height"] as? NSNumber, let timestamp = json["time"] as? NSNumber {
				// Darwin
				self.height = BlockType.IndexType(height.uint64Value)
				self.timestamp = BlockType.TimestampType(timestamp.uint64Value)
			}
			else if let height = json["height"] as? Int, let timestamp = json["time"] as? Int {
				// Linux
				self.height = BlockType.IndexType(height)
				self.timestamp = BlockType.TimestampType(timestamp)
			}
			else {
				return nil
			}

		}
		else {
			return nil
		}
	}

	public static func ==(lhs: Index<BlockType>, rhs: Index<BlockType>) -> Bool {
		return lhs.genesis == rhs.genesis &&
			lhs.peers == rhs.peers &&
			lhs.highest == rhs.highest &&
			lhs.height == rhs.height &&
			lhs.timestamp == rhs.timestamp
	}

	var json: [String: Any] {
		return [
			"highest": self.highest.stringValue,
			"height": NSNumber(value: self.height),
			"genesis": self.genesis.stringValue,
			"time": NSNumber(value: self.timestamp),
			"peers": self.peers.map { $0.absoluteString }
		]
	}
}

public protocol PeerConnectionDelegate {
	associatedtype LedgerType: Ledger
	func peer(connection: PeerConnection<LedgerType>, requests gossip: Gossip<LedgerType>, counter: Int)
	func peer(connected _: PeerConnection<LedgerType>)
	func peer(disconnected _: PeerConnection<LedgerType>)
}

public class PeerConnection<LedgerType: Ledger> {
	public typealias GossipType = Gossip<LedgerType>
	public typealias Callback = (Gossip<LedgerType>) -> ()

	public let mutex = Mutex()
	private var counter = 0
	private var callbacks: [Int: Callback] = [:]
	public weak var delegate: Peer<LedgerType>? = nil

	fileprivate init(isIncoming: Bool) {
		self.counter = isIncoming ? 1 : 0
	}

	public func receive(data: [Any]) {
		if data.count == 2, let counter = data[0] as? Int, let gossipData = data[1] as? [String: Any] {
			do {
				let g = try GossipType(json: gossipData)
				self.mutex.locked {
					if counter != 0, let cb = callbacks[counter] {
						self.callbacks.removeValue(forKey: counter)
						DispatchQueue.global().async {
							cb(g)
						}
					}
					else {
						// Unsolicited
						Log.debug("[Gossip] Get \(counter): \(g)")
						if let d = self.delegate {
							DispatchQueue.global().async {
								d.peer(connection: self, requests: g, counter: counter)
							}
						}
						else {
							Log.error("[Server] cannot handle gossip \(counter) for \(self): no delegate. Message is \(g.json)")
						}
					}
				}
			}
			catch {
				Log.warning("[Gossip] Received invalid gossip \(error.localizedDescription): \(gossipData)")
			}
		}
		else {
			Log.warning("[Gossip] Received malformed gossip: \(data)")
		}
	}

	public final func reply(counter: Int, gossip: GossipType) throws {
		try self.mutex.locked {
			let d = try JSONSerialization.data(withJSONObject: [counter, gossip.json], options: [])
			try self.send(data: d)
		}
	}

	public final func request(gossip: GossipType, callback: Callback? = nil) throws {
		let c = self.mutex.locked { () -> Int in
			counter += 2
			if let c = callback {
				self.callbacks[counter] = c
			}
			return counter
		}

		try self.mutex.locked {
			Log.debug("[PeerConnection] send request \(c)")
			let d = try JSONSerialization.data(withJSONObject: [c, gossip.json], options: [])
			try self.send(data: d)
		}
	}

	public func send(data: Data) throws {
		fatalError("Should be subclassed")
	}
}

enum PeerConnectionError: LocalizedError {
	case protocolVersionMissing
	case protocolVersionUnsupported(version: String)
	case notConnected

	var errorDescription: String? {
		switch self {
		case .protocolVersionMissing: return "the client did not indicate a protocol version"
		case .protocolVersionUnsupported(version: let v): return "protocol version '\(v)' is not supported"
		case .notConnected: return "the peer is not connected"
		}
	}
}

public class PeerIncomingConnection<LedgerType: Ledger>: PeerConnection<LedgerType>, CustomDebugStringConvertible {
	let connection: WebSocketConnection

	init(connection: WebSocketConnection) throws {
		guard let protocolVersion = connection.request.headers["Sec-WebSocket-Protocol"]?.first else { throw PeerConnectionError.protocolVersionMissing }
		if protocolVersion != LedgerType.ParametersType.protocolVersion {
			throw PeerConnectionError.protocolVersionUnsupported(version: protocolVersion)
		}

		self.connection = connection
		super.init(isIncoming: true)
	}

	deinit {
		self.connection.close()
	}

	func close() {
		self.connection.close()
	}

	public override func send(data: Data) throws {
		self.connection.send(message: data, asBinary: false)
	}

	public var debugDescription: String {
		return "PeerIncomingConnection(\(self.connection.request.remoteAddress) -> \(self.connection.request.urlURL.absoluteString)";
	}
}

#if !os(Linux)
public class PeerOutgoingConnection<LedgerType: Ledger>: PeerConnection<LedgerType>, WebSocketDelegate {
	let connection: Starscream.WebSocket

	public init?(to url: URL, from uuid: UUID? = nil, at port: Int? = nil) {
		assert((uuid != nil && port != nil) || (uuid == nil && port == nil), "either set both a port and uuid or set neither (passive mode)")

		if var uc = URLComponents(url: url, resolvingAgainstBaseURL: false) {
			// Set source peer port and UUID
			if let uuid = uuid, let port = port {
				if port <= 0 {
					return nil
				}

				uc.queryItems = [
					URLQueryItem(name: LedgerType.ParametersType.uuidRequestKey, value: uuid.uuidString),
					URLQueryItem(name: LedgerType.ParametersType.portRequestKey, value: String(port))
				]
			}

			self.connection = Starscream.WebSocket(url: uc.url!, protocols: [LedgerType.ParametersType.protocolVersion])
			super.init(isIncoming: false)
		}
		else {
			return nil
		}
	}

	init(connection: Starscream.WebSocket) {
		self.connection = connection
		super.init(isIncoming: false)
	}

	public func connect() {
		self.connection.timeout = 10
		self.connection.delegate = self
		self.connection.callbackQueue = DispatchQueue.global(qos: .background)

		if !self.connection.isConnected {
			self.connection.connect()
		}
	}

	deinit {
		self.delegate?.peer(disconnected: self)
	}

	public override func send(data: Data) throws {
		if self.connection.isConnected {
			self.connection.write(data: data)
		}
		else {
			throw PeerConnectionError.notConnected
		}
	}

	public func websocketDidConnect(socket: Starscream.WebSocket) {
		Log.debug("[Gossip] Connected outgoing to \(socket.currentURL)")
		self.delegate?.peer(connected: self)
	}

	public func websocketDidReceiveData(socket: Starscream.WebSocket, data: Data) {
		do {
			if let obj = try JSONSerialization.jsonObject(with: data, options: []) as? [Any] {
				self.receive(data: obj)
			}
			else {
				Log.error("[Gossip] Outgoing socket received malformed data")
			}
		}
		catch {
			Log.error("[Gossip] Outgoing socket received malformed data: \(error)")
		}
	}

	public func websocketDidDisconnect(socket: Starscream.WebSocket, error: NSError?) {
		Log.debug("[Gossip] Disconnected outgoing to \(socket.currentURL) \(error?.localizedDescription ?? "unknown error")")
		self.delegate?.peer(disconnected: self)
		self.delegate = nil
		connection.delegate = nil
	}

	public func websocketDidReceiveMessage(socket: Starscream.WebSocket, text: String) {
		self.websocketDidReceiveData(socket: socket, data: text.data(using: .utf8)!)
	}
}
#endif

public class Peer<LedgerType: Ledger>: PeerConnectionDelegate {
	typealias BlockchainType = LedgerType.BlockchainType
	typealias BlockType = BlockchainType.BlockType
	typealias TransactionType = BlockType.TransactionType

	public let url: URL

	/** Time at which we last received a response or request from this peer. Nil when that never happened. */
	public internal(set) var lastSeen: Date? = nil

	/** The time difference observed during the last index request */
	public internal(set) var timeDifference: TimeInterval? = nil

	public internal(set) var state: PeerState
	fileprivate(set) var connection: PeerConnection<LedgerType>? = nil
	weak var node: Node<LedgerType>?
	public let mutex = Mutex()

	init(url: URL, state: PeerState, connection: PeerConnection<LedgerType>?, delegate: Node<LedgerType>) {
		assert(Peer<LedgerType>.isValidPeerURL(url: url), "Peer URL must be valid")
		self.url = url
		self.state = state
		self.connection = connection
		self.node = delegate
		connection?.delegate = self
	}

	public var uuid: UUID {
		return UUID(uuidString: self.url.user!)!
	}

	static public func isValidPeerURL(url: URL) -> Bool {
		if let uc = URLComponents(url: url, resolvingAgainstBaseURL: false) {
			// URL must contain a port, host and user part
			if uc.port == nil || uc.host == nil || uc.user == nil {
				return false
			}

			// The user in the URL must be a valid node identifier (UUID)
			if UUID(uuidString: uc.user!) == nil {
				return false
			}
			return true
		}
		return false
	}

    /** Returns true when a peer action was performed, false when no action was required. */
    public func advance() -> Bool {
		return self.mutex.locked { () -> Bool in
			Log.debug("Advance peer \(url) from state \(self.state)")
			do {
				if let n = node {
					switch self.state {
					case .failed(error: _, at: let date):
						// Failed peers become 'new' after a certain amount of time, so we can retry
						if Date().timeIntervalSince(date) > LedgerType.ParametersType.peerRetryAfterFailureInterval {
							self.connection = nil
							self.state = .new
						}
                        return false

					case .new:
						// Perhaps reconnect to this peer
						#if !os(Linux)
							if url.port == nil || url.port! == 0 {
								self.state = .ignored(reason: "disconnected, and peer does not accept incoming connections")
							}
							else if let pic = PeerOutgoingConnection<LedgerType>(to: url, from: n.uuid, at: n.server.port) {
								pic.delegate = self
                                self.state = .connecting(since: Date())
                                self.connection = pic
								Log.debug("[Peer] connect outgoing \(url)")
                                pic.connect()
							}
						#else
							// Outgoing connections are not supported on Linux (yet!)
							self.state = .ignored(reason: "disconnected, and cannot make outgoing connections")
						#endif
                        return true

					case .connected, .queried:
						try self.query()
                        return true

					case .passive, .ignored(reason: _):
						// Do nothing (perhaps ping in the future?)
						return false

					case .connecting(since: let date), .querying(since: let date):
						// Reset hung peers
						if Date().timeIntervalSince(date) > LedgerType.ParametersType.peerRetryAfterFailureInterval {
							self.connection = nil
							self.state = .new
						}
                        return true
					}
				}
                else {
                    return false
                }
			}
			catch {
				self.fail(error: "advance error: \(error.localizedDescription)")
                return false
			}
		}
	}

	public func fail(error: String) {
		self.mutex.locked {
            Log.info("[Peer] \(self.url.absoluteString) failing: \(error)")
			self.connection = nil
			self.state = .failed(error: error, at: Date())
		}
	}

	private func query() throws {
		if let n = self.node, let c = self.connection {
			self.mutex.locked {
				self.state = .querying(since: Date())
			}

			let requestTime = Date()

			try c.request(gossip: .query) { reply in
				self.mutex.locked {
					self.lastSeen = Date()

					if case .index(let index) = reply {
						Log.debug("[Peer] Receive index reply: \(index)")

						// Update observed time difference
						let requestEndTime = Date()
						self.timeDifference = requestEndTime.timeIntervalSince(requestTime) / 2.0

						// Update peer status
						if index.genesis != n.ledger.longest.genesis.signature! {
							// Peer believes in another genesis, ignore him
							self.connection = nil
							self.state = .ignored(reason: "believes in other genesis")
						}
						else {
							self.state = .queried
						}

						// New peers?
						for p in index.peers {
							n.add(peer: p)
						}

						// Request the best block from this peer
						n.receive(best: Candidate(hash: index.highest, height: index.height, peer: self.uuid))
					}
					else if case .passive = reply {
						self.state = .passive
					}
					else {
						self.fail(error: "Invalid reply received to query request")
					}
				}
			}
		}
	}

	public func peer(connection: PeerConnection<LedgerType>, requests gossip: Gossip<LedgerType>, counter: Int) {
		do {
			self.lastSeen = Date()
			Log.debug("[Peer] receive request \(counter)")
			switch gossip {
			case .forget:
				try self.node?.forget(peer: self)
				self.state = .ignored(reason: "peer requested to be forgotten")

			case .transaction(let trData):
				let tr = try TransactionType(json: trData)
				_ = try self.node?.receive(transaction: tr, from: self)

			case .block(let blockData):
				do {
					let b = try BlockType.read(json: blockData)
					try self.node?.receive(block: b, from: self, wasRequested: false)
				}
				catch {
					self.fail(error: "Received invalid unsolicited block")
				}

			case .fetch(let h):
				try self.node?.ledger.mutex.locked {
					if let block = try self.node?.ledger.longest.get(block: h) {
						assert(block.isSignatureValid, "returning invalid blocks, that can't be good")
						assert(try! BlockType.read(json: block.json).isSignatureValid, "JSON goes wild")

						try connection.reply(counter: counter, gossip: .block(block.json))
					}
					else {
						try connection.reply(counter: counter, gossip: .error("not found"))
					}
				}

			case .query:
				// We received a query from the other end
				if let n = self.node {
					let idx = n.ledger.mutex.locked {
						return Index<BlockchainType.BlockType>(
							genesis: n.ledger.longest.genesis.signature!,
							peers: Array(n.validPeers),
							highest: n.ledger.longest.highest.signature!,
							height: n.ledger.longest.highest.index,
							timestamp: BlockchainType.BlockType.TimestampType(Date().timeIntervalSince1970)
						)
					}
					try connection.reply(counter: counter, gossip: .index(idx))
				}
				break

			default:
				// These are not requests we handle. Ignore clients that don't play by the rules
				self.state = .ignored(reason: "peer sent invalid request \(gossip)")
				break
			}
		}
		catch {
			Log.error("[Peer] handle Gossip request failed: \(error.localizedDescription)")
		}
	}

	public func peer(connected _: PeerConnection<LedgerType>) {
		self.mutex.locked {
			if case .connecting = self.state {
				Log.debug("[Peer] \(url) connected outgoing")
				self.state = .connected
			}
			else {
				Log.error("[Peer] \(url) connected while not connecting")
			}
		}
	}

	public func peer(disconnected _: PeerConnection<LedgerType>) {
		self.mutex.locked {
			Log.debug("[Peer] \(url) disconnected outgoing")
			self.connection = nil
			self.fail(error: "disconnected")
		}
	}
}

public enum PeerState {
	case new // Peer has not yet connected
	case connecting(since: Date)
	case connected // The peer is connected but has not been queried yet
	case querying(since: Date) // The peer is currently being queried
	case queried // The peer has last been queried successfully
	case passive // Peer is active, but should not be queried (only listens passively)
	case ignored(reason: String) // The peer is ourselves or believes in another genesis, ignore it forever
	case failed(error: String, at: Date) // Talking to the peer failed for some reason, ignore it for a while
}
