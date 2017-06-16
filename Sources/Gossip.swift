import Foundation
import Kitura
import KituraRequest
import LoggerAPI
import KituraWebSocket
import Starscream

internal extension Block {
	var json: [String: Any] {
		return [
			"hash": self.signature!.stringValue,
			"index": self.index,
			"nonce": self.nonce,
			"payload": self.payloadData.base64EncodedString(),
			"previous": self.previous.stringValue
		]
	}

	static func read(json: [String: Any]) throws -> Self {
		if let nonce = json["nonce"] as? NSNumber,
			let signature = json["hash"] as? String,
			let height = json["index"] as? NSNumber,
			let previous = json["previous"] as? String,
			let payloadBase64 = json["payload"] as? String,
			let payload = Data(base64Encoded: payloadBase64),
			let previousHash = Hash(string: previous),
			let signatureHash = Hash(string: signature) {
				var b = try Self.init(index: UInt(height), previous: previousHash, payload: payload)
				b.nonce = UInt(nonce)
				b.signature = signatureHash
				return b
			}
			else {
				throw BlockError.formatError
			}
	}
}

class Server<BlockchainType: Blockchain>: WebSocketService {
	typealias BlockType = BlockchainType.BlockType

	private let version = 1
	let router = Router()
	let port: Int
	private let mutex = Mutex()
	private var gossipConnections = [String: PeerIncomingConnection]()
	weak var node: Node<BlockchainType>?

	init(node: Node<BlockchainType>, port: Int) {
		self.node = node
		self.port = port

		WebSocket.register(service: self, onPath: "/")

		// Not part of the API used between nodes
		router.get("/", handler: self.handleIndex)
		router.get("/api/block/:hash", handler: self.handleGetBlock)
		router.get("/api/orphans", handler: self.handleGetOrphans)
		router.get("/api/head", handler: self.handleGetLast)
		router.get("/api/journal", handler: self.handleGetJournal)

		Kitura.addHTTPServer(onPort: port, with: router)
	}

	func connected(connection: WebSocketConnection) {
		Log.info("[Server] gossip connected \(connection.id)")
		let pic = PeerIncomingConnection(connection: connection)

		self.mutex.locked {
			self.gossipConnections[connection.id] = pic
		}

		self.node?.add(peer: pic)
	}

	func disconnected(connection: WebSocketConnection, reason: WebSocketCloseReasonCode) {
		Log.info("[Server] disconnected gossip \(connection.id); reason=\(reason)")
		self.mutex.locked {
			self.gossipConnections.removeValue(forKey: connection.id)
		}
	}

	func received(message: Data, from: WebSocketConnection) {
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

	func received(message: String, from: WebSocketConnection) {
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

	/** Other peers will GET this, or POST with a JSON object containing their own UUID (string) and port (number). */
	private func handleIndex(request: RouterRequest, response: RouterResponse, next: @escaping () -> Void) throws {
		let longest = self.node!.ledger.longest

		response.send(json: [
			"version": self.version,
			"uuid": self.node!.uuid.uuidString,

			"longest": [
				"highest": longest.highest.json,
				"genesis": longest.genesis.json
			],

			"peers": self.node!.peers.map { (url, p) -> [String: Any] in
				return p.mutex.locked {
					let desc: String
					switch p.state {
					case .new: desc = "new"
					case .connected(_): desc = "connected"
					case .connecting(_): desc = "connecting"
					case .failed(error: let e): desc = "error(\(e))"
					case .ignored(reason: let e): desc = "ignored(\(e))"
					case .queried(_): desc = "queried"
					case .querying(_): desc = "querying"
					}

					return [
						"url": url.absoluteString,
						"state": desc
					]
				}
			}
		])
		next()
	}

	private func handleGetBlock(request: RouterRequest, response: RouterResponse, next: @escaping () -> Void) throws {
		if let hashString = request.parameters["hash"], let hash = Hash(string: hashString) {
			if let ledger = self.node?.ledger {
				let block = try ledger.mutex.locked {
					return try ledger.longest.get(block: hash)
				}

				if let b = block {
					assert(b.isSignatureValid, "returning invalid blocks, that can't be good")
					assert(try! BlockType.read(json: b.json).isSignatureValid, "JSON goes wild")

					response.send(json: b.json)

					next()
				}
				else {
					_ = response.send(status: .notFound)
				}
			}
			else {
				_ = response.send(status: .internalServerError)
			}
		}
		else {
			_ = response.send(status: .badRequest)
		}
	}

	private func handleGetOrphans(request: RouterRequest, response: RouterResponse, next: @escaping () -> Void) throws {
		let hashes = Array(self.node!.ledger.orphansByHash.keys.map { $0.stringValue })
		response.send(json: [
			"status": "ok",
			"orphans": hashes
		])
		next()
	}

	private func handleGetLast(request: RouterRequest, response: RouterResponse, next: @escaping () -> Void) throws {
		let chain = self.node!.ledger.longest
		var b: BlockType? = chain.highest
		var data: [[String: Any]] = []
		for _ in 0..<10 {
			if let block = b {
				data.append([
					"index": block.index,
					"hash": block.signature!.stringValue
				])
				b = try chain.get(block: block.previous)
			}
			else {
				break
			}
		}

		response.send(json: [
			"status": "ok",
			"blocks": data
		])
		next()
	}

	private func handleGetJournal(request: RouterRequest, response: RouterResponse, next: @escaping () -> Void) throws {
		let chain = self.node!.ledger.longest
		var b: BlockType? = chain.highest
		var data: [String] = [];
		while let block = b {
			data.append(String(data: block.payloadData, encoding: .utf8)!)
			b = try chain.get(block: block.previous)
		}

		response.send(json: [
			"status": "ok",
			"blocks": Array(data.reversed())
		])
		next()
	}
}

public enum Gossip {
	static let version = 1

	public struct Index {
		let genesis: Hash
		let peers: [URL]
		let highest: Hash
		let height: UInt

		init(genesis: Hash, peers: [URL], highest: Hash, height: UInt) {
			self.genesis = genesis
			self.peers = peers
			self.highest = highest
			self.height = height
		}

		init?(json: [String: Any]) {
			if
				let genesisHash = json["genesis"] as? String,
				let highestHash = json["highest"] as? String,
				let genesis = Hash(string: genesisHash),
				let highest = Hash(string: highestHash),
				let height = json["height"] as? Int,
				let peers = json["peers"] as? [String]
			{
				self.genesis = genesis
				self.highest = highest
				self.height = UInt(height)
				self.peers = peers.flatMap { return URL(string: $0) }
			}
			else {
				return nil
			}
		}

		var json: [String: Any] {
			return [
				"highest": self.highest.stringValue,
				"height": self.height,
				"genesis": self.genesis.stringValue,
				"peers": self.peers.flatMap { $0.absoluteString }
			]
		}
	}

	case query // -> index
	case index(Index)
	case block([String: Any]) // no reply
	case fetch(Hash) // -> block
	case error(String)

	static let actionKey = "t"

	init?(json: [String: Any]) {
		if let q = json[Gossip.actionKey] as? String {
			if q == "query" {
				self = .query
			}
			else if q == "block", let blockData = json["block"] as? [String: Any] {
				self = .block(blockData)
			}
			else if q == "fetch", let hash = json["hash"] as? String, let hashValue = Hash(string: hash) {
				self = .fetch(hashValue)
			}
			else if q == "index", let idx = json["index"] as? [String: Any], let index = Index(json: idx) {
				self = .index(index)
			}
			else if q == "error", let message = json["message"] as? String {
				self = .error(message)
			}
			else {
				return nil
			}
		}
		else {
			return nil
		}
	}

	var json: [String: Any] {
		switch self {
		case .query:
			return [Gossip.actionKey: "query"]

		case .block(let b):
			return [Gossip.actionKey: "block", "block": b]

		case .index(let i):
			return [Gossip.actionKey: "index", "index": i.json]

		case .fetch(let h):
			return [Gossip.actionKey: "fetch", "hash": h.stringValue]

		case .error(let m):
			return [Gossip.actionKey: "error", "message": m]
		}
	}
}

public class PeerConnection {
	public typealias Callback = (Gossip) -> ()
	public let mutex = Mutex()
	private var counter = 0
	private var callbacks: [Int: Callback] = [:]
	public weak var delegate: PeerConnectionDelegate?

	fileprivate init(isIncoming: Bool) {
		self.counter = isIncoming ? 1 : 0
	}

	public func receive(data: [Any]) {
		if data.count == 2, let counter = data[0] as? Int, let gossipData = data[1] as? [String: Any] {
			if let g = Gossip(json: gossipData) {
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
							Log.error("[Server] cannot handle gossip \(counter): no delegate")
						}
					}
				}
			}
			else {
				Log.warning("[Gossip] Receive unknown gossip: \(gossipData)")
			}
		}
		else {
			Log.warning("[Gossip] Receive malformed: \(data)")
		}
	}

	public final func reply(counter: Int, gossip: Gossip) throws {
		try self.mutex.locked {
			let d = try JSONSerialization.data(withJSONObject: [counter, gossip.json], options: [])
			try self.send(data: d)
		}
	}

	public final func request(gossip: Gossip, callback: Callback? = nil) throws {
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

public class PeerIncomingConnection: PeerConnection {
	let connection: WebSocketConnection

	init(connection: WebSocketConnection) {
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
		self.connection.send(message: data)
	}
}

public protocol PeerConnectionDelegate: class {
	func peer(connected: PeerOutgoingConnection)
	func peer(disconnected: PeerOutgoingConnection)
	func peer(connection: PeerConnection, requests: Gossip, counter: Int)
}

public class PeerOutgoingConnection: PeerConnection, WebSocketDelegate {
	let connection: Starscream.WebSocket

	init(connection: Starscream.WebSocket) {
		self.connection = connection
		connection.timeout = 10

		super.init(isIncoming: false)
		connection.delegate = self
		connection.callbackQueue = DispatchQueue.global(qos: .background)
	}

	deinit {
		self.delegate?.peer(disconnected: self)
	}

	public override func send(data: Data) throws {
		self.connection.write(data: data)
	}

	public func websocketDidConnect(socket: Starscream.WebSocket) {
		Log.info("[Gossip] Connected outgoing to \(socket.currentURL)")
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
		Log.info("[Gossip] Disconnected outgoing to \(socket.currentURL) \(error?.localizedDescription ?? "unknown error")")
		self.delegate?.peer(disconnected: self)
	}

	public func websocketDidReceiveMessage(socket: Starscream.WebSocket, text: String) {
		self.websocketDidReceiveData(socket: socket, data: text.data(using: .utf8)!)
	}
}

class Peer<BlockchainType: Blockchain>: PeerConnectionDelegate {
	typealias BlockType = BlockchainType.BlockType
	let url: URL
	private(set) var state: PeerState
	weak var node: Node<BlockchainType>?
	public let mutex = Mutex()

	public var connection: PeerConnection? {
		return self.state.connection
	}

	init(url: URL, state: PeerState, delegate: Node<BlockchainType>) {
		self.url = url
		self.state = state
		self.node = delegate
	}

	public func advance() {
		self.mutex.locked {
			Log.debug("Advance peer \(url) from state \(self.state)")
			do {
				if let n = node {
					switch self.state {
					case .new, .failed(_):
						let ws = Starscream.WebSocket(url: url)
						ws.headers["X-UUID"] = n.uuid.uuidString
						ws.headers["X-Port"] = String(n.server.port)
						ws.headers["X-Version"] = String(Gossip.version)
						let pic = PeerOutgoingConnection(connection: ws)
						pic.delegate = self
						Log.info("[Peer] connect outgoing \(url)")
						ws.connect()
						self.state = .connecting(pic)

					case .connected(_), .queried(_):
						try self.query()

					default:
						// Do nothing
						break
					}
				}
			}
			catch {
				self.state = .failed(error: "advance error: \(error.localizedDescription)")
			}
		}
	}

	public func fail(error: String) {
		Log.info("[Peer] \(self.url.absoluteString) failing: \(error)")
		self.mutex.locked {
			self.state = .failed(error: error)
		}
	}

	private func query() throws {
		if let n = self.node, let c = self.state.connection {
			self.mutex.locked {
				self.state = .querying(c)
			}

			try c.request(gossip: .query) { reply in
				self.mutex.locked {
					if case .index(let index) = reply {
						Log.debug("[Peer] Receive index reply: \(index)")
						// Update peer status
						if index.genesis != n.ledger.longest.genesis.signature! {
							// Peer believes in another genesis, ignore him
							self.state = .ignored(reason: "believes in other genesis")
						}
						else {
							self.state = .queried(c)
						}

						// New peers?
						for p in index.peers {
							n.add(peer: p)
						}

						n.receive(candidate: Candidate(hash: index.highest, height: index.height, peer: self.url))
					}
					else {
						self.state = .failed(error: "Invalid reply received to query request")
					}
				}
			}
		}
	}

	public func peer(connection: PeerConnection, requests gossip: Gossip, counter: Int) {
		do {
			Log.debug("[Peer] receive request \(counter)")
			switch gossip {
			case .block(let blockData):
				do {
					let b = try BlockType.read(json: blockData)
					try self.node?.receive(block: b, from: self)
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
				}

			case .query:
				// We received a query from the other end
				if let n = self.node {
					let idx = n.ledger.mutex.locked {
						return Gossip.Index(
							genesis: n.ledger.longest.genesis.signature!,
							peers: Array(n.validPeers),
							highest: n.ledger.longest.highest.signature!,
							height: n.ledger.longest.highest.index
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

	public func peer(connected _: PeerOutgoingConnection) {
		self.mutex.locked {
			if case .connecting(let c) = self.state {
				Log.info("[Peer] \(url) connected outgoing")
				self.state = .connected(c)
			}
			else {
				Log.error("[Peer] \(url) connected while not connecting")
			}
		}
	}

	public func peer(disconnected _: PeerOutgoingConnection) {
		self.mutex.locked {
			Log.info("[Peer] \(url) disconnected outgoing")
			self.state = .new
		}
	}
}

public enum PeerState {
	case new // Peer has not yet connected
	case connecting(PeerConnection)
	case connected(PeerConnection) // The peer is connected but has not been queried yet
	case querying(PeerConnection) // The peer is currently being queried
	case queried(PeerConnection) // The peer has last been queried successfully
	case ignored(reason: String) // The peer is ourselves or believes in another genesis, ignore it forever
	case failed(error: String) // Talking to the peer failed for some reason, ignore it for a while

	var connection: PeerConnection? {
		switch self {
		case .connected(let c), .queried(let c), .querying(let c):
			return c

		case .new, .failed(error: _), .ignored, .connecting(_):
			return nil
		}
	}
}
