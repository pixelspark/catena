import Foundation
import Kitura
import Dispatch
import LoggerAPI

struct Candidate<BlockType: Block>: Equatable, Hashable {
	static func ==(lhs: Candidate<BlockType>, rhs: Candidate<BlockType>) -> Bool {
		return lhs.hash == rhs.hash && lhs.peer == rhs.peer
	}

	var hashValue: Int {
		return hash.hashValue ^ height.hashValue ^ peer.hashValue
	}

	let hash: BlockType.HashType
	let height: UInt64
	let peer: UUID
}

public protocol PeerDatabase {
	/** Remember a peer in the peer database cache. Note that the peer database should only remember the most recent
	address for a given node ID. */
	func rememberPeer(url: URL) throws

	/** Remove the remembered address for the given node ID (if it was remembered at all, otherwise fail silently). */
	func forgetPeer(uuid: UUID) throws
}

public class Node<LedgerType: Ledger> {
	public typealias BlockchainType = LedgerType.BlockchainType
	public typealias BlockType = BlockchainType.BlockType

	public let uuid: UUID
	public private(set) var ledger: LedgerType! = nil
	public private(set) var miner: Miner<LedgerType>! = nil
	public private(set) var server: Server<LedgerType>! = nil
	public private(set) var peers: [UUID: Peer<LedgerType>] = [:]
	public var peerDatabase: PeerDatabase? = nil

	private var fetcher: Fetcher<LedgerType>! = nil
	private let workerQueue = DispatchQueue.global(qos: .background)

	private let tickTimer: DispatchSourceTimer
	fileprivate let mutex = Mutex()
	private var queryQueue: [UUID] = []

	/** The list of peers that can be advertised to other nodes for peer exchange. */
	var validPeers: Set<URL> {
		return Set(peers.flatMap { (uuid, peer) -> URL? in
			return peer.mutex.locked {
				switch peer.state {
				case .failed(_), .querying(_), .new, .ignored, .connected(_), .connecting(_), .passive:
					return nil

				case .queried(_):
					return peer.url
				}
			}
		})
	}

	public init(ledger: LedgerType, port: Int) {
		self.uuid = UUID()
		self.tickTimer = DispatchSource.makeTimerSource(flags: [], queue: self.workerQueue)
		self.ledger = ledger
		self.miner = Miner(node: self)
		self.server = Server(node: self, port: port)
		self.fetcher = Fetcher(node: self)
	}

	public var url: URL {
		var uc = URLComponents()
		uc.user = self.uuid.uuidString
		uc.port = self.server.port
		uc.host = "127.0.0.1"
		uc.scheme = "ws"
		return uc.url!
	}

	/** Append a transaction to the memory pool (maintained by the miner). */
	public func receive(transaction: BlockType.TransactionType, from peer: Peer<LedgerType>?) throws {
		// Is this transaction in-order?
		if try self.ledger.canAccept(transaction: transaction, pool: self.miner.block) {
			let isNew = try self.miner.append(transaction: transaction)

			// Did we get this block from someone else and is it new? Then rebroadcast
			if isNew {
				Log.info("[Node] Re-broadcasting transaction \(transaction) to peers as it is new")
				let transactionGossip = Gossip<LedgerType>.transaction(transaction.json)
				self.peers.forEach { (url, otherPeer) in
					if peer == nil || otherPeer.url != peer!.url {
						switch otherPeer.state {
						case .queried, .passive:
							if let otherConnection = otherPeer.connection {
								do {
									try otherConnection.request(gossip: transactionGossip)
								}
								catch {
									// Not a problem if this fails
								}
							}

						default:
							// Do not send
							break
						}
					}
				}
			}
		}
		else {
			Log.error("[Node] Not appending transaction \(transaction) to memory pool: ledger says it isn't acceptable")
		}
	}

	func forget(peer: Peer<LedgerType>) throws {
		try self.mutex.locked {
			Log.info("[Node] Peer \(peer.url) wants us to forget him!")
			// Remove peer (should also be removed from queryQueue, but that's expensive, and tick() will deal with this
			self.peers[peer.uuid] = nil
			try self.peerDatabase?.forgetPeer(uuid: peer.uuid)
		}
	}

	public func add(peer url: URL) {
		if Peer<LedgerType>.isValidPeerURL(url: url), let uuid = UUID(uuidString: url.user!) {
			self.mutex.locked {
				if self.peers[uuid] == nil {
					let isSelf = self.uuid == uuid
					let peer = Peer<LedgerType>(url: url, state: isSelf ? .ignored(reason: "added, but is ourselves") : .new, connection: nil, delegate: self)
					self.peers[uuid] = peer

					if case .new = peer.state {
						do {
							try self.peerDatabase?.rememberPeer(url: url)
						}
						catch {
							Log.error("[Node] Peer database remember failed: \(error.localizedDescription)")
						}

						self.queryQueue.append(uuid)
					}
				}
			}
		}
		else {
			Log.error("[Node] Invalid peer URL: \(url.absoluteString)")
		}
	}

	func add(peer connection: PeerIncomingConnection<LedgerType>) {
		var reverseURL = URLComponents()
		reverseURL.scheme = "ws"
		reverseURL.host = connection.connection.request.remoteAddress

		let queryParameters = connection.connection.request.urlURL.parameters

		if
			let connectingUUIDString = queryParameters[ProtocolConstants.uuidRequestKey],
			let connectingUUID = UUID(uuidString: connectingUUIDString) {

			// Did the connecting peer specify a port?
			let reversePort: Int
			if let connectingPortString = queryParameters[ProtocolConstants.portRequestKey],
			let connectingPort = Int(connectingPortString),
			connectingPort > 0, connectingPort < 65535 {
				reversePort = connectingPort
			}
			else {
				// Set reverse port to '0' to indicate an outgoing connection is not possible
				reversePort = 0
			}

			reverseURL.user = connectingUUIDString
			reverseURL.port = reversePort
			let url = reverseURL.url!

			// Check whether the connecting peer is ourselves
			let isSelf = connectingUUID == self.uuid
			if isSelf {
				Log.info("[Server] dropping connection \(reverseURL): is ourselves")
				connection.close()
				return
			}

			let peer = Peer<LedgerType>(url: url, state: isSelf ? .ignored(reason: "is ourselves") : .connected, connection: isSelf ? nil : connection, delegate: self)
			connection.delegate = peer

			// Check whether the connecting peer is looking for someone else
			do {
				// Find the user URL string, either from the HTTP request URL, or from the Origin header
				let user: String
				if let u = connection.connection.request.urlURL.user {
					user = u
				}
				else if let o = connection.connection.request.headers["Origin"], !o.isEmpty, let uc = URLComponents(string: o[0]), let u = uc.user {
					user = u
				}
				else {
					try connection.request(gossip: .forget)
					Log.info("[Server] dropping connection \(reverseURL): did not specify node UUID to connect to (request URL: '\(connection.connection.request.urlURL.absoluteString)')")
					connection.close()
					return
				}

				// Check if the URL user is a valid UUID
				if let userUUID = UUID(uuidString: user) {
					if userUUID != self.uuid {
						Log.info("[Server] dropping connection \(reverseURL): is looking for different UUID \(userUUID)")
						try connection.request(gossip: .forget)
						connection.close()
						return
					}
				}
				else {
					// Requested node UUID is invalid
					try connection.request(gossip: .forget)
					Log.info("[Server] dropping connection \(reverseURL): invalid UUID '\(user)'")
					connection.close()
					return
				}
			}
			catch {
				Log.error("[Server] could not properly reject peer \(reverseURL) looking for someone else: \(error.localizedDescription)")
			}

			self.mutex.locked {
				if let alreadyConnected = self.peers[connectingUUID] {
					switch alreadyConnected.state {
					case .connected, .queried, .querying:
						// Reject connection because we are already connected!
						return

					default:
						if let ls = alreadyConnected.lastSeen, ls.timeIntervalSince(Date()) < ProtocolConstants.peerReplaceInterval {
							Log.info("Not replacing connection \(alreadyConnected) (state=\(alreadyConnected.state) with \(peer): the peer was recently seen")
							return
						}
						Log.info("Replacing connection \(alreadyConnected) (state=\(alreadyConnected.state)) with \(peer)")
						// Accept new connection as it is better than what we have
						break
					}
				}

				self.peers[connectingUUID] = peer
				do {
					try self.peerDatabase?.rememberPeer(url: url)
				}
				catch {
					Log.error("[Node] Peer database remember failed: \(error.localizedDescription)")
				}
				self.queryQueue.append(connectingUUID)
			}
		}
		else {
			Log.warning("[Node] not accepting incoming peer \(reverseURL.host!): it has no UUID or the UUID is equal to ours, or is incompatible")
		}
	}

	/** Received a 'best block' offer from another peer (in reply to query) */
	func receive(best: Candidate<BlockType>) {
		self.fetcher.request(candidate: best)
	}

	/** Peer can be nil when the block originated from ourselves (i.e. was mined). */
	func receive(block: BlockType, from peer: Peer<LedgerType>?, wasRequested: Bool) throws {
		if block.isSignatureValid && block.isPayloadValid() {
			Log.info("[Node] receive block #\(block.index) from \(peer?.url.absoluteString ?? "self")")

			let isNew = try self.ledger.mutex.locked { () -> Bool in
				let isNew = try self.ledger.isNew(block: block) && self.ledger.longest.highest.index < block.index
				let wasAppended = try self.ledger.receive(block: block)
				if let p = peer, wasRequested && !wasAppended && block.index > 0 {
					let (fetchIndex, fetchHash) = self.ledger.orphans.earliestRootFor(orphan: block)

					if fetchIndex > 0 {
						Log.info("[Node] received an orphan block #\(block.index), let's see if peer has its predecessor \(fetchHash) at #\(fetchIndex)")
						self.fetcher.request(candidate: Candidate<BlockType>(hash: fetchHash, height: fetchIndex, peer: p.uuid))
					}
				}
				return isNew
			}

			// Did we get this block from someone else and is it new? Then rebroadcast
			if let p = peer, isNew && !wasRequested {
				Log.info("[Node] Re-broadcasting block \(block) to peers as it is new")
				let blockGossip = Gossip<LedgerType>.block(block.json)

				self.peers.forEach { (url, otherPeer) in
					if otherPeer.url != p.url {
						if case .connected = otherPeer.state, let otherConnection = otherPeer.connection {
							do {
								try otherConnection.request(gossip: blockGossip)
							}
							catch {
								// Not a problem if this fails
							}
						}
					}
				}
			}
		}
		else {
			Log.info("[Node] received invalid block #\(block.index) from \(peer?.url.absoluteString ?? "self")")
		}
	}

	func mined(block: BlockType) throws {
		try self.receive(block: block, from: nil, wasRequested: false)

		// Send our peers the good news!
		self.mutex.locked {
			for (_, peer) in self.peers {
				switch peer.state {
				case .queried, .connected, .passive:
					if let peerConnection = peer.connection {
						self.workerQueue.async {
							do {
								Log.debug("[Node] posting mined block \(block.index) to peer \(peer.url)")
								try peerConnection.request(gossip: .block(block.json))
							}
							catch {
								Log.error("[Node] Error sending mined block post: \(error.localizedDescription)")
							}
						}
					}

				case .failed(error: _), .ignored, .querying, .connecting, .new:
					break
				}
			}
		}
	}

	private func tick() {
		self.mutex.locked {
			// Take the first from the query queue...
			if let p = self.queryQueue.first {
				self.queryQueue.remove(at: 0)
				if let peer = self.peers[p] {
					peer.advance()
					return
				}
			}

			if self.peers.isEmpty {
				Log.warning("[Node] Have no peers!")
			}

			// Re-query all peers that are not already being queried
			for (url, _) in self.peers {
				self.queryQueue.append(url)
			}
		}
	}

	public func start(blocking: Bool) {
		self.tickTimer.setEventHandler { [unowned self] _ in
			self.tick()
		}
		self.tickTimer.scheduleRepeating(deadline: .now(), interval: 2.0)
		self.tickTimer.resume()

		if blocking {
			Kitura.run()
		}
		else {
			Kitura.start()
		}
	}
}

fileprivate class Fetcher<LedgerType: Ledger> {
	typealias BlockchainType = LedgerType.BlockchainType
	typealias BlockType = BlockchainType.BlockType

	private let workerQueue = DispatchQueue.global(qos: .background)
	private var candidateQueue = OrderedSet<Candidate<BlockType>>()
	private weak var node: Node<LedgerType>?
	private var running = false
	private let mutex = Mutex()

	init(node: Node<LedgerType>) {
		self.node = node
	}

	func request(candidate: Candidate<BlockType>) {
		self.mutex.locked {
			self.candidateQueue.append(candidate)

			if !self.running {
				self.tick()
			}
		}
	}

	private func tick() {
		self.mutex.locked {
			assert(!self.running, "already running!")
			self.running = true

			// Do we need to fetch any blocks?
			if let candidate = self.candidateQueue.first {
				self.candidateQueue.remove(at: 0)
				Log.info("[Node] fetch candidate \(candidate)")
				self.fetch(candidate: candidate) {
					self.mutex.locked {
						self.running = false

						if !self.candidateQueue.isEmpty {
							self.tick()
						}
					}
				}
			}
		}
	}

	private func fetch(candidate: Candidate<BlockType>, callback: @escaping () -> ()) {
		self.workerQueue.async {
			if let n = self.node, let p = n.peers[candidate.peer] {
				if let c = p.mutex.locked(block: { return p.connection }) {
					do {
						try c.request(gossip: Gossip<LedgerType>.fetch(candidate.hash)) { reply in
							if case .block(let blockData) = reply {
								do {
									let block = try BlockType.read(json: blockData)

									if block.isSignatureValid {
										Log.debug("[Fetcher] fetch returned valid block: \(block)")

										if block.index != candidate.height || block.signature! != candidate.hash {
											Log.error("[Fetcher] peer returned a different block (#\(block.index)) than we requested (#\(candidate.height))!")
											n.mutex.locked {
												n.peers[candidate.peer]?.fail(error: "invalid block")
											}
											return callback()
										}

										try n.mutex.locked {
											let peer = n.peers[candidate.peer]
											try n.receive(block: block, from: peer, wasRequested: true)
										}
									}
									else {
										Log.warning("[Fetcher] fetch returned invalid block (signature invalid); setting peer \(candidate.peer) invalid")
										n.mutex.locked {
											n.peers[candidate.peer]?.fail(error: "invalid block")
										}
									}
								}
								catch {
									Log.error("[Fetcher] Error parsing result from fetch: \(error.localizedDescription)")
									n.mutex.locked {
										n.peers[candidate.peer]?.fail(error: "invalid reply to fetch")
									}
								}
							}
							else {
								Log.error("[Fetcher] Received invalid reply to fetch: \(reply)")
								n.mutex.locked {
									n.peers[candidate.peer]?.fail(error: "invalid reply to fetch")
								}
							}

							return callback()
						}
					}
					catch {
						Log.error("[Fetcher] Fetch error: \(error)")
						n.mutex.locked {
							n.peers[candidate.peer]?.fail(error: "fetch error: \(error)")
						}

						return callback()
					}
				}
				else {
					Log.info("[Fetcher] fetch candidate peer is not connected: \(candidate)")
					return callback()
				}
			}
			else {
				Log.info("[Fetcher] fetch candidate peer has disappeared: \(candidate)")
				return callback()
			}
		}
	}
}
