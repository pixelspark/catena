import Foundation
import Kitura
import Dispatch
import LoggerAPI

struct Candidate<BlockType: Block>: Equatable {
	static func ==(lhs: Candidate<BlockType>, rhs: Candidate<BlockType>) -> Bool {
		return lhs.hash == rhs.hash && lhs.peer == rhs.peer
	}

	let hash: BlockType.HashType
	let height: UInt64
	let peer: URL
}

protocol PeerDatabase {
	func rememberPeer(url: URL) throws
}

class Node<BlockchainType: Blockchain> {
	typealias BlockType = BlockchainType.BlockType
	private(set) var server: Server<BlockchainType>! = nil
	private(set) var miner: Miner<BlockchainType>! = nil
	let ledger: Ledger<BlockchainType>
	let uuid: UUID

	var peerDatabase: PeerDatabase? = nil

	private let tickTimer: DispatchSourceTimer
	private let mutex = Mutex()
	private let workerQueue = DispatchQueue.global(qos: .background)
	private(set) var peers: [URL: Peer<BlockchainType>] = [:]
	private var queryQueue: [URL] = []
	private var candidateQueue: [Candidate<BlockType>] = []

	var validPeers: Set<URL> {
		return Set(peers.flatMap { (url, peer) -> URL? in
			return peer.mutex.locked {
				switch peer.state {
				case .failed(_), .querying(_), .new, .ignored, .connected(_), .connecting(_):
					return nil

				case .queried(_):
					return url
				}
			}
		})
	}

	init(ledger: Ledger<BlockchainType>, port: Int) {
		self.uuid = UUID()
		self.tickTimer = DispatchSource.makeTimerSource(flags: [], queue: self.workerQueue)
		self.ledger = ledger
		self.miner = Miner(node: self)
		self.server = Server(node: self, port: port)
	}

	/** Append a transaction to the memory pool (maintained by the miner). */
	func receive(transaction: BlockType.TransactionType, from peer: Peer<BlockchainType>?) throws {
		// Is this transaction in-order?
		if try self.ledger.canAccept(transaction: transaction, pool: self.miner.block) {
			let isNew = try self.miner.append(transaction: transaction)

			// Did we get this block from someone else and is it new? Then rebroadcast
			if isNew {
				Log.info("[Node] Re-broadcasting transaction \(transaction) to peers as it is new")
				let transactionGossip = Gossip<BlockchainType>.transaction(transaction.json)
				self.peers.forEach { (url, otherPeer) in
					if peer == nil || otherPeer.url != peer!.url {
						if case .connected = otherPeer.state, let otherConnection = otherPeer.connection {
							do {
								try otherConnection.request(gossip: transactionGossip)
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
			Log.error("[Node] Not appending transaction \(transaction) to memory pool: ledger says it isn't acceptable")
		}
	}

	func add(peer url: URL) {
		self.mutex.locked {
			if self.peers[url] == nil {
				self.peers[url] = Peer<BlockchainType>(url: url, state: .new, connection: nil, delegate: self)
				do {
					try self.peerDatabase?.rememberPeer(url: url)
				}
				catch {
					Log.error("[Node] Peer database remember failed: \(error.localizedDescription)")
				}
				self.queryQueue.append(url)
			}
		}
	}

	func add(peer connection: PeerIncomingConnection<BlockchainType>) {
		var reverseURL = URLComponents()
		reverseURL.scheme = "ws"
		reverseURL.host = connection.connection.request.remoteAddress

		let queryParameters = connection.connection.request.urlURL.parameters

		if
			let connectingUUIDString = queryParameters[ProtocolConstants.uuidRequestKey],
			let connectingUUID = UUID(uuidString: connectingUUIDString),
			let connectingPortString = queryParameters[ProtocolConstants.portRequestKey],
			let connectingPort = Int(connectingPortString),
			connectingPort > 0, connectingPort < 65535 {
			reverseURL.user = connectingUUIDString
			reverseURL.port = connectingPort
			let url = reverseURL.url!

			// Check whether the connecting peer is ourselves
			let isSelf = connectingUUID == self.uuid
			if isSelf {
				Log.info("[Server] dropping connection \(reverseURL): is ourselves")
				connection.close()
				return
			}

			// Check whether the connecting peer is looking for someone else
			if let user = connection.connection.request.urlURL.user {
				if let userUUID = UUID(uuidString: user) {
					if userUUID != self.uuid {
						Log.info("[Server] dropping connection \(reverseURL): is looking for different UUID \(userUUID)")
						connection.close()
						return
					}
				}
				else {
					// Requested node UUID is invalid
					Log.info("[Server] dropping connection \(reverseURL): invalid UUID '\(user)'")
					connection.close()
					return
				}
			}

			let peer = Peer<BlockchainType>(url: url, state: isSelf ? .ignored(reason: "is ourselves") : .connected, connection: isSelf ? nil : connection, delegate: self)
			connection.delegate = peer

			self.mutex.locked {
				if let alreadyConnected = self.peers[url] {
					switch alreadyConnected.state {
					case .connected, .queried, .querying:
						// Reject connection because we are already connected!
						return

					default:
						Log.info("Replacing connection \(alreadyConnected) (state=\(alreadyConnected.state)) with \(peer)")
						// Accept new connection as it is better than what we have
						break
					}
				}

				self.peers[url] = peer
				do {
					try self.peerDatabase?.rememberPeer(url: url)
				}
				catch {
					Log.error("[Node] Peer database remember failed: \(error.localizedDescription)")
				}
				self.queryQueue.append(url)
			}
		}
		else {
			Log.warning("[Node] not accepting incoming peer \(reverseURL.host!): it has no UUID or the UUID is equal to ours, or is incompatible")
		}
	}

	func queueRequest(candidate: Candidate<BlockType>) {
		self.mutex.locked {
			if candidate.height > self.ledger.longest.highest.index {
				self.candidateQueue.append(candidate)
			}
		}
	}

	/** Peer can be nil when the block originated from ourselves (i.e. was mined). */
	func receive(block: BlockType, from peer: Peer<BlockchainType>?, wasRequested: Bool) throws {
		if block.isSignatureValid && block.isPayloadValid() {
			Log.info("[Node] receive block #\(block.index) from \(peer?.url.absoluteString ?? "self")")

			let isNew = try self.mutex.locked { () -> Bool in
				let isNew = try self.ledger.isNew(block: block) && self.ledger.longest.highest.index < block.index
				let wasAppended = try self.ledger.receive(block: block)
				if let p = peer, wasRequested && !wasAppended && block.index > 0 {
					var fetchHash = block.previous
					var fetchIndex = block.index - 1
					while let orphan = self.ledger.orphansByHash[fetchHash], orphan.index > 0 {
						fetchHash = orphan.previous
						fetchIndex = orphan.index - 1
					}

					if fetchIndex > 0 {
						Log.info("[Node] received an orphan block #\(block.index), let's see if peer has its predecessor \(fetchHash) at #\(fetchIndex)")
						self.queueRequest(candidate: Candidate<BlockType>(hash: fetchHash, height: fetchIndex, peer: p.url))
					}
				}
				return isNew
			}

			// Did we get this block from someone else and is it new? Then rebroadcast
			if let p = peer, isNew && !wasRequested {
				Log.info("[Node] Re-broadcasting block \(block) to peers as it is new")
				let blockGossip = Gossip<BlockchainType>.block(block.json)

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
				case .queried, .connected:
					if let peerConnection = peer.connection {
						self.workerQueue.async {
							do {
								Log.debug("[Node] posting mined block \(block.index) to peer \(peer)")
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
			// Do we need to fetch any blocks?
			if let candidate = self.candidateQueue.first {
				self.candidateQueue.remove(at: 0)
				Log.info("[Node] fetch candidate \(candidate)")
				self.fetch(candidate: candidate)
			}

			// Take the first from the query queue...
			if let p = self.queryQueue.first {
				self.queryQueue.remove(at: 0)
				self.peers[p]!.advance()
				return
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

	private func fetch(candidate: Candidate<BlockType>) {
		self.workerQueue.async {
			if let p = self.peers[candidate.peer] {
				if let c = p.mutex.locked(block: { return p.connection }) {
					do {
						try c.request(gossip: Gossip<BlockchainType>.fetch(candidate.hash)) { reply in
							if case .block(let blockData) = reply {
								do {
									let block = try BlockType.read(json: blockData)

									if block.isSignatureValid {
										Log.debug("[Node] fetch returned valid block: \(block)")
										try self.ledger.mutex.locked {
											let peer = self.peers[candidate.peer]
											try self.receive(block: block, from: peer, wasRequested: true)
											if block.index > 0 {
												// Do we already have the previous block?
												if let orphan = self.ledger.orphansByHash[block.previous] {
													Log.info("[Node] fetch previous block is in cache, remind node")
													try self.receive(block: orphan, from: peer, wasRequested: true)
												}
												else {
													let pb = try self.ledger.longest.get(block: block.previous)
													if pb == nil {
														// Ledger is looking for the previous block for this block, go get it from the peer we got this from
														self.workerQueue.async {
															self.fetch(candidate: Candidate(hash: block.previous, height: block.index-1, peer: candidate.peer))
														}
													}
												}
											}
										}
									}
									else {
										Log.warning("[Node] fetch returned invalid block (signature invalid); setting peer \(candidate.peer) invalid")
										self.mutex.locked {
											self.peers[candidate.peer]?.fail(error: "invalid block")
										}
									}
								}
								catch {
									Log.error("[Gossip] Error parsing result from fetch: \(error.localizedDescription)")
									self.mutex.locked {
										self.peers[candidate.peer]?.fail(error: "invalid reply to fetch")
									}
								}
							}
							else {
								Log.error("[Gossip] Received invalid reply to fetch: \(reply)")
								self.mutex.locked {
									self.peers[candidate.peer]?.fail(error: "invalid reply to fetch")
								}
							}
						}
					}
					catch {
						Log.error("[Node] Fetch error: \(error)")
						self.mutex.locked {
							self.peers[candidate.peer]?.fail(error: "fetch error: \(error)")
						}
					}
				}
				else {
					Log.info("[Node] fetch candidate peer is not connected: \(candidate)")
				}
			}
			else {
				Log.info("[Node] fetch candidate peer has disappeared: \(candidate)")
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
