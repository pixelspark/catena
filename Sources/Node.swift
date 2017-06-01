import Foundation
import Kitura
import Dispatch
import LoggerAPI

struct Candidate<BlockType: Block>: Equatable {
	static func ==(lhs: Candidate<BlockType>, rhs: Candidate<BlockType>) -> Bool {
		return lhs.hash == rhs.hash && lhs.peer == rhs.peer
	}

	let hash: Hash
	let height: UInt
	let peer: URL
}

class Node<BlockType: Block> {
	private(set) var server: Server<BlockType>! = nil
	private(set) var miner: Miner<BlockType>! = nil
	let ledger: Ledger<BlockType>
	let uuid: UUID

	private let tickTimer: DispatchSourceTimer
	private let mutex = Mutex()
	private let workerQueue = DispatchQueue.global(qos: .background)
	private(set) var peers: [URL: Peer<BlockType>] = [:]
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

	init(ledger: Ledger<BlockType>, port: Int) {
		self.uuid = UUID()
		self.tickTimer = DispatchSource.makeTimerSource(flags: [], queue: self.workerQueue)
		self.ledger = ledger
		self.miner = Miner(node: self)
		self.server = Server(node: self, port: port)
	}

	func submit(payload: Data) {
		self.miner.submit(payload: payload)
	}

	func add(peer url: URL) {
		self.mutex.locked {
			if self.peers[url] == nil {
				self.peers[url] = Peer<BlockType>(url: url, state: .new, delegate: self)
				self.queryQueue.append(url)
			}
		}
	}

	func add(peer connection: PeerIncomingConnection) {
		var uc = URLComponents()
		uc.scheme = "ws"
		uc.host = connection.connection.request.remoteAddress

		if
			let connectingUUIDString = connection.connection.request.headers["X-UUID"]?.first,
			let connectingUUID = UUID(uuidString: connectingUUIDString),
			let connectingPortString = connection.connection.request.headers["X-Port"]?.first,
			let connectingPort = Int(connectingPortString),
			let connectingVersionString = connection.connection.request.headers["X-Version"]?.first,
			let connectingVersion = Int(connectingVersionString),
			connectingVersion == Gossip.version,
			connectingPort > 0, connectingPort < 65535 {
			uc.user = connectingUUIDString
			uc.port = connectingPort
			let url = uc.url!

			let isSelf = connectingUUID == self.uuid
			if isSelf {
				connection.close()
			}

			let peer = Peer<BlockType>(url: url, state: isSelf ? .ignored(reason: "is ourselves") : .connected(connection), delegate: self)
			connection.delegate = peer

			self.mutex.locked {
				self.peers[url] = peer
				if !isSelf {
					self.queryQueue.append(url)
				}
			}
		}
		else {
			Log.warning("[Node] not accepting incoming peer \(uc.host!): it has no UUID or the UUID is equal to ours, or is incompatible")
		}
	}

	func receive(candidate: Candidate<BlockType>) {
		self.mutex.locked {
			if candidate.height > self.ledger.longest.highest.index {
				self.candidateQueue.append(candidate)
			}
		}
	}

	func receive(block: BlockType) {
		self.mutex.locked {
			self.ledger.receive(block: block)
		}
	}

	func mined(block: BlockType) {
		_ = self.ledger.receive(block: block)

		// Send our peers the good news!
		self.mutex.locked {
			for (_, peer) in self.peers {
				switch peer.state {
				case .queried(let ps), .connected(let ps):
					self.workerQueue.async {
						do {
							Log.debug("[Node] posting mined block \(block.index) to peer \(peer)")
							try ps.request(gossip: .block(block.json))
						}
						catch {
							Log.error("[Node] Error sending mined block post: \(error.localizedDescription)")
						}
					}

				case .failed(error: _), .ignored, .querying, .connecting(_), .new:
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
			if let p = self.peers[candidate.peer], let c = p.mutex.locked(block: { return p.connection }) {

				do {
					try c.request(gossip: .fetch(candidate.hash)) { reply in
						if case .block(let blockData) = reply {
							do {
								let block = try BlockType.read(json: blockData)

								if block.isSignatureValid {
									Log.info("[Node] fetch returned valid block: \(block)")
									self.ledger.mutex.locked {
										_ = self.ledger.receive(block: block)
										if block.index > 0 &&
											self.ledger.orphansByPreviousHash[block.previous] != nil &&
											self.ledger.orphansByHash[block.previous] == nil &&
											self.ledger.longest.blocks[block.previous] == nil {
											// Ledger is looking for the previous block for this block, go get it from the peer we got this from
											self.workerQueue.async {
												self.fetch(candidate: Candidate(hash: block.previous, height: block.index-1, peer: candidate.peer))
											}
										}
									}
								}
								else {
									Log.warning("[Node] fetch returned invalid block; setting peer invalid")
									self.mutex.locked {
										self.peers[candidate.peer]?.fail(error: "invalid block")
									}
								}
							}
							catch {
								Log.error("[Gossip] Received invalid block format to fetch: \(reply)")
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
		}
	}

	public func start() {
		self.tickTimer.setEventHandler { [unowned self] _ in
			self.tick()
		}
		self.tickTimer.scheduleRepeating(deadline: .now(), interval: 2.0)
		self.tickTimer.resume()

		Kitura.start()
	}
}
