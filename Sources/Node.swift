import Foundation
import Kitura
import Dispatch
import LoggerAPI

class Miner<BlockType: Block> {
	private weak var node: Node<BlockType>?
	private var queue: [Data] = []
	private let mutex = Mutex()
	private var counter: UInt = 0
	private var mining = false
	public var enabled = true

	init(node: Node<BlockType>) {
		self.node = node
		self.counter = random(UInt.self)
	}

	func submit(payload: Data) {
		self.mutex.locked {
			self.queue.append(payload)
		}
		self.start()
	}

	private func start() {
		let shouldStart = self.mutex.locked { () -> Bool in
			if !self.enabled {
				Log.info("[Miner] mining is not enabled")
				return false
			}

			if !self.mining {
				self.mining = true
				return true
			}
			return false
		}

		if shouldStart {
			tick()
		}
	}

	private func tick() {
		DispatchQueue.global(qos: .background).async {
			if let mined = self.mine() {
				print("[Miner] mined \(mined)")
				self.node?.mined(block: mined)
				self.tick() // Next block!
			}
			else {
				// Nothing to mine
				self.mutex.locked {
					self.mining = false
				}
			}
		}
	}

	private func mine() -> BlockType? {
		var stop = self.mutex.locked { return !self.enabled }

		var block: BlockType? = nil
		var currentPayload: Data? = nil

		while !stop {
			let b = autoreleasepool { () -> BlockType? in

				var nonce: UInt = 0
				var base: BlockType? = nil
				var difficulty: Int = 0

				self.mutex.locked { () -> () in
					if let n = node {
						base = self.node?.ledger.longest.highest
						if let base = base {
							self.counter += 1
							if let payload = self.queue.first {
								if block == nil || currentPayload != payload {
									currentPayload = payload
									block = try! BlockType(index: base.index + 1, previous: base.signature!, payload: payload)
								}
								else {
									block!.index = base.index + 1
									block!.previous = base.signature!
								}

								difficulty = n.ledger.longest.difficulty
								nonce = self.counter
							}
						}
					}
				}

				if var b = block, let base = base {
					b.nonce = nonce
					b.previous = base.signature!
					b.index = base.index + 1
					let hash = b.signedData.hash
					if hash.difficulty >= difficulty {
						b.signature = hash
						self.mutex.locked {
							if let f = self.queue.first, f == block?.payloadData {
								self.queue.removeFirst()
							}
						}
						return b
					}
				}
				else {
					stop = true
					return nil
				}

				return nil
			}

			if let b = b {
				return b
			}
		}

		return nil
	}
}

enum PeerStatus {
	case new		// The peer is new
	case querying	// The peer is currently being queried
	case queried	// The peer has last been queried successfully
	case failed(error: String)	// Talking to the peer failed for some reason, ignore it for a while
	case ignored	// The peer is ourselves or believes in another genesis, ignore it forever
}

class Node<BlockType: Block> {
	struct Candidate: Equatable {
		static func ==(lhs: Node<BlockType>.Candidate, rhs: Node<BlockType>.Candidate) -> Bool {
			return lhs.hash == rhs.hash && lhs.peer == rhs.peer
		}

		let hash: Hash
		let peer: Peer<BlockType>
	}

	private(set) var server: Server<BlockType>! = nil
	private(set) var miner: Miner<BlockType>! = nil
	let ledger: Ledger<BlockType>
	let uuid: UUID

	private let tickTimer: DispatchSourceTimer
	private let mutex = Mutex()
	private let workerQueue = DispatchQueue.global(qos: .background)
	private(set) var peers: [Peer<BlockType>: PeerStatus] = [:]
	private var queryQueue: [Peer<BlockType>] = []
	private var blockQueue: [Candidate] = []

	var validPeers: Set<Peer<BlockType>> {
		return Set(peers.flatMap { (peer, status) -> Peer<BlockType>? in
			switch status {
			case .failed(_), .querying, .new, .ignored:
				return nil

			case .queried:
				return peer
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

	func add(peer: Peer<BlockType>) {
		self.mutex.locked {
			if self.peers[peer] == nil {
				self.peers[peer] = .new
				self.queryQueue.append(peer)
			}
		}
	}

	func mined(block: BlockType) {
		_ = self.ledger.receive(block: block)

		// Send our peers the good news!
		self.mutex.locked {
			for (peer, status) in self.peers {
				switch status {
				case .queried, .new:
					self.workerQueue.async {
						Log.debug("[Node] posting mined block \(block.index) to peer \(peer)")
						peer.post(block: block)
					}

				case .failed(error: _), .ignored, .querying:
					break
				}
			}
		}
	}

	private func tick() {
		self.mutex.locked {
			// Do we need to fetch any blocks?
			if let candidate = self.blockQueue.first {
				self.blockQueue.remove(at: 0)
				Log.debug("[Node] fetch candidate \(candidate)")
				self.fetch(candidate: candidate)
			}

			// Take the first from the query queue...
			if let p = self.queryQueue.first {
				self.queryQueue.remove(at: 0)
				self.query(peer: p)
				return
			}

			// Re-query all peers that are not already being queried
			for (peer, status) in self.peers {
				switch status {
				case .new, .queried, .failed(error: _):
					self.queryQueue.append(peer)
					return // One at a time
				case .querying, .ignored:
					break
				}
			}
		}
	}

	private func fetch(candidate: Candidate) {
		self.workerQueue.async {
			candidate.peer.fetch(hash: candidate.hash) { result in
				switch result {
				case .success(let block):
					if block.isSignatureValid {
						Log.info("[Node] fetch returned valid block: \(block)")
						self.ledger.mutex.locked {
							_ = self.ledger.receive(block: block)
							if self.ledger.orphansByPreviousHash[block.previous] != nil && self.ledger.orphansByHash[block.previous] == nil && self.ledger.longest.blocks[block.previous] == nil {
								// Ledger is looking for the previous block for this block, go get it from the peer we got this from
								self.workerQueue.async {
									self.fetch(candidate: Candidate(hash: block.previous, peer: candidate.peer))
								}
							}
						}
					}
					else {
						Log.warning("[Node] fetch returned invalid block; setting peer invalid")
						self.mutex.locked {
							self.peers[candidate.peer] = .failed(error: "invalid block")
						}
					}

				case .failure(let e):
					Log.warning("[Node] fetch failed: \(e)")
				}
			}
		}
	}

	private func query(peer: Peer<BlockType>) {
		self.mutex.locked {
			if let status = self.peers[peer] {
				switch status {
				case .new, .queried, .failed(_):
					self.peers[peer] = .querying
					self.workerQueue.async {
						peer.ping(from: self) { result in
							switch result {
							case .success(let index):
								// Update peer state
								self.mutex.locked {
									if index.uuid == self.uuid {
										// Peer is ourself! Ignore ourself.
										self.peers[peer] = .ignored
									}
									else if index.genesis != self.ledger.longest.genesis.signature! {
										// Peer believes in another genesis, ignore him
										self.peers[peer] = .ignored
									}
									else {
										self.peers[peer] = .queried
									}
								}

								// New peers?
								for p in index.peers {
									if let u = URL(string: p) {
										self.add(peer: Peer<BlockType>(URL: u))
									}
								}

								// See if there are new blocks
								self.ledger.mutex.locked {
									if index.height > self.ledger.longest.highest.index {
										// There might be a longer chain at another node!
										self.mutex.locked {
											let candidate = Candidate(hash: index.highest, peer: peer)
											if !self.blockQueue.contains(candidate) && self.ledger.orphansByHash[candidate.hash] == nil {
												Log.info("[Node] Peer \(peer) has a chain that is \(index.height - self.ledger.longest.highest.index) blocks ahead; pursuing chain")
												self.blockQueue.append(candidate)
											}
										}
									}
								}

							case .failure(let e):
								self.mutex.locked {
									self.peers[peer] = .failed(error: e)
								}
							}
						}
					}

				case .querying, .ignored:
					// Already querying or ignored forever
					break
				}
			}
			else {
				fatalError("[Node] querying unknown peer; add(peer:) first!")
			}
		}
	}

	public func start() {
		self.tickTimer.setEventHandler { [weak self] _ in
			self?.tick()
		}
		self.tickTimer.scheduleRepeating(deadline: .now(), interval: 2.0)
		self.tickTimer.resume()

		Kitura.start()
	}
}
