import Foundation
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
		self.counter = UInt(abs(random(Int.self)))
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
				Log.info("[Miner] mined \(mined)")
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
