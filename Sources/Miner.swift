import Foundation
import Dispatch
import LoggerAPI

class Miner<BlockchainType: Blockchain> {
	typealias BlockType = BlockchainType.BlockType

	private weak var node: Node<BlockchainType>?
	private var block: BlockType? = nil
	private let mutex = Mutex()
	private var counter: UInt = 0
	public var isEnabled = true
	private(set) var isMining = false

	init(node: Node<BlockchainType>) {
		self.node = node
		self.counter = UInt(abs(random(Int.self)))
	}

	func append(callback: ((BlockType?) throws -> BlockType)) rethrows {
		try self.mutex.locked {
			self.block = try callback(self.block)
		}
		self.start()
	}

	private func start() {
		let shouldStart = self.mutex.locked { () -> Bool in
			if !self.isEnabled {
				Log.info("[Miner] mining is not enabled")
				return false
			}

			if !self.isMining {
				self.isMining = true
				return true
			}
			return false
		}

		if shouldStart {
			DispatchQueue.global(qos: .background).async {
				self.mine()
				self.mutex.locked {
					self.isMining = false
				}
			}
		}
	}

	private func mine() {
		var stop = self.mutex.locked { return !self.isEnabled }

		while !stop {
			autoreleasepool { () -> () in
				self.mutex.locked { () -> () in
					stop = !self.isEnabled

					if let n = node {
						let difficulty = n.ledger.longest.difficulty

						if let base = self.node?.ledger.longest.highest {
							// Set up the block
							self.counter += 1
							if var b = self.block {
								b.index = base.index + 1
								b.previous = base.signature!
								b.nonce = self.counter

								// See if this combination is good enough
								let hash = b.dataForSigning.hash
								if hash.difficulty >= difficulty {
									// We found a block!
									b.signature = hash
									self.block = nil
									stop = true
									DispatchQueue.global(qos: .background).async {
										Log.info("[Miner] mined block #\(b.index)")
										do {
											try n.mined(block: b)
										}
										catch {
											Log.info("[Miner] mined block #\(b.index), but node fails: \(error.localizedDescription)")
										}
									}
								}
							}
							else {
								// No block to mine
								stop = true
							}
						}
					}
					else {
						// Node has been destroyed
						stop = true
					}
				}
			}
		}
	}
}
