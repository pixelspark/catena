import Foundation
import Dispatch
import LoggerAPI

class Miner<BlockchainType: Blockchain> {
	typealias BlockType = BlockchainType.BlockType
	typealias HashType = BlockType.HashType

	private weak var node: Node<BlockchainType>?
	private(set) var block: BlockType? = nil
	private let mutex = Mutex()
	private var counter: BlockType.NonceType = 0
	public var isEnabled = true
	private(set) var isMining = false

	init(node: Node<BlockchainType>) {
		self.node = node
		self.counter = random(BlockType.NonceType.self)
	}

	/** Returns true when the transaction is new, or false if it isn't. */
	func append(transaction: BlockType.TransactionType) throws -> Bool {
		let isNew = try self.mutex.locked { () -> Bool in
			if self.block == nil {
				self.block = BlockType()
			}

			return try self.block!.append(transaction: transaction)
		}

		if isNew {
			self.start()
		}
		return isNew
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
							self.counter += BlockType.NonceType(1)
							if var b = self.block {
								b.index = base.index + 1
								b.previous = base.signature!
								b.nonce = self.counter

								// See if this combination is good enough
								let hash = HashType(of: b.dataForSigning)
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
