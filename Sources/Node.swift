import Foundation
import Kitura

fileprivate class Miner<BlockType: Block> {
	private let ledger: Ledger<BlockType>
	private var queue: [BlockType] = []
	private let mutex = Mutex()
	private var counter: UInt = 0
	private var mining = false

	init(ledger: Ledger<BlockType>) {
		self.ledger = ledger
	}

	private var base: BlockType {
		return self.ledger.longest.highest
	}

	func submit(block: BlockType) {
		self.mutex.locked {
			self.queue.append(block)
		}
		self.start()
	}

	private func start() {
		let shouldStart = self.mutex.locked { () -> Bool in 
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
				print("Miner mined \(mined)")
				self.ledger.receive(block: mined)
				// Send mined block to delegate
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
		while true {
			let (block, nonce, base, difficulty) = self.mutex.locked { () -> (BlockType?, UInt, BlockType, Int) in
				self.counter += 1
				let b = self.queue.first
				return (b, self.counter, self.base, self.ledger.longest.difficulty)
			}

			if var b = block {
				b.nonce = nonce
				b.previous = base.signature!
				b.index = base.index + 1
				let hash = b.signedData.hash
				if hash.difficulty >= difficulty {
					b.signature = hash
					self.mutex.locked {
						if let f = self.queue.first, f == block! {
							self.queue.removeFirst()
						}
					}
					return b
				}
			}
			else {
				break // Nothing left to mine
			}
		}

		return nil
	}
}

class Node<BlockType: Block> {
	let server: Server<BlockType>
	fileprivate let miner: Miner<BlockType>

	init(ledger: Ledger<BlockType>, port: Int) {
		self.server = Server(ledger: ledger, port: port)
		self.miner = Miner(ledger: ledger)
	}

	func submit(block: BlockType) {
		self.miner.submit(block: block)
	}

	public func start() {
		Kitura.start()
	}
}
