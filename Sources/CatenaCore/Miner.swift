import Foundation
import Dispatch
import LoggerAPI

public class Miner<LedgerType: Ledger> {
	enum MinerError: LocalizedError {
		case unsignedTransactionCannotBeMined

		var localizedDescription: String {
			switch self {
			case .unsignedTransactionCannotBeMined:
				return "an unsigned transaction cannot be mined"
			}
		}
	}

	public typealias BlockchainType = LedgerType.BlockchainType
	public typealias BlockType = BlockchainType.BlockType
	public typealias HashType = BlockType.HashType

	public let miner: BlockType.IdentityType
	public private(set) var block: BlockType? = nil
	public var isEnabled = true

	private weak var node: Node<LedgerType>?
	private let mutex = Mutex()
	private var counter: BlockType.NonceType = 0
	private(set) var isMining = false
	private var aside = OrderedSet<BlockType.TransactionType>()

	init(node: Node<LedgerType>, miner: BlockType.IdentityType) {
		self.node = node
		self.miner = miner
		self.counter = random(BlockType.NonceType.self)
	}

	/** Returns true when the transaction is new, or false if it isn't. */
	func append(transaction: BlockType.TransactionType) throws -> Bool {
		let isNew = try self.mutex.locked { () -> Bool in
			if self.block == nil {
				self.block = try BlockType.template(for: self.miner)
			}

			// Only signed transactions can be mined. This also checks the transaction maximum size
			if !transaction.isSignatureValid {
				throw MinerError.unsignedTransactionCannotBeMined
			}

			// Is there room left in the block?
			if self.block!.hasRoomFor(transaction: transaction) {
				return try self.block!.append(transaction: transaction)
			}
			else {
				// Not now
				self.aside.append(transaction)
				return false
			}
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
				do {
					try self.mine()
				}
				catch {
					Log.error("[Miner] \(error.localizedDescription)")
				}

				self.mutex.locked {
					self.isMining = false
				}
			}
		}
	}

	private func restart() throws {
		try self.mutex.locked {
			if self.block == nil {
				self.block = try BlockType.template(for: self.miner)
			}

			// Do we have transactions set aside for the next block?
			if !self.aside.isEmpty {
				// Keep adding transactions until block is full or set is empty
				while let next = self.aside.first {
					if self.block!.hasRoomFor(transaction: next) {
						do {
							// This can fail, but transactions in the 'aside' set just get one chance
							_ = try self.append(transaction: next)
						}
						catch {
							// For some reason this transaction fails to append, and it is not about the size. Just forget it
							Log.error("[Miner] transaction \(next) failed to add from aside: \(error.localizedDescription)")
						}
						_ = self.aside.removeFirst()
					}
					else {
						break
					}
				}

				self.start()
			}
		}
	}

	private func mine() throws {
		var stop = self.mutex.locked { () -> Bool in
			if var b = self.block, self.isEnabled {
				// FIXME: also update timestamp every few retries
				b.date = Date()
				return false
			}
			else {
				return true
			}
		}

		while !stop {
			autoreleasepool { () -> () in
				self.mutex.locked { () -> () in
					stop = !self.isEnabled

					if let n = node {
						if let base = self.node?.ledger.longest.highest {
							let difficulty = n.ledger.longest.difficulty(forBlockFollowing: base)
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

		try self.restart()
	}
}
