import Foundation
import Dispatch
import LoggerAPI

public class Miner<LedgerType: Ledger> {
	enum MinerError: LocalizedError {
		case unsignedTransactionCannotBeMined

		var errorDescription: String? {
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
	public var isEnabled = true

	private weak var node: Node<LedgerType>?
	private let mutex = Mutex()
	private var counter: BlockType.NonceType = 0
	private(set) var isMining = false

	/** The queue holds all transactions that should be considered for inclusion in a new block. They
	are very likely to be executable and pass signature checks. */
	private var queue = OrderedSet<BlockType.TransactionType>()

	/** The aside set includes all transactions that are likely to be executable in the future. */
	private var aside = OrderedSet<BlockType.TransactionType>()

	/** Number of hashes the miner will try before reconstructing the template block. */
	private let uninterruptedTries = 4096

	init(node: Node<LedgerType>, miner: BlockType.IdentityType) {
		self.node = node
		self.miner = miner
		self.counter = random(BlockType.NonceType.self)
	}

	/** Returns true when the transaction is new, or false if it isn't. */
	func append(transaction: BlockType.TransactionType) throws -> Bool {
		let isNew = try self.mutex.locked { () -> Bool in
			// Only signed transactions can be mined. This also checks the transaction maximum size
			if !transaction.isSignatureValid {
				throw MinerError.unsignedTransactionCannotBeMined
			}

			if self.queue.contains(member: transaction) {
				return false
			}

			self.queue.append(transaction)
			return true
		}
		return isNew
	}

	public var transactionsSetAside: Set<BlockType.TransactionType> {
		return self.mutex.locked {
			return Set(self.aside)
		}
	}

	public var queuedTransactions: Set<BlockType.TransactionType> {
		return self.mutex.locked {
			return Set(self.queue)
		}
	}

	/** Add a transaction to the 'aside' table, which means it will be considered for the next mining block (use this for
	transactions that are not currently acceptable but may become acceptable in the future). Returns true if the
	transaction is not yet in the aside list, and false if it already is. */
	public func setAside(transaction: BlockType.TransactionType) -> Bool {
		return self.mutex.locked {
			if self.aside.contains(member: transaction) {
				return false
			}

			while self.aside.count > LedgerType.ParametersType.maximumAsideTransactions {
				_ = self.aside.remove(at: 0)
			}

			self.aside.append(transaction)
			return true
		}
	}

	public func start() {
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

	private func mine() throws {
		var stop = false

		while !stop {
			// Form a new block
			var block = try BlockType.template(for: self.miner)
			var includedTransactions: [BlockType.TransactionType] = []
			var difficulty: BlockType.WorkType! = nil

			// FIXME: also update timestamp every few retries
			block.date = Date()

			try self.mutex.locked {
				if !self.isEnabled {
					Log.info("[Miner] ending mining session: not enabled")
					stop = true
					return
				}

				if let n = self.node {
					let base = n.ledger.longest.highest
					difficulty = try! n.ledger.longest.difficulty(forBlockFollowing: base)

					// Set up the block
					self.counter += BlockType.NonceType(1)
					block.index = base.index + 1
					block.previous = base.signature!
					block.nonce = self.counter
				}
				else {
					stop = true
					return
				}

				// Insert transactions from the queue
				var setAside: [BlockType.TransactionType] = []
				for transaction in self.queue {
					if block.hasRoomFor(transaction: transaction) {
						if (try! block.append(transaction: transaction)) {
							includedTransactions.append(transaction)
						}
						else {
							Log.info("[Miner] Setting aside \(transaction) in mining loop")
							setAside.append(transaction)
						}
					}
					else {
						break
					}
				}

				// Keep adding transactions to the queue until block is full or set is empty
				if let n = self.node {
					self.aside = try self.aside.filter { next in
						if block.hasRoomFor(transaction: next) {
							switch try n.ledger.canAccept(transaction: next, pool: block) {
							case .now:
								do {
									/* If a transaction fails, it stays in the 'aside' set. The transactions in the aside set
									need to be pruned every now and then. */
									_ = try self.append(transaction: next)
									if !(try block.append(transaction: next)) {
										fatalError("block should be appendable, block said it had room!")
									}
									includedTransactions.append(next)
									Log.info("[Miner] Promoting aside \(next) to queue in mining loop")
									return false
								}
								catch {
									// For some reason this transaction fails to append, and it is not about the size. Just forget it
									Log.error("[Miner] transaction \(next) failed to add from aside: \(error.localizedDescription)")
									return false
								}
							case .future:
								return true

							case .never:
								return false
							}
						}
						else {
							return true
						}
					}
				}

				// Move some transactions over to the aside set
				setAside.forEach { tr in
					self.aside.append(tr)
				}

				if includedTransactions.isEmpty {
					Log.info("[Miner] ending mining session: no transactions included (aside=\(self.aside.count))")
					stop = true
					return
				}
			}

			// Actually go mine
			autoreleasepool { () -> () in
				if stop {
					Log.info("[Miner] ending mining session block valid=\(block.isPayloadValid())")
					return
				}

				assert(block.isPayloadValid(), "only valid blocks should have been created")

				// Try some hashes!
				for _ in 0..<self.uninterruptedTries {
					block.nonce += 1
					// See if this combination is good enough
					let hash = HashType(of: block.dataForSigning)
					if hash.difficulty >= difficulty {
						// We found a block!
						block.signature = hash

						self.mutex.locked {
							self.counter = block.nonce

							// Remove mined transactions from the queue
							includedTransactions.forEach { tr in
								self.queue.remove(tr)
								Log.info("[Miner] Dequeued \(tr) in mining loop")
							}

							// Inform the node of the good news
							if let n = self.node {
								DispatchQueue.global(qos: .background).async {
									Log.info("[Miner] mined block #\(block.index) required difficulty=\(difficulty!) found \(block.signature!.difficulty)")
									do {
										try n.mined(block: block)
									}
									catch {
										Log.info("[Miner] mined block #\(block.index), but node fails: \(error.localizedDescription)")
									}
								}
							}
						}

						break
					}
				}

				self.mutex.locked {
					self.counter = block.nonce
				}
			}
		}
	}
}
