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

	public var transactionsSetAside: Set<BlockType.TransactionType> {
		return self.mutex.locked {
			return Set(self.aside)
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

	private func restart() throws {
		try self.mutex.locked {
			if self.block == nil {
				self.block = try BlockType.template(for: self.miner)
			}

			// Do we have transactions set aside for the next block?
			if let n = node, !self.aside.isEmpty {
				// Keep adding transactions until block is full or set is empty
				self.aside = try self.aside.filter { next in
					if self.block!.hasRoomFor(transaction: next) {
						switch try n.ledger.canAccept(transaction: next, pool: self.block!) {
						case .now:
							do {
								/* If a transaction fails, it stays in the 'aside' set. The transactions in the aside set
								need to be pruned every now and then. */
								_ = try self.append(transaction: next)
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
				var block: BlockType! = nil
				var difficulty: BlockType.WorkType! = nil

				// Obtain the mining parameters and template block
				self.mutex.locked { () -> () in
					stop = !self.isEnabled

					if let n = node {
						if let base = self.node?.ledger.longest.highest {
							difficulty = try! n.ledger.longest.difficulty(forBlockFollowing: base)

							// Set up the block
							self.counter += BlockType.NonceType(1)
							if var b = self.block {
								b.index = base.index + 1
								b.previous = base.signature!
								b.nonce = self.counter
								block = b
							}
						}
						else {
							stop = true
						}
					}
					else {
						stop = true
					}
				}

				if stop || !block.isPayloadValid() {
					return
				}

				// Try some hashes!
				for _ in 0..<self.uninterruptedTries {
					block.nonce += 1
					// See if this combination is good enough
					let hash = HashType(of: block.dataForSigning)
					if hash.difficulty >= difficulty {
						// We found a block!
						block.signature = hash
						stop = true

						self.mutex.locked {
							self.block = nil
							self.counter = block.nonce

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

		try self.restart()
	}
}
