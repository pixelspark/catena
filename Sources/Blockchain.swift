import Foundation
import Cryptor
import LoggerAPI

public protocol Blockchain {
	associatedtype BlockType: Block

	/** The block currently at the head of this blockchain. */
	var highest: BlockType { get }

	/** The genesis block (is required to have index=0). */
	var genesis: BlockType { get }

	/** The number of zero bits a signature is required to have at the beginning to be considered a valid proof of work. */
	var difficulty: Int { get }

	/** Returns the block with the given signature hash, if it is on this chain. Returns nil when there is no block on
	this chain with the given hash. */
	func get(block: BlockType.HashType) throws -> BlockType?

	/** Append the given block to the head of this chain. The block needs to have the current head block as previous block,
	and needs to be valid. Will return true when the block was actually appended, and false if it wasn't. */
	func append(block: BlockType) throws -> Bool

	/** 'Rewind' the blockchain so that the `to` block becomes the new head. The `to` block needs to be on this chain, or
	the function will throw an error. */
	func unwind(to: BlockType) throws
}

public protocol Transaction: Hashable {
	init(json: [String: Any]) throws
	var isSignatureValid: Bool { get }
	var json: [String: Any] { get }
}

public protocol Block: CustomDebugStringConvertible, Equatable {
	typealias NonceType = UInt64
	typealias IndexType = UInt64

	associatedtype TransactionType: Transaction
	associatedtype HashType: Hash

	/** The position of this block in the blockchain. The block with index 0 is considered the genesis block, and has a
	zero previous hash. For all other blocks, the index of the block with the previous hash should have an index that is
	exactly one lower than the index of this block. */
	var index: IndexType { get set }

	/** The hash of the previous block in this chain, or a zero hash in case this block is the genesis block (index=0). */
	var previous: HashType { get set }

	/** The nonce used to generate a valid proof-of-work signature for this block. */
	var nonce: NonceType { get set }

	/** The signature hash for this block, or nil if the block has not been signed yet. */
	var signature: HashType? { get set }

	/** The payload of this block. */
	var payloadData: Data { get }

	/** The payload data of this block actually used for signing. */
	var payloadDataForSigning: Data { get }

	/** Create a new, empty block with index=0 and previous set to the zero hash. */
	init()

	/** Create a block with the given index, previous hash and payload data. */
	init(index: UInt64, previous: HashType, payload: Data) throws

	/** Append a transaction to the payload data of this block. Returns true if the transaction was appended, and false
	when it wasn't (e.g. when the block already contains the transaction). */
	mutating func append(transaction: TransactionType) throws -> Bool

	/** Whether the block can accomodate the `transaction`, disregarding any validation of the transaction itself. */
	func hasRoomFor(transaction: TransactionType) -> Bool

	/** Perform validation on the payload itself (e.g. signatures on contained transactions) and returns whether the
	payload is valid. */
	func isPayloadValid() -> Bool
}

public protocol Hash: Hashable, CustomDebugStringConvertible {
	var hash: Data { get }
	var difficulty: Int { get }

	static var zeroHash: Self { get }

	init(hash: Data)
	init(of: Data)
	init?(hash: String)
	var stringValue: String { get }
}

extension String {
	var hexDecoded: Data? {
		var error = false
		let s = Array(self.characters)
		let numbers = stride(from: 0, to: s.count, by: 2).map() { (idx: Int) -> UInt8 in
			let res = strtoul(String(s[idx ..< Swift.min(idx + 2, s.count)]), nil, 16)
			if res > UInt(UInt8.max) {
				error = true
				return UInt8(0)
			}
			return UInt8(res)
		}

		if error {
			return nil
		}

		return Data(bytes: numbers)
	}
}


extension UInt8 {
	var numberOfLeadingZeroBits: Int {
		var n = 0
		var b = self
		if b == 0 {
			n += 8
		}
		else {
			while (b & 0x80) == 0 {
				b <<= 1
				n += 1
			}
		}
		return n
	}
}

public struct SHA256Hash: Hash {
	public let hash: Data

	public static var zeroHash: SHA256Hash {
		return SHA256Hash(hash: Data(bytes: [UInt8](repeating: 0,  count: Int(32))))
	}

	public init?(hash string: String) {
		if let d = string.hexDecoded, d.count == Int(32) {
			self.hash = d
		}
		else {
			return nil
		}
	}

	public init(hash: Data) {
		assert(hash.count == Int(32))
		self.hash = hash
	}

	public init(of: Data) {
		self.hash = of.sha256
	}

	public var debugDescription: String {
		return self.stringValue
	}

	public var stringValue: String {
		return self.hash.map { String(format: "%02hhx", $0) }.joined()
	}

	public var difficulty: Int {
		var n = 0
		for byte in self.hash {
			n += byte.numberOfLeadingZeroBits
			if byte != 0 {
				break
			}
		}
		return n
	}

	public var hashValue: Int {
		return self.hash.hashValue
	}
}

public func == (lhs: SHA256Hash, rhs: SHA256Hash) -> Bool {
	return lhs.hash == rhs.hash
}

enum BlockError: LocalizedError {
	case formatError

	var errorDescription: String? {
		switch self {
		case .formatError: return "block format error"
		}
	}
}

extension Data {
	mutating func appendRaw<T>(_ item: T) {
		var item = item
		let ptr = withUnsafePointer(to: &item) { ptr in
			return UnsafeRawPointer(ptr)
		}
		self.append(ptr.assumingMemoryBound(to: UInt8.self), count: MemoryLayout<T>.size)
	}
}

extension Block {
	var isSignatureValid: Bool {
		if let s = self.signature {
			return HashType(of: self.dataForSigning) == s
		}
		return false
	}

	var isAGenesisBlock: Bool {
		return self.previous == HashType.zeroHash && self.index == 0
	}

	var dataForSigning: Data {
		let pd = self.payloadDataForSigning
		var data = Data(capacity: pd.count + previous.hash.count + 2 * MemoryLayout<UInt>.size)

		data.appendRaw(self.index.littleEndian)
		data.appendRaw(self.nonce.littleEndian)

		previous.hash.withUnsafeBytes { bytes in
			data.append(bytes, count: previous.hash.count)
		}

		pd.withUnsafeBytes { bytes in
			data.append(bytes, count: pd.count)
		}

		return data
	}

	/** Mine this block (note: use this only for the genesis block, Miner provides threaded mining) */
	mutating func mine(difficulty: Int) {
		while true {
			self.nonce += 1
			let hash = HashType(of: self.dataForSigning)
			if hash.difficulty >= difficulty {
				self.signature = hash
				return
			}
		}
	}
}

class Ledger<BlockchainType: Blockchain>: CustomDebugStringConvertible {
	typealias BlockType = BlockchainType.BlockType
	typealias HashType = BlockType.HashType

	var longest: BlockchainType
	let mutex = Mutex()
	var orphansByHash: [HashType: BlockType] = [:]
	var orphansByPreviousHash: [HashType: BlockType] = [:]

	init(longest: BlockchainType) {
		self.longest = longest
	}

	let spliceLimit: UInt = 1

	func isNew(block: BlockType) throws -> Bool {
		return try self.mutex.locked {
			// We already know this block and it is currently an orphan
			if self.orphansByHash[block.signature!] != nil {
				return false
			}

			// We already know this block, it is on-chain
			if try self.longest.get(block: block.signature!) != nil {
				return false
			}

			// Block hasn't been seen by us before or was forgotten
			return true
		}
	}

	/** Returns whether a transaction is valid for inclusion in the transaction memory pool (to be mined). The optional
	`pool` argument may refer to a block that contains transactions currently in the memory pool (for mining). */
	func canAccept(transaction: BlockType.TransactionType, pool: BlockType?) throws -> Bool {
		fatalError("Needs to be implemented")
	}

	func receive(block: BlockType) throws -> Bool {
		Log.debug("[Ledger] receive block #\(block.index) \(block.signature!.stringValue)")
		return try self.mutex.locked { () -> Bool in
			if block.isSignatureValid {
				// Were we waiting for this block?
				var block = block
				while let next = self.orphansByPreviousHash[block.signature!] {
					self.orphansByHash[block.signature!] = block
					self.orphansByPreviousHash[block.previous] = block
					block = next
				}

				// This block can simply be appended to the chain
				if try self.longest.append(block: block) {
					Log.info("[Ledger] can append directly")
					return true
				}
				else {
					// Block cannot be directly appended. 
					if block.index > self.longest.highest.index {
						// The block is newer
						var root: BlockType? = block
						var stack: [BlockType] = []
						while true {
							if let r = root {
								if try self.longest.get(block: r.previous) == nil {
									if let prev = self.orphansByHash[r.previous] {
										root = prev
										stack.append(r)
									}
									else {
										// We don't have an intermediate block. Save head block as orphan
										self.orphansByHash[block.signature!] = block
										self.orphansByPreviousHash[block.previous] = block
										Log.debug("[Ledger] missing intermediate block: \(r.index-1) with hash \(r.previous.stringValue)")
										return false
									}
								}
								else {
									// Root has previous in chain
									stack.append(r)
									break
								}
							}
							else {
								Log.info("[Ledger] unrooted!")
								break
							}
						}

						// We found a root earlier in the chain. Unwind to the root and apply all blocks
						if let r = root {
							if let splice = try self.longest.get(block: r.previous) {
								assert(splice.index == (r.index - 1))

								if splice.signature! != self.longest.highest.signature! {
									Log.debug("[Ledger] splicing to \(splice.index), then fast-forwarding to \(block.index)")
									try self.longest.unwind(to: splice)
								}

								Log.info("[Ledger] head is now at \(self.longest.highest.index) \(self.longest.highest.signature!.stringValue)")
								for b in stack.reversed() {
									Log.debug("[Ledger] appending \(b.index) \(b.signature!.stringValue) prev=\(b.previous.stringValue)")
									if !(try self.longest.append(block: b)) {
										fatalError("this block should be appendable! #\(b.index) hash=\(b.signature!.stringValue) prev=\(b.previous.stringValue), upon #\(self.longest.highest.index) \(self.longest.highest.signature!.stringValue)")
									}
								}
								return true
							}
							else {
								Log.debug("[Ledger] splicing not possible for root=\(r.signature!), which requires \(r.previous) to be in cahin")
								return false
							}
						}
						else {
							Log.debug("[Ledger] splicing not possible (no root)")
							return false
						}
					}
					else {
						// The block is older, but it may be of use later. Save as orphan
						self.orphansByHash[block.signature!] = block
						self.orphansByPreviousHash[block.previous] = block
						return false
					}
				}
			}
			else {
				return false
			}
		}
	}

	var debugDescription: String {
		return self.mutex.locked {
			"Client [longest height=\(self.longest.highest.index) \(self.longest.highest.signature!.stringValue)]"
		}
	}
}
