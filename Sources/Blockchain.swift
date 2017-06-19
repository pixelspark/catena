import Foundation
import Cryptor
import LoggerAPI

extension Data {
	var hash: Hash {
		return Hash(self.sha256)
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

public struct Hash: Equatable, Hashable, CustomDebugStringConvertible {
	let hash: Data

	static var zeroHash: Hash {
		return Hash(Data(bytes: [UInt8](repeating: 0,  count: Int(32))))
	}

	init?(string: String) {
		if let d = string.hexDecoded, d.count == Int(32) {
			self.hash = d
		}
		else {
			return nil
		}
	}

	init(_ hash: Data) {
		assert(hash.count == Int(32))
		self.hash = hash
	}

	init(of: Data) {
		self.hash = of.sha256
	}

	public var debugDescription: String {
		return self.stringValue
	}

	var stringValue: String {
		return self.hash.map { String(format: "%02hhx", $0) }.joined()
	}

	var difficulty: Int {
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

public func == (lhs: Hash, rhs: Hash) -> Bool {
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

public protocol Transaction {
	var isSignatureValid: Bool { get }
}

protocol Blockchain {
	associatedtype BlockType: Block

	var highest: BlockType { get }
	var genesis: BlockType { get }
	var difficulty: Int { get }

	func get(block: Hash) throws -> BlockType?
	func append(block: BlockType) throws -> Bool
	func unwind(to: BlockType) throws
}

public protocol Block: CustomDebugStringConvertible, Equatable {
	associatedtype TransactionType: Transaction

	var index: UInt { get set }
	var previous: Hash { get set }
	var nonce: UInt { get set }
	var signature: Hash? { get set }
	var payloadData: Data { get }
	var payloadDataForSigning: Data { get }

	init()
	init(index: UInt, previous: Hash, payload: Data) throws
	mutating func append(transaction: TransactionType) throws
	func isPayloadValid() -> Bool
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
			return self.dataForSigning.hash == s
		}
		return false
	}

	var isAGenesisBlock: Bool {
		return self.previous == Hash.zeroHash
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
			let hash = self.dataForSigning.hash
			if hash.difficulty >= difficulty {
				self.signature = hash
				return
			}
		}
	}
}

class Ledger<BlockchainType: Blockchain>: CustomDebugStringConvertible {
	typealias BlockType = BlockchainType.BlockType

	var longest: BlockchainType
	let mutex = Mutex()
	var orphansByHash: [Hash: BlockType] = [:]
	var orphansByPreviousHash: [Hash: BlockType] = [:]

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
										Log.info("[Ledger] missing intermediate block: \(r.index-1) with hash \(r.previous.stringValue)")
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
								break
							}
						}

						// We found a root earlier in the chain. Unwind to the root and apply all blocks
						if let r = root {
							if let splice = try self.longest.get(block: r.previous) {
								if splice.signature! != self.longest.highest.signature! {
									Log.info("[Ledger] splicing to \(r.index), then fast-forwarding to \(block.index)")
									try self.longest.unwind(to: splice)
								}

								Log.info("[Ledger] head is now at \(self.longest.highest.index) \(self.longest.highest.signature!.stringValue)")
								for b in stack.reversed() {
									Log.info("[Ledger] appending \(b.index) \(b.signature!.stringValue)")
									if !(try self.longest.append(block: b)) {
										fatalError("this block should be appendable!")
									}
								}
								return true
							}
							else {
								Log.info("[Ledger] splicing not possible for root=\(r.signature!), which requires \(r.previous) to be in cahin")
								return false
							}
						}
						else {
							Log.info("[Ledger] splicing not possible (no root)")
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
