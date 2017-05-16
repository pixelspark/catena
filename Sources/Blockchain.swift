import Foundation
import CommonCrypto

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
		return Hash(Data(bytes: [UInt8](repeating: 0,  count: Int(CC_SHA256_DIGEST_LENGTH))))
	}

	init?(string: String) {
		if let d = string.hexDecoded, d.count == Int(CC_SHA256_DIGEST_LENGTH) {
			self.hash = d
		}
		else {
			return nil
		}
	}

	init(_ hash: Data) {
		assert(hash.count == Int(CC_SHA256_DIGEST_LENGTH))
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

public protocol Block: CustomDebugStringConvertible, Equatable {
	var index: UInt { get set }
	var previous: Hash { get set }
	var nonce: UInt { get set }
	var signature: Hash? { get set }
	var payloadData: Data { get }

	init(index: UInt, previous: Hash, payload: Data)
}

func ==<T: Block>(lhs: T, rhs: T) -> Bool {
	return lhs.signedData == rhs.signedData
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
			return self.signedData.hash == s
		}
		return false
	}

	var signedData: Data {
		var data = Data(capacity: self.payloadData.count + previous.hash.count + 2 * MemoryLayout<UInt>.size)

		data.appendRaw(self.index.littleEndian)
		data.appendRaw(self.nonce.littleEndian)

		previous.hash.withUnsafeBytes { bytes in
			data.append(bytes, count: previous.hash.count)
		}

		self.payloadData.withUnsafeBytes { bytes in
			data.append(bytes, count: self.payloadData.count)
		}

		return data

		/*let md = NSMutableData()
		var index = self.index
		var nonce = self.nonce
		md.append(&index, length: MemoryLayout<UInt>.size)
		md.append(&nonce, length: MemoryLayout<UInt>.size)
		md.append(previous.hash)
		md.append(self.payloadData)
		return md as Data*/
	}

	/** Mine this block (note: use this only for the genesis block, Miner provides threaded mining) */
	mutating func mine(difficulty: Int) {
		while true {
			self.nonce += 1
			let hash = self.signedData.hash
			if hash.difficulty >= difficulty {
				self.signature = hash
				return
			}
		}
	}
}

class Blockchain<BlockType: Block>: CustomDebugStringConvertible {
	var highest: BlockType
	let genesis: BlockType
	var blocks: [Hash: BlockType] = [:]

	required init(genesis: BlockType) {
		assert(genesis.index == 0)
		self.genesis = genesis
		self.highest = genesis
		self.blocks[genesis.signature!] = genesis
	}

	func append(block: BlockType) -> Bool {
		if block.previous == self.highest.signature! && block.index == (self.highest.index + 1) && block.isSignatureValid && block.signature!.difficulty >= self.difficulty {
			self.highest = block
			self.blocks[block.signature!] = block
			return true
		}
		return false
	}

	func unwind(to: BlockType) {
		while highest != to {
			if let previous = blocks[highest.previous] {
				highest = previous
			}
			else {
				fatalError("unwound full chain")
			}
		}
	}

	var difficulty: Int {
		return self.genesis.signature!.difficulty
	}

	var debugDescription: String {
		return "Blockchain [height=\(self.highest.index) highest=\(self.highest.signature!.stringValue)]";
	}
}

class Ledger<BlockType: Block>: CustomDebugStringConvertible {
	let longest: Blockchain<BlockType>
	let mutex = Mutex()
	var orphansByHash: [Hash: BlockType] = [:]
	var orphansByPreviousHash: [Hash: BlockType] = [:]

	init(genesis: BlockType) {
		self.longest = Blockchain(genesis: genesis)
	}

	let spliceLimit: UInt = 1

	func receive(block: BlockType, depth: UInt = 0) -> Bool {
		return self.mutex.locked { () -> Bool in
			// TODO: if block.index >> longest.highest.index, we should investigate switching to a different chain

			if block.isSignatureValid {
				if self.longest.append(block: block) {
					// This block fits right at the top of our chain
					self.didAppend(block: block)

					// Can we adopt an orphan?
					if let adoptable = self.orphansByPreviousHash[block.signature!] {
						self.orphansByPreviousHash[block.signature!] = nil
						self.orphansByHash[adoptable.signature!] = nil
						_ = self.receive(block: adoptable, depth: 0)
					}

					return true
				}
				else if depth > spliceLimit, (block.index + depth) > (self.longest.highest.index + spliceLimit), let existingPrevious = self.longest.blocks[block.previous] {
					// We have `depth` blocks following and we can fast-forward to this block by splicing here!
					Swift.print("We have \(depth) blocks waiting, can splice at \(existingPrevious.index) to get at \(block.index+depth) (we are at \(self.longest.highest.index))")
					self.longest.unwind(to: existingPrevious)
					return self.receive(block: block, depth: 0)
				}
				else {
					if (block.index + depth) > (self.longest.highest.index + spliceLimit), let orphan = self.orphansByHash[block.previous] {
						if self.receive(block: orphan, depth: depth + 1) {
							return true
						}
						else {
							self.orphansByHash[block.signature!] = block
							self.orphansByPreviousHash[block.previous] = block
							return false
						}
					}
					else {
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

	func didAppend(block: BlockType) {
		// For overriding
	}

	var debugDescription: String {
		return "Client [longest height=\(self.longest.highest.index) \(self.longest.highest.signature!.stringValue)]"
	}
}
