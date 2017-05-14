import Foundation
import CommonCrypto

extension Data {
	var hash: Hash {
		return Hash(self.sha256)
	}
}

struct Hash: Equatable, Hashable {
	let hash: Data

	static var zeroHash: Hash {
		return Hash(Data(bytes: [UInt8](repeating: 0,  count: Int(CC_SHA256_DIGEST_LENGTH))))
	}

	init(_ hash: Data) {
		assert(hash.count == Int(CC_SHA256_DIGEST_LENGTH))
		self.hash = hash
	}

	init(of: Data) {
		self.hash = of.sha256
	}

	var stringValue: String {
		return self.hash.map { String(format: "%02hhx", $0) }.joined()
	}

	var difficulty: Int {
		// Number of leading zeroes
		var n = 0
		for byte in self.hash {
			if byte != 0 {
				break
			}
			n += 1
		}
		return n
	}

	var hashValue: Int {
		return self.hash.hashValue
	}
}

func == (lhs: Hash, rhs: Hash) -> Bool {
	return lhs.hash == rhs.hash
}

protocol Block: CustomDebugStringConvertible, Equatable {
	var index: UInt { get set }
	var previous: Hash { get set }
	var nonce: UInt { get set }
	var signature: Hash? { get set }
	var payloadData: Data { get }
}

func ==<T: Block>(lhs: T, rhs: T) -> Bool {
	return lhs.signedData == rhs.signedData
}

extension Block {
	var isSignatureValid: Bool {
		if let s = self.signature {
			return self.signedData.hash == s
		}
		return false
	}

	var signedData: Data {
		let md = NSMutableData()
		var index = self.index
		var nonce = self.nonce
		md.append(&index, length: MemoryLayout<UInt>.size)
		md.append(&nonce, length: MemoryLayout<UInt>.size)
		md.append(previous.hash)
		md.append(self.payloadData)
		return md as Data
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

	required init(genesis: BlockType) {
		assert(genesis.index == 0)
		self.genesis = genesis
		self.highest = genesis
	}

	func append(block: BlockType) -> Bool {
		if block.previous == self.highest.signature! && block.index == (self.highest.index + 1) && block.isSignatureValid && block.signature!.difficulty >= self.difficulty {
			self.highest = block
			return true
		}
		return false
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
	////var pool: [Hash: BlockType] = [:]

	init(genesis: BlockType) {
		self.longest = Blockchain(genesis: genesis)
	}

	func receive(block: BlockType) {
		if block.isSignatureValid && block.index > self.longest.highest.index {
			self.mutex.locked {
				if !self.longest.append(block: block) {
					// TODO put limit on the number of blocks cached (not too old and not too new)
					// TODO when block.index >> self.longest.highest.index, start thinking about an alternative chain
					/////self.pool[block.signature!] = block
				}
				else {
					self.didAppend(block: block)
					// Remove all blocks with a lower index from the cache
				}
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
