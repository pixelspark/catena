import Foundation
import Cryptor
import LoggerAPI

public protocol Ledger {
	associatedtype BlockchainType: Blockchain
	associatedtype ParametersType: Parameters

	var longest: BlockchainType { get }
	var orphans: Orphans<BlockchainType.BlockType> { get }
	var mutex: Mutex { get }
	var spliceLimit: UInt { get }

	/** Returns whether a transaction is valid for inclusion in the transaction memory pool (to be mined). The optional
	`pool` argument may refer to a block that contains transactions currently in the memory pool (for mining). */
	func canAccept(transaction: BlockchainType.BlockType.TransactionType, pool: BlockchainType.BlockType?) throws -> Bool
}

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

	/** Whether calling append(block) would work if `to` were the head of the chain. */
	func canAppend(block: BlockType, to: BlockType) throws -> Bool

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
	typealias VersionType = UInt64
	typealias IdentityType = SHA256Hash // Hash used for identities (hashes of public keys)

	associatedtype TransactionType: Transaction
	associatedtype HashType: Hash

	/** The block version. This can be used to switch signature algorithms or signature payload definitions within the
	same blockchain. The field may also contain flags in the future. */
	var version: VersionType { get set }

	/** The position of this block in the blockchain. The block with index 0 is considered the genesis block, and has a
	zero previous hash. For all other blocks, the index of the block with the previous hash should have an index that is
	exactly one lower than the index of this block. */
	var index: IndexType { get set }

	/** The hash of the previous block in this chain, or a zero hash in case this block is the genesis block (index=0). */
	var previous: HashType { get set }

	/** The nonce used to generate a valid proof-of-work signature for this block. */
	var nonce: NonceType { get set }

	/** The identity of the miner of this block (hash of PublicKey). */
	var miner: IdentityType { get set }

	/** The block's timestamp */
	var timestamp: Date { get set }

	/** The signature hash for this block, or nil if the block has not been signed yet. */
	var signature: HashType? { get set }

	/** The payload of this block. */
	var payloadData: Data { get }

	/** The payload data of this block actually used for signing. */
	var payloadDataForSigning: Data { get }

	/** Create a block with the given index, previous hash and payload data. */
	init(version: VersionType, index: IndexType, nonce: NonceType, previous: HashType, miner: IdentityType, timestamp: Date, payload: Data) throws

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

public protocol Parameters {
	/** Key that is used in gossip messages to indicate the mssage type */
	static var actionKey: String { get }

	/** WebSocket protocol version string */
	static var protocolVersion: String { get }

	/** Query string key name used to send own UUID */
	static var uuidRequestKey: String { get }

	/** Query string key name used to send own port */
	static var portRequestKey: String { get }

	/** Time a node with wait before replacing an inactive connection to a peer with a newly proposed one for the same
	UUID, but with a different address/port. */
	static var peerReplaceInterval: TimeInterval { get }

	/** Maximum number of seconds that have passed since a node was last seen for the node to be included in the set of
	advertised nodes. */
	static var peerMaximumAgeForAdvertisement: TimeInterval { get }

	/** DNS-SD service type used to advertise Gossip service. */
	static var serviceType: String { get }

	/** mDNS domain in which the Gossip service is advertised to other peers in the same LAN. */
	static var serviceDomain: String { get }

	/** The amount of time a block's timestamp may be in the future (compared to 'network time'). */
	static var futureBlockThreshold: TimeInterval { get }
}

extension Blockchain {
	/** The default implementation checks whether hashes and indexes are succeeding, and whether the block signature is
	valid and conforms to the current difficulty level.
	FIXME: check what happens when difficulty level changes! */
	public func canAppend(block: BlockType, to: BlockType) throws -> Bool {
		if block.previous == to.signature!
			&& block.index == (to.index + 1)
			&& block.isSignatureValid
			&& block.signature!.difficulty >= self.difficulty {

			// Check block timestamp against median timestamp of previous blocks
			/* Note: a block timestamp should also not be too far in the future, but this is checked by Node when
			receiving a block from someone else. */
			if let ts = try self.medianHeadTimestamp(startingAt: to) {
				// Block timestamp must be above median timestamp of last x blocks
				return block.timestamp.timeIntervalSince(ts) >= 0.0
			}
			else {
				// No timestamps to compare to
				return true
			}
		}
		else {
			return false
		}
	}

	func medianHeadTimestamp(startingAt: BlockType, maximumLength: Int = 10) throws -> Date? {
		var times: [TimeInterval] = []
		var block: BlockType? = startingAt

		for _ in 0..<maximumLength {
			if let b = block, !b.isAGenesisBlock {
				times.append(b.timestamp.timeIntervalSince1970)
				block = try self.get(block: b.previous)
			}
			else {
				break
			}
		}

		if times.isEmpty {
			return nil
		}

		return Date(timeIntervalSince1970: times.median)
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

extension Block {
	static func template(for miner: IdentityType) throws -> Self {
		/* Note that the previous hash here does not refer to any hash (but is just the hash of "fake"). This is done
		so that the block is not seen as a genesis block, which would trigger assertions on attempting to append
		transactions to it (that is not allowed for a genesis block) */
		let data = Data()
		let fakeHash = HashType(of: data)
		return try Self(version: VersionType(1), index: 1, nonce: 0, previous: fakeHash, miner: miner, timestamp: Date(), payload: Data())
	}

	public static func genesis(seed: String, version: VersionType) throws -> Self {
		return try Self(version: version, index: 0, nonce: 0, previous: HashType.zeroHash, miner: IdentityType.zeroHash, timestamp: Date(), payload: Data())
	}

	public var isSignatureValid: Bool {
		if let s = self.signature {
			return HashType(of: self.dataForSigning) == s
		}
		return false
	}

	public var isAGenesisBlock: Bool {
		return self.previous == HashType.zeroHash && self.index == 0
	}

	public var dataForSigning: Data {
		let pd = self.payloadDataForSigning

		// Calculate the size of the data object to prevent multiple copies
		let fieldSizes = MemoryLayout<VersionType>.size + MemoryLayout<IndexType>.size + MemoryLayout<NonceType>.size
		let size = pd.count + miner.hash.count + previous.hash.count + fieldSizes + MemoryLayout<Int64>.size
		var data = Data(capacity: size)

		data.appendRaw(self.version.littleEndian)
		data.appendRaw(self.index.littleEndian)
		data.appendRaw(self.nonce.littleEndian)

		previous.hash.withUnsafeBytes { bytes in
			data.append(bytes, count: previous.hash.count)
		}

		self.miner.hash.withUnsafeBytes { bytes in
			data.append(bytes, count: self.miner.hash.count)
		}

		if !self.isAGenesisBlock {
			data.appendRaw(UInt64(self.timestamp.timeIntervalSince1970).littleEndian)
		}

		pd.withUnsafeBytes { bytes in
			data.append(bytes, count: pd.count)
		}

		return data
	}

	/** Mine this block (note: use this only for the genesis block, Miner provides threaded mining) */
	public mutating func mine(difficulty: Int) {
		self.timestamp = Date()

		/* Note: if mining takes longer than a few hours, the mined block will not be accepted. As the difficulty level
		is generally low for a genesis block and this is only used for genesis blocks anyway, this should not be an
		issue. */
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

public class Orphans<BlockType: Block> {
	private let mutex = Mutex()
	private var orphansByHash: [BlockType.HashType: BlockType] = [:]
	private var orphansByPreviousHash: [BlockType.HashType: BlockType] = [:]

	public init() {
	}

	func remove(orphan block: BlockType) {
		self.mutex.locked {
			self.orphansByPreviousHash[block.previous] = nil
			self.orphansByHash[block.signature!] = nil
		}
	}

	func add(orphan block: BlockType) {
		self.mutex.locked {
			self.orphansByHash[block.signature!] = block
			self.orphansByPreviousHash[block.previous] = block
		}
	}

	func get(orphan hash: BlockType.HashType) -> BlockType? {
		return self.mutex.locked {
			return self.orphansByHash[hash]
		}
	}

	func get(successorOf hash: BlockType.HashType) -> BlockType? {
		return self.mutex.locked {
			return self.orphansByPreviousHash[hash]
		}
	}

	/** Find the earliest orphan block preceding the given orphan block. */
	func earliestRootFor(orphan block: BlockType) -> (index: BlockType.IndexType, signature: BlockType.HashType) {
		var fetchHash = block.previous
		var fetchIndex = block.index - 1
		while let orphan = self.orphansByHash[fetchHash], orphan.index > 0 {
			fetchHash = orphan.previous
			fetchIndex = orphan.index - 1
		}

		return (fetchIndex, fetchHash)
	}
}

extension Ledger {
	func isNew(block: BlockchainType.BlockType) throws -> Bool {
		return try self.mutex.locked {
			// We already know this block and it is currently an orphan
			if orphans.get(orphan: block.signature!) != nil {
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

	func receive(block: BlockchainType.BlockType) throws -> Bool {
		Log.debug("[Ledger] receive block #\(block.index) \(block.signature!.stringValue)")
		return try self.mutex.locked { () -> Bool in
			if block.isSignatureValid {
				// Were we waiting for this block?
				var block = block
				while let next = self.orphans.get(successorOf: block.signature!) {
					self.orphans.add(orphan: block)
					block = next
				}

				// This block can simply be appended to the chain
				if try self.longest.append(block: block) {
					self.orphans.remove(orphan: block)
					Log.info("[Ledger] can append directly")
					return true
				}
				else {
					// Block cannot be directly appended.
					if block.index > self.longest.highest.index {
						// The block is newer
						var root: BlockchainType.BlockType? = block
						var stack: [BlockchainType.BlockType] = []
						while true {
							if let r = root {
								if let prev = self.orphans.get(orphan: r.previous) {
									// Previous block is an orphan as well, so not on-chain. Find the next one
									root = prev
									stack.append(r)
								}
								else if try self.longest.get(block: r.previous) == nil {
									// The previous block is not an orphan but not on-chain either. This means we are missing an orphan
									self.orphans.add(orphan: block)
									Log.debug("[Ledger] missing intermediate block: \(r.index-1) with hash \(r.previous.stringValue)")
									return false
								}
								else {
									// Sidechain is rooted in chain, stop here
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

								// Check whether this sidechain can be appended
								var prev = splice
								for b in stack.reversed() {
									if try !longest.canAppend(block: b, to: prev) {
										Log.info("[Ledger] cannot append sidechain: block \(b.index) won't append to \(prev.index)")
										return false
									}
									prev = b
								}

								// First cut the tree up to the splice point for the sidechain
								if splice.signature! != self.longest.highest.signature! {
									Log.debug("[Ledger] splicing to \(splice.index), then fast-forwarding to \(block.index)")
									try self.longest.unwind(to: splice)
								}
								Log.info("[Ledger] head is now at \(self.longest.highest.index) \(self.longest.highest.signature!.stringValue)")

								// Append the full sidechain
								for b in stack.reversed() {
									if !(try longest.append(block: b)) {
										fatalError("block should have been appendable: \(b)")
									}
									self.orphans.remove(orphan: block)
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
						self.orphans.add(orphan: block)
						return false
					}
				}
			}
			else {
				return false
			}
		}
	}
}

extension Block {
	public var json: [String: Any] {
		return [
			"version": self.version,
			"hash": self.signature!.stringValue,
			"index": self.index,
			"nonce": self.nonce,
			"miner": self.miner.stringValue,
			"timestamp": Int(self.timestamp.timeIntervalSince1970),
			"payload": self.payloadData.base64EncodedString(),
			"previous": self.previous.stringValue
		]
	}

	public static func read(json: [String: Any]) throws -> Self {
		if let nonce = json["nonce"] as? NSNumber,
			let signature = json["hash"] as? String,
			let height = json["index"] as? NSNumber,
			let version = json["version"] as? NSNumber,
			let timestamp = json["timestamp"] as? NSNumber,
			let previous = json["previous"] as? String,
			let payloadBase64 = json["payload"] as? String,
			let minerSHA256 = json["miner"] as? String,
			let minerHash = IdentityType(hash: minerSHA256),
			let payload = Data(base64Encoded: payloadBase64),
			let previousHash = HashType(hash: previous),
			let signatureHash = HashType(hash: signature) {
			// FIXME: .uint64 is not generic (NonceType/IndexType may change to something else
			var b = try Self.init(
				version: VersionType(version.uint64Value),
				index: IndexType(height.uint64Value),
				nonce: NonceType(nonce.uint64Value),
				previous: previousHash,
				miner: minerHash,
				timestamp: Date(timeIntervalSince1970: TimeInterval(timestamp.doubleValue)),
				payload: payload
			)
			b.signature = signatureHash
			return b
		}
		else {
			throw BlockError.formatError
		}
	}
}
