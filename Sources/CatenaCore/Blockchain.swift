import Foundation
import Cryptor
import LoggerAPI

/** Whether a transaction is eligible for being processed. */
public enum Eligibility {
	/** The transaction will never be accepted **/
	case never

	/** The transaction can be accepted right now */
	case now

	/** The transaction may be accepted in the future, but not right now. */
	case future
}

/** Represents a ledger built on top of a blockchain. The ledger decides which blockchain is considered the 'truth' (it
executes the 'longest chain rule'). */
public protocol Ledger {
	associatedtype BlockchainType: Blockchain
	associatedtype ParametersType: Parameters

	/** The blockchain currently considered to be the 'longest' and therefore the (current) truth. Implementations will
	typically apply a 'longest chain rule' where the chain that has required the most work to be constructed (and
	therefore is the least likely to be replaced by another chain with even more work) is selected. */
	var longest: BlockchainType { get }

	/** Blocks that the ledger knows about, are not currently on the longest chain, but may be needed in the future to
	make the chain longer, are stored here. */
	var orphans: Orphans<BlockchainType.BlockType> { get }
	var mutex: Mutex { get }

	/** Returns whether a transaction is valid for inclusion in the transaction memory pool (to be mined). The optional
	`pool` argument may refer to a block that contains transactions currently in the memory pool (for mining). */
	func canAccept(transaction: BlockchainType.BlockType.TransactionType, pool: BlockchainType.BlockType?) throws -> Eligibility
}

/** Represents a blockchain containing blocks. */
public protocol Blockchain {
	/** The type of the blocks that will be in this blockchain. */
	associatedtype BlockType: Block

	/** The block currently at the head of this blockchain. */
	var highest: BlockType { get }

	/** The genesis block (is required to have index=0). */
	var genesis: BlockType { get }

	/** The number of zero bits a signature is required to have at the beginning to be considered a valid proof of work. */
	func difficulty(forBlockFollowing: BlockType) throws -> BlockType.WorkType

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

/** Represents a transaction in a block. */
public protocol Transaction: Hashable {
	/** Initialize a Transaction from the serialized representation. */
	init(json: [String: Any]) throws

	/** True if the signature of the transaction is valid, false otherwise. */
	var isSignatureValid: Bool { get }

	/** The serialized representation for this transaction. Must be readable by `init(json:)`. */
	var json: [String: Any] { get }
}

/** Represents a block in a blockchain. */
public protocol Block: CustomDebugStringConvertible, Equatable {
	typealias NonceType = UInt64
	typealias IndexType = UInt64
	typealias VersionType = UInt64
	typealias TimestampType = UInt64
	typealias WorkType = UInt64
	typealias IdentityType = SHA256Hash // Hash used for identities (hashes of public keys)

	/** The type of the transactions that will be contained by this block. */
	associatedtype TransactionType: Transaction

	/** The type of hash that will be used to sign and identify blocks. Signing a block also doubles as proof-of-work. */
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

	/** The block's timestamp as encoded in the block. */
	var timestamp: TimestampType { get set }

	/** The signature hash for this block, or nil if the block has not been signed yet. */
	var signature: HashType? { get set }

	/** The payload of this block. */
	var payloadData: Data { get }

	/** The payload data of this block actually used for signing. */
	var payloadDataForSigning: Data { get }

	/** The amount of work performed for this block. */
	var work: WorkType { get }

	/** Create a block with the given index, previous hash and payload data. */
	init(version: VersionType, index: IndexType, nonce: NonceType, previous: HashType, miner: IdentityType, timestamp: TimestampType, payload: Data) throws

	/** Append a transaction to the payload data of this block. Returns true if the transaction was appended, and false
	when it wasn't (e.g. when the block already contains the transaction). */
	mutating func append(transaction: TransactionType) throws -> Bool

	/** Whether the block can accomodate the `transaction`, disregarding any validation of the transaction itself. */
	func hasRoomFor(transaction: TransactionType) -> Bool

	/** Perform validation on the payload itself (e.g. signatures on contained transactions) and returns whether the
	payload is valid. */
	func isPayloadValid() -> Bool
}

public enum HashError: LocalizedError {
	case invalidHashLength
	case invalidEncoding

	public var errorDescription: String? {
		switch self {
		case .invalidHashLength: return "invalid hash length"
		case .invalidEncoding: return "invalid hash encoding"
		}
	}
}

/** Represents a cryptographically secure hash that can be applied to any data (usually block data). */
public protocol Hash: Hashable, CustomDebugStringConvertible {
	var hash: Data { get }
	var difficulty: Int { get }

	/** Hash that is all zeroes. */
	static var zeroHash: Self { get }

	init(hash: Data) throws
	init(of: Data)
	init(hash: String) throws
	var stringValue: String { get }
}

/** Contains parameters for a particular type of Ledger. */
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

	/** The number of blocks a longer chain needs to be longer than the current one in order for the Ledger to switch to
	the longer chain. */
	static var spliceLimit: UInt { get }

	/** The time after which a failed peer is tried again (reset to state new) */
	static var peerRetryAfterFailureInterval: TimeInterval { get }
	
	/** The minimum time between the start of the processing of an incoming peer request observed. */
	static var maximumPeerRequestRate: TimeInterval { get }
	
	/** The maximum number of queued requests per peer (additional requests will be silently dropped). */
	static var maximumPeerRequestQueueSize: Int { get }

	/** The maximum number of extra blocks returned in response to a fetch request. */
	static var maximumExtraBlocks: Int { get }

	/** The maximum number of transactions that are kept aside by a peer (when they cannot be directly
	accepted into a block, but perhaps in the future). */
	static var maximumAsideTransactions: Int { get }
}

/** Default values for ledger parameters. Override as you see fit. */
public extension Parameters {
	/** The default action key is 't' */
	public static var actionKey: String { return "t" }

	/** The default uuid request key is 'uuid' */
	public static var uuidRequestKey: String { return "uuid" }

	/** The default port request key is 'port' */
	public static var portRequestKey: String { return "port" }

	/** The default service domain is 'local.' */
	public static var serviceDomain: String { return "local." }

	/** The default peer replace interval is 1 minute. */
	public static var peerReplaceInterval: TimeInterval { return 60.0 }

	/** The default maximum peer age for advertisement is one hour. */
	public static var peerMaximumAgeForAdvertisement: TimeInterval { return 3600.0 }

	/** The default future block threshold is two hours */
	public static var futureBlockThreshold: TimeInterval { return 2 * 3600.0 }

	/** The default service type is _`protocolVersion`._tcp. */
	public static var serviceType: String { return "_\(self.protocolVersion)._tcp." }

	/** The default splice limit is 1. */
	public static var spliceLimit: UInt { return 1 }

	/** By default, peers are retried after one hour. */
	public static var peerRetryAfterFailureInterval: TimeInterval { return 3600.0 }

	/** By default, peers may perform four requests per second. */
	public static var maximumPeerRequestRate: TimeInterval { return 0.25 }

	/** The maximum number of outstanding requests per peer at any time is 25, by default. */
	public static var maximumPeerRequestQueueSize: Int { return 25 }

	/** A peer may send up to 10 extra blocks in reply to a fetch block if requested, by default. */
	public static var maximumExtraBlocks: Int { return 10 }

	/** A maximum of 1024 transactions may be kept in the aside buffer before the oldest will be pruned. */
	public static var maximumAsideTransactions: Int { return 1024 }
}

extension Blockchain {
	/** The default implementation checks whether hashes and indexes are succeeding, and whether the block signature is
	valid and conforms to the current difficulty level. */
	public func canAppend(block: BlockType, to: BlockType) throws -> Bool {
		if block.previous == to.signature! && block.index == (to.index + 1) && block.isSignatureValid {
			let diff = try self.difficulty(forBlockFollowing: to)
			if block.signature!.difficulty >= diff {
				/* Check block timestamp against median timestamp of previous blocks. The last 11 blocks are looked at by
				default. */
				/* Note: a block timestamp should also not be too far in the future, but this is checked by Node when
				receiving a block from someone else. */
				if let ts = try self.medianHeadTimestamp(startingAt: to, maximumLength: 11) {
					// Block timestamp must be above median timestamp of last x blocks
					return block.date.timeIntervalSince(ts) >= 0.0
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
		else {
			return false
		}
	}

	/** The median of the timestamps of the last `maximumLength` blocks, or nil if there are no blocks. The genesis block
	timestamp is arbitrary (not included in its signature) and therefore never included. */
	public func medianHeadTimestamp(startingAt: BlockType, maximumLength: Int) throws -> Date? {
		var times: [TimeInterval] = []
		var block: BlockType? = startingAt

		for _ in 0..<maximumLength {
			if let b = block, !b.isAGenesisBlock {
				times.append(b.date.timeIntervalSince1970)
				block = try self.get(block: b.previous)
			}
			else {
				break
			}
		}

		if times.isEmpty {
			return nil
		}

		return Date(timeIntervalSince1970: TimeInterval(times.median))
	}
}

public struct SHA256Hash: Hash {
	public let hash: Data

	public static var zeroHash: SHA256Hash {
		return try! SHA256Hash(hash: Data(bytes: [UInt8](repeating: 0,  count: Int(32))))
	}

	public init(hash string: String) throws {
		if let d = string.hexDecoded {
			guard d.count == Int(32) else { throw HashError.invalidHashLength }
			self.hash = d
			assert(self.hash.count == Int(32))
		}
		else {
			throw HashError.invalidEncoding
		}
	}

	public init(hash: Data) throws {
		guard hash.count == Int(32) else { throw HashError.invalidHashLength }
		self.hash = hash
	}

	public init(of: Data) {
		self.hash = of.sha256
	}

	public var debugDescription: String {
		return self.stringValue
	}

	public var stringValue: String {
		assert(hash.count == Int(32))
		return self.hash.map { String(format: "%02hhx", $0) }.joined()
	}

	public var difficulty: Int {
		var n = 0
		for byte in self.hash {
			n += byte.leadingZeroBitCount
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
	/** Returns a new, empty 'template' block that can be used for mining. */
	public static func template(for miner: IdentityType) throws -> Self {
		/* Note that the previous hash here does not refer to any hash (but is just the hash of "fake"). This is done
		so that the block is not seen as a genesis block, which would trigger assertions on attempting to append
		transactions to it (that is not allowed for a genesis block) */
		let data = Data()
		let fakeHash = HashType(of: data)
		let ts = TimestampType(Date().timeIntervalSince1970)
		return try Self(version: VersionType(1), index: 1, nonce: 0, previous: fakeHash, miner: miner, timestamp: ts, payload: Data())
	}

	/** Returns an (unsigned) genesis block for the given seed and version. */
	public static func genesis(seed: String, version: VersionType) throws -> Self {
		let ts = TimestampType(Date().timeIntervalSince1970)
		return try Self(version: version, index: 0, nonce: 0, previous: HashType.zeroHash, miner: IdentityType.zeroHash, timestamp: ts, payload: seed.data(using: .utf8)!)
	}

	/** Returns true when the block's signature is valid, false when the block is unsigned or when the signature is invalid. */
	public var isSignatureValid: Bool {
		if let s = self.signature {
			return HashType(of: self.dataForSigning) == s
		}
		return false
	}

	public var work: WorkType {
		return WorkType(self.signature?.difficulty ?? 0)
	}

	/** Returns true if this block is a genesis (i.e. can only be the first block in a chain), false otherwise. The block
	signature is not required to be valid. */
	public var isAGenesisBlock: Bool {
		return self.previous == HashType.zeroHash && self.index == 0
	}

	/** Hash of the payload data for signing. This hash is included in the block data to be signed. Implementations could
	also for instance return a Merkle tree root for the payload here. */
	public var payloadRoot: HashType {
		return HashType(of: self.payloadDataForSigning)
	}

	/** The block data that is to be signed. */
	public var dataForSigning: Data {
		let pd = self.payloadRoot

		// Calculate the size of the data object to prevent multiple copies
		let fieldSizes = MemoryLayout<VersionType>.size + MemoryLayout<IndexType>.size + MemoryLayout<NonceType>.size
		let size = pd.hash.count + miner.hash.count + previous.hash.count + fieldSizes + MemoryLayout<Int64>.size
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
			data.appendRaw(self.timestamp.littleEndian)
		}

		pd.hash.withUnsafeBytes { bytes in
			data.append(bytes, count: pd.hash.count)
		}

		return data
	}

	/** Mine this block (note: use this only for the genesis block, Miner provides threaded mining) */
	public mutating func mine(difficulty: Int) {
		self.date = Date()

		/* Note: if mining takes longer than a few hours, the mined block will not be accepted. As the difficulty level
		is generally low for a genesis block and this is only used for genesis blocks anyway, this should not be an
		issue. */
		while true {
			// FIXME replace with unsafeAdding in Swift 4
			if self.nonce == NonceType.max {
				self.nonce = NonceType.min
			}
			else {
				self.nonce = self.nonce + NonceType(1)
			}

			let hash = HashType(of: self.dataForSigning)
			if hash.difficulty >= difficulty {
				self.signature = hash
				return
			}
		}
	}
}

/** Set of blocks that are not yet part of a complete chain, but are to be remembered in case intermediate blocks are
found that can complete a chain. */
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

	/** Notify the ledger of a new block. An attempt is made to append the block to the currently longest chain. If that
	fails, the block is saved for later, in case (together with other blocks) a longer chain can be formed. */
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
				if try self.longest.canAppend(block: block, to: self.longest.highest) && self.longest.append(block: block) {
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
									let p = longest.highest
									if !(try longest.canAppend(block: b, to: p) && longest.append(block: b)) {
										fatalError("block should have been appendable: \(b) to \(p)")
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
		var nonce = self.nonce.littleEndian
		let nonceData = Data(bytes: &nonce, count: MemoryLayout<NonceType>.size)

		return [
			"version": NSNumber(value: self.version),
			"hash": self.signature!.stringValue,
			"index": NSNumber(value: self.index),
			"nonce": nonceData.base64EncodedString(),
			"miner": self.miner.stringValue,
			"timestamp": NSNumber(value: self.timestamp),
			"payload": self.payloadData.base64EncodedString(),
			"previous": self.previous.stringValue
		]
	}

	/** The block's timestamp converted to a date */
	public var date: Date {
		get {
			return Date(timeIntervalSince1970: TimeInterval(self.timestamp))
		}
		set {
			self.timestamp = TimestampType(newValue.timeIntervalSince1970)
		}
	}

	public static func read(json: [String: Any]) throws -> Self {
		if let signature = json["hash"] as? String,
			let previous = json["previous"] as? String,
			let payloadBase64 = json["payload"] as? String,
			let minerSHA256 = json["miner"] as? String,
			let payload = Data(base64Encoded: payloadBase64),
			let nonceBase64 = json["nonce"] as? String,
			let nonceData = Data(base64Encoded: nonceBase64) {

			let minerHash = try IdentityType(hash: minerSHA256)
			let previousHash = try HashType(hash: previous)
			let signatureHash = try HashType(hash: signature)

			// Decode nonce from base64
			var nonceValue: NonceType = 0
			let buffer = UnsafeMutableBufferPointer(start: &nonceValue, count: 1)
			guard nonceData.copyBytes(to: buffer) == MemoryLayout<NonceType>.size else {
				throw BlockError.formatError
			}
			if nonceValue.littleEndian != nonceValue {
				nonceValue = nonceValue.byteSwapped
			}

			// Read numeric stuff (this apparently is inconsistent between Darwin/Linux)
			if let height = json["index"] as? NSNumber,
				let version = json["version"] as? NSNumber,
				let timestamp = json["timestamp"] as? NSNumber {
				var b = try Self.init(
					version: VersionType(version.uint64Value),
					index: IndexType(height.uint64Value),
					nonce: nonceValue,
					previous: previousHash,
					miner: minerHash,
					timestamp: TimestampType(timestamp.uint64Value),
					payload: payload
				)

				b.signature = signatureHash
				return b
			}
			else if let height = json["index"] as? Int,
				let version = json["version"] as? Int,
				let timestamp = json["timestamp"] as? Int {
				var b = try Self.init(
					version: VersionType(version),
					index: IndexType(height),
					nonce: nonceValue,
					previous: previousHash,
					miner: minerHash,
					timestamp: TimestampType(timestamp),
					payload: payload
				)

				b.signature = signatureHash
				return b
			}
			else {
				throw BlockError.formatError
			}
		}
		else {
			throw BlockError.formatError
		}
	}
}
