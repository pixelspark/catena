import XCTest
@testable import CatenaCore

private struct TestTransaction: Transaction {
	var data: [String: Any]

	init(json: [String: Any]) throws {
		self.data = json
	}

	var isSignatureValid: Bool {
		return true
	}

	var json: [String: Any] {
		return self.data
	}

	private var dataForSigning: Data {
		var d = Data()
		for (k, v) in self.data {
			d.appendRaw(k)
			d.appendRaw(v)
		}
		return d
	}

	public var hashValue: Int {
		return self.dataForSigning.hashValue
	}

	public static func ==(lhs: TestTransaction, rhs: TestTransaction) -> Bool {
		return lhs.dataForSigning == rhs.dataForSigning
	}
}

private struct TestBlock: Block {
	typealias HashType = SHA256Hash
	typealias TransactionType = TestTransaction

	var index: Block.IndexType
	var previous: SHA256Hash
	var nonce: Block.NonceType
	var signature: SHA256Hash? = nil

	var payloadData: Data
	var payloadDataForSigning: Data { return payloadData }
	var version: Block.VersionType
	var miner: Block.IdentityType
	var timestamp: Date = Date()

	init(version: Block.VersionType, index: Block.IndexType, nonce: Block.NonceType, previous: SHA256Hash, miner: Block.IdentityType, timestamp: Date, payload: Data) throws {
		self.version = version
		self.index = index
		self.nonce = nonce
		self.previous = previous
		self.miner = miner
		self.timestamp = timestamp
		self.payloadData = payload
	}

	mutating func append(transaction: TestTransaction) throws -> Bool {
		return true
	}

	func hasRoomFor(transaction: TestTransaction) -> Bool {
		return true
	}

	func isPayloadValid() -> Bool {
		return true
	}

	var debugDescription: String {
		return "TestBlock \(self.index)"
	}

	static func ==(lhs: TestBlock, rhs: TestBlock) -> Bool {
		return lhs.index == rhs.index && (lhs.signature == rhs.signature || lhs.payloadData == rhs.payloadData)
	}
}

private class TestChain: Blockchain {
	var highest: TestBlock {
		return self.chain.last!
	}

	var genesis: TestBlock {
		return self.chain.first!
	}

	var difficulty: Int
	var chain: [TestBlock] = []

	init(genesis: TestBlock) {
		self.chain = [genesis]
		self.difficulty = 2
	}

	func get(block: SHA256Hash) throws -> TestBlock? {
		for b in chain {
			if b.signature! == block {
				return b
			}
		}
		return nil
	}

	func append(block: TestBlock) throws -> Bool {
		if try self.canAppend(block: block, to: self.highest) {
			self.chain.append(block)
			return true
		}
		return false
	}

	func unwind(to: TestBlock) throws {
		if let idx = self.chain.index(of: to) {
			self.chain = Array(self.chain[0...idx])
		}
		else {
			fatalError("invalid unwind")
		}
	}

	typealias BlockType = TestBlock

}

private struct TestParameters: Parameters {
	public static let actionKey: String = "t"
	public static let protocolVersion = "test-v1"
	public static let uuidRequestKey = "uuid"
	public static let portRequestKey = "port"
	public static let peerReplaceInterval: TimeInterval = 60.0
	public static let peerMaximumAgeForAdvertisement: TimeInterval = 3600.0
	public static let serviceType = "_test._tcp."
	public static let serviceDomain = "local."
	public static let futureBlockThreshold = 2 * 3600.0
}
private class TestLedger: Ledger {
	typealias ParametersType = TestParameters

	typealias BlockchainType = TestChain

	var longest: TestChain
	var orphans = Orphans<TestBlock>()
	var mutex: Mutex = Mutex()
	var spliceLimit: UInt = 1

	init(genesis: TestBlock) {
		longest = TestChain(genesis: genesis)
	}

	func canAccept(transaction: TestTransaction, pool: TestBlock?) throws -> Bool {
		return true
	}
}

class CatenaTests: XCTestCase {
	func testFundamentals() throws {
		XCTAssertEqual(SHA256Hash(of: "Catena".data(using: .utf8)!).stringValue.lowercased(), "13ab80a5ba95216129ea9d996937b4ed57faf7473e81288d99689da4d5f1d483")
		XCTAssertEqual(SHA256Hash.zeroHash, SHA256Hash.zeroHash)
		XCTAssert(SHA256Hash(of: "Catena".data(using: .utf8)!).hash.base58checkEncoded(version: 3) == "6ytyLFvgmcKJPr9YMgnto3hnt5g7vkPFrppNNvN96pNLwbRxAe")
		XCTAssert(SHA256Hash(of: "Catena".data(using: .utf8)!).hash.base58checkEncoded(version: 1) == "36QcsMryvFs2kgjjCSnz9r9xerEpX4dgagbt5Ya9nobMdaUVVK")
		XCTAssert(SHA256Hash(of: "Catena".data(using: .utf8)!).hash.base64EncodedString() == "E6uApbqVIWEp6p2ZaTe07Vf690c+gSiNmWidpNXx1IM=")
		XCTAssert(String(data: SHA256Hash(of: "Catena".data(using: .utf8)!).hash.base64EncodedData(), encoding: .utf8)! == "E6uApbqVIWEp6p2ZaTe07Vf690c+gSiNmWidpNXx1IM=")

		let ident = try Identity()
		let pk = ident.publicKey.stringValue
		let rpk = PublicKey(string: pk)
		XCTAssert(ident.publicKey == rpk)

		let d = "Hello".data(using: .utf8)!
		let signed = try ident.publicKey.sign(data: d, with: ident.privateKey)
		XCTAssert(try ident.publicKey.verify(message: d, signature: signed))
	}

	func testLedger() throws {
		var genesis = try TestBlock(version: 1, index: 0, nonce: 0, previous: SHA256Hash.zeroHash, miner: SHA256Hash.zeroHash, timestamp: Date(), payload: Data())
		genesis.mine(difficulty: 2)
		XCTAssert(genesis.isSignatureValid && genesis.isAGenesisBlock)
		let ledger = TestLedger(genesis: genesis)

		XCTAssert(ledger.longest.genesis.isAGenesisBlock)
		XCTAssert(ledger.longest.genesis.isSignatureValid)

		// Attempt to append a valid block
		let miner = try Identity()
		let minerID = SHA256Hash(of: miner.publicKey.data)

		var b = try TestBlock(version: 1, index: 1, nonce: 0, previous: ledger.longest.genesis.signature!, miner: minerID, timestamp: Date(), payload: Data())
		b.mine(difficulty: ledger.longest.difficulty)
		XCTAssert(try ledger.receive(block: b))
		XCTAssert(ledger.longest.highest == b)

		// Attempt to append an invalid block
		var c = try TestBlock(version: 1, index: 1, nonce: 0, previous: ledger.longest.genesis.signature!, miner: minerID, timestamp: Date(), payload: Data())
		c.mine(difficulty: ledger.longest.difficulty)
		c.nonce = 0
		XCTAssert(!(try ledger.receive(block: c)))

		// Attempt to add an outdated block should fail
		var d = try TestBlock(version: 1, index: 1, nonce: 0, previous: ledger.longest.genesis.signature!, miner: minerID, timestamp: Date(), payload: Data())
		d.mine(difficulty: ledger.longest.difficulty)
		XCTAssert(!(try ledger.receive(block: d)))

		// Attempt to add an easier block should fail
		var e = try TestBlock(version: 1, index: 2, nonce: 0, previous: b.signature!, miner: minerID, timestamp: Date(), payload: Data())
		e.mine(difficulty: 1)
		XCTAssert(!(try ledger.receive(block: e)))
		e.mine(difficulty: ledger.longest.difficulty)
		XCTAssert(try ledger.receive(block: e))


		XCTAssert(ledger.longest.highest == e)
	}

    static var allTests = [
        ("testLedger", testLedger),
        ("testFundamentals", testFundamentals)
    ]
}
