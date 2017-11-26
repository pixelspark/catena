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
	typealias TimestampType = UInt64

	var index: Block.IndexType
	var previous: SHA256Hash
	var nonce: Block.NonceType
	var signature: SHA256Hash? = nil

	var payloadData: Data
	var payloadDataForSigning: Data { return payloadData }
	var version: Block.VersionType
	var miner: Block.IdentityType
	var timestamp: TimestampType = 0

	init(version: Block.VersionType, index: Block.IndexType, nonce: Block.NonceType, previous: SHA256Hash, miner: Block.IdentityType, timestamp: TimestampType, payload: Data) throws {
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

	var transactions: [TestTransaction] {
		return []
	}
}

private class TestChain: Blockchain {
	func difficulty(forBlockFollowing: TestBlock) throws -> Block.WorkType {
		return 2
	}

	var highest: TestBlock {
		return self.chain.last!
	}

	var genesis: TestBlock {
		return self.chain.first!
	}

	var chain: [TestBlock] = []

	init(genesis: TestBlock) {
		self.chain = [genesis]
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

private class TestLedger: Ledger {
	typealias BlockchainType = TestChain

	struct ParametersType: Parameters {
		public static let protocolVersion = "test-v1"
	}

	var longest: TestChain
	var orphans = Orphans<TestBlock>()
	var mutex: Mutex = Mutex()

	init(genesis: TestBlock) {
		longest = TestChain(genesis: genesis)
	}

	func canAccept(transaction: TestTransaction, pool: TestBlock?) throws -> Eligibility {
		return .now
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

		XCTAssert(UInt8(0).leadingZeroBitCount == 8)
		XCTAssert(UInt8(1).leadingZeroBitCount == 7)
		XCTAssert(UInt8(2).leadingZeroBitCount == 6)
		XCTAssert(UInt8(4).leadingZeroBitCount == 5)
		XCTAssert(UInt8(8).leadingZeroBitCount == 4)
		XCTAssert(UInt8(16).leadingZeroBitCount == 3)
		XCTAssert(UInt8(32).leadingZeroBitCount == 2)
		XCTAssert(UInt8(64).leadingZeroBitCount == 1)
		XCTAssert(UInt8(128).leadingZeroBitCount == 0)
		XCTAssert(UInt8(129).leadingZeroBitCount == 0)

		let identityA = try Identity()
		let pk = identityA.publicKey.stringValue
		let rpk = PublicKey(string: pk)
		XCTAssert(identityA.publicKey == rpk)

		let identityB = try Identity()

		let d = "Hello".data(using: .utf8)!
		let signedA = try identityA.publicKey.sign(data: d, with: identityA.privateKey)
		let signedB = try identityB.publicKey.sign(data: d, with: identityB.privateKey)

		let other = "hello".data(using: .utf8)!
		let otherSignedA = try identityA.publicKey.sign(data: other, with: identityA.privateKey)
		let otherSignedB = try identityB.publicKey.sign(data: other, with: identityB.privateKey)

		XCTAssert(try identityA.publicKey.verify(message: d, signature: signedA))
		XCTAssert(!(try identityA.publicKey.verify(message: d, signature: otherSignedA)))
		XCTAssert(!(try identityA.publicKey.verify(message: d, signature: signedB)))
		XCTAssert(!(try identityA.publicKey.verify(message: d, signature: otherSignedB)))

		XCTAssert(try identityA.publicKey.verify(message: other, signature: otherSignedA))
		XCTAssert(!(try identityA.publicKey.verify(message: other, signature: signedA)))
		XCTAssert(!(try identityA.publicKey.verify(message: other, signature: signedB)))
		XCTAssert(!(try identityA.publicKey.verify(message: other, signature: otherSignedB)))
	}

	func testSerialization() throws {
		let gen = SHA256Hash(of: "foo".data(using: .utf8)!)
		let peer = URL(string: "ws://BFF43B46-164D-41AC-B73E-733782E58839@z3.pixelspark.nl:8338")!
		let height = UInt64(100)
		let miner = SHA256Hash(of: try Identity().publicKey.data)
		let payload = "bar".data(using: .utf8)!
		var highest = try TestBlock(version: 1, index: height, nonce: 0, previous: gen, miner: miner, timestamp: UInt64(Date().timeIntervalSince1970), payload: payload)
		highest.mine(difficulty: 2)
		let index = Index<TestBlock>(genesis: gen, peers: [peer], highest: highest.signature!, height: height, timestamp: UInt64(Date().timeIntervalSince1970))

		let deserialized = try Index<TestBlock>(json: index.json)
		print("o=\(index.json), s=\(deserialized.json)")
		XCTAssert(index.height == deserialized.height)
		XCTAssert(index.highest == deserialized.highest)
		XCTAssert(index.genesis == deserialized.genesis)
		XCTAssert(index.timestamp == deserialized.timestamp)
		XCTAssert(index.peers == deserialized.peers)
		XCTAssert(index == deserialized, "deserialized index must match original index")

		let jsonData = try JSONSerialization.data(withJSONObject: index.json, options: [])
		let deserializedJSON = try JSONSerialization.jsonObject(with: jsonData, options: [])
		let deserializedFromJSON = try Index<TestBlock>(json: deserializedJSON as! [String: Any])
		XCTAssert(deserializedFromJSON == index, "deserialized index from JSON must match original index")
	}

	func testLedger() throws {
		let ts = UInt64(Date().timeIntervalSince1970)
		var genesis = try TestBlock(version: 1, index: 0, nonce: 0, previous: SHA256Hash.zeroHash, miner: SHA256Hash.zeroHash, timestamp: ts, payload: Data())
		genesis.mine(difficulty: 2)
		XCTAssert(genesis.isSignatureValid && genesis.isAGenesisBlock)
		let ledger = TestLedger(genesis: genesis)

		XCTAssert(ledger.longest.genesis.isAGenesisBlock)
		XCTAssert(ledger.longest.genesis.isSignatureValid)

		// Attempt to append a valid block
		let miner = try Identity()
		let minerID = SHA256Hash(of: miner.publicKey.data)

		var b = try TestBlock(version: 1, index: 1, nonce: 0, previous: ledger.longest.genesis.signature!, miner: minerID, timestamp: ts, payload: Data())
		b.mine(difficulty: try ledger.longest.difficulty(forBlockFollowing: ledger.longest.genesis))
		var r: [TestTransaction] = []
		XCTAssert(try ledger.receive(block: b, recovered: &r))
		XCTAssert(ledger.longest.highest == b)

		// Attempt to append an invalid block
		var c = try TestBlock(version: 1, index: 1, nonce: 0, previous: ledger.longest.genesis.signature!, miner: minerID, timestamp: ts, payload: Data())
		c.mine(difficulty: try ledger.longest.difficulty(forBlockFollowing: ledger.longest.genesis))
		c.nonce = 0
		XCTAssert(!(try ledger.receive(block: c, recovered: &r)))

		// Attempt to add an outdated block should fail
		var d = try TestBlock(version: 1, index: 1, nonce: 0, previous: ledger.longest.genesis.signature!, miner: minerID, timestamp: ts, payload: Data())
		d.mine(difficulty: try ledger.longest.difficulty(forBlockFollowing: ledger.longest.genesis))
		XCTAssert(!(try ledger.receive(block: d, recovered: &r)))

		// Attempt to add an easier block should fail
		var e = try TestBlock(version: 1, index: 2, nonce: 0, previous: b.signature!, miner: minerID, timestamp: ts, payload: Data())

		// Force block to have signature with difficulty=1
		while e.signature == nil || e.signature!.difficulty != 1 {
			e.signature = nil
			e.mine(difficulty: 1)
		}
		XCTAssert(!(try ledger.longest.canAppend(block: e, to: ledger.longest.highest)))
		XCTAssert(!(try ledger.receive(block: e, recovered: &r)))
		e.signature = nil
		e.mine(difficulty: try ledger.longest.difficulty(forBlockFollowing: ledger.longest.highest))
		XCTAssert(try ledger.receive(block: e, recovered: &r))


		XCTAssert(ledger.longest.highest == e)
	}

    static var allTests = [
        ("testLedger", testLedger),
        ("testFundamentals", testFundamentals)
    ]
}
