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
	var payloadDataForSigning: Data

	init() {
		self.index = 0
		self.previous = HashType.zeroHash
		self.payloadData = Data()
		self.nonce = 0
		self.payloadDataForSigning = Data()
	}

	init(index: UInt64, previous: SHA256Hash, payload: Data) throws {
		self.index = index
		self.previous = previous
		self.payloadData = payload
		self.payloadDataForSigning = self.payloadData
		self.nonce = 0
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
		if self.canAppend(block: block, to: self.highest) {
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
	func testFundamentals() {
		XCTAssertEqual(SHA256Hash(of: "Catena".data(using: .utf8)!).stringValue.lowercased(), "13ab80a5ba95216129ea9d996937b4ed57faf7473e81288d99689da4d5f1d483")
		XCTAssertEqual(SHA256Hash.zeroHash, SHA256Hash.zeroHash)
	}

	func testLedger() throws {
		var genesis = try TestBlock(index: 0, previous: SHA256Hash.zeroHash, payload: Data())
		genesis.mine(difficulty: 2)
		XCTAssert(genesis.isSignatureValid && genesis.isAGenesisBlock)
		let ledger = TestLedger(genesis: genesis)

		XCTAssert(ledger.longest.genesis.isAGenesisBlock)
		XCTAssert(ledger.longest.genesis.isSignatureValid)

		// Attempt to append a valid block
		var b = try TestBlock(index: 1, previous: ledger.longest.genesis.signature!, payload: Data())
		b.mine(difficulty: ledger.longest.difficulty)
		XCTAssert(try ledger.receive(block: b))
		XCTAssert(ledger.longest.highest == b)

		// Attempt to append an invalid block
		var c = try TestBlock(index: 1, previous: ledger.longest.genesis.signature!, payload: Data())
		c.mine(difficulty: ledger.longest.difficulty)
		c.nonce = 0
		XCTAssert(!(try ledger.receive(block: c)))

		// Attempt to add an outdated block should fail
		var d = try TestBlock(index: 1, previous: ledger.longest.genesis.signature!, payload: Data())
		d.mine(difficulty: ledger.longest.difficulty)
		XCTAssert(!(try ledger.receive(block: d)))

		// Attempt to add an easier block should fail
		var e = try TestBlock(index: 2, previous: b.signature!, payload: Data())
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
