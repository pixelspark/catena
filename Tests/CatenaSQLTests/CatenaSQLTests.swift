import XCTest
import CatenaCore
import LoggerAPI
import HeliumLogger
@testable import CatenaSQL

class CatenaSQLTests: XCTestCase {
	override func setUp() {
		let logger = HeliumLogger(.info)
		logger.details = false
		Log.logger = logger
	}

	func testPeerDatabase() throws {
		let db = SQLiteDatabase()
		try db.open(":memory:")
		let pd = try SQLPeerDatabase(database: db, table: SQLTable(name: "peers"))

		let uuid = UUID(uuidString: "4fc4ff52-7b3a-11e7-a4d6-535380c31ab9")!
		let p1 = URL(string: "ws://4fc4ff52-7b3a-11e7-a4d6-535380c31ab9@1.2.3.4:1234")!
		let p2 = URL(string: "ws://4fc4ff52-7b3a-11e7-a4d6-535380c31ab9@1.2.3.4:9999")!
		try pd.rememberPeer(url: p1)
		XCTAssert(try pd.peers().count == 1)

		// Database should remember only one address per node ID
		try pd.rememberPeer(url: p2)
		XCTAssert(try pd.peers().count == 1)

		// Database should properly forget peers
		try pd.forgetPeer(uuid: uuid)
		XCTAssert(try pd.peers().count == 0)
	}

	func testTransaction() throws {
		let id = try Identity()
		let otherID = try Identity()
		let st = SQLStatement.create(table: SQLTable(name: "foo"), schema: SQLSchema(primaryKey: SQLColumn(name: "id"), columns: (SQLColumn(name: "id"), .int)))
		let tr = try SQLTransaction(statement: st, invoker: id.publicKey, counter: 0)
		XCTAssert(!tr.isSignatureValid, "transaction signature must not be valid")

		// Try signing with the wrong key
		try tr.sign(with: otherID.privateKey)
		XCTAssert(!tr.isSignatureValid, "transaction signature must not be valid")

		try tr.sign(with: id.privateKey)
		XCTAssert(tr.isSignatureValid, "transaction signature must be valid")

		// Serialization
		let str = try SQLTransaction(json: tr.json)
		XCTAssert(tr == str, "serialized transaction must be equal to original")

		// JSON serialization
		let json = try JSONSerialization.data(withJSONObject: tr.json, options: [])
		let jsonTr = try SQLTransaction(json: try JSONSerialization.jsonObject(with: json, options: []) as! [String: Any])
		XCTAssert(tr == jsonTr, "serialized JSON transaction must be equal to original")

		// JSON verification
		XCTAssertThrowsError(try SQLTransaction(json: [:]))

		// Signatures
		XCTAssert(tr.isSignatureValid, "transaction signature must be valid")

		let pubHash = SHA256Hash(of: id.publicKey.data)
		var b = try SQLBlock.template(for: pubHash)

		XCTAssert(!tr.shouldAlwaysBeReplayed, "this is not a transaction that requires replaying")
		XCTAssert(!b.isPayloadValid(), "block payload is valid (but shouldn't be because block is empty)")
		XCTAssert(b.hasRoomFor(transaction: tr), "block can accomodate transaction")
		XCTAssert(try b.append(transaction: tr), "block can append transaction")
		XCTAssert(b.isPayloadValid(), "block payload is valid")
		XCTAssert(!(try b.append(transaction: tr)), "block does not append existing transaction")
	}

	func testTemplating() {
		let parser = SQLParser()
		let q = "INSERT INTO foo (\"x\") VALUES (?what:5);"
		XCTAssert(parser.parse(q))

		switch parser.root! {
		case .statement(let s):
			let templateStatement = s.unbound
			let sql = templateStatement.sql(dialect: SQLStandardDialect())
			let hash = SHA256Hash(of: sql.data(using: .utf8)!)
			XCTAssert(hash.stringValue == "f2d1d9be4a547f5583cd7b43f322b284655f12b193be713e0940132fc8cbb2d3")

		default:
			XCTFail()
		}
	}

	func testBlockchain() {
		let root = try! Identity()
		let seed = "foo".data(using: .utf8)!
		var genesis = try! SQLBlock(version: SQLBlock.basicVersion, index: 0, nonce: 0, previous: SHA256Hash.zeroHash, miner: SHA256Hash(of: root.publicKey.data), timestamp: 0, payload: seed)
		genesis.mine(difficulty: 10)

		let b = try! SQLBlockchain(genesis: genesis, database: ":memory:", replay: true)
		XCTAssert(try! b.difficulty(forBlockFollowing: genesis) == genesis.work, "block following genesis follows genesis difficulty")

		// config block
		var configBlock = try! SQLBlock(version: SQLBlock.basicVersion, index: 1, nonce: 0, previous: genesis.signature!, miner: SHA256Hash(of: root.publicKey.data), timestamp: 0, payload: Data())

		let statement = SQLStatement.create(table: SQLTable(name: "grants"), schema: SQLGrants.schema)
		let configTransaction = try! SQLTransaction(statement: statement, invoker: root.publicKey, counter: 0)
		try! configTransaction.sign(with: root.privateKey)
		XCTAssert(configBlock.hasRoomFor(transaction: configTransaction), "hasRoomFor")
		XCTAssert(try! configBlock.append(transaction: configTransaction), "append transaction")
		configBlock.mine(difficulty: try! b.difficulty(forBlockFollowing: b.highest))
		XCTAssert(try! b.canAppend(block: configBlock, to: b.highest))
		XCTAssert(try! b.append(block: configBlock), "can append")

		// Subsequent blocks
		var blocks: [SQLBlock] = []
		var newBlock = configBlock
		var date = Date()
		for i in 2..<b.difficultyRetargetInterval {
			newBlock = try! SQLBlock(version: SQLBlock.basicVersion, index: i, nonce: 0, previous: newBlock.signature!, miner: SHA256Hash(of: root.publicKey.data), timestamp: 0, payload: Data())

			let statement = SQLStatement.create(table: SQLTable(name: "foo_\(i)"), schema: SQLSchema(columns: (SQLColumn(name: "x"), SQLType.text)))
			let newTransaction = try! SQLTransaction(statement: statement, invoker: root.publicKey, counter: i - 1)
			try! newTransaction.sign(with: root.privateKey)
			XCTAssert(newBlock.hasRoomFor(transaction: newTransaction), "hasRoomFor")
			XCTAssert(try! newBlock.append(transaction: newTransaction), "append transaction")
			newBlock.mine(difficulty: try! b.difficulty(forBlockFollowing: b.highest), timestamp: date.addingTimeInterval(TimeInterval(i * 60)))
			XCTAssert(try! b.canAppend(block: newBlock, to: b.highest))
			XCTAssert(try! b.append(block: newBlock), "can append")
			blocks.append(newBlock)
		}

		// Test ledger
		let ledger = try! SQLLedger(genesis: genesis, database: ":memory:", replay: true)
		XCTAssert(try ledger.canAccept(transaction: configTransaction, pool: nil) == .now)
		XCTAssert(try ledger.receive(block: configBlock))

		// Check to see if new blocks are accepted
		for b in blocks {
			XCTAssert(try! ledger.isNew(block: b))
			XCTAssert(try! ledger.receive(block: b))
		}
		XCTAssert(ledger.longest.highest == blocks.last!, "tip of chain should be last block")

		// Check to see if existing blocks are denied entry
		for b in blocks {
			XCTAssert(!(try! ledger.isNew(block: b)))
			XCTAssert(!(try! ledger.receive(block: b)))
		}

		// Mine an alternative chain and see how that goes
		var altBlocks: [SQLBlock] = []
		let altChain = try! SQLBlockchain(genesis: genesis, database: ":memory:", replay: true)
		XCTAssert(try! altChain.append(block: configBlock))
		date = Date().addingTimeInterval(3600)
		newBlock = configBlock
		for i in 2..<(2*b.difficultyRetargetInterval) {
			let prev = newBlock
			newBlock = try! SQLBlock(version: SQLBlock.basicVersion, index: i, nonce: 0, previous: prev.signature!, miner: SHA256Hash(of: root.publicKey.data), timestamp: 0, payload: Data())

			let statement = SQLStatement.create(table: SQLTable(name: "bar_\(i)"), schema: SQLSchema(columns: (SQLColumn(name: "x"), SQLType.text)))
			let newTransaction = try! SQLTransaction(statement: statement, invoker: root.publicKey, counter: i - 1)
			try! newTransaction.sign(with: root.privateKey)
			XCTAssert(newBlock.hasRoomFor(transaction: newTransaction), "hasRoomFor")
			XCTAssert(try! newBlock.append(transaction: newTransaction), "append transaction")
			newBlock.mine(difficulty: try! altChain.difficulty(forBlockFollowing: prev), timestamp: date.addingTimeInterval(TimeInterval(i * 60)))
			XCTAssert(try! !b.canAppend(block: newBlock, to: b.highest))
			XCTAssert(try! !b.append(block: newBlock), "can append")
			altBlocks.append(newBlock)
			XCTAssert(try! altChain.append(block: newBlock))
		}

		for b in altBlocks {
			XCTAssert(try! ledger.isNew(block: b))
			_ = try! ledger.receive(block: b)
		}
		XCTAssert(ledger.longest.highest == altBlocks.last!, "tip of chain should be last block")
	}

	func testUsers() throws {
		let db = SQLiteDatabase()
		try db.open(":memory:")
		let u = try SQLUsersTable(database: db, table: SQLTable(name: "users"))

		let id = try Identity()
		try u.setCounter(for: id.publicKey, to: 42)
		XCTAssert(try u.counter(for: id.publicKey)! == 42, "counter must be set")

		try u.setCounter(for: id.publicKey, to: 43)
		XCTAssert(try u.counter(for: id.publicKey)! == 43, "counter must be updates")

		let ctrs = try u.counters()
		XCTAssert(ctrs.count == 1 && ctrs[SHA256Hash(of: id.publicKey.data).hash]! == 43, "counters must be ok")
	}

	func testArchive() throws {
		let db = SQLiteDatabase()
		try db.open(":memory:")
		let arch = try SQLBlockArchive(table: SQLTable(name: "archive"), database: db)

		let id = try Identity()

		var b = try SQLBlock(version: 1, index: 1, nonce: UInt64.max, previous: SHA256Hash.zeroHash, miner: SHA256Hash(of: id.publicKey.data), timestamp: UInt64(Date().timeIntervalSince1970), payload: "foo".data(using: .utf8)!)
		b.mine(difficulty: 2)
		try arch.archive(block: b)

		let c = try arch.get(block: b.signature!)
		XCTAssert(b == c, "archived block must be the same as the original")
	}

	func testGrants() throws {
		let db = SQLiteDatabase()
		try db.open(":memory:")

		let user = try Identity()
		let otherUser = try Identity()
		let grantsTable = SQLTable(name: "grants")
		let g = try SQLGrants(database: db, table: grantsTable)
		try g.create()

		// Insert some privileges
		let ins = SQLInsert(orReplace: false, into: grantsTable, columns: ["user","kind","table"].map { SQLColumn(name: $0) }, values: [
			[.literalBlob(user.publicKey.data.sha256), .literalString(SQLPrivilege.insert(table: nil).privilegeName), .literalString("test")]
		])
		try _ = db.perform(SQLStatement.insert(ins).sql(dialect: db.dialect))

		// Check privileges
		XCTAssert(try g.check(privileges: [SQLPrivilege.insert(table: SQLTable(name: "test"))], forUser: user.publicKey))
		XCTAssert(try !(g.check(privileges: [SQLPrivilege.insert(table: SQLTable(name: "TEST"))], forUser: user.publicKey)))
		XCTAssert(try !(g.check(privileges: [SQLPrivilege.create(table: SQLTable(name: "test"))], forUser: user.publicKey)))
		XCTAssert(try !(g.check(privileges: [SQLPrivilege.insert(table: SQLTable(name: "test"))], forUser: otherUser.publicKey)))
	}

	func testParser() throws {
		let p = SQLParser()

		let valid = [
			"SELECT 1+1;",
			"SELECT a FROM b;",
			"SELECT a FROM b WHERE c=d;",
			"SELECT a FROM b WHERE c=d ORDER BY z ASC;",
			"SELECT DISTINCT a FROM b WHERE c=d ORDER BY z ASC;",
            "SELECT DISTINCT a FROM b WHERE c=d ORDER BY z ASC LIMIT 10;",
			"DELETE FROM a WHERE x=y;",
			"UPDATE a SET z=y WHERE a=b;",
			"INSERT INTO x (a,b,c) VALUES (1,2,3),(4,5,6);",
			"CREATE TABLE x(a TEXT, b TEXT, c TEXT PRIMARY KEY);",
			"INSERT INTO x (a,b,c) VALUES (?x, ?yy, ?zy1);",
			"INSERT INTO x (a,b,c) VALUES (?xy, ?yy:1, ?zy:'123', ?foo:$bar);",
			"SELECT FOO();",
			"SELECT FOO()+BAR();",
			"SELECT FOO(BAR(BAZ()));",
			"SELECT FOO(BAR(BAZ(1+2+3), 2+3), 4);",
            "INSERT INTO grants (\"user\", \"kind\", \"table\") VALUES (X\'b6c8c9c9cd55f5914e29941390a5b69e3d5d59bc11bacb27bf0b9940b4398a33\',\'insert\',\'test\');"
		]

		let invalid = [
			"SELECT 1+1", // missing ';'
			"SELECT $0x;", // Variable name cannot start with digit
			"SELECT ?0x;", // Parameter name cannot start with digit
			"SELECT ?empty:;", // Parameter has no value
			"SELECT ?empty:?other;", // Parameter value may not be another parameter
			"SELECT ?empty:(1+1);", // Parameter value may not be an expression
			"SELECT ?empty:?other:1;", // Parameter value may not be another parameter
			"SELECT ?empty:column;", // Parameter value may not be another column (non-constant)
			"SELECT ?empty:*;", // Parameter value may not be all columns (non-constant)
            "SELECT DISTINCT a FROM b WHERE c=d ORDER BY z ASC LIMIT x;", // limit has non-int
            "SELECT DISTINCT a FROM b WHERE c=d ORDER BY z ASC LIMIT 1.5;", // limit has non-int
            "SELECT DISTINCT a FROM b WHERE c=d ORDER BY z ASC LIMIT -5;", // limit has non-positive int
            "SELECT DISTINCT a FROM b WHERE c=d ORDER BY z ASC LIMIT x+1;", // limit has non-int
		]

		for v in valid {
			XCTAssert(p.parse(v), "Failed to parse \(v)")
		}

		for v in invalid {
			XCTAssert(!p.parse(v), "Parsed, but shouldn't have: \(v)")
		}

		// Test canonicalization
		let canon = [
			"select null;": "SELECT NULL;",
			"select foo;": "SELECT \"foo\";"
		];

		for (before, after) in canon {
			XCTAssert(p.parse(before),"Must parse")
			if case .statement(let s) = p.root! {
				XCTAssert(after == s.sql(dialect: SQLStandardDialect()), "canonical form mismatch for '\(before)': '\(after)' != '\(s.sql(dialect: SQLStandardDialect()))'")
			}
			else {
				XCTFail()
			}
		}
	}

	func testSQLBackend() throws {
		let p = SQLParser()
		let mem = SQLiteDatabase()
		let invoker = try Identity()
		let block = try SQLBlock(version: 1, index: 1, nonce: 1, previous: SHA256Hash.zeroHash, miner: SHA256Hash(of: invoker.publicKey.data), timestamp: 1, payload: Data())
		try mem.open(":memory:")
		let md = try SQLMetadata(database: mem)
		let ctx = SQLContext(metadata: md, invoker: invoker.publicKey, block: block, parameterValues: [:])
		let ex = SQLExecutive(context: ctx, database: mem)

		// See if the backend visitor properly rejects stuff
		let failing = [
			"SELECT ?x, ?x:2;", // Unbound parameter
			"SELECT ?x:1, ?x:2;", // Parameter value mismatch
			"SELECT $foo;", // Unknown variable
		];

		for f in failing {
			XCTAssert(p.parse(f), "Failed to parse \(f)")
			if case .statement(let s) = p.root! {
				XCTAssertThrowsError(try ex.perform(s))
			}
			else {
				XCTFail("parsing failed")
			}
		}
	}

    static var allTests = [
        ("testPeerDatabase", testPeerDatabase),
        ("testGrants", testGrants),
        ("testParser", testParser),
        ("testSQLBackend", testSQLBackend),
    ]
}
