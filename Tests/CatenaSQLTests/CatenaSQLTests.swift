import XCTest
import CatenaCore
@testable import CatenaSQL

class CatenaSQLTests: XCTestCase {
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
		XCTAssert(b.isPayloadValid(), "block payload is valid")
		XCTAssert(b.hasRoomFor(transaction: tr), "block can accomodate transaction")
		XCTAssert(try b.append(transaction: tr), "block can append transaction")
		XCTAssert(b.isPayloadValid(), "block payload is valid")
		XCTAssert(!(try b.append(transaction: tr)), "block does not append existing transaction")
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
			[.literalBlob(user.publicKey.data.sha256), .literalString(SQLPrivilege.Kind.insert.rawValue), .literalString("test")]
		])
		try _ = db.perform(SQLStatement.insert(ins).sql(dialect: db.dialect))

		// Check privileges
		XCTAssert(try g.check(privileges: [SQLPrivilege(kind: .insert, table: SQLTable(name: "test"))], forUser: user.publicKey))
		XCTAssert(try !(g.check(privileges: [SQLPrivilege(kind: .insert, table: SQLTable(name: "TEST"))], forUser: user.publicKey)))
		XCTAssert(try !(g.check(privileges: [SQLPrivilege(kind: .create, table: SQLTable(name: "test"))], forUser: user.publicKey)))
		XCTAssert(try !(g.check(privileges: [SQLPrivilege(kind: .insert, table: SQLTable(name: "test"))], forUser: otherUser.publicKey)))
	}

	func testParser() throws {
		let p = SQLParser()

		let valid = [
			"SELECT 1+1;",
			"SELECT a FROM b;",
			"SELECT a FROM b WHERE c=d;",
			"SELECT a FROM b WHERE c=d ORDER BY z ASC;",
			"SELECT DISTINCT a FROM b WHERE c=d ORDER BY z ASC;",
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
		]

		for v in valid {
			XCTAssert(p.parse(v), "Failed to parse \(v)")
		}

		for v in invalid {
			XCTAssert(!p.parse(v), "Parsed, but shouldn't have: \(v)")
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
