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

    static var allTests = [
        ("testPeerDatabase", testPeerDatabase),
    ]
}
