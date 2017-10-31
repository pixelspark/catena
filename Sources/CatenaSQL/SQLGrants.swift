import Foundation
import LoggerAPI
import CatenaCore

public enum SQLPrivilege: Equatable {
	/** Allows the creation of a table with the name indicated, or any table if the table parameter is nil. */
	case create(table: SQLTable?)

	/** Allows deleting from a table with the name indicated, or any table if the table parameter is nil. */
	case delete(table: SQLTable?)

	/** Allows dropping a table with the name indicated, or any table if the table parameter is nil. */
	case drop(table: SQLTable?)

	/** Allows inserting into a table with the name indicated, or any table if the table parameter is nil. */
	case insert(table: SQLTable?)

	/** Allows updating a table with the name indicated, or any table if the table parameter is nil. */
	case update(table: SQLTable?)

	/** Allows executing any query whose template hash matches the indicated hash. */
	case template(hash: SHA256Hash)

	/**  Privilege that is never granted (used to indicate operations that are never allowed) */
	case never

	public static func ==(lhs: SQLPrivilege, rhs: SQLPrivilege) -> Bool {
		switch (lhs, rhs) {
		case (.create(table: let l), 	.create(table: let r)): return l == r
		case (.delete(table: let l), 	.delete(table: let r)): return l == r
		case (.drop(table: let l), 		.drop(table: let r)): return l == r
		case (.insert(table: let l), 	.insert(table: let r)): return l == r
		case (.update(table: let l), 	.update(table: let r)): return l == r
		case (.template(hash: let l), 	.template(hash: let r)): return l == r
		case (.never, .never): return true
		default: return false
		}
	}

	public var privilegeName: String {
		switch self {
		case .create(table: _): return "create"
		case .update(table: _): return "update"
		case .delete(table: _): return "delete"
		case .drop(table: _): return "drop"
		case .insert(table: _): return "insert"
		case .template(hash: _): return "template"
		case .never: return "never"
		}
	}

	var table: SQLTable? {
		switch self {
		case .create(table: let t), .update(table: let t), .delete(table: let t), .drop(table: let t), .insert(table: let t):
			return t

		case .template, .never:
			return nil
		}
	}
}

public extension SQLStatement {
	var requiredPrivileges: [SQLPrivilege] {
		switch self {
		case .create(table: let t, schema: _): return [SQLPrivilege.create(table: t)]
		case .delete(from: let t, where: _): return [SQLPrivilege.delete(table: t)]
		case .drop(table: let t): return [SQLPrivilege.drop(table: t)]
		case .select(_): return []
		case .show(_): return []
		case .update(let update): return [SQLPrivilege.update(table: update.table)]
		case .insert(let ins): return [SQLPrivilege.insert(table: ins.into)]
		case .createIndex(table: _, index: _): return [SQLPrivilege.never]
		case .fail: return []
		}
	}
}

public class SQLGrants {
	public static let schema = SQLSchema(columns:
		(SQLColumn(name: "kind"), .text),
        (SQLColumn(name: "user"), .blob),
        (SQLColumn(name: "table"), .blob)
	)

	let table: SQLTable
	let database: Database

	public init(database: Database, table: SQLTable) throws {
		self.database = database
		self.table = table
	}

	public func create() throws {
		try _ = self.database.perform(SQLStatement.create(table: SQLTable(name: SQLMetadata.grantsTableName), schema: SQLGrants.schema).sql(dialect: self.database.dialect))
	}

	/** Checks whether the indicated user holds the required privileges. When the function throws, the caller should
	always assume 'no privileges'. */
	public func check(privileges: [SQLPrivilege], forUser user: CatenaCore.PublicKey) throws -> Bool {
		for p in privileges {
			var subjectCheckExpression = SQLExpression.unary(.isNull, .column(SQLColumn(name: "table")))

			// Determine how to check for this privilege type
			switch p {
			case .never:
				// The 'never' privilege is never granted
				return false

			case .create(table: let t), .delete(table: let t), .update(table: let t), .drop(table: let t), .insert(table: let t):
				if let table = t {
					let specificCheckExpression = SQLExpression.binary(.column(SQLColumn(name: "table")), .equals, .literalString(table.name))
					subjectCheckExpression = SQLExpression.binary(subjectCheckExpression, .or, specificCheckExpression)
				}

			case .template(hash: let hash):
				subjectCheckExpression = SQLExpression.binary(.column(SQLColumn(name: "table")), .equals, .literalBlob(hash.hash))
			}

			// Find privileges
			let select = SQLStatement.select(SQLSelect(
				these: [SQLExpression.literalInteger(1)],
				from: self.table,
				joins: [],
				where: SQLExpression.binary(
					SQLExpression.binary(
						SQLExpression.binary(SQLExpression.column(SQLColumn(name: "user")), .equals, .literalBlob(user.data.sha256)),
						SQLBinary.and,
						SQLExpression.binary(SQLExpression.column(SQLColumn(name: "kind")), .equals, .literalString(p.privilegeName))
					),
					SQLBinary.and,
					subjectCheckExpression
				),
				distinct: false,
				orders: []))

			let r = try self.database.perform(select.sql(dialect: self.database.dialect))
			if !r.hasRow {
				Log.debug("[SQLGrants] privilege NOT present: \(p) for user \(user.data.sha256.base64EncodedString())")
				return false
			}
			Log.debug("[SQLGrants] privilege present: \(p) for user \(user)")
		}
		return true
	}
}
