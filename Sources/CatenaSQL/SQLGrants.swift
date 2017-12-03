import Foundation
import LoggerAPI
import CatenaCore

public enum SQLPrivilegeError: LocalizedError {
	case unknownPrivilegeError

	public var errorDescription: String? {
		switch self {
		case .unknownPrivilegeError: return "unknown privilege"
		}
	}
}

/** Database-level privileges concern tables and queries inside a database (e.g. a privilege does not
cover 'create database' or 'drop database' statements). */
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

	/** Allows granting rights to other users (on a specific table or on all of them in the database) */
	case grant(table: SQLTable?)

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
		case (.grant(table: let l), 	.grant(table: let r)): return  l == r
		case (.never, .never): return true
		default: return false
		}
	}

	public static var validPrivilegeNames = ["create", "update", "delete", "drop", "insert", "template", "grant"]

	public static func privilege(name: String, with: Data?) throws -> SQLPrivilege {
		switch name.lowercased() {
		case "template":
			if let h = with {
				return try SQLPrivilege.template(hash: SHA256Hash(hash: h))
			}

		default:
			break
		}
		throw SQLPrivilegeError.unknownPrivilegeError
	}

	/** Create a privilege by name and subject (name is case-insensitive) */
	public static func privilege(name: String, on: String?) throws -> SQLPrivilege {
		switch name.lowercased() {
		case "create": return SQLPrivilege.create(table: on != nil ? SQLTable(name: on!) : nil)
		case "update": return SQLPrivilege.update(table: on != nil ? SQLTable(name: on!) : nil)
		case "delete": return SQLPrivilege.delete(table: on != nil ? SQLTable(name: on!) : nil)
		case "drop": return SQLPrivilege.drop(table: on != nil ? SQLTable(name: on!) : nil)
		case "insert": return SQLPrivilege.insert(table: on != nil ? SQLTable(name: on!) : nil)
		case "grant": return SQLPrivilege.grant(table: on != nil ? SQLTable(name: on!) : nil)
		default:
			throw SQLPrivilegeError.unknownPrivilegeError
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
		case .grant(table: _): return "grant"
		}
	}

	var table: SQLTable? {
		switch self {
		case .create(table: let t), .update(table: let t), .delete(table: let t), .drop(table: let t), .insert(table: let t), .grant(table: let t):
			return t

		case .template, .never:
			return nil
		}
	}
}

public extension SQLStatement {
	/** Returns the set of privileges required to execute this statement. Note that for control/compound
	statements, this returns the set of privileges required only for the statement itself, not subseqent
	statements (e.g. for an IF statement, the privileges for the branch statements should be checked
	separately). */
	var requiredPrivileges: [SQLPrivilege] {
		switch self {
		case .createDatabase(database: _): return [.never]
		case .dropDatabase(database: _): return [.never]
		case .createTable(table: let t, schema: _): return [SQLPrivilege.create(table: t)]
		case .delete(from: let t, where: _): return [SQLPrivilege.delete(table: t)]
		case .dropTable(table: let t): return [SQLPrivilege.drop(table: t)]
		case .select(_): return []
		case .show(_): return []
		case .update(let update): return [SQLPrivilege.update(table: update.table)]
		case .insert(let ins): return [SQLPrivilege.insert(table: ins.into)]
		case .createIndex(table: _, index: _): return [SQLPrivilege.never]
		case .fail: return []
		case .`if`: return []
		case .block(_): return []
		case .describe(_): return []
		case .grant(let pr, to: _): return [SQLPrivilege.grant(table: pr.table)]
		case .revoke(let pr, to: _): return [SQLPrivilege.grant(table: pr.table)]
		}
	}
}

public class SQLGrants {
	public static let schema = SQLSchema(columns:
		(SQLColumn(name: "database"), .text),
		(SQLColumn(name: "kind"), .text),
        (SQLColumn(name: "user"), .blob),
        (SQLColumn(name: "table"), .blob)
	)

	let table: SQLTable
	let database: Database

	public init(database: Database, table: SQLTable) throws {
		self.database = database
		self.table = table

		// Create grants table, etc.
		let create = SQLStatement.createTable(table: table, schema: SQLGrants.schema)

		try database.transaction {
			if try !database.exists(table: table.name) {
				try _ = database.perform(create.sql(dialect: database.dialect))
			}
		}
	}

	public func create() throws {
		try _ = self.database.perform(SQLStatement.createTable(table: SQLTable(name: SQLMetadata.grantsTableName), schema: SQLGrants.schema).sql(dialect: self.database.dialect))
	}

	/** Checks whether the indicated user holds the required privileges. When the function throws,
	the caller should always assume 'no privileges'. Returns 'false' for privileges that are never
	granted (including those at the database level). */
	public func check(privileges: [SQLPrivilege], forUser user: CatenaCore.PublicKey, in database: SQLDatabase) throws -> Bool {
		for p in privileges {
			var subjectCheckExpression = SQLExpression.unary(.isNull, .column(SQLColumn(name: "table")))

			switch p {
			case .never:
				// The 'never' privilege is never granted
				return false

			case .create(table: let t), .delete(table: let t), .update(table: let t), .drop(table: let t), .insert(table: let t), .grant(table: let t):
				if let table = t {
					// Permission must be for the table we are creating/deleting/updating/dropping/inserting (in/from), or for NULL (any table)
					let specificCheckExpression = SQLExpression.binary(.column(SQLColumn(name: "table")), .equals, .literalString(table.name))
					subjectCheckExpression = SQLExpression.binary(subjectCheckExpression, .or, specificCheckExpression)
				}

			case .template(hash: let hash):
				// Permission must be for the template hash (a permission for a NULL template grant does not have any effects)
				subjectCheckExpression = SQLExpression.binary(.column(SQLColumn(name: "table")), .equals, .literalBlob(hash.hash))
			}

			// Find privileges
			let select = SQLStatement.select(SQLSelect(
				these: [SQLExpression.literalInteger(1)],
				from: self.table,
				joins: [],
				where: .binary(
					.binary(
						SQLExpression.binary(
							// Privilege must be for this user or for all users (user=NULL)
							SQLExpression.binary(
								SQLExpression.binary(SQLExpression.column(SQLColumn(name: "user")), .equals, .literalBlob(user.data.sha256)),
								SQLBinary.or,
								SQLExpression.unary(.isNull, SQLExpression.column(SQLColumn(name: "user")))
							),
							SQLBinary.and,
							SQLExpression.binary(SQLExpression.column(SQLColumn(name: "kind")), .equals, .literalString(p.privilegeName))
						),
						SQLBinary.and,
						subjectCheckExpression
					),
					.and,
					.binary(.column(SQLColumn(name: "database")), .equals, .literalString(database.name))
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
