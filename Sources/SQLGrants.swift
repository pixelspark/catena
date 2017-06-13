import Foundation
import LoggerAPI

struct SQLPrivilege {
	enum Kind: String {
		case create = "create"
		case delete = "delete"
		case drop = "drop"
		case insert = "insert"
		case never = "never" // privilege that is never granted (operations that are never allowed)
	}

	var kind: Kind
	var table: SQLTable? = nil
}

extension SQLStatement {
	var requiredPrivileges: [SQLPrivilege] {
		switch self {
		case .create(table: let t, schema: _): return [SQLPrivilege(kind: .create, table: t)]
		case .delete(from: let t, where: _): return [SQLPrivilege(kind: .delete, table: t)]
		case .drop(table: let t): return [SQLPrivilege(kind: .drop, table: t)]
		case .select(_): return []
		case .update: return [SQLPrivilege(kind: .never, table: nil)]
		case .insert(let ins): return [SQLPrivilege(kind: .insert, table: ins.into)]
		}
	}
}

class SQLGrants {
	static let schema = SQLSchema(columns:
		(SQLColumn(name: "kind"), .text),
        (SQLColumn(name: "user"), .blob),
        (SQLColumn(name: "table"), .blob)
	)

	let table: SQLTable
	let database: Database

	init(database: Database, table: SQLTable) throws {
		self.database = database
		self.table = table
	}

	/** Checks whether the indicated user holds the required privileges. When the function throws, the caller should
	always assume 'no privileges'. */
	func check(privileges: [SQLPrivilege], forUser user: PublicKey) throws -> Bool {
		for p in privileges {
			switch p.kind {
			case .never:
				// The 'never' privilege is never granted
				return false

			default:
				var tableCheckExpression = SQLExpression.unary(.isNull, .column(SQLColumn(name: "table")))
				if let t = p.table {
					let specificCheckExpression = SQLExpression.binary(.column(SQLColumn(name: "table")), .equals, .literalString(t.name))
					tableCheckExpression = SQLExpression.binary(tableCheckExpression, .or, specificCheckExpression)
				}

				let select = SQLStatement.select(SQLSelect(
					these: [SQLExpression.literalInteger(1)],
					from: self.table,
					where: SQLExpression.binary(
						SQLExpression.binary(
							SQLExpression.binary(SQLExpression.column(SQLColumn(name: "user")), .equals, .literalBlob(user.data)),
							SQLBinary.and,
							SQLExpression.binary(SQLExpression.column(SQLColumn(name: "kind")), .equals, .literalString(p.kind.rawValue))
						),
						SQLBinary.and,
						tableCheckExpression
					),
					distinct: false))

				let r = try self.database.perform(select.sql(dialect: self.database.dialect))
				if !r.hasRow {
					Log.debug("[SQLGrants] privilege NOT present: \(p) for user \(user)")
					return false
				}
				Log.debug("[SQLGrants] privilege present: \(p) for user \(user)")
			}
		}
		return true
	}
}
