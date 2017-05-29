import Foundation
import SwiftParser
import LoggerAPI

class SQLTransaction {
	let root: SQLStatement

	enum SQLTransactionError: Error {
		case formatError
		case syntaxError
	}

	init(statement: String) throws {
		let parser = SQLParser()
		if !parser.parse(statement) {
			Log.debug("[SQLTransaction] Parsing failed: \(statement)")
			Log.debug(parser.debugDescription)
			throw SQLTransactionError.syntaxError
		}

		// Top-level item must be a statement
		if let root = parser.root, case .statement(let sq) = root {
			self.root = sq
		}
		else {
			throw SQLTransactionError.syntaxError
		}
	}

	convenience init(data: Any) throws {
		if let q = data as? String {
			try self.init(statement: q)
		}
		else {
			throw SQLTransactionError.formatError
		}
	}

	var data: Any {
		return self.root.sql
	}

	var identifier: Hash {
		return Hash(of: self.root.sql.data(using: .utf8)!)
	}
}

struct SQLTable: Equatable {
	var name: String

	var sql: String {
		return name.lowercased()
	}

	static func ==(lhs: SQLTable, rhs: SQLTable) -> Bool {
		return lhs.name.lowercased() == rhs.name.lowercased()
	}
}

struct SQLColumn: Equatable {
	var name: String

	var sql: String {
		return name.lowercased()
	}

	static func ==(lhs: SQLColumn, rhs: SQLColumn) -> Bool {
		return lhs.name.lowercased() == rhs.name.lowercased()
	}
}

enum SQLStatement {
	case create
	case delete
	case drop
	case insert(into: SQLTable, columns: [SQLColumn], values: [[SQLExpression]])
	case select(these: [SQLExpression], from: SQLTable?)
	case update

	var isMutating: Bool {
		switch self {
		case .create, .drop, .delete, .update, .insert(into:_, columns:_, values:_):
			return true

		case .select(_):
			return false
		}
	}

	var sql: String {
		switch self {
		case .create:
			return "CREATE;"

		case .delete:
			return "DELETE;"

		case .drop:
			return "DROP;"

		case .insert(into: let into, columns: let cols, values: let tuples):
			let colSQL = cols.map { $0.sql }.joined(separator: ", ")
			let tupleSQL = tuples.map { tuple in
				let ts = tuple.map { $0.sql }.joined(separator: ",")
				return "(\(ts))"
			}.joined(separator: ", ")

			return "INSERT INTO \(into.sql) (\(colSQL)) VALUES \(tupleSQL);"

		case .update:
			return "UPDATE;"

		case .select(let exprs, from: let table):
			let selectList = exprs.map { $0.sql }.joined(separator: ", ")
			if let t = table {
				return "SELECT \(selectList) FROM \(t.sql);"
			}
			else {
				return "SELECT \(selectList);"
			}
		}
	}
}

enum SQLExpression {
	case literalInteger(Int)
	case literalString(String)
	case column(String)

	var sql: String {
		switch self {
		case .literalString(let s):
			// TODO: escaping
			return "'\(s)'"

		case .literalInteger(let i):
			return "\(i)"

		case .column(let c):
			// TODO: escaping?
			return "\(c)"
		}
	}
}

enum SQLFragment {
	case statement(SQLStatement)
	case expression(SQLExpression)
	case tuple([SQLExpression])
	case columnList([SQLColumn])
	case tableIdentifier(SQLTable)
}

internal class SQLParser: Parser, CustomDebugStringConvertible {
	private var stack: [SQLFragment] = []

	private func pushLiteralString() {
		// TODO: escaping
		self.stack.append(.expression(.literalString(self.text)))
	}

	var debugDescription: String {
		return "\(self.stack)"
	}

	var root: SQLFragment? {
		return self.stack.last
	}

	public override func rules() {
		// Literals
		let firstCharacter: ParserRule = (("a"-"z")|("A"-"Z"))
		let followingCharacter: ParserRule = (firstCharacter | ("0"-"9") | literal("_"))
		add_named_rule("lit-int", rule: (("0"-"9")+) => {
			if let n = Int(self.text) {
				self.stack.append(.expression(.literalInteger(n)))
			}
		})

		add_named_rule("lit-column", rule: (firstCharacter ~ (followingCharacter*)/~) => {
			self.stack.append(.expression(.column(self.text)))
		})

		add_named_rule("lit-string", rule: Parser.matchLiteral("'") ~ Parser.matchAnyCharacterExcept([Character("'")])* => pushLiteralString ~ Parser.matchLiteral("'"))
		add_named_rule("lit", rule: ^"lit-int" | ^"lit-column" | ^"lit-string")

		// Expressions
		add_named_rule("ex", rule: ^"lit")

		// FROM
		add_named_rule("id-table", rule: firstCharacter ~ followingCharacter*)

		// SELECT
		add_named_rule("tuple", rule: Parser.matchList(^"ex" => {
			if case .expression(let ne) = self.stack.popLast()! {
				if let last = self.stack.popLast(), case .tuple(let exprs) = last {
					self.stack.append(.tuple(exprs + [ne]))
				}
				else {
					self.stack.append(.tuple([ne]))
				}
			}
		}, separator: Parser.matchLiteral(",")))

		// INSERT
		add_named_rule("column-list", rule: Parser.matchList(^"lit-column" => {
			if case .expression(let ne) = self.stack.popLast()!, case .column(let colName) = ne {
				if let last = self.stack.popLast(), case .columnList(let exprs) = last {
					self.stack.append(.columnList(exprs + [SQLColumn(name: colName)]))
				}
				else {
					self.stack.append(.columnList([SQLColumn(name: colName)]))
				}
			}
			else {
				// This cannot be
				fatalError("Parser programming error")
			}
			}, separator: Parser.matchLiteral(",")))


		// Statement types
		add_named_rule("select-dql-statement", rule: Parser.matchLiteralInsensitive("SELECT")
			~~ (^"tuple" => {
				if let last = self.stack.popLast(), case .tuple(let exprs) = last {
					self.stack.append(.statement(.select(these: exprs, from: nil)))
				}
			})
			~~ (Parser.matchLiteralInsensitive("FROM") ~~ ^"id-table" => {
				if let last = self.stack.popLast(), case .statement(let st) = last, case .select(these: let exprs, from: _) = st {
					self.stack.append(.statement(.select(these: exprs, from: SQLTable(name: self.text))))
				}
			})/~
		)

		add_named_rule("create-ddl-statement", rule: Parser.matchLiteralInsensitive("CREATE"))
		add_named_rule("drop-ddl-statement", rule: Parser.matchLiteralInsensitive("DROP"))
		add_named_rule("update-dml-statement", rule: Parser.matchLiteralInsensitive("UPDATE"))
		add_named_rule("delete-dml-statement", rule: Parser.matchLiteralInsensitive("DELETE"))

		add_named_rule("insert-dml-statement", rule: (
			Parser.matchLiteralInsensitive("INSERT INTO")
				~~ (^"id-table" => { self.stack.append(.tableIdentifier(SQLTable(name: self.text))) })
				~~ ((Parser.matchLiteral("(") => { self.stack.append(.columnList([])) }) ~~ ^"column-list" ~~ Parser.matchLiteral(")"))
				~~ Parser.matchLiteralInsensitive("VALUES") ~~ ((Parser.matchLiteral("(") => { self.stack.append(.tuple([])) }) ~~ ^"tuple" ~~ Parser.matchLiteral(")"))
		) => {
			if  case .tuple(let rs) = self.stack.popLast()!,
				case .columnList(let cs) = self.stack.popLast()!,
				case .tableIdentifier(let tn) = self.stack.popLast()! {
				self.stack.append(.statement(.insert(into: tn, columns: cs, values: [rs])))
			}
		})

		// Statement categories
		add_named_rule("dql-statement", rule: ^"select-dql-statement")
		add_named_rule("ddl-statement", rule: ^"create-ddl-statement" | ^"drop-ddl-statement")
		add_named_rule("dml-statement", rule: ^"update-dml-statement" | ^"insert-dml-statement" | ^"delete-dml-statement")

		// Statement
		add_named_rule("statement", rule: (^"ddl-statement" | ^"dml-statement" | ^"dql-statement") ~~ Parser.matchLiteral(";"))
		start_rule = (^"statement")*!*
	}
}

fileprivate extension Parser {
	static func matchEOF() -> ParserRule {
		return {(parser: Parser, reader: Reader) -> Bool in
			return reader.eof()
		}
	}

	static func matchAnyCharacterExcept(_ characters: [Character]) -> ParserRule {
		return {(parser: Parser, reader: Reader) -> Bool in
			if reader.eof() {
				return false
			}

			let pos = reader.position
			let ch = reader.read()
			for exceptedCharacter in characters {
				if ch==exceptedCharacter {
					reader.seek(pos)
					return false
				}
			}
			return true
		}
	}

	static func matchAnyFrom(_ rules: [ParserRule]) -> ParserRule {
		return {(parser: Parser, reader: Reader) -> Bool in
			let pos = reader.position
			for rule in rules {
				if(rule(parser, reader)) {
					return true
				}
				reader.seek(pos)
			}

			return false
		}
	}

	static func matchList(_ item: @escaping ParserRule, separator: @escaping ParserRule) -> ParserRule {
		return item ~~ (separator ~~ item)*
	}

	static func matchLiteralInsensitive(_ string:String) -> ParserRule {
		return {(parser: Parser, reader: Reader) -> Bool in
			let pos = reader.position

			for ch in string.characters {
				let flag = (String(ch).caseInsensitiveCompare(String(reader.read())) == ComparisonResult.orderedSame)

				if !flag {
					reader.seek(pos)
					return false
				}
			}
			return true
		}
	}

	static func matchLiteral(_ string:String) -> ParserRule {
		return {(parser: Parser, reader: Reader) -> Bool in
			let pos = reader.position

			for ch in string.characters {
				if ch != reader.read() {
					reader.seek(pos)
					return false
				}
			}
			return true
		}
	}
}


