import Foundation
import LoggerAPI

class NodeQueryServer: QueryServer {
	var node: Node<SQLBlock>

	init(node: Node<SQLBlock>, port: Int, family: Family = .ipv6) {
		self.node = node
		super.init(port: port, family: family)
	}

	override func query(_ query: String, connection: QueryClientConnection) {
		let ledger = node.ledger as! SQLLedger

		Log.info("[Query] Execute: \(query)")

		do {
			let transaction = try SQLTransaction(statement: query)
			if transaction.root.isMutating {
				// This needs to go to the ledger
				let pl = SQLPayload(transactions: [transaction])
				self.node.submit(payload: pl.data)
				try connection.send(error: "OK \(transaction.identifier.stringValue) \(transaction.root.sql(dialect: SQLStandardDialect()))", severity: .info)
			}
			else {
				// This we can execute right now
				do {
					let result = try ledger.permanentHistory.database.perform(query)

					if case .row = result.state {
						// Send columns
						let fields = result.columns.map { col in
							return PQField(name: col, tableId: 0, columnId: 0, type: .text, typeModifier: 0)
						}
						try connection.send(description: fields)

						while case .row = result.state {
							let values = result.values.map { val in
								return PQValue.text(val)
							}
							try connection.send(row: values)
							result.step()
						}
					}
					try connection.sendQueryComplete(tag: "SELECT")
				}
				catch {
					try? connection.send(error: error.localizedDescription)
					connection.close()
				}
			}
		}
		catch {
			// TODO get some more information from the parser
			try? connection.send(error: "Syntax error")
		}
	}
}
