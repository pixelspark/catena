import Foundation

class NodeQueryServer: QueryServer {
	var node: Node<SQLBlock>

	init(node: Node<SQLBlock>, port: Int, family: Family = .ipv6) {
		self.node = node
		super.init(port: port, family: family)
	}

	override func query(_ query: String, connection: QueryClientConnection) {
		let ledger = node.ledger as! SQLLedger

		print("EXEC: \(query)")

		do {
			switch ledger.database.perform(query) {
			case .success(let result):
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

			case .failure(let e):
				try connection.send(error: e)
			}
		}
		catch {
			try? connection.send(error: error.localizedDescription)
			connection.close()
		}
	}
}
