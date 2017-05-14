import Foundation
import Kitura

extension Block {
	var json: [String: Any?] {
		return [
			"nonce": self.nonce,
			"hash": self.signature?.stringValue,
			"height": self.index,
			"size": self.signedData.count
		]
	}
}

class Server<BlockType: Block> {
	let router = Router()
	let ledger: Ledger<BlockType>

	init(ledger: Ledger<BlockType>, port: Int) {
		self.ledger = ledger

		router.get("/", handler: self.handleIndex)
		router.get("/block/:hash", handler: self.handleGetBlock)
		Kitura.addHTTPServer(onPort: port, with: router)
	}

	private func handleIndex(request: RouterRequest, response: RouterResponse, next: @escaping () -> Void) throws {
		response.send(json: [
			"status": "ok",

			"longest": self.ledger.mutex.locked { return [
				"highest": self.ledger.longest.highest.json,
				"genesis": self.ledger.longest.genesis.json
			]}
		])
		next()
	}

	private func handleGetBlock(request: RouterRequest, response: RouterResponse, next: @escaping () -> Void) throws {
		response.send(json: ["status": "ok", "block": [
			"hash": request.parameters["hash"]
		]])
		next()
	}
}
