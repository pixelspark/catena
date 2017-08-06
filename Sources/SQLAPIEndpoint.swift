import Foundation
import Kitura

class SQLAPIEndpoint {
	let node: Node<SQLBlockchain>

	init(node: Node<SQLBlockchain>, router: Router) {
		self.node = node

		router.get("/api", handler: self.handleIndex)
		router.get("/api/block/:hash", handler: self.handleGetBlock)
		router.get("/api/head", handler: self.handleGetLast)
		router.get("/api/journal", handler: self.handleGetJournal)
		router.get("/api/pool", handler: self.handleGetPool)
		router.get("/api/users", handler: self.handleGetUsers)
	}

	private func handleIndex(request: RouterRequest, response: RouterResponse, next: @escaping () -> Void) throws {
		let longest = self.node.ledger.longest

		response.send(json: [
			"uuid": self.node.uuid.uuidString,

			"longest": [
				"highest": longest.highest.json,
				"genesis": longest.genesis.json
			],

			"peers": self.node.peers.map { (url, peer) -> [String: Any] in
				return peer.mutex.locked {
					let desc: String
					switch peer.state {
					case .new: desc = "new"
					case .connected(_): desc = "connected"
					case .connecting(_): desc = "connecting"
					case .failed(error: let e): desc = "error(\(e))"
					case .ignored(reason: let e): desc = "ignored(\(e))"
					case .queried(_): desc = "queried"
					case .querying(_): desc = "querying"
					case .passive: desc = "passive"
					}

					var res = [
						"url": peer.url.absoluteString,
						"state": desc
					]

					#if !os(Linux)
						res["lastSeen"] = peer.lastSeen?.iso8601FormattedLocalDate ?? "never"
					#endif

					return res
				}
			}
		])
		next()
	}

	private func handleGetBlock(request: RouterRequest, response: RouterResponse, next: @escaping () -> Void) throws {
		if let hashString = request.parameters["hash"], let hash = SQLBlock.HashType(hash: hashString) {
			let block = try self.node.ledger.mutex.locked {
				return try self.node.ledger.longest.get(block: hash)
			}

			if let b = block {
				assert(b.isSignatureValid, "returning invalid blocks, that can't be good")
				response.send(json: b.json)

				next()
			}
			else {
				_ = response.send(status: .notFound)
			}
		}
		else {
			_ = response.send(status: .badRequest)
		}
	}

	private func handleGetPool(request: RouterRequest, response: RouterResponse, next: @escaping () -> Void) throws {
		let pool = self.node.miner.block?.payload.transactions.map { return $0.json } ?? []

		response.send(json: [
			"status": "ok",
			"pool": pool
		])
		next()
	}

	private func handleGetUsers(request: RouterRequest, response: RouterResponse, next: @escaping () -> Void) throws {
		let data = try self.node.ledger.longest.withUnverifiedTransactions { chain in
			return try chain.meta.users.counters()
		}

		var users: [String: Int] = [:]
		data.forEach { user, counter in
			users[user.base64EncodedString()] = counter
		}

		response.send(json: [
			"status": "ok",
			"users": users
		])
		next()
	}

	private func handleGetLast(request: RouterRequest, response: RouterResponse, next: @escaping () -> Void) throws {
		let chain = self.node.ledger.longest
		var b: SQLBlock? = chain.highest
		var data: [[String: Any]] = []
		for _ in 0..<10 {
			if let block = b {
				data.append([
					"index": block.index,
					"hash": block.signature!.stringValue
					])
				b = try chain.get(block: block.previous)
			}
			else {
				break
			}
		}

		response.send(json: [
			"status": "ok",
			"blocks": data
			])
		next()
	}

	private func handleGetJournal(request: RouterRequest, response: RouterResponse, next: @escaping () -> Void) throws {
		let chain = self.node.ledger.longest
		var b: SQLBlock? = chain.highest
		var data: [String] = [];
		while let block = b {
			data.append("")

			for tr in block.payload.transactions.reversed() {
				data.append(tr.statement.sql(dialect: SQLStandardDialect()))
			}

			data.append("-- #\(block.index): \(block.signature!.stringValue)")

			if block.index == 0 {
				break
			}
			b = try chain.get(block: block.previous)
			assert(b != nil, "Could not find block #\(block.index-1):\(block.previous.stringValue) in storage while on-chain!")
		}

		response.headers.setType("text/plain", charset: "utf8")
		response.send(data.reversed().joined(separator: "\r\n"))
		next()
	}
}
