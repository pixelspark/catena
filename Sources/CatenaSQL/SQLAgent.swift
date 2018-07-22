import Foundation
import Kitura
import CatenaCore
import LoggerAPI

/** The SQL agent coordinates participation in a distributed blockchain-based database. */
public class SQLAgent {
	let node: Node<SQLLedger>
	private var counters: [PublicKey: SQLTransaction.CounterType] = [:]
	private let mutex = Mutex()

	public init(node: Node<SQLLedger>) {
		self.node = node
	}

	/** Submit a transaction, after issue'ing a consecutive counter value to it and signing it with the private key
	provided. */
	public func submit(transaction: SQLTransaction, signWith key: PrivateKey) throws -> Bool {
		return try autoreleasepool { () -> Bool in
			try self.mutex.locked {
				if let previous = self.counters[transaction.invoker] {
					Log.debug("[SQLAgent] last counter for \(transaction.invoker) was \(previous)")
					transaction.counter = previous + SQLTransaction.CounterType(1)
				}
				else {
					// Look up the counter value
					try self.node.ledger.longest.withUnverifiedTransactions { chain in
						if let previous = try chain.meta.users.counter(for: transaction.invoker) {
							Log.debug("[SQLAgent] last counter for \(transaction.invoker) was \(previous) according to ledger")
							transaction.counter = previous + SQLTransaction.CounterType(1)
						}
						else {
							Log.debug("[SQLAgent] no previous counter for \(transaction.invoker)")
							transaction.counter = SQLTransaction.CounterType(0)
						}
					}
				}

				Log.debug("[SQLAgent] using counter \(transaction.counter) for \(transaction.invoker)")
				self.counters[transaction.invoker] = transaction.counter
			}

			// Submit
			try transaction.sign(with: key)
			return try self.node.receive(transaction: transaction, from: nil)
		}
	}
}

private class SQLAPIEndpointCORS: RouterMiddleware {
	let allowCorsOrigin: String?

	init(allowOrigin: String?) {
		self.allowCorsOrigin = allowOrigin
	}

	public func handle(request: RouterRequest, response: RouterResponse, next: @escaping () -> Void) throws {
		if let ac = self.allowCorsOrigin {
			response.headers.append("Access-Control-Allow-Origin", value: ac)
			response.headers.append("Access-Control-Allow-Methods", value: "GET, POST, OPTIONS")
			response.headers.append("Content-Type", value: "application/json")
			response.headers.append("Access-Control-Allow-Headers", value: "Content-Type, Accept")
			if request.method == .options {
				_ = response.send(status: .OK)
			}
			else {
				next()
			}
		}
		else {
			next()
		}
	}
}

public class SQLAPIEndpoint {
	let agent: SQLAgent

	/** Set 'allowCorsOrigin' to the domain name(s) from which requests may be made. Set to nil to
	disallow any requests from other domains, or set to '*' to allow from any domain. */
	public init(agent: SQLAgent, router: Router, allowCorsOrigin: String?) {
		self.agent = agent

		// API used by the web client
		let mw = SQLAPIEndpointCORS(allowOrigin: allowCorsOrigin)
		router.options("/api/*", middleware: mw)
		router.get("/api/*", middleware: mw)
		router.post("/api/*", middleware: mw)
		router.get("/api", handler: self.handleIndex)
		router.get("/api/counter/:hash", handler: self.handleGetCounter)
		router.post("/api/query", handler: self.handleQuery)

		// Debug APIs
		router.get("/debug/block/:hash", handler: self.handleGetBlock)
		router.get("/debug/journal", handler: self.handleGetJournal)
		router.get("/debug/pool", handler: self.handleGetPool)
		router.get("/debug/users", handler: self.handleGetUsers)

		router.all("/", middleware: StaticFileServer(path: "./Resources"))
	}

	private func handleQuery(request: RouterRequest, response: RouterResponse, next: @escaping () -> Void) throws {
		do {
			var data = Data(capacity: 1024)
			try _ = request.read(into: &data)
			let query = try JSONSerialization.jsonObject(with: data, options: [])

			// Parse the statement
			if let q = query as? [String: Any], let sql = q["sql"] as? String, let database = q["database"] as? String {
				let rawStatement = try SQLStatement(sql)

				// Did the client send parameters to fill in?
				let suppliedParameters = (q["parameters"] as? [String: Any]) ?? [:]
				let translatedParameters = suppliedParameters.mapValues { (v: Any) -> SQLExpression in
					if let num = v as? NSNumber, v is NSNumber {
						return SQLExpression.literalInteger(num.intValue)
					}
					else if let str = v as? String {
						return SQLExpression.literalString(str)
					}
					else {
						return SQLExpression.null
					}
				}

				let statement = try rawStatement.bound(to: translatedParameters).visit(FrontEndStatementVisitor())

				// Collect parameter information
				let parameters = statement.parameters
				let unboundParameters = parameters.compactMap({ (k, v) -> String? in
					if case .unboundParameter(_) = v {
						return k
					}
					return nil
				})

				let jsonParameters = parameters.mapValues { (e: SQLExpression) -> Any? in
					switch e {
					case .unboundParameter(name: _): return nil
					case .literalString(let s): return s
					case .literalInteger(let i): return i
					case .literalUnsigned(let u): return u
					default: return e.sql(dialect: SQLStandardDialect())
					}
				}
				.filter { (_, v) -> Bool in return v != nil }
				.mapValues { v -> Any in return v as Any }

				// Calculate a template hash
				let templateStatement = parameters.isEmpty ? statement : statement.unbound

				// If there are unbound parameters, return an error
				if !unboundParameters.isEmpty {
					_ = response.status(.notAcceptable)
					response.send(json: [
						"unbound": unboundParameters,
						"parameters": jsonParameters,
						"template": templateStatement.sql(dialect: SQLStandardDialect()),
						"templateHash": templateStatement.templateHash.stringValue
					])
					return
				}
                
                // Mutating statements are not executed - client needs to sign and submit a transaction for those
                if statement.isPotentiallyMutating {
                    _ = response.status(.notAcceptable)

					/* We do send back the SQL query as we would write it using the standard SQL dialect
					(transactions need to contain SQL that is formatted exactly following the dialect;
					sending back the SQL in that dialect saves the clients from implementing their own
					SQL parser/formatter). */
                    response.send(json: [
						"message": "Performing mutating queries through this API is not supported at this time.",
						"sql": statement.sql(dialect: SQLStandardDialect()),
						"template": templateStatement.sql(dialect: SQLStandardDialect()),
						"templateHash": templateStatement.templateHash.stringValue,
						"parameters": jsonParameters
					])
                    try response.end()
                }
                else {
                    try self.agent.node.ledger.longest.withUnverifiedTransactions { chain in
                        let anon = try Identity()
						let context = SQLContext(
							database: SQLDatabase(name: database),
							metadata: chain.meta,
							invoker: anon.publicKey,
							block: chain.highest,
							parameterValues: [:]
						)
                        let ex = SQLExecutive(context: context, database: chain.database)
						let result = try ex.perform(statement)
                        
                        var res: [String: Any] = [
                            "sql": sql
                        ];
                        
                        if case .row = result.state {
                            res["columns"] = result.columns
                            
                            var rows: [[Any]] = []
                            while case .row = result.state {
                                let values = result.values.map { val in
                                    return val.json
                                }
                                rows.append(values)
                                result.step()
                            }
                            res["rows"] = rows
                        }
                        
                        response.send(json: res)
                        try response.end()
                    }
                }
            }
            else {
                response.status(.badRequest)
                try response.end()
            }
        }
        catch {
            _ = response.status(.internalServerError)
            response.send(json: ["message": error.localizedDescription])
        }
    }

	private func handleIndex(request: RouterRequest, response: RouterResponse, next: @escaping () -> Void) throws {
		let longest = self.agent.node.ledger.longest

		var networkTime: [String: Any] = [:]

		if let nt = self.agent.node.medianNetworkTime {
			let d = Date()
			networkTime["ownTime"] = d.iso8601FormattedUTCDate
			networkTime["ownTimestamp"] = Int(d.timeIntervalSince1970)
			networkTime["medianNetworkTime"] = nt.iso8601FormattedUTCDate
			networkTime["medianNetworkTimestamp"] = Int(nt.timeIntervalSince1970)
			networkTime["ownOffsetFromMedianNetworkTimeMs"] = Int(d.timeIntervalSince(nt)*1000.0)
		}

		response.send(json: [
			"uuid": self.agent.node.uuid.uuidString,

			"time": networkTime,

			"longest": [
				"highest": longest.highest.json,
				"genesis": longest.genesis.json,
				"difficulty": try longest.difficulty(forBlockFollowing: longest.highest)
			],

			"peers": self.agent.node.peers.map { (url, peer) -> [String: Any] in
				return peer.mutex.locked {
					let desc: String
					switch peer.state {
					case .new: desc = "new"
					case .connected: desc = "connected"
					case .connecting(since: let d): desc = "connecting since \(d.iso8601FormattedLocalDate)"
					case .failed(error: let e, at: let d): desc = "error(\(e)) at \(d.iso8601FormattedLocalDate)"
					case .ignored(reason: let e): desc = "ignored(\(e))"
					case .queried: desc = "queried"
					case .querying(since: let d): desc = "querying since \(d.iso8601FormattedLocalDate)"
					case .passive: desc = "passive"
					}

					var res: [String: Any] = [
						"url": peer.url.absoluteString,
						"state": desc
					]

					if let ls = peer.lastSeen {
						res["lastSeen"] = ls.iso8601FormattedLocalDate
					}

					if let td = peer.timeDifference {
						res["time"] =  Date().addingTimeInterval(td).iso8601FormattedLocalDate
					}

					return res
				}
			}
		])
		next()
	}

	private func handleGetBlock(request: RouterRequest, response: RouterResponse, next: @escaping () -> Void) throws {
		if let hashString = request.parameters["hash"] {
			let hash = try SQLBlock.HashType(hash: hashString)
			
			let block = try self.agent.node.ledger.mutex.locked {
				return try self.agent.node.ledger.longest.get(block: hash)
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
		let pool = self.agent.node.miner.queuedTransactions.map { return $0.json }
		let aside = self.agent.node.miner.transactionsSetAside.map { return $0.json }

		response.send(json: [
			"pool": pool,
			"aside": aside
		])
		next()
	}

	private func handleGetUsers(request: RouterRequest, response: RouterResponse, next: @escaping () -> Void) throws {
		let data = try self.agent.node.ledger.longest.withUnverifiedTransactions { chain in
			return try chain.meta.users.counters()
		}

		var users: [String: Int] = [:]
		data.forEach { user, counter in
			users[user.base64EncodedString()] = counter
		}

		response.send(json: [
			"users": users
		])
		next()
	}

	private func handleGetCounter(request: RouterRequest, response: RouterResponse, next: @escaping () -> Void) throws {
		if let hashString = request.parameters["hash"], let hash = PublicKey(string: hashString) {
			let data = try self.agent.node.ledger.longest.withUnverifiedTransactions { chain in
				return try chain.meta.users.counter(for: hash)
			}

			if let ctr = data {
				response.send(json: [
					// FIXME: 32-bit integer while a counter is 64-bit
					"counter": Int(ctr)
				])
			}
			else {
				response.send(json: [:])
			}
			next()
		}
	}

	private func handleGetJournal(request: RouterRequest, response: RouterResponse, next: @escaping () -> Void) throws {
		let chain = self.agent.node.ledger.longest
		var b: SQLBlock? = chain.highest
		var data: [String] = [];
		while let block = b {
			data.append("")

			for tr in block.payload.transactions.reversed() {
				data.append(tr.statement.sql(dialect: SQLStandardDialect()) + " -- @\(tr.counter)")
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

#if os(Linux)
    @discardableResult internal func autoreleasepool<T>(_ block: () throws -> (T)) rethrows -> T {
        return try block()
    }
#endif
