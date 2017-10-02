import Foundation
import Kitura
import Dispatch
import LoggerAPI
import Socket

import class NetService.NetService
import protocol NetService.NetServiceDelegate
import class NetService.NetServiceBrowser
import protocol NetService.NetServiceBrowserDelegate

struct Candidate<BlockType: Block>: Equatable, Hashable {
	static func ==(lhs: Candidate<BlockType>, rhs: Candidate<BlockType>) -> Bool {
		return lhs.hash == rhs.hash && lhs.peer == rhs.peer
	}

	var hashValue: Int {
		return hash.hashValue ^ height.hashValue ^ peer.hashValue
	}

	let hash: BlockType.HashType
	let height: UInt64
	let peer: UUID
}

/** A database that stores URLs to known peers, for reconnecting in the future. */
public protocol PeerDatabase {
	/** Remember a peer in the peer database cache. Note that the peer database should only remember the most recent
	address for a given node ID. */
	func rememberPeer(url: URL) throws

	/** Remove the remembered address for the given node ID (if it was remembered at all, otherwise fail silently). */
	func forgetPeer(uuid: UUID) throws
}

/** Handles presence of the node on the network. */
public class Node<LedgerType: Ledger> {
	public typealias BlockchainType = LedgerType.BlockchainType
	public typealias BlockType = BlockchainType.BlockType

	public let uuid: UUID
	public private(set) var ledger: LedgerType! = nil
	public private(set) var miner: Miner<LedgerType>! = nil
	public private(set) var server: Server<LedgerType>! = nil
	public private(set) var peers: [UUID: Peer<LedgerType>] = [:]
	public var peerDatabase: PeerDatabase? = nil

	private var fetcher: Fetcher<LedgerType>! = nil
	private let workerQueue = DispatchQueue.global(qos: .background)

	private let tickTimer: DispatchSourceTimer
	fileprivate let mutex = Mutex()
	private var queryQueue: [UUID] = []

	private var localNodeAnnouncement: LocalNodeAnnouncement<LedgerType>? = nil
	private var localNodeBrowser: LocalNodeBrowser<LedgerType>? = nil

	/** The list of peers that can be advertised to other nodes for peer exchange. */
	var validPeers: Set<URL> {
		return Set(peers.flatMap { (uuid, peer) -> URL? in
			return peer.mutex.locked {
				switch peer.state {
				case .failed(_), .querying(_), .new, .ignored, .connected, .connecting(_), .passive:
					return nil

				case .queried:
					if let ls = peer.lastSeen, Date().timeIntervalSince(ls) < LedgerType.ParametersType.peerMaximumAgeForAdvertisement {
						return peer.url
					}
					return nil
				}
			}
		})
	}

	public var medianNetworkTime: Date? {
		var diffs: [Double] = []

		self.mutex.locked {
			// TODO limit the number of peers used for this?
			for (_, peer) in peers {
				switch peer.state {
				case .queried:
					if let d = peer.timeDifference {
						diffs.append(d)
					}

				default:
					continue
				}
			}
		}

		if diffs.isEmpty {
			return nil
		}

		diffs.sort()
		return Date().addingTimeInterval(diffs.median)
	}

	public init(ledger: LedgerType, port: Int, miner: BlockType.IdentityType, uuid: UUID = UUID()) throws {
		self.uuid = uuid
		self.tickTimer = DispatchSource.makeTimerSource(flags: [], queue: self.workerQueue)
		self.ledger = ledger
		self.miner = Miner(node: self, miner: miner)
		self.server = Server(node: self, port: port)
		self.fetcher = Fetcher(node: self)
	}

	public var url: URL {
		var uc = URLComponents()
		uc.user = self.uuid.uuidString
		uc.port = self.server.port
		uc.host = "127.0.0.1"
		uc.scheme = "ws"
		return uc.url!
	}

	/** Whether the node should announce itself to peers on the local network (using Bonjour). Setting this to 'true'
	registers the service in the network. */
	public var announceLocally: Bool = false {
		didSet {
			self.localNodeAnnouncement = announceLocally ? LocalNodeAnnouncement(node: self) : nil
		}
	}

	/** Whether the node should look for nodes that are announcing themselves in the local network. */
	public var discoverLocally: Bool = false {
		didSet {
			self.localNodeBrowser = discoverLocally ? LocalNodeBrowser(node: self) : nil
		}
	}

	/** Append a transaction to the memory pool (maintained by the miner). Returns true if the transaction was successfully
	appended (to the miner memory pool) or was already in the miner pool. */
	public func receive(transaction: BlockType.TransactionType, from peer: Peer<LedgerType>?) throws -> Bool {
		// Do not accept any transactions from peers we should be ignoring
		if let p = peer {
			switch p.state {
			case .connecting, .new, .querying, .ignored(reason: _), .failed(error: _):
				Log.info("[Node] Ignoring block received from peer \(p) because peer's state is \(p.state)")
				return false

			case .queried, .connected, .passive:
				break
			}
		}

		// Is this transaction in-order?
		switch try self.ledger.canAccept(transaction: transaction, pool: self.miner.block) {
		case .now:
			let isNew = try self.miner.append(transaction: transaction)
			if isNew {
				self.rebroadcast(transaction: transaction, from: peer)
			}
			return true

		case .future:
			let isNew = self.miner.setAside(transaction: transaction)
			if isNew {
				self.rebroadcast(transaction: transaction, from: peer)
			}
			return true

		case .never:
			Log.error("[Node] Not appending transaction \(transaction) to memory pool: ledger says it isn't acceptable")
			return false
		}
	}

	private func rebroadcast(transaction: BlockType.TransactionType, from peer: Peer<LedgerType>?) {
		// Did we get this block from someone else and is it new? Then rebroadcast
		Log.info("[Node] Re-broadcasting transaction \(transaction) to peers as it is new")
		let transactionGossip = Gossip<LedgerType>.transaction(transaction.json)
		self.peers.forEach { (url, otherPeer) in
			if peer == nil || otherPeer.url != peer!.url {
                let otherState = otherPeer.mutex.locked { return otherPeer.state }
				switch otherState {
				case .queried, .passive:
					if let otherConnection = otherPeer.connection {
						do {
							try otherConnection.request(gossip: transactionGossip)
						}
						catch {
							// Not a problem if this fails
						}
					}

				default:
					// Do not send
					break
				}
			}
		}
	}

	func forget(peer: Peer<LedgerType>) throws {
		try self.mutex.locked {
			Log.info("[Node] Peer \(peer.url) wants us to forget him!")
			// Remove peer (should also be removed from queryQueue, but that's expensive, and tick() will deal with this
			self.peers[peer.uuid] = nil
			try self.peerDatabase?.forgetPeer(uuid: peer.uuid)
		}
	}

	public func add(peer url: URL) {
		if Peer<LedgerType>.isValidPeerURL(url: url), let uuid = UUID(uuidString: url.user!) {
			self.mutex.locked {
				if self.peers[uuid] == nil {
					let isSelf = self.uuid == uuid
					let peer = Peer<LedgerType>(url: url, state: isSelf ? .ignored(reason: "added, but is ourselves") : .new, connection: nil, delegate: self)
					self.peers[uuid] = peer

					if case .new = peer.state {
						do {
							try self.peerDatabase?.rememberPeer(url: url)
						}
						catch {
							Log.error("[Node] Peer database remember failed: \(error.localizedDescription)")
						}

						self.queryQueue.append(uuid)
					}
				}
			}
		}
		else {
			Log.error("[Node] Invalid peer URL: \(url.absoluteString)")
		}
	}

	func add(peer connection: PeerIncomingConnection<LedgerType>) {
		var reverseURL = URLComponents()
		reverseURL.scheme = "ws"
		reverseURL.host = connection.connection.request.remoteAddress

		let queryParameters = connection.connection.request.urlURL.parameters

		if
			let connectingUUIDString = queryParameters[LedgerType.ParametersType.uuidRequestKey],
			let connectingUUID = UUID(uuidString: connectingUUIDString) {

			// Did the connecting peer specify a port?
			let reversePort: Int
			if let connectingPortString = queryParameters[LedgerType.ParametersType.portRequestKey],
			let connectingPort = Int(connectingPortString),
			connectingPort > 0, connectingPort < 65535 {
				reversePort = connectingPort
			}
			else {
				// Set reverse port to '0' to indicate an outgoing connection is not possible
				reversePort = 0
			}

			reverseURL.user = connectingUUIDString
			reverseURL.port = reversePort
			let url = reverseURL.url!

			// Check whether the connecting peer is ourselves
			let isSelf = connectingUUID == self.uuid
			if isSelf {
				Log.debug("[Server] dropping connection \(reverseURL): is ourselves")
				connection.close()
				return
			}

			let peer = Peer<LedgerType>(url: url, state: isSelf ? .ignored(reason: "is ourselves") : .connected, connection: isSelf ? nil : connection, delegate: self)
			connection.delegate = peer

			// Check whether the connecting peer is looking for someone else
			do {
				// Find the user URL string, either from the HTTP request URL, or from the Origin header
				let user: String?
				if let u = connection.connection.request.urlURL.user {
					user = u
				}
				else if let o = connection.connection.request.headers["Origin"], !o.isEmpty, let uc = URLComponents(string: o[0]), let u = uc.user {
					user = u
				}
				else {
                    Log.debug("[Server] dropping connection \(reverseURL): did not specify node UUID to connect to (request URL: '\(connection.connection.request.urlURL.absoluteString)'); adding as passive peer")
                    peer.state = .passive
                    user = nil
				}
                
                if let user = user {
                    // Check if the URL user is a valid UUID
                    if let userUUID = UUID(uuidString: user) {
                        if userUUID != self.uuid {
                            Log.debug("[Server] dropping connection \(reverseURL): is looking for different UUID \(userUUID)")
                            try connection.request(gossip: .forget)
                            connection.close()
                            return
                        }
                    }
                    else {
                        // Requested node UUID is invalid
                        try connection.request(gossip: .forget)
                        Log.debug("[Server] dropping connection \(reverseURL): invalid UUID '\(user)'")
                        connection.close()
                        return
                    }
                }
			}
			catch {
				Log.error("[Server] could not properly reject peer \(reverseURL) looking for someone else: \(error.localizedDescription)")
			}

			self.mutex.locked {
				if let alreadyConnected = self.peers[connectingUUID]  {
					switch alreadyConnected.state {
					case .connected, .queried, .querying:
						// Reject connection because we are already connected!
						return
                        
                    case .passive:
                        // Always replace passive connections
                        break

					default:
						if let ls = alreadyConnected.lastSeen, ls.timeIntervalSince(Date()) < LedgerType.ParametersType.peerReplaceInterval {
							Log.info("Not replacing connection \(alreadyConnected.uuid) (state=\(alreadyConnected.state) with \(peer.uuid): the peer was recently seen")
							return
						}
						Log.info("Replacing connection \(alreadyConnected) (state=\(alreadyConnected.state)) with \(peer)")
						// Accept new connection as it is better than what we have
						break
					}
				}

				self.peers[connectingUUID] = peer
				do {
					try self.peerDatabase?.rememberPeer(url: url)
				}
				catch {
					Log.error("[Node] Peer database remember failed: \(error.localizedDescription)")
				}
				self.queryQueue.append(connectingUUID)
			}
		}
		else {
			Log.warning("[Node] not accepting incoming peer \(reverseURL.host!): it has no UUID or the UUID is equal to ours, or is incompatible")
		}
	}

	/** Received a 'best block' offer from another peer (in reply to query) */
	func receive(best: Candidate<BlockType>) {
		self.fetcher.request(candidate: best)
	}

	/** Peer can be nil when the block originated from ourselves (i.e. was mined). */
	func receive(block: BlockType, from peer: Peer<LedgerType>?, wasRequested: Bool) throws {
		// Do not accept any blocks from peers we should be ignoring
		if let p = peer {
			switch p.state {
			case .connecting, .new, .querying, .ignored(reason: _), .failed(error: _):
				Log.info("[Node] Ignoring block received from peer \(p) because peer's state is \(p.state)")
				return

			case .queried, .connected, .passive:
				break
			}
		}

		// Only process the block if at least the signature and payload are valid
		if block.isSignatureValid && block.isPayloadValid() {
			Log.info("[Node] receive block #\(block.index) from \(peer?.url.absoluteString ?? "self")")

			// Blocks that were received over the network may not be too far in the future
			if let nt = self.medianNetworkTime {
				if abs(Date().timeIntervalSince(nt)) > 24.0 * 3600.0 {
					// We are more than 24 hours behind or ahead of the network, notify user
					Log.warning("Our time (\(Date().iso8601FormattedUTCDate)) is more than 24 hours away from median network time (\(nt.iso8601FormattedUTCDate). Please check local clock.")
				}

				if nt.timeIntervalSince(block.date) < -LedgerType.ParametersType.futureBlockThreshold {
					Log.info("[Node] block #\(block.index) from \(peer?.url.absoluteString ?? "self") has a timestamp (\(block.date.iso8601FormattedUTCDate)) that is too far in the future; ignoring")
					return
				}
			}

			let isNew = try self.ledger.mutex.locked { () -> Bool in
				let isNew = try self.ledger.isNew(block: block) && self.ledger.longest.highest.index < block.index
				let wasAppended = try self.ledger.receive(block: block)
				if let p = peer, wasRequested && !wasAppended && block.index > 0 {
					let (fetchIndex, fetchHash) = self.ledger.orphans.earliestRootFor(orphan: block)

					if fetchIndex > 0 {
						if try self.ledger.longest.get(block: fetchHash) == nil {
							Log.info("[Node] received an orphan block #\(block.index), let's see if peer has its predecessor \(fetchHash) at #\(fetchIndex)")
							self.fetcher.request(candidate: Candidate<BlockType>(hash: fetchHash, height: fetchIndex, peer: p.uuid))
						}
						else {
							Log.info("[Node] received an orphan block #\(block.index), predecessor \(fetchHash) is at #\(fetchIndex) on-chain")
						}
					}
				}
				return isNew
			}

			// Did we get this block from someone else and is it new? Then rebroadcast
			if let p = peer, isNew && !wasRequested {
				Log.info("[Node] Re-broadcasting block \(block) to peers as it is new")
				let blockGossip = Gossip<LedgerType>.block(block.json)

				self.peers.forEach { (url, otherPeer) in
					if otherPeer.url != p.url {
						if case .connected = otherPeer.state, let otherConnection = otherPeer.connection {
							do {
								try otherConnection.request(gossip: blockGossip)
							}
							catch {
								// Not a problem if this fails
							}
						}
					}
				}
			}
		}
		else {
			Log.info("[Node] received invalid block #\(block.index) from \(peer?.url.absoluteString ?? "self")")
		}
	}

	func mined(block: BlockType) throws {
		try self.receive(block: block, from: nil, wasRequested: false)

		// Send our peers the good news!
		self.mutex.locked {
			for (_, peer) in self.peers {
				switch peer.state {
				case .queried, .connected, .passive:
					if let peerConnection = peer.connection {
						self.workerQueue.async {
							do {
								Log.debug("[Node] posting mined block \(block.index) to peer \(peer.url)")
								try peerConnection.request(gossip: .block(block.json))
							}
							catch {
								Log.error("[Node] Error sending mined block post: \(error.localizedDescription)")
							}
						}
					}

				case .failed(error: _), .ignored, .querying, .connecting, .new:
					break
				}
			}
		}
	}

	private func tick() {
		self.mutex.locked {
			// Take the first N from the query queue...
			var concurrent = 5

			while let p = self.queryQueue.first {
				self.queryQueue.remove(at: 0)
				if let peer = self.peers[p] {
                    if peer.advance() {
                        concurrent -= 1
                    }
                    
					if concurrent == 0 {
						return
					}
				}
			}

			if self.peers.isEmpty {
				Log.warning("[Node] Have no peers!")
			}

			// Re-query all peers that are not already being queried
			for (url, _) in self.peers {
				self.queryQueue.append(url)
			}
		}
	}

	public func start(blocking: Bool) {
		self.tickTimer.setEventHandler { [unowned self] in
			self.tick()
		}
		self.tickTimer.schedule(deadline: .now(), repeating: 2.0)
		self.tickTimer.resume()

		if blocking {
			Kitura.run()
		}
		else {
			Kitura.start()
		}
	}
}

fileprivate class LocalNodeBrowser<LedgerType: Ledger>: NSObject, NetServiceBrowserDelegate {
	weak var node: Node<LedgerType>?
	let browser: NetServiceBrowser

	init(node: Node<LedgerType>) {
		self.node = node
		self.browser = NetServiceBrowser()
		super.init()
		self.browser.delegate = self
		self.browser.searchForServices(ofType: LedgerType.ParametersType.serviceType, inDomain: LedgerType.ParametersType.serviceDomain)
	}

	deinit {
		Log.debug("[LocalDiscovery] browser deinited")
	}

	public func netServiceBrowserWillSearch(_ browser: NetServiceBrowser) {
		Log.debug("[LocalDiscovery] will search: \(browser)")
	}

	public func netServiceBrowser(_ browser: NetServiceBrowser, didNotSearch error: Error) {
		Log.error("[LocalDiscovery] did not search search: \(browser) \(error.localizedDescription)")
	}

	public func netServiceBrowser(_ browser: NetServiceBrowser, didFind service: NetService, moreComing: Bool) {
		Log.info("[LocalDiscovery] did find local peer: \(service) \(moreComing)")

		if let n = self.node {
			var uc = URLComponents()
			uc.host = service.hostName
			uc.port = service.port
			uc.user = service.name
			uc.scheme = "ws"

			if let url = uc.url {
				Log.debug("[LocalDiscovery] suggesting \(url)")
				n.mutex.locked {
					n.add(peer: url)
				}
			}
		}
	}

	public func netServiceBrowser(_ browser: NetServiceBrowser, didRemove service: NetService, moreComing: Bool) {
		Log.debug("[LocalDiscovery] did remove: \(service) \(moreComing)")
	}

	public func netServiceBrowserDidStopSearch(_ browser: NetServiceBrowser) {
		Log.debug("[LocalDiscovery] did stop search \(browser)")
	}
}

fileprivate class LocalNodeAnnouncement<LedgerType: Ledger>: NSObject, NetServiceDelegate {
	let service: NetService

	init(node: Node<LedgerType>) {
		self.service = NetService(domain: LedgerType.ParametersType.serviceDomain, type: LedgerType.ParametersType.serviceType, name: node.uuid.uuidString, port: Int32(node.server.port))
		super.init()
		self.service.delegate = self
		self.service.publish(options: [.noAutoRename])
	}

	deinit {
		Log.debug("[LocalDiscovery] removing advertisement]")
		self.service.stop()
	}

	func netServiceWillPublish(_ sender: NetService) {
		Log.debug("[LocalDiscovery] will publish: \(sender)")
	}

	func netServiceDidPublish(_ sender: NetService) {
		Log.debug("[LocalDiscovery] did publish: \(sender)")
	}

	func netService(_ sender: NetService, didNotPublish error: Error) {
		Log.error("[LocalDiscovery] did not publish: \(sender)")
	}

	func netServiceDidStop(_ sender: NetService) {
		Log.debug("[LocalDiscovery] did stop: \(sender)")
	}

	func netService(_ sender: NetService, didAcceptConnectionWith socket: Socket) {
		// This should never happen
		fatalError("didAcceptConnection called but we didn't ask NetService to accept connections for us")
	}
}

fileprivate class Fetcher<LedgerType: Ledger> {
	typealias BlockchainType = LedgerType.BlockchainType
	typealias BlockType = BlockchainType.BlockType

	private let workerQueue = DispatchQueue.global(qos: .background)
	private var candidateQueue = OrderedSet<Candidate<BlockType>>()
	private weak var node: Node<LedgerType>?
	private var running = false
	private let mutex = Mutex()

	init(node: Node<LedgerType>) {
		self.node = node
	}

	func request(candidate: Candidate<BlockType>) {
		self.mutex.locked {
			self.candidateQueue.append(candidate)

			if !self.running {
				self.tick()
			}
		}
	}

	private func tick() {
		self.mutex.locked {
			assert(!self.running, "already running!")
			self.running = true

			// Do we need to fetch any blocks?
			if let candidate = self.candidateQueue.first {
				self.candidateQueue.remove(at: 0)
				Log.info("[Node] fetch candidate \(candidate)")
				self.fetch(candidate: candidate) {
					self.mutex.locked {
						self.running = false

						if !self.candidateQueue.isEmpty {
							self.tick()
						}
					}
				}
			}
		}
	}

	private func fetch(candidate: Candidate<BlockType>, callback: @escaping () -> ()) {
		self.workerQueue.async {
			if let n = self.node, let p = n.peers[candidate.peer] {
				if let c = p.mutex.locked(block: { return p.connection }) {
					do {
						try c.request(gossip: Gossip<LedgerType>.fetch(candidate.hash)) { reply in
							if case .block(let blockData) = reply {
								do {
									let block = try BlockType.read(json: blockData)

									if block.isSignatureValid {
										Log.debug("[Fetcher] fetch returned valid block: \(block)")

										if block.index != candidate.height || block.signature! != candidate.hash {
											Log.error("[Fetcher] peer returned a different block (#\(block.index)) than we requested (#\(candidate.height))!")
											n.mutex.locked {
												n.peers[candidate.peer]?.fail(error: "invalid block")
											}
											return callback()
										}

										try n.mutex.locked {
											let peer = n.peers[candidate.peer]
											try n.receive(block: block, from: peer, wasRequested: true)
										}
									}
									else {
										Log.warning("[Fetcher] fetch returned invalid block (signature invalid); setting peer \(candidate.peer) invalid")
										n.mutex.locked {
											n.peers[candidate.peer]?.fail(error: "invalid block")
										}
									}
								}
								catch {
									Log.error("[Fetcher] Error parsing result from fetch: \(error.localizedDescription)")
									n.mutex.locked {
										n.peers[candidate.peer]?.fail(error: "invalid reply to fetch")
									}
								}
							}
							else {
								Log.error("[Fetcher] Received invalid reply to fetch: \(reply)")
								n.mutex.locked {
									n.peers[candidate.peer]?.fail(error: "invalid reply to fetch")
								}
							}

							return callback()
						}
					}
					catch {
						Log.error("[Fetcher] Fetch error: \(error)")
						n.mutex.locked {
							n.peers[candidate.peer]?.fail(error: "fetch error: \(error)")
						}

						return callback()
					}
				}
				else {
					Log.info("[Fetcher] fetch candidate peer is not connected: \(candidate)")
					return callback()
				}
			}
			else {
				Log.info("[Fetcher] fetch candidate peer has disappeared: \(candidate)")
				return callback()
			}
		}
	}
}
