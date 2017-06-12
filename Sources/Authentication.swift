import Foundation
import Base58
import Ed25519

struct Identity {
	let privateKey: PrivateKey
	let publicKey: PublicKey

	init(publicKey: PublicKey, privateKey: PrivateKey) {
		self.publicKey = publicKey
		self.privateKey = privateKey
	}

	init() throws {
		let s = try Seed()
		let keyPair = KeyPair(seed: s)
		self.privateKey = PrivateKey(data: Data(bytes: keyPair.privateKey.bytes))
		self.publicKey = PublicKey(data: Data(bytes: keyPair.publicKey.bytes))
	}
}

class Key: CustomStringConvertible {
	let data: Data
	let version: UInt8

	fileprivate init?(string: String, version: UInt8) {
		self.version = version

		if let d = Data.decodeChecked(base58: string, version: version) {
			self.data = d
		}
		else {
			return nil
		}
	}

	fileprivate init(data: Data, version: UInt8) {
		self.data = data
		self.version = version
	}

	var stringValue: String {
		return self.data.base58checkEncoded(version: self.version)
	}

	var description: String {
		return self.stringValue
	}
}

class PrivateKey: Key {
	private static let base58version: UInt8 = 11

	init?(string: String) {
		super.init(string: string, version: PrivateKey.base58version)
	}

	init(data: Data) {
		super.init(data: data, version: PrivateKey.base58version)
	}
}

class PublicKey: Key {
	private static let base58version: UInt8 = 88

	init?(string: String) {
		super.init(string: string, version: PublicKey.base58version)
	}

	init(data: Data) {
		super.init(data: data, version: PublicKey.base58version)
	}

	func sign(data: Data, with privateKey: PrivateKey) throws -> Data {
		let pair = try KeyPair(publicKey: [UInt8](self.data), privateKey: [UInt8](privateKey.data))
		let sigBytes = pair.sign([UInt8](data))
		return Data(sigBytes)
	}

	func verify(message: Data, signature: Data) throws -> Bool {
		return try Ed25519.PublicKey([UInt8](self.data)).verify(signature: [UInt8](signature), message: [UInt8](message))
	}
}
