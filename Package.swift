// swift-tools-version:3.1

import PackageDescription

let package = Package(
    name: "Catena",
    dependencies: [
		.Package(url: "https://github.com/pixelspark/sqlite.git", majorVersion: 3),
		.Package(url: "https://github.com/IBM-Swift/Kitura.git", majorVersion: 1, minor: 7),
		.Package(url: "https://github.com/jatoben/CommandLine",  "3.0.0-pre1"),
		.Package(url: "https://github.com/IBM-Swift/Kitura-Request.git", majorVersion: 0),
		.Package(url: "https://github.com/IBM-Swift/BlueCryptor.git", majorVersion: 0),
		.Package(url: "https://github.com/pixelspark/swift-parser-generator.git", majorVersion: 1),
		.Package(url: "https://github.com/IBM-Swift/HeliumLogger.git", majorVersion: 1),
		.Package(url: "https://github.com/IBM-Swift/Kitura-WebSocket", majorVersion: 0, minor: 8),
		.Package(url: "https://github.com/daltoniam/Starscream.git", majorVersion: 2),
		.Package(url: "https://github.com/vzsg/ed25519.git", majorVersion: 0, minor: 1),
		.Package(url: "https://github.com/pixelspark/base58.git", majorVersion: 1),
		//.Package(url: "https://github.com/IBM-Swift/BlueSocket", majorVersion: 0, minor: 12)
	]
)
