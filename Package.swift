// swift-tools-version:4.0

import PackageDescription

var deps: [Package.Dependency] = [
	.package(url: "https://github.com/pixelspark/sqlite.git", from: Version("3.0.0")),
	.package(url: "https://github.com/IBM-Swift/Kitura.git", from: Version("1.7.0")),
	.package(url: "https://github.com/jatoben/CommandLine", from: Version("3.0.0-pre1")),
	.package(url: "https://github.com/IBM-Swift/Kitura-Request.git", from: Version("0.0.0")),
	.package(url: "https://github.com/IBM-Swift/BlueCryptor.git", from: Version("0.0.0")),
	.package(url: "https://github.com/pixelspark/swift-parser-generator.git", from: Version("1.0.0")),
	.package(url: "https://github.com/IBM-Swift/HeliumLogger.git", from: Version("1.0.0")),
	.package(url: "https://github.com/IBM-Swift/Kitura-WebSocket", from: Version("0.8.0")),
	.package(url: "https://github.com/vzsg/ed25519.git", from: Version("0.1.0")),
	.package(url: "https://github.com/pixelspark/base58.git", from: Version("1.0.0")),
	.package(url: "https://github.com/Bouke/NetService.git", from: Version("0.0.0"))
]

var ccDeps: [Target.Dependency] = [
	"SQLite",
	"Ed25519",
	"Base58",
	"HeliumLogger",
	"Kitura",
	"KituraRequest",
	"Kitura-WebSocket",
	"NetService"
]

#if !os(Linux)
	// Starscream is used for outgoing WebSocket connections; unfortunately it is not available on Linux
	deps.append(.package(url: "https://github.com/daltoniam/Starscream.git", from: Version("2.0.0")))
	ccDeps.append("Starscream")
#endif

let package = Package(
    name: "Catena",
    dependencies: deps,

    targets: [
		.target(
			name: "Catena",
			dependencies: [
				"CatenaCore",
				"CatenaSQL",
				"CommandLine"
			]
		),
		.target(
			name: "CatenaSQL",
			dependencies: [
				"CatenaCore",
				"SwiftParser"
			]
		),
		.target(
			name: "CatenaCore",
			dependencies: ccDeps
		)
	]
)
