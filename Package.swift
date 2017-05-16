// swift-tools-version:3.1

import PackageDescription

let package = Package(
    name: "popsiql",
    dependencies: [
		.Package(url: "https://github.com/stephencelis/CSQLite.git", majorVersion: 0),
		.Package(url: "https://github.com/venj/CommonCrypto.git", versions: Version(0,3,0) ..< Version(1,0,0)),
		.Package(url: "https://github.com/IBM-Swift/Kitura.git", majorVersion: 1, minor: 7),
		.Package(url: "https://github.com/jatoben/CommandLine",  "3.0.0-pre1"),
		.Package(url: "https://github.com/IBM-Swift/Kitura-Request.git", majorVersion: 0),
		.Package(url: "https://github.com/dparnell/swift-parser-generator.git", majorVersion: 1),
	]
)
