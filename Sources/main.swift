import Foundation
import Kitura
import CommandLineKit

let databaseFileOption = StringOption(shortFlag: "d", longFlag: "database", required: false, helpMessage: "Backing database file (default: popsiql.sqlite)")
let seedOption = StringOption(shortFlag: "s", longFlag: "seed", required: false, helpMessage: "Genesis block seed string (default: empty)")
let helpOption = BoolOption(shortFlag: "h", longFlag: "help", helpMessage: "Show usage")
let portOption = IntOption(shortFlag: "p", longFlag: "port", helpMessage: "Listen port (default: 8338)")

let cli = CommandLineKit.CommandLine()
cli.addOptions(databaseFileOption, helpOption, seedOption, portOption)

do {
	try cli.parse()
}
catch {
	cli.printUsage(error)
	exit(EX_USAGE)
}

if helpOption.wasSet {
	cli.printUsage()
	exit(EX_OK)
}

let database = Database()
if !database.open(databaseFileOption.value ?? "popsiql.sqlite") {
	fatalError("Could not open database")
}

print(database.perform("DROP TABLE IF EXISTS test"))
print(database.perform("CREATE TABLE test (x INT)"))
print(database.perform("INSERT INTO test (x) VALUES (1), (2), (3)"))

var genesisBlock = SQLBlock(index: 0, previous: Hash.zeroHash, payload: SQLPayload(statement: seedOption.value ?? ""))
genesisBlock.mine(difficulty: 2)
print("Genesis block=\(genesisBlock.debugDescription)) \(genesisBlock.isSignatureValid)")
var ledger = SQLLedger(genesis: genesisBlock, database: database)

let node = Node<SQLBlock>(ledger: ledger, port: portOption.value ?? 8338)
node.start()

var i = 0
while true {
	i += 1
	let q = "INSERT INTO test (x) VALUES (\(i))"
	let block = SQLBlock(index: 0, previous: Hash.zeroHash, payload: SQLPayload(statement: q))
	node.submit(block: block)
	sleep(10)
}
