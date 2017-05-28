# Catena - An SQL-based blockchain

Catena is a blockchain that is based on SQL. 

## Building

### macOS

Catena builds on macOS. You need a recent version of XCode (>=8.3.2) on your system. Use the following commands to clone
the Catena repository and build in debug configuration:

````
git clone https://github.com/pixelspark/catena.git catena
cd catena
swift build
````

It is also possible to generate an XCode project and build Catena from it:

````
swift package generate-xcodeproj
````

### Linux

Building on Linux should be possible (all dependencies are supported on Linux). First ensure Swift 3.1 is installed. Then
ensure clang and required libraries are present:

````
apt install clang build-essential libsqlite3-dev libcurl4-openssl-dev openssl libssl-dev
git clone https://github.com/pixelspark/catena.git catena
cd catena
swift build
````

The above was tested on Debian 8 (Jessie) using the Ubuntu 14.04 release of Swift 3.1.

## Running

The following command starts Catena and joins the chain at the demo server:

````
./.build/debug/Catena -j pixelspark.nl:8338
````

To start two peers locally, use the following:

````
./.build/debug/Catena -p 8338 
./.build/debug/Catena -p 8340 -j 127.0.0.1:8338
````

Catena provides an HTTP interface on port 8338 (default), which is used for communicating between peers. The (private) 
SQL interface is available on port 8334 (by default). If you set a different HTTP port (using the '-p'  command line
switch), the SQL interface will assume that port+1. You can connect to the SQL interface using the PostgreSQL command
line client:

````
psql -h localhost -p 8334
````

To enable block mining, add the '-m' command line switch. 

## FAQ

### Is Catena a drop-in replacement for a regular SQL database?

No. The goal of Catena is to make it as easy as possible for developers and administrators that are used to working with 
SQL to adopt blockchain technology. Catena supports the PostgreSQL (pq) wire protocol to submit queries, which allows
Catena to be used from many different languages (such as PHP, Go, C/C++). However, there are fundamental differences 
between Catena and 'regular' database systems:

* Catena currently does not support many SQL features.
* Catena's consistency model is very different from other databases. In particular, any changes you make are not immediately visible nor confirmed. Transactions may roll back at any time depending on which transactions are included in the 'winning' blockchain.
* Catena will (in the future) check user privileges when changing or adding data, but can never prevent users from seeing all data (all users that are connected to a Catena blockchain can 'see' all transactions). Of course it is possible to set up a private chain.

### Which SQL features are supported by Catena?

Catena supports a limited subset of SQL (Catena implements its own SQL parser to sanitize and canonicalize SQL queries).
Currently, the following types of statements are supported:

* INSERT INTO table (x, y, z) VALUES ('text', 1337);
* SELECT x, y FROM table;

Column and table names are case-insensitive, must start with an alphabetic (a-Z) character, and may subsequently contain numbers and underscores. SQL keywords (such as 'SELECT') are case-insensitive. All statements must end with a semicolon. 

In the future, the Catena parser will be expanded to support more types of statements.

### What kind of blockchain is implemented by Catena?

Catena uses a Blockchain based on SHA-256 hashes for proof of work, with configurable difficulty. Blocks contain 
transactions which contain SQL statements. Catena is written from scratch and is therefore completely different from
Bitcoin, Ethereum etc.

### How does a Catena node talk to other nodes?

Catena nodes expose an HTTP interface. A node periodically connects to the HTTP interface of all other nodes it knows 
about (initially specified from the command line) to fetch block information and exchange peers. 

### What is the consistency model for Catena?

A Catena blockchain contains SQL statements that, when executed in order, lead to the agreed-upon state of the database. 
Only SQL statements that modify data or structure are included in the blockchain. This is very similar to replication logs
used by e.g. MySQL ('binlog').

SQL statements are grouped in transactions, which become part of a block. Once a block as been accepted in the blockchain and
is succeeded by a sufficient number of newer blocks, the block has become an immutable part of the blockchain ledger.

As new blocks still run the risk of being 'replaced' by competing blocks that have been mined (which may or may not include
a recent transaction), the most recent transactions run the risk of being rolled back. 

### How are changes to a Catena blockchain authenticated?

Currently, no authentication has been built in. In the future, Catena will provide a grants system (like in regular databases)
using public keypairs. A transaction that modifies a certain table or row needs to be signed with a keypair that has the
required grants. Grants are stored in a special 'grants' table (which, in turn, can be modified by those that have a 
grant to modify that table).

To prevent replaying signed transactions, Catena will record a transaction number for each public key, which is atomically 
incremented for every transaction that is executed. A transaction will not execute (again) if it has a lower transaction
number than the latest number recorded in the blockchain.

### Where does the name come from?

Catena is Italian for 'chain'.

### Can I run a private Catena chain?
Chains are identified by their genesis (first) block's hash. To create a private chain, use the '-s'  option to specify 
a different starting seed. 

## MIT license

````
Copyright (c) 2017 Pixelspark, Tommy van der Vorst

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
````

## Contributing

We welcome contributions of all kinds - from typo fixes to complete refactors and new features. Just be sure to contact us if you want to work on something big, to prevent double effort. You can help in the following ways:

* Open an issue with suggestions for improvements
* Submit a pull request (bug fix, new feature, improved documentation)

Note that before we can accept any new code to the repository, we need you to confirm in writing that your contribution is made available to us under the terms of the MIT license.
