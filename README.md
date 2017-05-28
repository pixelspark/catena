# Catena - An SQL-based blockchain

## What is Catena?

Catena is a blockchain that is based on SQL. Applications talk to Catena using the PostgreSQL wire protocol.

## The name

Catena is Italian for 'chain'.'

## Building
Catena builds on macOS. You need a recent version of XCode (>=8.3.2) on your system. Use the following commands to clone
the Catena repository and build in debug configuration:

````
git clone https://github.com/pixelspark/catena.git catena
cd catena
swift build
````

Building on Linux should be possible (all dependencies are supported on Linux) but is untested.

## Running

The following starts Catena and joins the chain at the demo server:

````
./.build/debug/Catena -j pixelspark.nl:8338
````

Catena provides an HTTP interface on port 8338 (default), which is used for communicating between peers. The (private) 
SQL interface is available on port 8334 (by default). If you set a different HTTP port (using the '-p'  command line
switch), the SQL interface will assume that port+1. You can connect to the SQL interface using the PostgreSQL command
line client:

````
psql -h localhost -p 8334
````

To enable block mining, add the '-m' command line switch. 

### Suported queries

TBD.

### Private chain
Chains are identified by their genesis (first) block's hash. To create a private chain, use the '-s'  option to specify 
a different starting seed. 

## License

MIT.
