# SQL as understood by Catena

Catena supports a subset of SQL with the following limitations:

* Column and table names are case-insensitive, and must start with an alphabetic (a-Z) character, and may subsequently contain numbers and underscores. Column names may be placed between double quotes.
* SQL keywords (such as 'SELECT') are case-insensitive.
* Whitespace is allowed between different tokens in an SQL statement, but not inside (e.g. "123 45" will not parse).
* All statements must end with a semicolon.
* Values can be a string (between 'single quotes'), an integer, blobs (X'hex' syntax) or NULL.
* An expression can be a value, '*' a column name, or a supported operation
* Supported comparison operators are "=", "<>", "<", ">", ">=", "<="
* Supported mathematical operators are "+", "-", "/" and "*". The concatenation operator "||" is also supported.
* Other supported operators are the prefix "-" for negation, "NOT", and "x IS NULL" / "x IS NOT NULL"
* Currently only the types 'TEXT' , 'INT' and 'BLOB' are supported.
* The special `$invoker` variable can be used to refer to the SHA256-hash of the current public key of the transaction invoker
* Type semantics follow those of SQLite (for now)

In the future, the Catena parser will be expanded to support more types of statements. Only deterministic queries will
be supported (e.g. no functions that return current date/time or random values).

## Variables

Catena exposes several variables in queries:

| Variable | Type | Description |
|----------|-------|---------------|
| $invoker | BLOB (32 bytes) | The SHA-256 hash of the public key of the invoker of the query |
| $blockHeight | INT | The index of the block of which this query's transaction is part |
| $blockSignature | BLOB (32 bytes) | The signature of the block of which this query's transaction is part |
| $previousBlockSignature | BLOB (32 bytes) | The signature of the block before the block of which this query's transaction is part |
| $miner | BLOB (32 bytes) | The SHA-256 hash of the public key of the miner of the block that contains this transaction |
| $timestamp | INT | The UNIX timestamp of the block that contains this transaction |

## Parameters

Catena supports parametrization of queries. This will be used in the future to define stored procedures.

In Catena, a query can contain _bound_ and _unbound_ parameters. An _unbound_ parameter is a placeholder for a literal value.
Queries that contain unbound parameters cannot be executed - they are only used as templates, where parameters are later
substituted with _bound_ parameters or the bound values themselves. A _bound_ parameter is a parameter that has a value
bound to it. A query containing bound parameters can be executed once the bound parameters have been replaced with their
value.

Parameter names follow variable name rules (i.e. should start with an alphanumeric character, may contain numbers and
underscores afterwards). An unbound parameter is written as `?name`. A bound parameter is written as `?name:value` where
`value` is a constant literal (e.g. a string, integer, blob, null or a variable whose value is known before the query executes). Hence
`value` may not be another parameter or a column reference.


## Supported functions

* LENGTH(str): returns the length of string `str`
* ABS(num): returns the absolute value of number `num`

## Supported statement types

#### SELECT

#### INSERT

#### UPDATE

#### DELETE

#### CREATE TABLE

#### DROP TABLE

#### SHOW TABLES

Returns a list of all tables that are accessible (disregarding permissions) as a single table with column 'name' containing the
name of each table.

#### IF THEN ELSE END

A top-level IF statement can be used to control execution flow. The standard IF statement looks as follows:

	IF ?amount > 0 THEN UPDATE balance SET balance = balance + ?amount WHERE iban = ?iban ELSE FAIL END;

You can also add additional `ELSE IF` clauses:

	IF ?x < 10 THEN INSERT INTO foo(x) VALUES(?x) ELSE IF ?x < 20 THEN INSERT INTO bar(x) VALUES (?x) ELSE FAIL END;

The branches of an IF statement can only contain mutating statements (e.g. no SELECT).

When an `ELSE` clause is omitted, `ELSE FAIL` is implied:

	IF ?x < 10 THEN INSERT INTO foo(x) VALUES(?x) END;

The top-level IF-statement is very useful for restricting template grants to certain subsets of parameters.

#### FAIL

Ends execution of the statement and rolls back any change made in the transaction.
