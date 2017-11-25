# SQL as understood by Catena

Catena supports a subset of SQL with the following general remarks:

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
* Type semantics follow those of SQLite (for now)

In the future, the Catena parser will be expanded to support more types of statements. Only deterministic queries will
be supported (e.g. no functions that return current date/time or random values).

## Statements

### Statement types

The following statement types are supported in Catena.

#### SELECT

#### INSERT

#### UPDATE

#### DELETE

#### CREATE TABLE

#### DROP TABLE

#### DO...END

This syntax can be used to execute multiple statements sequentially: `DO x; y; z END;` (where x, y and
z are each statements). The result of the statement is the result of the *last* statement executed. Sequential
execution stops when any of the statements fails (and the transaction is rolled back completely).

#### DESCRIBE

Returns information on the defintion of a table's contents. The `DESCRIBE` statement must be called
following an identifier of an existing table (calling `DESCRIBE` for a table that does not exist will cause an
error). The rows in the returned table are in the order of the columns as they appear on the described table.

The returned table has the following columns:

| column | type | description |
|---------|-------|---------------|
| column | TEXT | The name of the column |
| type | TEXT | The type of the column: TEXT, INT, BLOB |
| in_primary_key | INT | 1 when the column is part of the table's primary key, 0 when it is not |
| not_null | INT | 1 when the column cannot be NULL, 0 otherwise |
| default_value | `type` | The default value for this column, or NULL when it has no default value |

#### SHOW

#### SHOW TABLES

Returns a list of all tables that are accessible (disregarding permissions) as a single table with column `name` containing the
name of each table.

#### SHOW ALL

Currently unimplemented; returns connection settings. The columns are named 'name', 'setting' and 'description'.

#### IF ... THEN ... ELSE ... END

A top-level IF statement can be used to control execution flow. The standard IF statement looks as follows:

````
IF ?amount > 0 THEN UPDATE balance SET balance = balance + ?amount WHERE iban = ?iban ELSE FAIL END;
````

You can also add additional `ELSE IF` clauses:

````
IF ?x < 10 THEN INSERT INTO foo(x) VALUES(?x) ELSE IF ?x < 20 THEN INSERT INTO bar(x) VALUES (?x) ELSE FAIL END;
````

The branches of an IF statement can only contain mutating statements (e.g. no SELECT).

When an `ELSE` clause is omitted, `ELSE FAIL` is implied:

````
IF ?x < 10 THEN INSERT INTO foo(x) VALUES(?x) END;
````

The top-level IF-statement is very useful for restricting template grants to certain subsets of parameters.

#### FAIL

Ends execution of the statement and rolls back any change made in the transaction.

### Limits

#### Nesting of subexpressions

There can be no more than *10* nested sub-expressions and/or sub-statements (both count to the same total). The folllowing add one nesting level:
* Sub-statements of an `IF` expression
* Sub-expressions between brackets
* The select statement inside an `EXISTS` expression.

## Expressions

### Variables

Catena exposes several variables in queries:

| Variable | Type | Description |
|----------|-------|---------------|
| $invoker | BLOB (32 bytes) | The SHA-256 hash of the public key of the invoker of the query |
| $blockHeight | INT | The index of the block of which this query's transaction is part |
| $blockSignature | BLOB (32 bytes) | The signature of the block of which this query's transaction is part |
| $previousBlockSignature | BLOB (32 bytes) | The signature of the block before the block of which this query's transaction is part |
| $blockMiner | BLOB (32 bytes) | The SHA-256 hash of the public key of the miner of the block that contains this transaction |
| $blockTimestamp | INT | The UNIX timestamp of the block that contains this transaction |

### Parameters

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

### Logical and comparison operators

Catena supports the standard comparison operators (=, <>, <=, >=, >, <) as well as the standard logic operators (AND, OR). Logic operators
result in an integer `1` (true) or `0` (false).

Non-zero values are (cf. SQLite semantics) interpreted as being true. Values that cast to a non-zero integer
are considered true as well (e.g. `SELECT 1 AND '1foo';` returns `1`, whereas `SELECT 1 AND '0foo';` returns `0`,).

### Functions

* LENGTH(str): returns the length of string `str`
* ABS(num): returns the absolute value of number `num`

### Subexpressions

* EXISTS(select): returns '1' when the `select` statement returns at least one row, '0' if it returns no rows. The select query may contain references to the outside query ('correlated' subquery).
