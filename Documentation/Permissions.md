
# Privileges

Before a transaction is executed, the query parser determines the privileges required for the query. Currently, the followjng
kinds of privileges are recognized:

* "create" (CREATE TABLE)
* "delete" (DELETE FROM)
* "drop" (DROP TABLE)
* "insert" (INSERT INTO)
* "update" (UPDATE)
* "grant" (GRANT and REVOKE)
* "template" (see below)

## Enforcement

For each mutating query that the system executes, a set of required privileges is determined for each
(sub)statement. The system then determines whether the user has all the required privileges. If privileges
are missing, the full transaction is rolled back. Privileges are checked in the following order:

If the statement requires 'database context' (e.g. is not a statement that can be executed outside a 'current' database):
* If the user has a *template grant*, the statement and its substatements are allowed to execute. No further privilege checks are performed for substatements of a template-granted statement.
* If the user has the required `regular` grants (either table-specific or not), the statement is allowed to go through. Sub-statements are checked independently once executed.

For statements that do not require database context or for which the above did not lead to permission to execute, the procedure is as follows:
* If the invoker of the transaction is the database owner, all operations are allowed on the database
* If the statement is a `CREATE DATABASE` statement and the name of the database created is equal to the current database, the statement is allowed.

Note that specific statements may add additional checks (e..g `CREATE DATABASE` will cause a rollback when the indicated database already exists).

### Regular grants

Privileges are checked against a special grants table which lists grants for each database and table.

The grants table (internally) has four columns:
* database": the database the grant applies to
* "user": the SHA-256 hash of the public key of the user that is allowed the privilege
* "kind": the privilege kind (one of the above strings)
* "table": the table to which the privilege applies. NULL if the privilege applies to all tables.

Only the columns `user`, `kind` and `table` are visible (e.g. using SHOW GRANTS) within a database.

Note: associated privileges are *not* automatically removed when referenced tables are dropped, nor are grants automatically created when a table is created.

A transaction can execute if the required privileges exist after processing of the block previous to the one
it is part of. A transaction hence cannot depend on privileges created in the same block. When a transaction
cannot execute due to missing privileges, it is simply ignored.

The SHA-256 hash of the invoker's public key is stored in `user` instead of the real public key. This ensures that the public key only
becomes known when it is used for the first time, mitigating possible future attacks against (weak) public keys. A similar
protection is present in Bitcoin (where transaction outputs are linked to a hash of the receiver's address).

### Template grants

From each mutating query a set of required privileges can be derived. These privileges are then matched
against privileges from the `grants` table. If privileges are missing for a particular statement, the system
derives the *template hash* for the statement and looks for a `template` grant. A template grants allows
the execution of a particular query as long as its template matches the template hash, regardless of the
presence of any other required privileges.

The template hash is the SHA256 hash of the UTF-8 encoded, canonically formatted SQL representation
of a statement whose parameters are all 'unbound'. For example, the following query:

````
INSERT INTO foo (x) VALUES (?what:5);
````

Becomes the following query when parameters are unbound:

````
INSERT INTO foo (x) VALUES (?what);
````

The canonical SQL representation of the above query is:

````
INSERT INTO "foo" ("x") VALUES (?what);
````

The SHA256 hash of the above query is `34d95e10ada95302bb6a16f1ad016b784a4057e670b345c80f855e616c334530`.
If the invoker of the statement has a grant in the `grants` table with kind `template` for the indicated hash,
it will be allowed to execute the first unmodified query. Note that it can also execute the same query with
any other value for the 'what' parameter (e.g. '?x:10').

A 'template' grant with a `NULL` template does not have any effects. A grant with kind `template` and user
set to `NULL` allows execution of the template for any user.

The invoker *must* use the same name for the bound parameter. With a grant for the above template, executing the
following query would not be allowed:

````
INSERT INTO "foo" ("x") VALUES (?somethingelse:5);
````

## Replay protection

A transaction includes a counter. A transaction will only execute if its counter is exactly equal to the counter value of the previous transaction executed by the invoker (in previous blocks or within the current block), or 0 when the invoker has not yet executed a transaction. When multiple transactions from the same invoker are in the same block, they are executed in the order of increasing counter. The relative ordering of the execution of transactions from different invokers is undefined.
