The `query` setting of the MySQL and PostgreSQL authentication and authorization backends now accepts only a query that reads data.

EMQX rejects a query that changes data, for example a query with `INSERT`, `UPDATE`, `DELETE`, `REPLACE`, `MERGE`, `UPSERT`, `TRUNCATE`, `DROP`, `ALTER`, `CREATE` or `SELECT ... INTO`. EMQX also rejects a query that has more than one SQL statement. The REST API returns an error for such a query, and EMQX does not load a configuration file that contains one.

Before you upgrade, check the `query` setting of each MySQL and PostgreSQL authenticator and authorization source.
