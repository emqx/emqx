# QuasarDB SQL Templates

`emqx_bridge_quasardb_sql` compiles a restricted QuasarDB 3.14.1
`INSERT INTO ... VALUES (...)` template with one row. Batch rendering repeats
that row. The compiler checks the SQL structure before it resolves any EMQX
placeholder.

A placeholder used as a complete value preserves integers, floats, `NULL`, and
documented timestamp forms. Other values become single-quoted strings. A
placeholder inside a single-quoted string is combined with the static text and
then escaped as one value. Use the quoted form to force timestamp-shaped text to
remain text. A quoted value that contains only one missing or `NULL` placeholder
becomes SQL `NULL` without quotes.

QuasarDB 3.14.1 escapes an apostrophe as `\'` and a backslash as `\\`. The
public grammar does not document these lexical details. The rules in
`src/emqx_bridge_quasardb_sql_lexer.xrl` and their byte boundaries were checked
directly through the pinned 3.14.1 ODBC driver. The test suite preserves those
results.

NUL is rejected in templates and rendered values. The QuasarDB query API takes
a null-terminated query, so no SQL literal can preserve an embedded NUL. The
native time-series API is required for BLOB values containing NUL.

## Supported Subset

- `INSERT INTO` with a mandatory column list and one template row.
- `$timestamp` as the first column and at least one ordinary column.
- Static ASCII bare identifiers and static double-quoted identifiers.
- Integers, decimal and exponent numbers, and `NULL`.
- Single-quoted BLOB and STRING literals with backslash escaping.
- Absolute timestamp forms with up to nanosecond precision.
- `now`, `today`, `yesterday`, `tomorrow`, `epoch`, and `end_of_time`, with
  optional parentheses.
- An optional trailing semicolon.

## Limitations

- Comments, multiple statements, dynamic identifiers, and multiple template
  rows are not supported.
- Qualified identifiers, generic functions, operators, grouping, booleans,
  durations, and timestamp arithmetic are not supported in INSERT values.
- QuasarDB stores an empty quoted STRING or BLOB as `NULL`.
- QuasarDB uses the minimum `INT64` value and `NaN` as null sentinels. Templates
  that require those exact values are not supported by the backend.

## Maintenance

The parser targets the QuasarDB version pinned by the integration test service.
Check these sources when that version changes:

- [INSERT grammar and value examples](https://doc.quasar.ai/3.14.1/queries/insert.html)
- [Table and column identifiers](https://doc.quasar.ai/3.14.1/queries/create_table.html)
- [Additional identifier constraints](https://doc.quasar.ai/3.14.1/queries/alter_table.html)
- [Absolute and special timestamps](https://doc.quasar.ai/3.14.1/queries/timestamps.html)
- [SELECT-only expressions and operators](https://doc.quasar.ai/3.14.1/queries/functions.html)
- [Null-terminated query API](https://doc.quasar.ai/3.14.1/cdoc/group__query.html#gabea1cc60780c4ff27ef8acef98dc3dbc)
- [ODBC integration](https://doc.quasar.ai/3.14.1/user-guide/integration/odbc.html)

QuasarDB does not publish its lexer or parser source. Re-run the lexical and
live round-trip tests when updating the supported QuasarDB version.
