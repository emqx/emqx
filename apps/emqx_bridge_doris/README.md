# Doris Batch SQL Templates

`emqx_doris_sql` compiles a restricted Doris `INSERT INTO ... VALUES (...)`
template with one row. Batch rendering repeats that row. Single-message actions
continue to use prepared statements through the shared MySQL protocol driver.

Dynamic UTF-8 text uses single-quoted literals with Doris backslash escapes,
including `\0` for NUL. Only invalid UTF-8 uses `UNHEX` to preserve the bytes.
The shared connector clears `NO_BACKSLASH_ESCAPES` and `ANSI_QUOTES` when
preparing statements and reconnecting.

Ordinary and raw (`R'...'`, `r"..."`) strings support interpolation. Raw strings
preserve their body bytes without decoding backslashes. The compiler normalizes
raw literals to avoid Doris 2.1.9's inconsistent handling of the `R` prefix.
`${$}` produces `$`, as in MySQL templates.

## Limitations

- Comments, dynamic identifiers, and multiple template rows are not supported.
- `INSERT SELECT`, row aliases, and `ON DUPLICATE KEY UPDATE` are not supported.
- `DEFAULT` is valid only as a direct row item.
- Digit-starting identifiers require backticks.
- MySQL hex literals and character-set introducers are not supported.

## Maintenance

The lexer and parser are hand-maintained against Doris 2.1.9, commit
`3390475e02a359380b98cc99c965b65f77827054`:

- [Upstream lexer](https://github.com/apache/doris/blob/3390475e02a359380b98cc99c965b65f77827054/fe/fe-core/src/main/antlr4/org/apache/doris/nereids/DorisLexer.g4)
- [Upstream parser](https://github.com/apache/doris/blob/3390475e02a359380b98cc99c965b65f77827054/fe/fe-core/src/main/antlr4/org/apache/doris/nereids/DorisParser.g4)
- [LogicalPlanBuilder](https://github.com/apache/doris/blob/3390475e02a359380b98cc99c965b65f77827054/fe/fe-core/src/main/java/org/apache/doris/nereids/parser/LogicalPlanBuilder.java#L2391-L2405)
- [LogicalPlanBuilderAssistant](https://github.com/apache/doris/blob/3390475e02a359380b98cc99c965b65f77827054/fe/fe-core/src/main/java/org/apache/doris/nereids/parser/LogicalPlanBuilderAssistant.java)

See `src/emqx_doris_sql_lexer.xrl` and `src/emqx_doris_sql_parser.yrl` for
rule-specific references and deliberate deviations. Recheck compatibility when
updating the supported Doris version.

Run the Doris CT suites in the app's Docker CT environment. They cover the
compiler, lexical boundaries, live value round trips, and batched actions.
