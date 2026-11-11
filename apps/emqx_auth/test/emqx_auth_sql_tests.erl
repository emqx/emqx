%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_sql_tests).

-include_lib("eunit/include/eunit.hrl").

-define(DIALECTS, [mysql, pgsql]).

read_only_test_() ->
    Queries = [
        <<"SELECT password_hash, salt FROM mqtt_user where username = ${username} LIMIT 1">>,
        <<"SELECT action, permission, topic FROM mqtt_acl where username = ${username}">>,
        <<"select * from t;">>,
        <<"select * from t;; ;">>,
        <<"SELECT * FROM t WHERE action = 'delete' OR action = 'update'">>,
        <<"SELECT REPLACE(username, 'a', 'b') FROM t">>,
        <<"SELECT * FROM t WHERE username = ${username} FOR UPDATE">>,
        <<"-- delete from t\nSELECT 1">>,
        <<"/* DROP TABLE t */ SELECT 1">>,
        <<"SELECT 1 /* ; DELETE FROM t */">>
    ],
    [
        {title(D, Q), ?_assertEqual(ok, validate(Q, D))}
     || D <- ?DIALECTS, Q <- Queries
    ].

read_only_mysql_test_() ->
    Queries = [
        <<"SELECT INSERT(username, 1, 1, 'x') FROM t">>,
        <<"SELECT `delete`, `update` FROM t">>,
        <<"SELECT 1 # ; DROP TABLE t">>,
        <<"SELECT 'a\\'; DELETE FROM t'">>,
        <<"SELECT \"; DELETE FROM t\"">>,
        <<"SELECT 1--1">>
    ],
    [{title(mysql, Q), ?_assertEqual(ok, validate(Q, mysql))} || Q <- Queries].

read_only_pgsql_test_() ->
    Queries = [
        <<"SELECT \"delete\", \"update\" FROM t">>,
        <<"SELECT * FROM t WHERE username = $1 FOR NO KEY UPDATE">>,
        <<"SELECT $$; DELETE FROM t$$">>,
        <<"SELECT $tag$; DROP TABLE t$tag$">>,
        <<"SELECT E'a\\'; DELETE FROM t'">>
    ],
    [{title(pgsql, Q), ?_assertEqual(ok, validate(Q, pgsql))} || Q <- Queries].

write_keyword_test_() ->
    Queries = [
        {<<"INSERT INTO t (a) VALUES (${username})">>, <<"INSERT">>},
        {<<"UPDATE t SET a = 1">>, <<"UPDATE">>},
        {<<"delete from t where username = ${username}">>, <<"DELETE">>},
        {<<"dElEtE FROM t">>, <<"DELETE">>},
        {<<"REPLACE INTO t VALUES (1)">>, <<"REPLACE">>},
        {<<"MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DO NOTHING">>, <<"MERGE">>},
        {<<"TRUNCATE t">>, <<"TRUNCATE">>},
        {<<"DROP TABLE t">>, <<"DROP">>},
        {<<"ALTER TABLE t ADD COLUMN a INT">>, <<"ALTER">>},
        {<<"CREATE TABLE t (a INT)">>, <<"CREATE">>},
        {<<"SELECT * INTO t2 FROM t">>, <<"INTO">>},
        {<<"/* comment */ DELETE FROM t">>, <<"DELETE">>},
        {<<"-- comment\nDELETE FROM t">>, <<"DELETE">>},
        {<<"WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d">>, <<"DELETE">>}
    ],
    [
        {title(D, Q), ?_assertEqual({error, write_error(Keyword)}, validate(Q, D))}
     || D <- ?DIALECTS, {Q, Keyword} <- Queries
    ].

write_keyword_mysql_test_() ->
    Queries = [
        {<<"/*!50000 DELETE FROM t */">>, <<"DELETE">>},
        {<<"INSERT t (a) VALUES (1)">>, <<"INSERT">>}
    ],
    [
        {title(mysql, Q), ?_assertEqual({error, write_error(Keyword)}, validate(Q, mysql))}
     || {Q, Keyword} <- Queries
    ].

multiple_statements_test_() ->
    Queries = [
        <<"SELECT 1; SELECT 2">>,
        <<"SELECT 1;\n-- comment\nSELECT 2">>
    ],
    [
        {title(D, Q), ?_assertEqual({error, multiple_statements_error()}, validate(Q, D))}
     || D <- ?DIALECTS, Q <- Queries
    ].

%% The same text has a different meaning in each dialect. MySQL reads `\'` as an
%% escaped quote. PostgreSQL ends the string at `\'`, and the text after it is a
%% second statement.
backslash_escape_test() ->
    Q = <<"SELECT 'a\\'; DELETE FROM t; -- '">>,
    ?assertEqual(ok, validate(Q, mysql)),
    ?assertEqual({error, multiple_statements_error()}, validate(Q, pgsql)).

%% MySQL reads `--` as a comment only when a space follows it.
mysql_double_dash_test() ->
    Q = <<"SELECT 1 --1; DELETE FROM t">>,
    ?assertEqual({error, multiple_statements_error()}, validate(Q, mysql)),
    ?assertEqual(ok, validate(Q, pgsql)).

string_query_test() ->
    ?assertEqual(ok, validate("SELECT 1", pgsql)),
    ?assertEqual({error, write_error(<<"DROP">>)}, validate("DROP TABLE t", pgsql)).

invalid_utf8_test() ->
    ?assertMatch({error, _}, validate(<<"SELECT ", 255>>, mysql)).

title(Dialect, Query) ->
    atom_to_list(Dialect) ++ ": " ++ binary_to_list(Query).

validate(Query, Dialect) ->
    emqx_auth_sql:validate_read_only(Query, Dialect).

write_error(Keyword) ->
    <<"The query must not change data. It contains the keyword ", Keyword/binary>>.

multiple_statements_error() ->
    <<"The query must contain only one SQL statement">>.
