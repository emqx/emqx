%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_utils_sql_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-import(emqx_utils_sql, [get_statement_type/1, split_insert/1]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

t_get_statement_type(_) ->
    ?assertEqual(select, get_statement_type("SELECT * FROM abc")),
    ?assertEqual(insert, get_statement_type("INSERT INTO abc (c1, c2, c3)VALUES(1, 2, 3)")),
    ?assertEqual(update, get_statement_type("UPDATE abc SET c1 = 1, c2 = 2, c3 = 3")),
    ?assertEqual(delete, get_statement_type("DELETE FROM abc WHERE c1 = 1")),
    ?assertEqual({error, unknown}, get_statement_type("drop table books")).

t_parse_insert(_) ->
    %% `values` in table name
    run_pi(
        <<"insert into tag_VALUES(tag_values,Timestamp) values (${tagvalues},${date})"/utf8>>,
        {<<"insert into tag_VALUES(tag_values,Timestamp)"/utf8>>, <<"(${tagvalues},${date})"/utf8>>}
    ),
    run_pi(
        <<"INSERT INTO Values_таблица (идентификатор, имя, возраст)   VALUES \t (${id}, 'Иван', 25)  "/utf8>>,
        {<<"INSERT INTO Values_таблица (идентификатор, имя, возраст)"/utf8>>,
            <<"(${id}, 'Иван', 25)"/utf8>>}
    ),

    %% `values` in column name
    run_pi(
        <<"insert into PI.dbo.tags(tag_values,Timestamp) values (${tagvalues},${date}  )"/utf8>>,
        {<<"insert into PI.dbo.tags(tag_values,Timestamp)"/utf8>>,
            <<"(${tagvalues},${date}  )"/utf8>>}
    ),

    run_pi(
        <<"INSERT INTO mqtt_test(payload, arrived) VALUES (${payload}, FROM_UNIXTIME((${timestamp}/1000)))"/utf8>>,
        {<<"INSERT INTO mqtt_test(payload, arrived)"/utf8>>,
            <<"(${payload}, FROM_UNIXTIME((${timestamp}/1000)))">>}
    ),

    run_pi(
        <<"insert into таблица (идентификатор,имя,возраст) VALUES(${id},'Алексей',30)"/utf8>>,
        {<<"insert into таблица (идентификатор,имя,возраст)"/utf8>>,
            <<"(${id},'Алексей',30)"/utf8>>}
    ),
    run_pi(
        <<"INSERT into 表格 (标识, 名字, 年龄) VALUES (${id}, '张三', 22)"/utf8>>,
        {<<"INSERT into 表格 (标识, 名字, 年龄)"/utf8>>, <<"(${id}, '张三', 22)"/utf8>>}
    ),
    run_pi(
        <<"  inSErt into 表格(标识,名字,年龄)values(${id},'李四', 35)"/utf8>>,
        {<<"inSErt into 表格(标识,名字,年龄)"/utf8>>, <<"(${id},'李四', 35)"/utf8>>}
    ),
    run_pi(
        <<"insert into PI.dbo.tags( tag_value,Timestamp)  VALUES\t\t(   ${tagvalues},   ${date} )"/utf8>>,
        {<<"insert into PI.dbo.tags( tag_value,Timestamp)"/utf8>>,
            <<"(   ${tagvalues},   ${date} )"/utf8>>}
    ),
    run_pi(
        <<"insert into PI.dbo.tags(tag_value , Timestamp )vALues(${tagvalues},${date})"/utf8>>,
        {<<"insert into PI.dbo.tags(tag_value , Timestamp )"/utf8>>,
            <<"(${tagvalues},${date})"/utf8>>}
    ),

    run_pi(
        <<"inSErt  INTO  table75 (column1, column2, column3) values (${one}, ${two},${three})"/utf8>>,
        {<<"inSErt  INTO  table75 (column1, column2, column3)"/utf8>>,
            <<"(${one}, ${two},${three})"/utf8>>}
    ),
    run_pi(
        <<"INSERT Into some_table      values\t(${tag1},   ${tag2}  )">>,
        {<<"INSERT Into some_table      "/utf8>>, <<"(${tag1},   ${tag2}  )">>}
    ),
    placeholders_not_allowed("insert into ${tab} (val1, val2) values (${val1}, ${val2})").

t_parse_insert_with_on_condition(_) ->
    Expected1 = {
        <<"insert into test_tab (val1, val2)"/utf8>>,
        <<"(${val1}, ${val2})">>,
        <<"duplicate key update id=id">>
    },
    run_pi(
        <<"insert into test_tab (val1, val2) values (${val1}, ${val2}) on duplicate key update id=id">>,
        Expected1
    ),
    run_pi(
        <<"insert into test_tab (val1, val2) values (${val1}, ${val2}) on duplicate key update id=id ">>,
        Expected1
    ),
    run_pi(
        <<"insert into test_tab (val1, val2) values (${val1}, ${val2}) on  duplicate key update id=id   ">>,
        Expected1
    ),
    run_pi(
        <<"insert into test_tab (val1, val2) values (${val1}, ${val2})on duplicate key update id=id  ">>,
        Expected1
    ),
    run_pi(
        <<"insert into test_tab (val1, val2) values (${val1}, ${val2})  on duplicate key update id=id">>,
        Expected1
    ),
    run_pi(
        <<"insert into test_tab (val1, val2) values (${val1}, ${val2}) ON duplicate key update id=id">>,
        Expected1
    ),
    run_pi(
        <<"insert into test_tab (val1, val2) values (${val1}, ${val2}) on conflict (val1) do nothing">>,
        {<<"insert into test_tab (val1, val2)"/utf8>>, <<"(${val1}, ${val2})">>,
            <<"conflict (val1) do nothing">>}
    ),
    placeholders_not_allowed(
        "insert into test_tab (val1, val2) values (${val1}, ${val2}) on duplicate key update id=${id}+1"
    ).

t_parse_insert_nested_brackets(_) ->
    InsertPart = <<"INSERT INTO test_tab (val1, val2)">>,
    ValueLs = [
        <<"(ABS(POWER((2 * POWER(ABS( (-3 + 1) * 4), (2 * (1 + ABS( (-3 + 1) * 4))))), (3 - POWER(4, 2)))), ",
            "POWER(ABS( (-3 + 1) * 4), (2 * (1 + ABS( (-3 + 1) * 4)))))">>,
        <<"(GREATEST(LEAST(5, 10), ABS(-7)), LEAST(GREATEST(3, 2), 9))">>,
        <<"(ABS(POWER((2 * 2), (3 - 1))), POWER(ABS(-3), (2 * (1 + 1))))">>,
        <<"(SQRT(POWER(4, 2)), MOD((10 + 3), (2 * 5)))">>,
        <<"(ROUND(CEIL(3.14159 * 2), 2), CEIL(ROUND(7.5, 1)))">>,
        <<"(FLOOR(SQRT(ABS(-8.99))), SIGN(POWER(-2, 3)))">>,
        <<"(TRUNCATE(RAND() * 100, 2), ROUND(RAND() * 10, 1))">>,
        <<"(EXP(LOG(POWER(2, 3))), LOG(EXP(5)))">>,
        <<"(COS(PI() / (3 - 1)), PI() / COS(PI() / 4))">>,
        <<"(SIN(TAN(PI() / 4)), TAN(SIN(PI() / 6)))">>
    ],

    [
        run_pi(<<InsertPart/binary, " VALUES ", ValueL/binary>>, {InsertPart, ValueL})
     || ValueL <- ValueLs
    ].

t_parse_insert_failed(_) ->
    incorrect_insert("drop table books"),
    incorrect_insert("SELECT * FROM abc"),
    incorrect_insert("UPDATE abc SET c1 = 1, c2 = 2, c3 = 3"),
    incorrect_insert("DELETE FROM abc WHERE c1 = 1"),
    incorrect_insert("insert intotable(a,b)values(1,2)"),
    incorrect_insert("insert into (a,val)values(1,'val')").

incorrect_insert(SQL) ->
    ?assertEqual({error, <<"Not an INSERT statement or incorrect SQL syntax">>}, split_insert(SQL)),
    ct:pal("SQL:~n~ts~n", [SQL]).

placeholders_not_allowed(SQL) ->
    ?assertEqual({error, <<"Placeholders are only allowed in VALUES part">>}, split_insert(SQL)),
    ct:pal("SQL:~n~ts~n", [SQL]).

run_pi(SQL, Expected) ->
    {ok, Parsed} = split_insert(SQL),
    ?assertEqual(Expected, Parsed),
    ct:pal("SQL: ~ts~n", [SQL]).
