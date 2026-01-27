%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_template_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx,
            emqx_auth
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_parse_sql(_Config) ->
    {UsedVars, Statement, RowTemplate} = emqx_auth_template:parse_sql(
        """
        SELECT * FROM users WHERE
        username = ${username},
        age = ${age},
        city = '${city.name}',
        country = ${$}{country},
        region = '${$}{region}'
        """,
        '?',
        ["username", "clientid"]
    ),

    %% UsedVars are subset of AllowedVars
    ?assertEqual(UsedVars, ["username"]),

    %% Variables are replaced with placeholders
    %%
    %% NOTE that '?' will not be recognized as a placeholder in the statement if
    %% sent to MySQL.
    %% This is supposed by current design: we expect variables to be placed
    %% where the placeholders can be.
    ?assertEqual(
        ~b"""
        SELECT * FROM users WHERE
        username = ?,
        age = ?,
        city = '?',
        country = ${country},
        region = '${region}'
        """,
        iolist_to_binary(Statement)
    ),
    RenderedRow = emqx_auth_template:render_sql_params(
        RowTemplate,
        #{
            username => <<"u">>,
            age => 42,
            city => <<"c">>,
            country => <<"co">>,
            region => <<"r">>
        }
    ),

    %% In a rendered row (values for placeholders),
    %% unallowed variables are replaced with their original names.
    ?assertEqual(
        [<<"u">>, <<"${age}">>, <<"${city.name}">>],
        RenderedRow
    ).
