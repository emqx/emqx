%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ee_conf_schema_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_namespace(_Config) ->
    ?assertEqual(
        emqx_conf_schema:namespace(),
        emqx_ee_conf_schema:namespace()
    ).

t_roots(_Config) ->
    BaseRoots = emqx_conf_schema:roots(),
    EnterpriseRoots = emqx_ee_conf_schema:roots(),

    ?assertEqual([], BaseRoots -- EnterpriseRoots),

    ?assert(
        lists:any(
            fun
                ({license, _}) -> true;
                (_) -> false
            end,
            EnterpriseRoots
        )
    ).

t_fields(_Config) ->
    ?assertEqual(
        emqx_conf_schema:fields("node"),
        emqx_ee_conf_schema:fields("node")
    ).

t_translations(_Config) ->
    [Root | _] = emqx_ee_conf_schema:translations(),
    ?assertEqual(
        emqx_conf_schema:translation(Root),
        emqx_ee_conf_schema:translation(Root)
    ).
