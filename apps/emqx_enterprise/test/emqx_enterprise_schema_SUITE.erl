%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_enterprise_schema_SUITE).

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
        emqx_enterprise_schema:namespace()
    ).

t_roots(_Config) ->
    EnterpriseRoots = emqx_enterprise_schema:roots(),
    ?assertMatch({license, _}, lists:keyfind(license, 1, EnterpriseRoots)).

t_fields(_Config) ->
    CeFields = emqx_conf_schema:fields("node"),
    EeFields = emqx_enterprise_schema:fields("node"),
    ?assertEqual(length(CeFields), length(EeFields)),
    lists:foreach(
        fun({{CeName, CeSchema}, {EeName, EeSchema}}) ->
            ?assertEqual(CeName, EeName),
            case CeName of
                "applications" ->
                    ok;
                _ ->
                    ?assertEqual({CeName, CeSchema}, {EeName, EeSchema})
            end
        end,
        lists:zip(CeFields, EeFields)
    ).

t_translations(_Config) ->
    [Root | _] = emqx_enterprise_schema:translations(),
    ?assertEqual(
        emqx_conf_schema:translation(Root),
        emqx_enterprise_schema:translation(Root)
    ).
