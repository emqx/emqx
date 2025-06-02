%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_test_lib).

-include("emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(nowarn_export_all).
-compile(export_all).

reset_authorizers() ->
    reset_authorizers(deny, false, []).

restore_authorizers() ->
    reset_authorizers(allow, true, []).

reset_authorizers(Nomatch, CacheEnabled, Source) ->
    {ok, _} = emqx:update_config(
        [authorization],
        #{
            <<"no_match">> => atom_to_binary(Nomatch),
            <<"cache">> => #{<<"enable">> => CacheEnabled},
            <<"sources">> => Source
        }
    ),
    ok.
%% Don't reset sources
reset_authorizers(Nomatch, CacheEnabled) ->
    {ok, _} = emqx:update_config([<<"authorization">>, <<"no_match">>], Nomatch),
    {ok, _} = emqx:update_config([<<"authorization">>, <<"cache">>, <<"enable">>], CacheEnabled),
    ok.

setup_config(BaseConfig, SpecialParams) ->
    Config = maps:merge(BaseConfig, SpecialParams),
    case emqx_authz:update(?CMD_REPLACE, [Config]) of
        {ok, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.

register_fake_sources(SourceTypes) ->
    lists:foreach(
        fun(Type) ->
            emqx_authz_source_registry:register(Type, emqx_authz_fake_source)
        end,
        SourceTypes
    ).

deregister_sources() ->
    {BuiltInTypes, _} = lists:unzip(?BUILTIN_SOURCES),
    SourceTypes = emqx_authz_source_registry:get(),
    lists:foreach(
        fun(Type) ->
            emqx_authz_source_registry:unregister(Type)
        end,
        SourceTypes -- BuiltInTypes
    ).

enable_node_cache(Enable) ->
    {ok, _} = emqx:update_config(
        [authorization, node_cache],
        #{<<"enable">> => Enable}
    ),
    ok.

reset_node_cache() ->
    emqx_auth_cache:reset(?AUTHZ_CACHE).

%%--------------------------------------------------------------------
%% Table-based test helpers
%%--------------------------------------------------------------------

all_with_table_case(Mod, TableCase, Cases) ->
    (emqx_common_test_helpers:all(Mod) -- [TableCase]) ++
        [{group, Name} || Name <- case_names(Cases)].

table_groups(TableCase, Cases) ->
    [{Name, [], [TableCase]} || Name <- case_names(Cases)].

case_names(Cases) ->
    lists:map(fun(Case) -> maps:get(name, Case) end, Cases).

get_case(Name, Cases) ->
    [Case] = [C || C <- Cases, maps:get(name, C) =:= Name],
    Case.

setup_default_permission(Case) ->
    DefaultPermission = maps:get(default_permission, Case, deny),
    emqx_authz_test_lib:reset_authorizers(DefaultPermission, false).

base_client_info() ->
    #{
        clientid => <<"clientid">>,
        username => <<"username">>,
        peerhost => {127, 0, 0, 1},
        zone => default,
        listener => 'tcp:default'
    }.

client_info(Overrides) ->
    maps:merge(base_client_info(), Overrides).

run_checks(#{checks := Checks} = Case) ->
    _ = setup_default_permission(Case),
    ClientInfoOverrides = maps:get(client_info, Case, #{}),
    ClientInfo = client_info(ClientInfoOverrides),
    lists:foreach(
        fun(Check) ->
            run_check(ClientInfo, Check)
        end,
        Checks
    ).

run_check(ClientInfo, Fun) when is_function(Fun, 0) ->
    run_check(ClientInfo, Fun());
run_check(ClientInfo, {ExpectedPermission, Action, Topic}) ->
    ?assertEqual(
        ExpectedPermission,
        emqx_access_control:authorize(
            ClientInfo,
            Action,
            Topic
        )
    ).
