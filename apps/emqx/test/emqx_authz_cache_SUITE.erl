%%--------------------------------------------------------------------
%% Copyright (c) 2019-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_cache_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    emqx_config:put([authorization, cache, excludes], [<<"nocache/#">>]),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(proplists:get_value(apps, Config)).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_cache_exclude(_) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    {ok, Client} = emqtt:start_link([{clientid, ClientId}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, <<"nocache/+/#">>, 0),
    emqtt:publish(Client, <<"nocache/1">>, <<"{\"x\":1}">>, 0),
    Caches = list_cache(ClientId),
    ?assertEqual([], Caches),
    emqtt:stop(Client).

t_clean_authz_cache(_) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    {ok, Client} = emqtt:start_link([{clientid, ClientId}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, <<"t2">>, 0),
    emqtt:publish(Client, <<"t1">>, <<"{\"x\":1}">>, 0),
    ClientPid = find_client_pid(ClientId),
    %% Same race as in t_drain_authz_cache: wait until the fire-and-forget
    %% publish has been authorized, otherwise its cache entry can be written
    %% after the channel handled `clean_authz_cache'.
    ?retry(100, 20, ?assert(is_publish_cached(ClientPid))),
    Caches = list_cache(ClientPid),
    ct:log("authz caches: ~p", [Caches]),
    ?assert(length(Caches) > 0),
    erlang:send(ClientPid, clean_authz_cache),
    ?assertEqual([], list_cache(ClientPid)),
    emqtt:stop(Client).

t_drain_authz_cache(_) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    {ok, Client} = emqtt:start_link([{clientid, ClientId}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, <<"t2">>, 0),
    emqtt:publish(Client, <<"t1">>, <<"{\"x\":1}">>, 0),
    ClientPid = find_client_pid(ClientId),
    %% The publish is fire-and-forget, so the channel can cache its authz
    %% result after drain_cache/0 recorded the eviction cutoff, and an entry
    %% cached after the cutoff survives the drain. Wait for it. The
    %% subscribe above is SUBACK-confirmed, so `length(Caches) > 0' alone
    %% does not prove the publish was authorized yet.
    ?retry(100, 20, ?assert(is_publish_cached(ClientPid))),
    Caches = list_cache(ClientPid),
    ct:log("authz caches: ~p", [Caches]),
    ?assert(length(Caches) > 0),
    emqx_authz_cache:drain_cache(),
    ?assertEqual([], list_cache(ClientPid)),
    ct:sleep(100),
    {ok, _, _} = emqtt:subscribe(Client, <<"t2">>, 0),
    ?assert(length(list_cache(ClientPid)) > 0),
    emqtt:stop(Client).

is_publish_cached(ClientPid) ->
    lists:any(
        fun
            ({{#{action_type := publish}, <<"t1">>}, _}) -> true;
            (_) -> false
        end,
        list_cache(ClientPid)
    ).

list_cache(ClientId) when is_binary(ClientId) ->
    ClientPid = find_client_pid(ClientId),
    list_cache(ClientPid);
list_cache(ClientPid) ->
    gen_server:call(ClientPid, list_authz_cache).

find_client_pid(ClientId) ->
    ?retry(_Inteval = 100, _Attempts = 10, do_find_client_pid(ClientId)).

do_find_client_pid(ClientId) ->
    case emqx_cm:lookup_channels(ClientId) of
        Pids when is_list(Pids) ->
            lists:last(Pids);
        _ ->
            throw({not_found, ClientId})
    end.
