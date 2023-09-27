%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_slow_subs_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_management/include/emqx_mgmt.hrl").
-include_lib("emqx_slow_subs/include/emqx_slow_subs.hrl").

-define(HOST, "http://127.0.0.1:18083/").

-define(API_VERSION, "v5").

-define(BASE_PATH, "api").
-define(NOW, erlang:system_time(millisecond)).
-define(CLUSTER_RPC_SHARD, emqx_cluster_rpc_shard).

-define(CONF_DEFAULT, <<
    ""
    "\n"
    "slow_subs\n"
    "{\n"
    " enable = true\n"
    " top_k_num = 5,\n"
    " expire_interval = 60s\n"
    " stats_type = whole\n"
    "}"
    ""
>>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    application:load(emqx_conf),
    ok = ekka:start(),
    ok = mria_rlog:wait_for_shards([?CLUSTER_RPC_SHARD], infinity),
    meck:new(emqx_alarm, [non_strict, passthrough, no_link]),
    meck:expect(emqx_alarm, activate, 3, ok),
    meck:expect(emqx_alarm, deactivate, 3, ok),

    ok = emqx_common_test_helpers:load_config(emqx_slow_subs_schema, ?CONF_DEFAULT),
    emqx_mgmt_api_test_util:init_suite([emqx_slow_subs]),
    {ok, _} = application:ensure_all_started(emqx_auth),
    Config.

end_per_suite(Config) ->
    ekka:stop(),
    mria:stop(),
    mria_mnesia:delete_schema(),
    meck:unload(emqx_alarm),

    application:stop(emqx_auth),
    emqx_mgmt_api_test_util:end_suite([emqx_slow_subs]),
    Config.

init_per_testcase(_, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(),
    application:ensure_all_started(emqx_slow_subs),
    timer:sleep(500),
    Config.

end_per_testcase(_, Config) ->
    application:stop(emqx_slow_subs),
    case erlang:whereis(node()) of
        undefined ->
            ok;
        P ->
            erlang:unlink(P),
            erlang:exit(P, kill)
    end,
    Config.

t_get_history(_) ->
    Now = ?NOW,
    Each = fun(I) ->
        ClientId = erlang:list_to_binary(io_lib:format("test_~p", [I])),
        ets:insert(?TOPK_TAB, #top_k{
            index = ?TOPK_INDEX(1, ?ID(ClientId, <<"topic">>)),
            last_update_time = Now
        })
    end,

    lists:foreach(Each, lists:seq(1, 5)),

    {ok, Data} = request_api(
        get,
        api_path(["slow_subscriptions"]),
        "page=1&limit=10",
        auth_header_()
    ),
    #{<<"data">> := [First | _]} = emqx_utils_json:decode(Data, [return_maps]),

    ?assertMatch(
        #{
            <<"clientid">> := <<"test_5">>,
            <<"topic">> := <<"topic">>,
            <<"last_update_time">> := Now,
            <<"node">> := _,
            <<"timespan">> := _
        },
        First
    ).

t_clear(_) ->
    ets:insert(?TOPK_TAB, #top_k{
        index = ?TOPK_INDEX(1, ?ID(<<"clientid">>, <<"topic">>)),
        last_update_time = ?NOW
    }),

    {ok, _} = request_api(
        delete,
        api_path(["slow_subscriptions"]),
        [],
        auth_header_()
    ),

    ?assertEqual(0, ets:info(?TOPK_TAB, size)).

t_settting(_) ->
    RawConf = emqx:get_raw_config([slow_subs]),
    RawConf2 = RawConf#{<<"stats_type">> => <<"internal">>},
    {ok, Data} = request_api(
        put,
        api_path(["slow_subscriptions", "settings"]),
        [],
        auth_header_(),
        RawConf2
    ),

    Return = decode_json(Data),
    Expect = emqx_config:fill_defaults(RawConf2),

    ?assertEqual(Expect, Return),

    timer:sleep(800),
    {ok, GetData} = request_api(
        get,
        api_path(["slow_subscriptions", "settings"]),
        [],
        auth_header_()
    ),
    GetReturn = decode_json(GetData),
    ?assertEqual(Expect, GetReturn).

decode_json(Data) ->
    emqx_utils_json:decode(Data, [return_maps]).

request_api(Method, Url, Auth) ->
    request_api(Method, Url, [], Auth, []).

request_api(Method, Url, QueryParams, Auth) ->
    request_api(Method, Url, QueryParams, Auth, []).

request_api(Method, Url, QueryParams, Auth, []) ->
    NewUrl =
        case QueryParams of
            "" -> Url;
            _ -> Url ++ "?" ++ QueryParams
        end,
    do_request_api(Method, {NewUrl, [Auth]});
request_api(Method, Url, QueryParams, Auth, Body) ->
    NewUrl =
        case QueryParams of
            "" -> Url;
            _ -> Url ++ "?" ++ QueryParams
        end,
    do_request_api(Method, {NewUrl, [Auth], "application/json", emqx_utils_json:encode(Body)}).

do_request_api(Method, Request) ->
    ct:pal("Method: ~p, Request: ~p", [Method, Request]),
    case httpc:request(Method, Request, [], [{body_format, binary}]) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _, Return}} when
            Code =:= 200 orelse Code =:= 204
        ->
            {ok, Return};
        {ok, {Reason, _, _}} ->
            {error, Reason}
    end.

auth_header_() ->
    emqx_mgmt_api_test_util:auth_header_().

api_path(Parts) ->
    ?HOST ++ filename:join([?BASE_PATH, ?API_VERSION] ++ Parts).
