%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_exhook_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_access_control.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(OTHER_CLUSTER_NAME_STRING, "test_emqx_cluster").

-define(CONF_DEFAULT, <<
    "exhook {\n"
    "  servers = [\n"
    "    { name = default,\n"
    "      url = \"http://127.0.0.1:9000\"\n"
    "    },\n"
    "    { name = enable,\n"
    "      enable = false,\n"
    "      url = \"http://127.0.0.1:9000\"\n"
    "    },\n"
    "    { name = error,\n"
    "      url = \"http://127.0.0.1:9001\"\n"
    "    },\n"
    "    { name = not_reconnect,\n"
    "      auto_reconnect = false,\n"
    "      url = \"http://127.0.0.1:9001\"\n"
    "    }\n"
    "  ]\n"
    "}\n"
>>).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Cfg) ->
    _ = emqx_exhook_demo_svr:start(),
    Cfg.

end_per_suite(_Cfg) ->
    emqx_exhook_demo_svr:stop().

init_per_testcase(TC, Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_conf, emqx_conf(TC)},
            {emqx_exhook, ?CONF_DEFAULT}
        ],
        #{work_dir => emqx_cth_suite:work_dir(TC, Config)}
    ),
    emqx_common_test_helpers:init_per_testcase(?MODULE, TC, [{tc_apps, Apps} | Config]).

end_per_testcase(TC, Config) ->
    _ = emqx_common_test_helpers:end_per_testcase(?MODULE, TC, Config),
    ok = emqx_cth_suite:stop(?config(tc_apps, Config)).

emqx_conf(t_cluster_name) ->
    io_lib:format("cluster.name = ~p", [?OTHER_CLUSTER_NAME_STRING]);
emqx_conf(_) ->
    #{}.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_access_failed_if_no_server_running('init', Config) ->
    ok = emqx_hooks:add('client.authorize', {emqx_authz, authorize, [[]]}, ?HP_AUTHZ),
    Config;
t_access_failed_if_no_server_running('end', _Config) ->
    emqx_hooks:del('client.authorize', {emqx_authz, authorize}).

t_access_failed_if_no_server_running(Config) ->
    ClientInfo = #{
        clientid => <<"user-id-1">>,
        username => <<"usera">>,
        peerhost => {127, 0, 0, 1},
        peerport => 3456,
        sockport => 1883,
        protocol => mqtt,
        mountpoint => undefined
    },
    ?assertMatch(
        allow,
        emqx_access_control:authorize(
            ClientInfo#{username => <<"gooduser">>},
            ?AUTHZ_PUBLISH,
            <<"acl/1">>
        )
    ),

    ?assertMatch(
        deny,
        emqx_access_control:authorize(
            ClientInfo#{username => <<"baduser">>},
            ?AUTHZ_PUBLISH,
            <<"acl/2">>
        )
    ),

    emqx_exhook_mgr:disable(<<"default">>),
    ?assertMatch(
        {stop, {error, not_authorized}},
        emqx_exhook_handler:on_client_authenticate(ClientInfo, #{auth_result => success})
    ),

    ?assertMatch(
        {stop, #{result := deny, from := exhook}},
        emqx_exhook_handler:on_client_authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t/1">>, #{
            result => allow, from => exhook
        })
    ),

    Message = emqx_message:make(<<"t/1">>, <<"abc">>),
    ?assertMatch(
        {stop, Message},
        emqx_exhook_handler:on_message_publish(Message)
    ),
    emqx_exhook_mgr:enable(<<"default">>),
    assert_get_basic_usage_info(Config).

t_lookup(_) ->
    Result = emqx_exhook_mgr:lookup(<<"default">>),
    ?assertMatch(#{name := <<"default">>, status := _}, Result),
    not_found = emqx_exhook_mgr:lookup(<<"not_found">>).

t_list(_) ->
    [H | _] = emqx_exhook_mgr:list(),
    ?assertMatch(
        #{
            name := _,
            status := _,
            hooks := _
        },
        H
    ).

t_unexpected(_) ->
    ok = gen_server:cast(emqx_exhook_mgr, unexpected),
    unexpected = erlang:send(erlang:whereis(emqx_exhook_mgr), unexpected),
    Result = gen_server:call(emqx_exhook_mgr, unexpected),
    ?assertEqual(Result, ok).

t_timer(_) ->
    Pid = erlang:whereis(emqx_exhook_mgr),
    refresh_tick = erlang:send(Pid, refresh_tick),
    _ = erlang:send(Pid, {timeout, undefined, {reload, <<"default">>}}),
    _ = erlang:send(Pid, {timeout, undefined, {reload, <<"not_found">>}}),
    _ = erlang:send(Pid, {timeout, undefined, {reload, <<"error">>}}),
    ok.

t_error_update_conf(_) ->
    Path = [exhook, servers],
    Name = <<"error_update">>,
    ErrorCfg = #{<<"name">> => Name},
    {error, not_found} = emqx_exhook_mgr:update_config(Path, {update, Name, ErrorCfg}),
    {error, not_found} = emqx_exhook_mgr:update_config(Path, {move, Name, top}),
    {error, not_found} = emqx_exhook_mgr:update_config(Path, {enable, Name, true}),

    ErrorAnd = #{<<"name">> => Name, <<"url">> => <<"http://127.0.0.1:9001">>},
    {ok, _} = emqx_exhook_mgr:update_config(Path, {add, ErrorAnd}),

    DisableAnd = #{
        <<"name">> => Name,
        <<"url">> => <<"http://127.0.0.1:9001">>,
        <<"enable">> => false
    },
    {ok, _} = emqx_exhook_mgr:update_config(Path, {update, Name, DisableAnd}),

    {ok, _} = emqx_exhook_mgr:update_config(Path, {delete, Name}),
    {error, not_found} = emqx_exhook_mgr:update_config(Path, {delete, Name}),
    ok.

t_update_conf(_Config) ->
    Path = [exhook],
    Conf = #{<<"servers">> := Servers} = emqx_config:get_raw(Path),
    ?assert(length(Servers) > 1),
    Servers1 = shuffle(Servers),
    ReOrderedConf = Conf#{<<"servers">> => Servers1},
    validate_servers(Path, ReOrderedConf, Servers1),
    [_ | Servers2] = Servers,
    DeletedConf = Conf#{<<"servers">> => Servers2},
    validate_servers(Path, DeletedConf, Servers2),
    [L1, L2 | Servers3] = Servers,
    UpdateL2 = L2#{<<"pool_size">> => 1, <<"request_timeout">> => <<"1s">>},
    UpdatedServers = [L1, UpdateL2 | Servers3],
    UpdatedConf = Conf#{<<"servers">> => UpdatedServers},
    validate_servers(Path, UpdatedConf, UpdatedServers),
    %% reset
    validate_servers(Path, Conf, Servers),
    ok.

validate_servers(Path, ReOrderConf, Servers1) ->
    {ok, _} = emqx_exhook_mgr:update_config(Path, ReOrderConf),
    ?assertEqual(ReOrderConf, emqx_config:get_raw(Path)),
    List = emqx_exhook_mgr:list(),
    ExpectL = lists:map(fun(#{<<"name">> := Name}) -> Name end, Servers1),
    L1 = lists:map(fun(#{name := Name}) -> Name end, List),
    ?assertEqual(ExpectL, L1).

t_error_server_info(_) ->
    not_found = emqx_exhook_mgr:server_info(<<"not_exists">>),
    ok.

t_metrics(_) ->
    ok = emqx_exhook_metrics:succeed(<<"default">>, 'client.connect'),
    ok = emqx_exhook_metrics:failed(<<"default">>, 'client.connect'),
    true = emqx_exhook_metrics:update(1000),
    timer:sleep(100),
    SvrMetrics = emqx_exhook_metrics:server_metrics(<<"default">>),
    ?assertMatch(#{succeed := _, failed := _, rate := _, max_rate := _}, SvrMetrics),

    SvrsMetrics = emqx_exhook_metrics:servers_metrics(),
    ?assertMatch(#{<<"default">> := #{succeed := _}}, SvrsMetrics),

    HooksMetrics = emqx_exhook_metrics:hooks_metrics(<<"default">>),
    ?assertMatch(#{'client.connect' := #{succeed := _}}, HooksMetrics),
    ok.

t_handler(_) ->
    %% connect
    {ok, C} = emqtt:start_link([
        {host, "localhost"},
        {port, 1883},
        {username, <<"gooduser">>},
        {clientid, <<"exhook_gooduser">>}
    ]),
    {ok, _} = emqtt:connect(C),

    %% pub/sub
    {ok, _, _} = emqtt:subscribe(C, <<"/exhook">>, qos0),
    timer:sleep(100),
    ok = emqtt:publish(C, <<"/exhook">>, <<>>, qos0),
    ok = emqtt:publish(C, <<"/ignore">>, <<>>, qos0),
    timer:sleep(100),
    {ok, _, _} = emqtt:unsubscribe(C, <<"/exhook">>),

    %% sys pub/sub
    ok = emqtt:publish(C, <<"$SYS">>, <<>>, qos0),
    {ok, _, _} = emqtt:subscribe(C, <<"$SYS/systest">>, qos1),
    timer:sleep(100),
    {ok, _} = emqtt:publish(C, <<"$SYS/systest">>, <<>>, qos1),
    ok = emqtt:publish(C, <<"$SYS/ignore">>, <<>>, qos0),
    timer:sleep(100),
    {ok, _, _} = emqtt:unsubscribe(C, <<"$SYS/systest">>),

    %% ack
    {ok, _, _} = emqtt:subscribe(C, <<"/exhook1">>, qos1),
    timer:sleep(100),
    {ok, _} = emqtt:publish(C, <<"/exhook1">>, <<>>, qos1),
    timer:sleep(100),
    emqtt:stop(C),
    timer:sleep(100),
    ok.

t_simulated_handler(_) ->
    ClientInfo = #{
        clientid => <<"user-id-1">>,
        username => <<"usera">>,
        peerhost => {127, 0, 0, 1},
        peerport => 3456,
        sockport => 1883,
        protocol => mqtt,
        mountpoint => undefined
    },
    %% resume/takeover
    ok = emqx_exhook_handler:on_session_resumed(ClientInfo, undefined),
    ok = emqx_exhook_handler:on_session_discarded(ClientInfo, undefined),
    ok = emqx_exhook_handler:on_session_takenover(ClientInfo, undefined),
    ok.

t_misc_test(_) ->
    "5.0.0" = emqx_exhook_proto_v1:introduced_in(),
    <<"test">> = emqx_exhook_server:name(#{name => <<"test">>}),
    _ = emqx_exhook_server:format(#{name => <<"test">>, hookspec => #{}}),
    ok.

t_cluster_name(_) ->
    ?assertEqual(?OTHER_CLUSTER_NAME_STRING, emqx_sys:cluster_name()),

    emqx_exhook_mgr:disable(<<"default">>),
    emqx_exhook_mgr:enable(<<"default">>),
    %% See emqx_exhook_demo_svr:on_provider_loaded/2
    ?assertEqual([], emqx_hooks:lookup('session.created')),
    ?assertEqual([], emqx_hooks:lookup('message_publish')),
    ?assertEqual(
        true,
        erlang:length(emqx_hooks:lookup('client.connected')) > 1
    ),
    emqx_exhook_mgr:disable(<<"default">>).

t_stop_timeout('init', Config) ->
    ok = snabbkaffe:start_trace(),
    ok = meck:new(emqx_exhook_demo_svr, [passthrough, no_history]),
    Config;
t_stop_timeout('end', _Config) ->
    %% ensure started for other tests
    {ok, _} = application:ensure_all_started(emqx_exhook),
    ok = snabbkaffe:stop(),
    ok = meck:unload(emqx_exhook_demo_svr).

t_stop_timeout(_) ->
    meck:expect(
        emqx_exhook_demo_svr,
        on_provider_unloaded,
        fun(Req, Md) ->
            %% ensure sleep time greater than emqx_exhook_mgr shutdown timeout
            timer:sleep(20000),
            meck:passthrough([Req, Md])
        end
    ),

    %% stop application
    ok = application:stop(emqx_exhook),
    ?block_until(#{?snk_kind := exhook_mgr_terminated}, 20000),

    %% all exhook hooked point should be unloaded
    Hooks = lists:flatmap(
        fun emqx_hooks:lookup/1,
        maps:keys(emqx_hookpoints:registered_hookpoints())
    ),
    ?assertEqual(
        [],
        [H || H = {callback, {emqx_exhook_handler, _, _}, _, _} <- Hooks]
    ).

t_ssl_clear(_) ->
    SvrName = <<"ssl_test">>,
    SSLConf = #{
        <<"enable">> => true,
        <<"cacertfile">> => cert_file("cafile"),
        <<"certfile">> => cert_file("certfile"),
        <<"keyfile">> => cert_file("keyfile"),
        <<"verify">> => <<"verify_peer">>
    },
    AddConf = #{
        <<"auto_reconnect">> => <<"60s">>,
        <<"enable">> => false,
        <<"failed_action">> => <<"deny">>,
        <<"name">> => <<"ssl_test">>,
        <<"pool_size">> => 16,
        <<"request_timeout">> => <<"5s">>,
        <<"ssl">> => SSLConf,
        <<"url">> => <<"http://127.0.0.1:9000">>
    },
    emqx_exhook_mgr:update_config([exhook, servers], {add, AddConf}),
    ListResult1 = list_pem_dir(SvrName),
    ?assertMatch({ok, [_, _, _]}, ListResult1),
    {ok, ResultList1} = ListResult1,

    UpdateConf = AddConf#{<<"ssl">> => SSLConf#{<<"keyfile">> => cert_file("keyfile2")}},
    emqx_exhook_mgr:update_config([exhook, servers], {update, SvrName, UpdateConf}),
    {ok, _} = emqx_tls_certfile_gc:force(),
    ListResult2 = list_pem_dir(SvrName),
    ?assertMatch({ok, [_, _, _]}, ListResult2),
    {ok, ResultList2} = ListResult2,

    FindKeyFile = fun(List) ->
        case lists:search(fun(E) -> lists:prefix("key", E) end, List) of
            {value, Value} ->
                Value;
            _ ->
                ?assert(false, "Can't find keyfile")
        end
    end,

    ?assertNotEqual(FindKeyFile(ResultList1), FindKeyFile(ResultList2)),

    emqx_exhook_mgr:update_config([exhook, servers], {delete, SvrName}),
    {ok, _} = emqx_tls_certfile_gc:force(),
    ?assertMatch({error, enoent}, list_pem_dir(SvrName)),
    ok.

%%--------------------------------------------------------------------
%% Cases Helpers
%%--------------------------------------------------------------------

assert_get_basic_usage_info(_Config) ->
    #{
        num_servers := NumServers,
        servers := Servers
    } = emqx_exhook:get_basic_usage_info(),
    ?assertEqual(1, NumServers),
    ?assertMatch([_], Servers),
    [#{driver := Driver, hooks := Hooks}] = Servers,
    ?assertEqual(grpc, Driver),
    ?assertEqual(
        [
            'client.authenticate',
            'client.authorize',
            'client.connack',
            'client.connect',
            'client.connected',
            'client.disconnected',
            'client.subscribe',
            'client.unsubscribe',
            'message.acked',
            'message.delivered',
            'message.dropped',
            'message.publish',
            'session.created',
            'session.discarded',
            'session.resumed',
            'session.subscribed',
            'session.takenover',
            'session.terminated',
            'session.unsubscribed'
        ],
        lists:sort(Hooks)
    ).

%%--------------------------------------------------------------------
%% Utils
%%--------------------------------------------------------------------

list_pem_dir(Name) ->
    Dir = filename:join([emqx:mutable_certs_dir(), "exhook", Name]),
    file:list_dir(Dir).

data_file(Name) ->
    Dir = code:lib_dir(emqx_exhook),
    {ok, Bin} = file:read_file(filename:join([Dir, "test", "data", Name])),
    Bin.

cert_file(Name) ->
    data_file(filename:join(["certs", Name])).

shuffle(List) ->
    Sorted = lists:sort(lists:map(fun(L) -> {rand:uniform(), L} end, List)),
    lists:map(fun({_, L}) -> L end, Sorted).
