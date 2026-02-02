%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gateway_nats_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-compile(export_all).
-compile(nowarn_export_all).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    application:load(emqx_gateway_nats),
    Apps = emqx_cth_suite:start(
        [emqx, emqx_conf, emqx_auth, emqx_gateway, emqx_gateway_nats],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    _ = application:ensure_all_started(emqx_gateway_nats),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)),
    ok.

init_per_testcase(_TestCase, Config) ->
    _ = emqx_gateway_conf:unload_gateway(nats),
    ct:sleep(100),
    Config.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_load_badconf_listener_in_use(_Config) ->
    Port = emqx_common_test_helpers:select_free_port(tcp),
    {ok, LSock} = gen_tcp:listen(Port, [binary, {active, false}]),
    Conf = nats_conf(Port),
    try
        ?assertMatch({error, {badconf, _}}, emqx_gateway_conf:load_gateway(nats, Conf))
    after
        gen_tcp:close(LSock)
    end.

t_load_update_unload(_Config) ->
    Port1 = emqx_common_test_helpers:select_free_port(tcp),
    Port2 = emqx_common_test_helpers:select_free_port(tcp),
    Conf1 = nats_conf(Port1),
    {ok, _} = emqx_gateway_conf:load_gateway(nats, Conf1),
    ok = assert_can_connect(Port1, 10),
    Raw0 = emqx:get_raw_config([gateway]),
    Raw1 = Raw0#{<<"nats">> => nats_raw_conf(Port2)},
    ?assertMatch({ok, _}, emqx:update_config([gateway], Raw1)),
    ok = assert_can_connect(Port2, 10),
    ok = emqx_gateway_conf:unload_gateway(nats),
    ?assertMatch({error, _}, gen_tcp:connect("127.0.0.1", Port2, [binary], 1000)).

t_update_error_listener_in_use(_Config) ->
    Port1 = emqx_common_test_helpers:select_free_port(tcp),
    Port2 = emqx_common_test_helpers:select_free_port(tcp),
    Conf1 = nats_conf(Port1),
    {ok, _} = emqx_gateway_conf:load_gateway(nats, Conf1),
    {ok, LSock} = gen_tcp:listen(Port2, [binary, {active, false}]),
    try
        GwConf0 = emqx:get_config([gateway, nats]),
        GwConf1 = emqx_utils_maps:deep_put([listeners, tcp, default, bind], GwConf0, Port2),
        ?assertMatch({error, _}, emqx_gateway:update(nats, GwConf1))
    after
        gen_tcp:close(LSock),
        _ = emqx_gateway_conf:unload_gateway(nats)
    end.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

nats_conf(Port) ->
    nats_conf_list([listener(<<"default">>, Port)]).

nats_conf_list(Listeners) ->
    #{
        <<"server_id">> => <<"emqx_nats_gateway">>,
        <<"server_name">> => <<"emqx_nats_gateway">>,
        <<"default_heartbeat_interval">> => <<"2s">>,
        <<"heartbeat_wait_timeout">> => <<"1s">>,
        <<"protocol">> => #{<<"max_payload_size">> => 1024},
        <<"listeners">> => Listeners
    }.

listener(Name, Port) ->
    #{
        <<"type">> => <<"tcp">>,
        <<"name">> => Name,
        <<"bind">> => Port
    }.

nats_raw_conf(Port) ->
    #{
        <<"server_id">> => <<"emqx_nats_gateway">>,
        <<"server_name">> => <<"emqx_nats_gateway">>,
        <<"default_heartbeat_interval">> => <<"2s">>,
        <<"heartbeat_wait_timeout">> => <<"1s">>,
        <<"protocol">> => #{<<"max_payload_size">> => 1024},
        <<"listeners">> => #{
            <<"tcp">> => #{
                <<"default">> => #{<<"bind">> => Port}
            }
        }
    }.

assert_can_connect(_Port, 0) ->
    exit({connect_failed, timeout});
assert_can_connect(Port, Attempts) ->
    case gen_tcp:connect("127.0.0.1", Port, [binary], 1000) of
        {ok, Socket} ->
            gen_tcp:close(Socket),
            ok;
        _Error ->
            timer:sleep(200),
            assert_can_connect(Port, Attempts - 1)
    end.
